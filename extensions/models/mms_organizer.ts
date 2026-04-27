/**
 * `@keeb/mms/organizer` model type — pop pending jobs from a MongoDB queue,
 * classify filenames with an Ollama-driven two-stage prompt, and move files
 * into the organized media tree.
 */
import { z } from "npm:zod@4";
import { MongoClient } from "npm:mongodb@6.12.0";
import { ollamaGenerate } from "./_lib/ollama.ts";

// Prompts loaded at runtime from prompts/ via context.repoDir

// --- Video file detection ---

const VIDEO_EXTENSIONS = new Set([
  ".mkv",
  ".mp4",
  ".avi",
  ".wmv",
  ".flv",
  ".mov",
  ".webm",
  ".m4v",
  ".mpg",
  ".mpeg",
  ".ts",
  ".vob",
  ".ogv",
  ".3gp",
]);

function isVideoFile(filename: string): boolean {
  const ext = filename.slice(filename.lastIndexOf(".")).toLowerCase();
  return VIDEO_EXTENSIONS.has(ext);
}

async function ollamaHealthCheck(ollamaUrl: string): Promise<void> {
  const resp = await fetch(`${ollamaUrl}/api/tags`);
  if (!resp.ok) {
    throw new Error(`Ollama is not reachable at ${ollamaUrl} (${resp.status})`);
  }
}

// --- Schemas ---

const GlobalArgsSchema = z.object({
  mongodbUri: z
    .string()
    .meta({ sensitive: true })
    .describe("MongoDB connection URI"),
  database: z
    .string()
    .default("media")
    .describe("MongoDB database name containing the jobs collection"),
  ollamaModel: z
    .string()
    .default("qwen3:14b")
    .describe("Ollama model to use for filename parsing"),
  ollamaUrl: z
    .string()
    .default("http://localhost:11434")
    .describe("Ollama API base URL"),
  stagingDir: z
    .string()
    .describe("Staging directory where completed downloads land"),
  mediaRoot: z
    .string()
    .describe("Root of the organized media tree (e.g. /home/keeb/media)"),
});

const JobSchema = z.object({
  jobId: z.string().describe("MongoDB document _id"),
  filename: z.string().describe("Original filename from the job"),
  mediaType: z.string().describe("Detected media type"),
  title: z.string().describe("Parsed title"),
  season: z.string().optional().describe("Season number if detected"),
  episode: z.string().optional().describe("Episode number if detected"),
  year: z.string().optional().describe("Release year if detected"),
  sourcePath: z.string().describe("Original file path in staging"),
  destPath: z.string().describe("Destination path after organization"),
  status: z
    .enum(["processed", "failed", "duplicate"])
    .describe("Processing result"),
  confidence: z.string().describe("LLM confidence level"),
  processedAt: z.iso.datetime().describe("When the job was processed"),
});

// --- Model ---

/** Swamp model definition for `@keeb/mms/organizer`. */
export const model = {
  type: "@keeb/mms/organizer",
  version: "2026.03.28.1",
  globalArguments: GlobalArgsSchema,
  resources: {
    job: {
      description: "Processed media job result",
      schema: JobSchema,
      lifetime: "infinite" as const,
      garbageCollection: 50,
    },
  },
  methods: {
    process: {
      description:
        "Pop pending jobs from MongoDB queue, classify with LLM, and move files to organized locations",
      arguments: z.object({
        maxJobs: z
          .number()
          .default(5)
          .describe("Maximum number of jobs to process in this run"),
      }),
      // deno-lint-ignore no-explicit-any
      execute: async (args: { maxJobs: number }, context: any) => {
        const {
          mongodbUri,
          database,
          ollamaModel,
          ollamaUrl,
          stagingDir,
          mediaRoot,
        } = context.globalArgs;

        // Load prompts from canonical files
        const FILENAME_TO_JSON_PROMPT = await Deno.readTextFile(
          `${context.repoDir}/prompts/filename-to-json.prompt`,
        );
        const JSON_TO_SAVE_PATH_PROMPT = await Deno.readTextFile(
          `${context.repoDir}/prompts/json-to-save-path.prompt`,
        );

        // Health check Ollama before popping any jobs
        await ollamaHealthCheck(ollamaUrl);

        // Connect to MongoDB
        const client = new MongoClient(mongodbUri);
        try {
          await client.connect();
          const db = client.db(database);
          const jobs = db.collection("jobs");

          // deno-lint-ignore no-explicit-any
          const handles: any[] = [];
          let processed = 0;

          while (processed < args.maxJobs) {
            // Atomic pop: find pending and set to in_progress
            const job = await jobs.findOneAndUpdate(
              { status: { $in: ["pending", "queued", null] } },
              {
                $set: {
                  status: "in_progress",
                  updated_at: new Date(),
                },
              },
              { returnDocument: "after" },
            );

            if (!job) {
              context.logger.info("No more pending jobs in queue");
              break;
            }

            const jobId = String(job._id);
            const filename = job.name as string;

            if (!filename) {
              context.logger.error(
                `Job ${jobId} missing 'name' field, marking failed`,
              );
              await markFailed(jobs, job._id, "Missing 'name' field");
              continue;
            }

            context.logger.info(`Processing job ${jobId}: ${filename}`);

            try {
              // Resolve source path
              const sourcePath = `${stagingDir.replace(/\/$/, "")}/${filename}`;

              // Collect files to process
              const filesToProcess: { path: string; name: string }[] = [];
              try {
                const stat = await Deno.stat(sourcePath);
                if (stat.isDirectory) {
                  for await (const entry of walkDir(sourcePath)) {
                    if (isVideoFile(entry.name)) {
                      filesToProcess.push({
                        path: entry.path,
                        name: entry.name,
                      });
                    }
                  }
                } else {
                  filesToProcess.push({ path: sourcePath, name: filename });
                }
              } catch {
                throw new Error(`Source not found: ${sourcePath}`);
              }

              if (filesToProcess.length === 0) {
                context.logger.info(`No video files in ${sourcePath}`);
                await markFailed(jobs, job._id, "No video files found");
                continue;
              }

              // Process each video file
              // deno-lint-ignore no-explicit-any
              let lastMetadata: any = null;
              let lastDest = "";

              for (const file of filesToProcess) {
                // Stage 1: filename → JSON metadata
                const metadataJson = await ollamaGenerate(
                  ollamaUrl,
                  ollamaModel,
                  FILENAME_TO_JSON_PROMPT,
                  file.name,
                );

                // deno-lint-ignore no-explicit-any
                let metadata: any;
                try {
                  metadata = JSON.parse(metadataJson);
                } catch {
                  throw new Error(
                    `LLM returned invalid JSON for "${file.name}": ${
                      metadataJson.slice(0, 200)
                    }`,
                  );
                }
                lastMetadata = metadata;

                // Stage 2: JSON → save path
                // The canonical prompt has /home/keeb/media hardcoded;
                // replace with the configured mediaRoot
                const pathPrompt = JSON_TO_SAVE_PATH_PROMPT.replaceAll(
                  "/home/keeb/media",
                  mediaRoot,
                );
                const destPath = (
                  await ollamaGenerate(
                    ollamaUrl,
                    ollamaModel,
                    pathPrompt,
                    metadataJson,
                  )
                ).trim();
                lastDest = destPath;

                // Move file
                await moveFile(file.path, destPath, file.name, context.logger);
              }

              // If source was a directory, clean it up
              try {
                const stat = await Deno.stat(sourcePath);
                if (stat.isDirectory) {
                  await Deno.remove(sourcePath, { recursive: true });
                  context.logger.info(
                    `Cleaned staging directory: ${sourcePath}`,
                  );
                }
              } catch {
                // Already moved or doesn't exist
              }

              // Mark completed in MongoDB
              await jobs.updateOne(
                { _id: job._id },
                {
                  $set: {
                    status: "done",
                    completed_at: new Date(),
                  },
                },
              );

              // Write swamp resource
              const instanceName = `job-${jobId}`;
              const handle = await context.writeResource("job", instanceName, {
                jobId,
                filename,
                mediaType: lastMetadata?.media_type ?? "unknown",
                title: lastMetadata?.title ?? filename,
                season: lastMetadata?.season != null
                  ? String(lastMetadata.season)
                  : undefined,
                episode: lastMetadata?.episode != null
                  ? String(lastMetadata.episode)
                  : undefined,
                year: lastMetadata?.year != null
                  ? String(lastMetadata.year)
                  : undefined,
                sourcePath: `${stagingDir}/${filename}`,
                destPath: lastDest,
                status: "processed" as const,
                confidence: lastMetadata?.confidence ?? "Low",
                processedAt: new Date().toISOString(),
              });
              handles.push(handle);
              processed++;

              context.logger.info(
                `Job ${jobId} complete: ${filename} → ${lastDest}`,
              );
            } catch (err) {
              context.logger.error(`Job ${jobId} failed: ${err}`);
              await markFailed(jobs, job._id, String(err));

              // Still record the failure as a resource
              const handle = await context.writeResource(
                "job",
                `job-${jobId}`,
                {
                  jobId,
                  filename,
                  mediaType: "unknown",
                  title: filename,
                  sourcePath: `${stagingDir}/${filename}`,
                  destPath: "",
                  status: "failed" as const,
                  confidence: "Low",
                  processedAt: new Date().toISOString(),
                },
              );
              handles.push(handle);
            }
          }

          context.logger.info(
            `Processed ${processed} jobs (${handles.length} total results)`,
          );
          return { dataHandles: handles };
        } finally {
          await client.close();
        }
      },
    },
  },
};

// --- Helpers ---

async function markFailed(
  // deno-lint-ignore no-explicit-any
  collection: any,
  // deno-lint-ignore no-explicit-any
  id: any,
  error: string,
): Promise<void> {
  await collection.updateOne(
    { _id: id },
    {
      $set: {
        status: "failed",
        error,
        failed_at: new Date(),
      },
    },
  );
}

async function moveFile(
  srcPath: string,
  destDir: string,
  filename: string,
  // deno-lint-ignore no-explicit-any
  logger: any,
): Promise<void> {
  // Create destination directory
  await Deno.mkdir(destDir, { recursive: true });

  const destPath = `${destDir.replace(/\/$/, "")}/${filename}`;

  // Check for duplicate
  try {
    const srcStat = await Deno.stat(srcPath);
    const destStat = await Deno.stat(destPath);
    if (destStat.size === srcStat.size) {
      logger.info(`Duplicate (same size), removing staging copy: ${srcPath}`);
      await Deno.remove(srcPath);
      return;
    }
    throw new Error(
      `Destination exists with different size: ${destPath} (src=${srcStat.size}, dest=${destStat.size})`,
    );
    // deno-lint-ignore no-explicit-any
  } catch (err: any) {
    if (err instanceof Deno.errors.NotFound) {
      // Destination doesn't exist — proceed with move
    } else if (err.message?.includes("Destination exists")) {
      throw err;
    }
    // else: some other stat error on dest, assume it doesn't exist
  }

  // Move (rename if same filesystem, copy+delete otherwise)
  try {
    await Deno.rename(srcPath, destPath);
    logger.info(`Moved ${srcPath} → ${destPath}`);
  } catch {
    // Cross-device: copy then delete
    await Deno.copyFile(srcPath, destPath);
    await Deno.remove(srcPath);
    logger.info(`Copied+deleted ${srcPath} → ${destPath}`);
  }
}

async function* walkDir(
  dir: string,
): AsyncGenerator<{ path: string; name: string }> {
  for await (const entry of Deno.readDir(dir)) {
    const path = `${dir}/${entry.name}`;
    if (entry.isDirectory) {
      yield* walkDir(path);
    } else if (entry.isFile) {
      yield { path, name: entry.name };
    }
  }
}
