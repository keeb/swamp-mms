import { z } from "npm:zod@4";
import { MongoClient } from "npm:mongodb@6.12.0";

// --- Embedded LLM prompts (ported from prompts/) ---

const FILENAME_TO_JSON_PROMPT = `# Media Filename Parser

Parse TV show, anime, and movie filenames into structured data.

## Input Format
Filename (with or without extension)

## Output Format
Return JSON with these fields:
- \`media_type\`: "tv_show", "anime", "movie", or "book"
- \`title\`: Clean title (spaces, proper capitalization)
- \`season\`: Season number (integer, null for movies/books)
- \`episode\`: Episode number (integer, null for movies/books)
- \`volume\`: Volume number (integer, null for non-books)
- \`episode_title\`: Episode title if present (null if not found)
- \`year\`: Release year if present (integer, null if not found)
- \`confidence\`: High/Medium/Low based on pattern match quality

## Detection Rules
**Movie**: Single file, often has year, no season/episode markers
**Anime**: Japanese titles, episode ranges like "01-12", "OVA", "Special"
**TV Show**: Standard S##E## or #x## patterns, Western show names
**Book**: Comic book archives (.cbz, .cbr), manga, graphic novels, often numbered volumes

## Common Patterns
- TV: \`Show.Name.S01E01.Episode.Title.ext\`
- Anime: \`Anime Title - 01 [1080p].mkv\` or \`Anime.Title.E01.ext\`
- Movie: \`Movie Title (2023) 1080p.mp4\`
- Book: \`Comic Title Vol 01.cbz\` or \`Manga Name - Chapter 001.cbr\`

## Special Cases (Anime Season Detection)
These rules OVERRIDE the default season detection:
- "Jujutsu Kaisen - Shimetsu Kaiyuu" or "Jujutsu Kaisen: Shimetsu Kaiyuu" → season: 3
- "Jujutsu Kaisen 2nd Season" → season: 2
- "Sousou no Frieren" or "Frieren" with "2nd Season" or "Season 2" → title: "Sousou no Frieren", season: 2
- "Ao no Miburo" with "2nd Season" or "Season 2" → title: "Ao no Miburo", season: 2
- Any anime with "2nd Season" in the title → season: 2
- Any anime with "3rd Season" in the title → season: 3

## Examples
Input: \`The.Sopranos.S01E01.Pilot.avi\`
Output: \`{"media_type": "tv_show", "title": "The Sopranos", "season": 1, "episode": 1, "episode_title": "Pilot", "year": null, "confidence": "High"}\`

Input: \`Attack on Titan - 01 [1080p].mkv\`
Output: \`{"media_type": "anime", "title": "Attack on Titan", "season": null, "episode": 1, "episode_title": null, "year": null, "confidence": "High"}\`

Input: \`The Matrix (1999) 1080p BluRay.mp4\`
Output: \`{"media_type": "movie", "title": "The Matrix", "season": null, "episode": null, "episode_title": null, "year": 1999, "confidence": "High"}\`

Input: \`Call of the Night v01 (2021) (Digital) (1r0n) (f2).cbz\`
Output: \`{"media_type": "book", "title": "Call of the Night", "season": null, "episode": null, "volume": 1, "episode_title": null, "year": 2021, "confidence": "High"}\`

Input: \`[Erai-raws] Jujutsu Kaisen: Shimetsu Kaiyuu - Zenpen - 05 [1080p]\`
Output: \`{"media_type": "anime", "title": "Jujutsu Kaisen", "season": 3, "episode": 5, "episode_title": null, "year": null, "confidence": "High"}\`

Input: \`[Erai-raws] Jujutsu Kaisen 2nd Season - 23 [1080p]\`
Output: \`{"media_type": "anime", "title": "Jujutsu Kaisen", "season": 2, "episode": 23, "episode_title": null, "year": null, "confidence": "High"}\`

Parse the filename and return only the JSON response.`;

const JSON_TO_SAVE_PATH_PROMPT =
  `You are a media file path generator. Your job is to take JSON media metadata and return the correct storage path based on these rules:

## Directory Structure:
- Anime: {mediaRoot}/video/anime/completed/{show-name}
- Movies: {mediaRoot}/video/movies
- TV Shows: {mediaRoot}/video/shows/{show-name}
- Books: {mediaRoot}/manga/{book-title}

## Naming Rules:
Follow these steps EXACTLY in order for EVERY title:
1. Convert EVERY character to lowercase (A-Z becomes a-z)
2. Replace ALL spaces with single hyphens
3. Remove trailing separators (spaces, hyphens, dots, underscores)
4. Remove duplicate hyphens (-- becomes -, --- becomes -, etc)
5. Strip leading/trailing hyphens

CRITICAL:
- Each space character must become exactly one hyphen. Do not combine words.
- EVERY letter must be lowercase. If you see any uppercase letters, convert them.

## Path Generation Logic:

**For Anime (media_type: "anime"):**
- Base path: {mediaRoot}/video/anime/completed/
- Append normalized title as directory name
- If season is provided, append /s{season_number}

**For Movies (media_type: "movie"):**
- Path: {mediaRoot}/video/movies
- Movies go directly in the movies folder, not in subdirectories

**For TV Shows (media_type: "tv" or "tv_show"):**
- Base path: {mediaRoot}/video/shows/
- Append normalized title as directory name
- If season is provided, append /s{season_number}
- NEVER create episode subdirectories
- ALL episodes from the same season go in the SAME season directory

**For Books (media_type: "book"):**
- Base path: {mediaRoot}/manga/
- Append normalized title as directory name
- If volume is provided, append /v{volume_number}
- NEVER create chapter subdirectories

## Output Format:
Return only the complete directory path as a string, no additional text or formatting.`;

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

// --- Ollama HTTP client ---

async function ollamaGenerate(
  ollamaUrl: string,
  model: string,
  prompt: string,
  input: string,
): Promise<string> {
  const fullPrompt = `${prompt}\n\n${input}`;
  const resp = await fetch(`${ollamaUrl}/api/generate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      model,
      prompt: fullPrompt,
      stream: false,
      options: { num_predict: 1024 },
    }),
  });

  if (!resp.ok) {
    throw new Error(
      `Ollama generate failed (${resp.status}): ${await resp.text()}`,
    );
  }

  const json = await resp.json();
  let response = (json.response ?? "").trim();

  // Strip markdown code block wrappers
  if (response.startsWith("```")) {
    response = response.replace(/^```(?:json)?\n?/, "").replace(/\n?```$/, "");
  }

  return response;
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
      execute: async (args: { maxJobs: number }, context) => {
        const {
          mongodbUri,
          database,
          ollamaModel,
          ollamaUrl,
          stagingDir,
          mediaRoot,
        } = context.globalArgs;

        // Health check Ollama before popping any jobs
        await ollamaHealthCheck(ollamaUrl);

        // Connect to MongoDB
        const client = new MongoClient(mongodbUri);
        try {
          await client.connect();
          const db = client.db(database);
          const jobs = db.collection("jobs");

          const handles = [];
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
              let lastMetadata = null;
              let lastDest = "";

              for (const file of filesToProcess) {
                // Stage 1: filename → JSON metadata
                const metadataJson = await ollamaGenerate(
                  ollamaUrl,
                  ollamaModel,
                  FILENAME_TO_JSON_PROMPT,
                  file.name,
                );

                let metadata;
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
                // Inject mediaRoot into the prompt
                const pathPrompt = JSON_TO_SAVE_PATH_PROMPT.replaceAll(
                  "{mediaRoot}",
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
  collection,
  id,
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
  logger,
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
  } catch (err) {
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
