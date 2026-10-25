import { z } from "npm:zod@4";
import { MongoClient, ObjectId } from "npm:mongodb@6.12.0";
import { ollamaGenerate } from "./_lib/ollama.ts";

// Prompts loaded at runtime from the bundled prompts/ via context.extensionFile

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

const PlaylistSchema = z.object({
  dir: z.string().describe("Directory the playlist covers"),
  path: z.string().describe("Path of the written .m3u file"),
  entries: z.number().describe("Number of video files in the playlist"),
  episodes: z
    .array(z.string())
    .describe("Playlist entries, in play order, relative to dir"),
  status: z
    .enum(["written", "unchanged", "empty"])
    .describe("Result: unchanged means content already matched on disk"),
  writtenAt: z.iso.datetime(),
});

type Playlist = z.infer<typeof PlaylistSchema>;

// One resource per run rather than one per .m3u — a playlist is fully derived
// from its directory, so the interesting identity is "which run touched what",
// not the playlist itself. Instance name is stable per target so version
// history reads as that target's regeneration history.
const PlaylistRunSchema = z.object({
  scope: z
    .string()
    .describe("What the run covered: a path, or the recent-jobs window"),
  dirs: z.number().describe("Directories considered"),
  written: z.number().describe("Playlists written or rewritten"),
  unchanged: z.number().describe("Playlists already up to date"),
  failed: z.number().describe("Directories that errored"),
  results: z.array(PlaylistSchema).describe("Per-directory results"),
  failures: z
    .array(z.object({ dir: z.string(), error: z.string() }))
    .describe("Per-directory failures, captured rather than aborting the run"),
  ranAt: z.iso.datetime(),
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
    .enum(["processed", "failed", "duplicate", "held"])
    .describe(
      "Processing result; held = not confident, left pending in staging (see reason)",
    ),
  confidence: z.string().describe("LLM confidence level"),
  reason: z.string().optional().describe("Why the gate held the job"),
  processedAt: z.iso.datetime().describe("When the job was processed"),
});

const PlannedFileSchema = z.object({
  path: z.string(),
  name: z.string(),
  destPath: z.string(),
  route: z.string().describe("special | one-piece | fast | llm"),
  metadata: z.record(z.string(), z.unknown()),
  showDirExists: z.boolean(),
  existingSeasons: z.array(z.string()),
  similarDirs: z.array(z.string()),
  splitRisk: z.boolean(),
  judgeKey: z.string().optional().describe(
    "Jev item name when this file needs a season_fits score",
  ),
});

const PendingSchema = z.object({
  createdAt: z.iso.datetime(),
  jobs: z.number(),
  names: z.array(z.string()).describe("Video filenames, deduplicated"),
});

/** One @keeb/scenerules parse item ({ input, parsed }). */
const SceneItemSchema = z.object({
  input: z.unknown(),
  parsed: z.object({
    style: z.string(),
    kind: z.string(),
    title: z.string().nullable(),
    country: z.string().nullable(),
    year: z.number().nullable(),
    season: z.number().nullable(),
    episodes: z.array(z.number()),
    confidence: z.enum(["high", "medium", "low"]),
  }).passthrough(),
});

const PlanSchema = z.object({
  createdAt: z.iso.datetime(),
  rulesHash: z.string().describe(
    "Hash of the placement rules this plan used; staged jobs re-plan when it changes",
  ),
  jobs: z.array(z.object({
    jobId: z.string(),
    name: z.string(),
    error: z.string().optional(),
    files: z.array(PlannedFileSchema),
  })),
  judge: z.object({
    items: z.array(z.object({
      name: z.string(),
      state: z.record(z.string(), z.unknown()),
    })),
    questions: z.record(z.string(), z.unknown()),
  }).describe("Jev ask_batch input for the LLM-placed files"),
});

// Jev rubric for an LLM-placed file. One fact, details in state. season_fits
// separated real mis-filings (<= 0.81) from correct placements (>= 0.85) in
// the 2026-10-25 eval; apply holds below `threshold`.
const JUDGE_QUESTIONS = {
  season_fits: {
    type: "noul",
    instructions:
      "The season folder at the end of `proposed` (or its absence) matches the season in `filename` and the show's `existing_seasons_of_proposed_show`.",
    criteria: {
      true: "season placement is right",
      false: "wrong or inconsistent season folder",
    },
  },
};

const EvalCaseSchema = z.object({
  filename: z.string(),
  expected: z.string().describe("Directory the file belongs in"),
  tag: z.string().optional().describe("Free-form label, e.g. tv_show, hard"),
});

const EvalSchema = z.object({
  name: z.string(),
  cases: z.number(),
  llmOnly: z.boolean(),
  structured: z.boolean(),
  catalogHints: z.boolean().optional(),
  ranAt: z.iso.datetime(),
  models: z.array(z.object({
    model: z.string(),
    correct: z.number(),
    accuracy: z.number().describe("correct / cases, 0-1"),
    errors: z.number().describe("Cases that threw (invalid JSON, HTTP)"),
    medianMs: z.number(),
    p95Ms: z.number(),
    byTag: z.record(
      z.string(),
      z.object({ cases: z.number(), correct: z.number() }),
    ),
    routes: z.record(z.string(), z.number()).optional().describe(
      "Cases decided by each route (special, fast, scene, llm, ...)",
    ),
    misses: z.array(z.object({
      filename: z.string(),
      tag: z.string().optional(),
      expected: z.string(),
      got: z.string(),
      route: z.string(),
    })),
  })),
});

const pct = (xs: number[], p: number) => {
  if (xs.length === 0) return 0;
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.floor(p * s.length))];
};

// --- Model ---

export const model = {
  type: "@keeb/mms/organizer",
  version: "2026.10.25.1",
  globalArguments: GlobalArgsSchema,
  resources: {
    job: {
      description: "Processed media job result",
      schema: JobSchema,
      lifetime: "infinite" as const,
      garbageCollection: 50,
    },
    playlist_run: {
      description: "Playlist (.m3u) generation run summary",
      schema: PlaylistRunSchema,
      lifetime: "infinite" as const,
      garbageCollection: 50,
    },
    pending: {
      description:
        "Video files the next plan will classify (input for a scene-rules parse)",
      schema: PendingSchema,
      lifetime: "infinite" as const,
      garbageCollection: 10,
    },
    plan: {
      description:
        "Where each pending job would go, with review-gate context and the Jev batch to score it. Written by plan, read by apply.",
      schema: PlanSchema,
      lifetime: "infinite" as const,
      garbageCollection: 20,
    },
    eval: {
      description:
        "Classification benchmark: accuracy and latency per Ollama model over labeled filenames",
      schema: EvalSchema,
      lifetime: "infinite" as const,
      garbageCollection: 20,
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
          context.extensionFile("prompts/filename-to-json.md"),
        );
        const ANIME_FILENAME_TO_JSON_PROMPT = await Deno.readTextFile(
          context.extensionFile("prompts/anime-filename-to-json.md"),
        );
        const JSON_TO_SAVE_PATH_PROMPT = await Deno.readTextFile(
          context.extensionFile("prompts/json-to-save-path.md"),
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
                const classified = await classifyFile(file.name, {
                  mediaRoot,
                  ollamaUrl,
                  ollamaModel,
                  prompts: {
                    filename: FILENAME_TO_JSON_PROMPT,
                    anime: ANIME_FILENAME_TO_JSON_PROMPT,
                    savePath: JSON_TO_SAVE_PATH_PROMPT,
                  },
                  structured: true,
                  catalogDirs: await listCatalogDirs(mediaRoot),
                });
                const { metadata, destPath } = classified;
                context.logger.info(
                  `${classified.route}: ${file.name} → ${destPath}`,
                );
                lastMetadata = metadata;
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
    pending: {
      description:
        "List the video files plan would classify next (same job selection), so a scene-rules parse can run on them first. Writes pending-<name> with names[]. Moves nothing.",
      arguments: z.object({
        maxJobs: z.number().default(100),
        retryHours: z.number().default(6),
        name: z.string().default("latest"),
      }),
      execute: async (
        args: { maxJobs: number; retryHours: number; name: string },
        // deno-lint-ignore no-explicit-any
        context: any,
      ) => {
        const { mongodbUri, database, ollamaModel, stagingDir } =
          context.globalArgs;
        const hash = await rulesHash(await loadPrompts(context), ollamaModel);
        const client = new MongoClient(mongodbUri);
        try {
          await client.connect();
          const jobs = await selectPending(
            client.db(database).collection("jobs"),
            hash,
            args.retryHours,
            args.maxJobs,
          );
          const names: string[] = [];
          for (const job of jobs) {
            try {
              for (
                const f of await resolveJobFiles(
                  stagingDir,
                  String(job.name ?? ""),
                )
              ) {
                names.push(f.name);
              }
            } catch { /* plan records the error */ }
          }
          context.logger.info(
            `${jobs.length} pending job(s), ${names.length} file(s)`,
          );
          const handle = await context.writeResource(
            "pending",
            `pending-${slugify(args.name) || "latest"}`,
            {
              createdAt: new Date().toISOString(),
              jobs: jobs.length,
              names: [...new Set(names)],
            },
          );
          return { dataHandles: [handle] };
        } finally {
          await client.close();
        }
      },
    },
    plan: {
      description:
        "Classify pending jobs without moving anything: destination, route and gate context per file, plus the Jev batch for LLM-placed files. Jobs a previous apply left in staging are re-planned as soon as the organizer's rules change, else every retryHours. Writes plan-<name>; apply executes it.",
      arguments: z.object({
        maxJobs: z.number().default(100).describe(
          "Maximum pending jobs to plan",
        ),
        retryHours: z.number().default(6).describe(
          "Re-plan a job left in staging after this long even if the rules haven't changed",
        ),
        scene: z.array(SceneItemSchema).default([]).describe(
          "@keeb/scenerules parse items for the pending files (see pending); enables the deterministic scene route",
        ),
        name: z.string().default("latest").describe(
          "Suffix for the plan-<name> resource",
        ),
      }),
      execute: async (
        args: {
          maxJobs: number;
          retryHours: number;
          scene: z.infer<typeof SceneItemSchema>[];
          name: string;
        },
        // deno-lint-ignore no-explicit-any
        context: any,
      ) => {
        const {
          mongodbUri,
          database,
          ollamaModel,
          ollamaUrl,
          stagingDir,
          mediaRoot,
        } = context.globalArgs;
        const prompts = await loadPrompts(context);
        // Only when a file actually needs the LLM: an all-deterministic plan
        // doesn't depend on Ollama being up.
        let health: Promise<void> | undefined;
        const beforeLlm = () => (health ??= ollamaHealthCheck(ollamaUrl));
        const catalogDirs = await listCatalogDirs(mediaRoot);
        const hash = await rulesHash(prompts, ollamaModel);
        const scene = new Map(
          args.scene.map((i) => [String(i.input), i.parsed as SceneParse]),
        );
        const client = new MongoClient(mongodbUri);
        try {
          await client.connect();
          const pending = await selectPending(
            client.db(database).collection("jobs"),
            hash,
            args.retryHours,
            args.maxJobs,
          );
          const planned = [];
          const judgeItems = [];
          for (const job of pending) {
            const jobId = String(job._id);
            const name = String(job.name ?? "");
            const entry = {
              jobId,
              name,
              error: undefined as string | undefined,
              files: [] as z.infer<typeof PlannedFileSchema>[],
            };
            try {
              if (!name) throw new Error("Missing 'name' field");
              const files = await resolveJobFiles(stagingDir, name);
              if (files.length === 0) throw new Error("No video files found");
              for (const [i, file] of files.entries()) {
                const c = await classifyFile(file.name, {
                  mediaRoot,
                  ollamaUrl,
                  ollamaModel,
                  prompts,
                  structured: true,
                  catalogDirs,
                  scene,
                  beforeLlm,
                });
                const g = await gateContext(c.destPath, catalogDirs);
                const judgeKey = c.route === "llm"
                  ? `org-${jobId}-${i}`
                  : undefined;
                if (judgeKey) {
                  judgeItems.push({
                    name: judgeKey,
                    state: {
                      filename: file.name,
                      proposed: c.destPath.slice(
                        mediaRoot.replace(/\/$/, "").length + 7,
                      ),
                      existing_seasons_of_proposed_show: g.existingSeasons,
                    },
                  });
                }
                entry.files.push({
                  path: file.path,
                  name: file.name,
                  destPath: c.destPath,
                  route: c.route,
                  metadata: c.metadata ?? {},
                  showDirExists: g.showDirExists,
                  existingSeasons: g.existingSeasons,
                  similarDirs: g.similarDirs,
                  splitRisk: g.splitRisk,
                  judgeKey,
                });
                context.logger.info(
                  `${c.route}: ${file.name} → ${c.destPath}${
                    g.splitRisk ? " (split risk)" : ""
                  }`,
                );
              }
            } catch (err) {
              entry.error = String(err);
              context.logger.error(`Plan ${jobId} (${name}): ${err}`);
            }
            planned.push(entry);
          }
          context.logger.info(
            `Planned ${planned.length} job(s); ${judgeItems.length} LLM-placed file(s) for Jev`,
          );
          const handle = await context.writeResource(
            "plan",
            `plan-${slugify(args.name) || "latest"}`,
            {
              createdAt: new Date().toISOString(),
              rulesHash: hash,
              jobs: planned,
              judge: { items: judgeItems, questions: JUDGE_QUESTIONS },
            },
          );
          return { dataHandles: [handle] };
        } finally {
          await client.close();
        }
      },
    },
    apply: {
      description:
        "Execute plan-<name>: move each job's files into the catalog only when every file is a confident placement. A split risk, a Jev season_fits score below threshold, or no score leaves the job pending in staging (held_at/held_reason/held_rules), to be re-planned when the rules change. Writes job-<id> per job.",
      arguments: z.object({
        name: z.string().default("latest").describe("Plan to apply"),
        judgeModel: z.string().default("jev").describe(
          "Model whose evaluation-<key> resources hold the season_fits scores",
        ),
        threshold: z.number().min(0).max(1).default(0.83).describe(
          "Hold LLM-placed files whose season_fits P(yes) is below this",
        ),
      }),
      execute: async (
        args: { name: string; judgeModel: string; threshold: number },
        // deno-lint-ignore no-explicit-any
        context: any,
      ) => {
        const { mongodbUri, database, stagingDir } = context.globalArgs;
        const planName = `plan-${slugify(args.name) || "latest"}`;
        const plans = await context.queryData(
          `modelName == "${context.definition.name}" && name == "${planName}" && has(attributes.jobs)`,
        );
        if (plans.length === 0) throw new Error(`No ${planName} found`);
        // deno-lint-ignore no-explicit-any
        const plan = plans.sort((a: any, b: any) =>
          String(b.createdAt).localeCompare(String(a.createdAt))
        )[0].attributes as z.infer<typeof PlanSchema>;

        const scores = new Map<string, number>();
        for (const item of plan.judge.items) {
          const recs = await context.queryData(
            `modelName == "${args.judgeModel}" && name == "evaluation-${item.name}" && createdAt >= "${plan.createdAt}" && has(attributes.answers)`,
          );
          if (recs.length === 0) continue;
          // deno-lint-ignore no-explicit-any
          const latest = recs.sort((a: any, b: any) =>
            String(b.createdAt).localeCompare(String(a.createdAt))
          )[0];
          const p = latest.attributes?.answers?.season_fits?.noul;
          if (typeof p === "number") {
            scores.set(item.name, p);
          }
        }

        const client = new MongoClient(mongodbUri);
        const handles = [];
        let moved = 0, held = 0, failed = 0;
        try {
          await client.connect();
          const jobs = client.db(database).collection("jobs");
          for (const pj of plan.jobs) {
            const _id = new ObjectId(pj.jobId);
            // Claim it; a job another run already took is skipped.
            const job = await jobs.findOneAndUpdate(
              { _id, status: { $in: ["pending", "queued", null] } },
              { $set: { status: "in_progress", updated_at: new Date() } },
              { returnDocument: "after" },
            );
            if (!job) continue;
            const base = {
              jobId: pj.jobId,
              filename: pj.name,
              sourcePath: `${stagingDir}/${pj.name}`,
            };
            if (pj.error) {
              await markFailed(jobs, _id, pj.error);
              failed++;
              handles.push(
                await context.writeResource("job", `job-${pj.jobId}`, {
                  ...base,
                  mediaType: "unknown",
                  title: pj.name,
                  destPath: "",
                  status: "failed" as const,
                  confidence: "Low",
                  reason: pj.error,
                  processedAt: new Date().toISOString(),
                }),
              );
              continue;
            }
            const reasons: string[] = [];
            for (const f of pj.files) {
              if (f.splitRisk) {
                reasons.push(
                  `${f.name}: new dir ${showDirOf(f.destPath)} next to ${
                    f.similarDirs.slice(0, 2).join(", ")
                  }`,
                );
              }
              if (f.judgeKey) {
                const p = scores.get(f.judgeKey);
                if (p == null) {
                  reasons.push(`${f.name}: no Jev season_fits score`);
                } else if (p < args.threshold) {
                  reasons.push(
                    `${f.name}: season_fits ${
                      p.toFixed(2)
                    } < ${args.threshold} for ${f.destPath}`,
                  );
                }
              }
            }
            const first = pj.files[pj.files.length - 1];
            // deno-lint-ignore no-explicit-any
            const md: any = first?.metadata ?? {};
            const record = {
              ...base,
              mediaType: md.media_type ?? "unknown",
              title: md.title ?? pj.name,
              season: md.season != null ? String(md.season) : undefined,
              episode: md.episode != null ? String(md.episode) : undefined,
              year: md.year != null ? String(md.year) : undefined,
              destPath: first?.destPath ?? "",
              confidence: md.confidence ?? "Low",
            };
            if (reasons.length) {
              // Not confident: leave it in staging, still pending, stamped so
              // plan skips it until the rules change or the retry window ends.
              await jobs.updateOne({ _id }, {
                $set: {
                  status: "pending",
                  held_at: new Date(),
                  held_reason: reasons.join("; "),
                  held_rules: plan.rulesHash,
                  planned_dest: pj.files.map((f) => f.destPath),
                  updated_at: new Date(),
                },
                $inc: { held_count: 1 },
              });
              held++;
              context.logger.warning(
                `STAGED ${pj.name}: ${reasons.join("; ")}`,
              );
              handles.push(
                await context.writeResource("job", `job-${pj.jobId}`, {
                  ...record,
                  status: "held" as const,
                  reason: reasons.join("; "),
                  processedAt: new Date().toISOString(),
                }),
              );
              continue;
            }
            try {
              for (const f of pj.files) {
                await moveFile(f.path, f.destPath, f.name, context.logger);
              }
              const src = `${stagingDir.replace(/\/$/, "")}/${pj.name}`;
              try {
                if ((await Deno.stat(src)).isDirectory) {
                  await Deno.remove(src, { recursive: true });
                }
              } catch { /* already moved */ }
              await jobs.updateOne({ _id }, {
                $set: { status: "done", completed_at: new Date() },
                $unset: { held_at: "", held_reason: "", held_rules: "" },
              });
              moved++;
              handles.push(
                await context.writeResource("job", `job-${pj.jobId}`, {
                  ...record,
                  status: "processed" as const,
                  processedAt: new Date().toISOString(),
                }),
              );
            } catch (err) {
              await markFailed(jobs, _id, String(err));
              failed++;
              context.logger.error(`Job ${pj.jobId} failed: ${err}`);
              handles.push(
                await context.writeResource("job", `job-${pj.jobId}`, {
                  ...record,
                  status: "failed" as const,
                  reason: String(err),
                  processedAt: new Date().toISOString(),
                }),
              );
            }
          }
        } finally {
          await client.close();
        }
        context.logger.info(
          `Applied ${planName}: ${moved} moved, ${held} left in staging, ${failed} failed`,
        );
        return { dataHandles: handles };
      },
    },
    evaluate: {
      description:
        "Benchmark Ollama models on labeled filenames through the real classification pipeline; writes eval-<name> with accuracy, latency and misses per model. Moves nothing.",
      arguments: z.object({
        cases: z.array(EvalCaseSchema).min(1).describe(
          "Filenames with the directory each belongs in",
        ),
        models: z.array(z.string()).min(1).describe(
          "Ollama models to compare, e.g. ['qwen3:14b', 'gemma4:26b']",
        ),
        llmOnly: z.boolean().default(true).describe(
          "Send every case to the LLM (skip special cases and the fast path), to compare models rather than the pipeline",
        ),
        structured: z.boolean().default(true).describe(
          "Constrain stage 1 with Ollama structured outputs (JSON Schema), as production does",
        ),
        catalogHints: z.boolean().default(true).describe(
          "Tell stage 2 which existing catalog dirs look like the show, as production does",
        ),
        scene: z.array(SceneItemSchema).default([]).describe(
          "@keeb/scenerules parse items for the case filenames; enables the scene route (ignored with llmOnly)",
        ),
        name: z.string().default("latest").describe(
          "Suffix for the eval-<name> resource",
        ),
      }),
      execute: async (
        args: {
          cases: z.infer<typeof EvalCaseSchema>[];
          models: string[];
          llmOnly: boolean;
          structured: boolean;
          catalogHints: boolean;
          scene: z.infer<typeof SceneItemSchema>[];
          name: string;
        },
        // deno-lint-ignore no-explicit-any
        context: any,
      ) => {
        const { ollamaUrl, mediaRoot } = context.globalArgs;
        const prompts: Prompts = {
          filename: await Deno.readTextFile(
            context.extensionFile("prompts/filename-to-json.md"),
          ),
          anime: await Deno.readTextFile(
            context.extensionFile("prompts/anime-filename-to-json.md"),
          ),
          savePath: await Deno.readTextFile(
            context.extensionFile("prompts/json-to-save-path.md"),
          ),
        };
        await ollamaHealthCheck(ollamaUrl);
        const catalogDirs = args.catalogHints
          ? await listCatalogDirs(mediaRoot)
          : undefined;
        const scene = new Map(
          args.scene.map((i) => [String(i.input), i.parsed as SceneParse]),
        );
        // the scene route matches against the catalog even without hints
        const sceneDirs = args.scene.length
          ? await listCatalogDirs(mediaRoot)
          : undefined;
        const norm = (p: string) => p.replace(/\/+$/, "");
        const results = [];
        for (const m of args.models) {
          // Warm the model so load time isn't charged to the first case.
          await ollamaGenerate(ollamaUrl, m, "Reply OK.", "ping").catch(
            () => {},
          );
          const times: number[] = [];
          const misses = [];
          const byTag: Record<string, { cases: number; correct: number }> = {};
          let correct = 0;
          let errors = 0;
          const routes: Record<string, number> = {};
          for (const c of args.cases) {
            const tag = c.tag ?? "untagged";
            byTag[tag] ??= { cases: 0, correct: 0 };
            byTag[tag].cases++;
            const t0 = performance.now();
            let got = "";
            let route = "error";
            try {
              const r = await classifyFile(c.filename, {
                mediaRoot,
                ollamaUrl,
                ollamaModel: m,
                prompts,
                llmOnly: args.llmOnly,
                structured: args.structured,
                catalogDirs: catalogDirs ?? sceneDirs,
                scene,
              });
              got = r.destPath;
              route = r.route;
              routes[route] = (routes[route] ?? 0) + 1;
            } catch (err) {
              errors++;
              got = `error: ${String(err).slice(0, 160)}`;
            }
            times.push(performance.now() - t0);
            if (norm(got) === norm(c.expected)) {
              correct++;
              byTag[tag].correct++;
            } else {
              misses.push({
                filename: c.filename,
                tag: c.tag,
                expected: c.expected,
                got,
                route,
              });
            }
          }
          const row = {
            model: m,
            correct,
            accuracy: Math.round((correct / args.cases.length) * 1000) / 1000,
            errors,
            medianMs: Math.round(pct(times, 0.5)),
            p95Ms: Math.round(pct(times, 0.95)),
            byTag,
            routes,
            misses,
          };
          context.logger.info(
            `${m}: ${correct}/${args.cases.length} (${
              (row.accuracy * 100).toFixed(1)
            }%), ${errors} error(s), median ${row.medianMs}ms, p95 ${row.p95Ms}ms`,
          );
          results.push(row);
        }
        const handle = await context.writeResource(
          "eval",
          `eval-${slugify(args.name) || "latest"}`,
          {
            name: args.name,
            cases: args.cases.length,
            llmOnly: args.llmOnly,
            structured: args.structured,
            catalogHints: args.catalogHints,
            ranAt: new Date().toISOString(),
            models: results,
          },
        );
        return { dataHandles: [handle] };
      },
    },
    test: {
      description:
        "Dry-run classification: parse a filename and show where it would be organized without moving anything",
      arguments: z.object({
        filename: z.string().describe("Filename to classify"),
        scene: z.array(SceneItemSchema).default([]).describe(
          "@keeb/scenerules parse items (as plan takes them); enables the scene route",
        ),
      }),
      execute: async (
        args: { filename: string; scene: z.infer<typeof SceneItemSchema>[] },
        // deno-lint-ignore no-explicit-any
        context: any,
      ) => {
        const { ollamaModel, ollamaUrl, mediaRoot } = context.globalArgs;
        const prompts: Prompts = {
          filename: await Deno.readTextFile(
            context.extensionFile("prompts/filename-to-json.md"),
          ),
          anime: await Deno.readTextFile(
            context.extensionFile("prompts/anime-filename-to-json.md"),
          ),
          savePath: await Deno.readTextFile(
            context.extensionFile("prompts/json-to-save-path.md"),
          ),
        };
        const catalogDirs = await listCatalogDirs(mediaRoot);
        // Same classification and gate context as plan/apply.
        const c = await classifyFile(args.filename, {
          mediaRoot,
          ollamaUrl,
          ollamaModel,
          prompts,
          structured: true,
          catalogDirs,
          scene: new Map(
            args.scene.map((i) => [String(i.input), i.parsed as SceneParse]),
          ),
        });
        const g = await gateContext(c.destPath, catalogDirs);
        context.logger.info(
          `${c.route}: ${args.filename} → ${c.destPath}${
            g.splitRisk
              ? ` (split risk: new dir next to ${g.similarDirs.join(", ")})`
              : ""
          }`,
        );
        const md = c.metadata ?? {};
        const handle = await context.writeResource(
          "job",
          `test-${Date.now()}`,
          {
            jobId: "dry-run",
            filename: args.filename,
            mediaType: md.media_type ?? "unknown",
            title: md.title ?? args.filename,
            season: md.season != null ? String(md.season) : undefined,
            episode: md.episode != null ? String(md.episode) : undefined,
            year: md.year != null ? String(md.year) : undefined,
            sourcePath: "(dry-run)",
            destPath: c.destPath,
            status: "processed" as const,
            confidence: md.confidence ?? "Low",
            reason: g.splitRisk
              ? `split risk: new dir next to ${g.similarDirs.join(", ")}`
              : `route: ${c.route}`,
            processedAt: new Date().toISOString(),
          },
        );
        return { dataHandles: [handle] };
      },
    },
    write_playlists: {
      description:
        "Write an .m3u playlist of every video file, in episode order, into a catalog directory (and optionally its subdirectories)",
      arguments: z.object({
        path: z
          .string()
          .describe(
            "Directory to generate for, e.g. .../shows/from/s1. With recursive, a subtree root like .../shows",
          ),
        recursive: z
          .boolean()
          .default(false)
          .describe("Walk subdirectories and generate a playlist in each"),
      }),
      execute: async (
        args: { path: string; recursive: boolean },
        // deno-lint-ignore no-explicit-any
        context: any,
      ) => {
        const { mediaRoot } = context.globalArgs;
        const root = args.path.replace(/\/+$/, "");

        const dirs = args.recursive
          ? await collectDirs(root, mediaRoot)
          : isPlaylistDir(root, mediaRoot)
          ? [root]
          : [];
        if (dirs.length === 0) {
          context.logger.warn(
            `No playlist-eligible directory under ${root} ` +
              `(category roots and staging are skipped by design)`,
          );
        }

        const handle = await runPlaylists(
          dirs,
          args.recursive ? `${root} (recursive)` : root,
          instanceName(root),
          context,
        );
        return { dataHandles: [handle] };
      },
    },
    write_playlists_recent: {
      description:
        "Regenerate .m3u playlists for every directory touched by recent organizer jobs",
      arguments: z.object({
        windowMinutes: z
          .number()
          .default(60)
          .describe("Look at organizer jobs processed in the last N minutes"),
      }),
      execute: async (
        args: { windowMinutes: number },
        // deno-lint-ignore no-explicit-any
        context: any,
      ) => {
        const { mediaRoot } = context.globalArgs;
        const cutoff = new Date(
          Date.now() - args.windowMinutes * 60 * 1000,
        ).toISOString();
        const predicate = `modelName == "${context.definition.name}" && ` +
          `specName == "job" && attributes.status == "processed" && ` +
          `attributes.processedAt > "${cutoff}"`;
        const records = await context.queryData(predicate);

        // Many jobs land in the same season directory; a playlist is derived
        // from the whole directory, so one regeneration per distinct dir is
        // enough regardless of how many episodes arrived.
        const dirs: string[] = [];
        const seen = new Set<string>();
        for (const rec of records) {
          // deno-lint-ignore no-explicit-any
          const destPath: string | undefined = (rec as any).attributes
            ?.destPath;
          if (!destPath) continue;
          const dir = destPath.replace(/\/+$/, "");
          if (seen.has(dir)) continue;
          seen.add(dir);
          if (!isPlaylistDir(dir, mediaRoot)) {
            context.logger.info(`Skipping non-show directory: ${dir}`);
            continue;
          }
          dirs.push(dir);
        }
        context.logger.info(
          `${records.length} job(s) since ${cutoff} → ${dirs.length} directory(s)`,
        );

        const handle = await runPlaylists(
          dirs,
          `organizer jobs since ${cutoff}`,
          "recent",
          context,
        );
        return { dataHandles: [handle] };
      },
    },
  },
};

// --- Playlist generation ---

// A playlist belongs to a season directory (shows/<slug>/s<N>) or a flat anime
// directory (anime/completed/<slug>) — never to a category root like movies/,
// which would collapse the entire library into one file.
const CONTAINER_DIRS = new Set(["anime/completed"]);

function isPlaylistDir(dir: string, mediaRoot: string): boolean {
  const videoRoot = `${mediaRoot.replace(/\/+$/, "")}/video`;
  if (!dir.startsWith(`${videoRoot}/`)) return false;
  const rel = dir.slice(videoRoot.length + 1).replace(/\/+$/, "");
  const segments = rel.split("/").filter(Boolean);
  // One segment is a category root (shows/, movies/, anime/).
  if (segments.length < 2) return false;
  if (segments[0] === "staging") return false;
  return !CONTAINER_DIRS.has(rel);
}

// Walk a subtree collecting every playlist-eligible directory. Used for
// backfills over an existing catalog.
async function collectDirs(
  root: string,
  mediaRoot: string,
): Promise<string[]> {
  const found: string[] = [];
  if (isPlaylistDir(root, mediaRoot)) found.push(root);
  for await (const entry of Deno.readDir(root)) {
    if (!entry.isDirectory) continue;
    found.push(...await collectDirs(`${root}/${entry.name}`, mediaRoot));
  }
  return found;
}

// Episode number is the only ordering signal that survives mixed release
// groups in one directory ([SubsPlease] ... - 07 alongside [DKB] ... S01E07).
// Season is a tiebreaker only when EVERY entry carries one — otherwise
// seasonless anime releases would sort as a phantom season 0 ahead of their
// own SxxEyy-named siblings.
function parseOrder(
  filename: string,
): { season?: number; episode: number } | null {
  const parsed = regexParseFilename(filename);
  if (!parsed?.episode) return null;
  const episode = parseInt(parsed.episode);
  if (!Number.isFinite(episode)) return null;
  const season = parsed.season ? parseInt(parsed.season) : undefined;
  return { season, episode };
}

function naturalCompare(a: string, b: string): number {
  return a.localeCompare(b, undefined, { numeric: true, sensitivity: "base" });
}

function sortEpisodes(files: string[]): string[] {
  const keys = new Map(files.map((f) => [f, parseOrder(f)]));
  const parsed = [...keys.values()].filter((k) => k != null);
  const useSeason = parsed.length > 0 &&
    parsed.every((k) => k!.season != null);

  return [...files].sort((a, b) => {
    const ka = keys.get(a) ?? null;
    const kb = keys.get(b) ?? null;
    // Unparseable names (extras, specials, oddly-named rips) sort last rather
    // than silently landing between episodes.
    if (!ka && !kb) return naturalCompare(a, b);
    if (!ka) return 1;
    if (!kb) return -1;
    if (useSeason && ka.season !== kb.season) {
      return (ka.season ?? 0) - (kb.season ?? 0);
    }
    if (ka.episode !== kb.episode) return ka.episode - kb.episode;
    return naturalCompare(a, b);
  });
}

// The playlist is named after its own directory (s1/ → s1.m3u), so it stays
// meaningful once Jellyfin or a player shows it detached from its path.
async function writePlaylist(
  dir: string,
  // deno-lint-ignore no-explicit-any
  logger: any,
): Promise<Playlist> {
  const files: string[] = [];
  for await (const entry of Deno.readDir(dir)) {
    if (entry.isFile && isVideoFile(entry.name)) files.push(entry.name);
  }

  const name = `${dir.slice(dir.lastIndexOf("/") + 1)}.m3u`;
  const path = `${dir}/${name}`;
  const now = new Date().toISOString();

  if (files.length === 0) {
    logger.info(`No video files in ${dir}, skipping playlist`);
    return {
      dir,
      path,
      entries: 0,
      episodes: [],
      status: "empty" as const,
      writtenAt: now,
    };
  }

  const episodes = sortEpisodes(files);
  // Relative entries — the playlist sits beside the media, so it survives the
  // catalog being mounted at a different prefix by a player or another host.
  const content = `#EXTM3U\n${episodes.join("\n")}\n`;

  let existing: string | null = null;
  try {
    existing = await Deno.readTextFile(path);
  } catch {
    // No playlist yet.
  }
  if (existing === content) {
    return {
      dir,
      path,
      entries: episodes.length,
      episodes,
      status: "unchanged" as const,
      writtenAt: now,
    };
  }

  await Deno.writeTextFile(path, content);
  logger.info(`Wrote ${path} (${episodes.length} entries)`);
  return {
    dir,
    path,
    entries: episodes.length,
    episodes,
    status: "written" as const,
    writtenAt: now,
  };
}

async function runPlaylists(
  dirs: string[],
  scope: string,
  instance: string,
  // deno-lint-ignore no-explicit-any
  context: any,
  // deno-lint-ignore no-explicit-any
): Promise<any> {
  const results: Playlist[] = [];
  const failures: { dir: string; error: string }[] = [];
  for (const dir of dirs) {
    try {
      results.push(await writePlaylist(dir, context.logger));
    } catch (err) {
      // A directory that vanished or is unreadable shouldn't abort the batch.
      context.logger.warn(`Skip ${dir}: ${err}`);
      failures.push({ dir, error: String(err) });
    }
  }

  const written = results.filter((r) => r.status === "written").length;
  const unchanged = results.filter((r) => r.status === "unchanged").length;
  context.logger.info(
    `Playlists: ${written} written, ${unchanged} unchanged, ` +
      `${failures.length} failed across ${dirs.length} directory(s)`,
  );

  return await context.writeResource("playlist_run", instance, {
    scope,
    dirs: dirs.length,
    written,
    unchanged,
    failed: failures.length,
    results,
    failures,
    ranAt: new Date().toISOString(),
  });
}

// Stable instance name keyed on a path so re-runs against the same target
// version one resource rather than creating a new instance per run.
function instanceName(path: string): string {
  return path
    .replace(/^\/+/, "")
    .replace(/[^a-zA-Z0-9]+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 200);
}

// --- Regex filename parser (mirrors mms_dedup fast-path) ---

function regexParseFilename(
  raw: string,
): { show: string; episode: string; season?: string } | null {
  // Strip [Group] prefix for all patterns
  const stripped = raw.replace(/^\[.*?\]\s*/, "");

  // S01E06 / S1E6 / s01e06
  const seMatch = stripped.match(/^(.+?)[.\s_-]+[Ss](\d{1,2})[Ee](\d{1,3})\b/);
  if (seMatch) {
    return {
      show: seMatch[1].replace(/\./g, " ").trim(),
      season: String(parseInt(seMatch[2])),
      episode: String(parseInt(seMatch[3])),
    };
  }

  // 1x06 / 01x06
  const xMatch = stripped.match(/^(.+?)[.\s_-]+(\d{1,2})x(\d{1,3})\b/);
  if (xMatch) {
    return {
      show: xMatch[1].replace(/\./g, " ").trim(),
      season: String(parseInt(xMatch[2])),
      episode: String(parseInt(xMatch[3])),
    };
  }

  // Anime style: Title - 06 [1080p], Title - 1106, Title - 49v2
  const animeMatch = stripped.match(/^(.+?)\s+-\s*(\d{2,4})(?:v\d+)?\b/);
  if (animeMatch) {
    return {
      show: animeMatch[1].trim(),
      episode: String(parseInt(animeMatch[2])),
    };
  }

  // Bare: Title 1106 [quality]
  const bareMatch = stripped.match(/^(.+?)\s+(\d{3,4})\s*[\[\(\.]/);
  if (bareMatch) {
    return {
      show: bareMatch[1].trim(),
      episode: String(parseInt(bareMatch[2])),
    };
  }

  return null;
}

// --- Deterministic fast-path classification ---
// Structured release-group filenames are unambiguously anime. The LLM's
// release-group override is descriptive, not deterministic — it has
// misclassified edge cases like "Dr. Stone S4" (Western-sounding title +
// season marker) despite the explicit rule. When regex can parse cleanly,
// skip the LLM entirely.
const ANIME_RELEASE_GROUPS = new Set([
  "subsplease",
  "erai-raws",
  "judas",
  "ember",
  "asw",
  "toonshub",
  "dkb",
  "doomdos",
]);

const ANIME_RELEASE_GROUP_RE = new RegExp(
  `\\b(${[...ANIME_RELEASE_GROUPS].join("|")})\\b`,
  "i",
);

// Patterns that require LLM-driven title remapping or complex season
// extraction that plain regex can't do. Fall through to the LLM when any
// match — these are the special cases enumerated in json-to-save-path.md.
const LLM_REMAP_TRIGGERS = [
  /yofukashi no uta/i, // → call-of-the-night
  /shimetsu kaiyuu/i, // Jujutsu Kaisen subtitle → season 3
  /\b2nd season\b/i,
  /\b3rd season\b/i,
  /jidou\s*hanbaiki/i, // alt romaji normalization
];

function getReleaseGroup(filename: string): string | null {
  const m = filename.match(ANIME_RELEASE_GROUP_RE);
  return m ? m[1].toLowerCase() : null;
}

function isAnimeByReleaseGroup(filename: string): boolean {
  return getReleaseGroup(filename) != null;
}

// Mirrors the save-path prompt's naming rules: lowercase, spaces→hyphens,
// dedupe hyphens, strip leading/trailing.
function slugify(s: string): string {
  return s
    .toLowerCase()
    .replace(/[:!?.,~'"()[\]]/g, "")
    .trim()
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

// Promote trailing " S##" in the regex-captured show name to a season field.
// Anime regex captures "Dr. Stone S4 - 28" as show="Dr. Stone S4"; split it.
function extractTrailingSeason(
  show: string,
): { title: string; season?: number } {
  const m = show.match(/^(.+?)\s+S(\d{1,2})$/i);
  if (m) return { title: m[1].trim(), season: parseInt(m[2]) };
  return { title: show };
}

function fastPathAnime(
  filename: string,
  mediaRoot: string,
): {
  title: string;
  season?: number;
  episode: number;
  destPath: string;
} | null {
  if (!isAnimeByReleaseGroup(filename)) return null;

  // Year-in-parens → likely a movie/film release, defer to LLM.
  if (/\(\d{4}\)/.test(filename)) return null;

  if (LLM_REMAP_TRIGGERS.some((rx) => rx.test(filename))) return null;

  const parsed = regexParseFilename(filename);
  if (!parsed || !parsed.episode) return null;

  const episode = parseInt(parsed.episode);
  // Bare regex patterns can pick up years as "episode"; anime episode counts
  // max out well under this threshold.
  if (!Number.isFinite(episode) || episode < 0 || episode > 1500) return null;

  const { title, season: trailingSeason } = extractTrailingSeason(parsed.show);
  const season = trailingSeason ??
    (parsed.season ? parseInt(parsed.season) : undefined);

  const slug = slugify(title);
  if (!slug) return null;

  const root = mediaRoot.replace(/\/$/, "");
  const base = `${root}/video/anime/completed/${slug}`;
  const destPath = season != null ? `${base}/s${season}` : base;

  return { title, season, episode, destPath };
}

// --- Deterministic Special Cases ---
// Shows whose release title doesn't map to the catalog directory, or whose
// season folder can't be read off the filename. Checked before the fast-path
// and the LLM, so the same release always lands in the same place. Add a row
// here instead of hand-moving files: the organizer is canonical, and a
// hand-move is undone by the next episode.
export type SpecialCase = {
  match: RegExp;
  kind: "anime" | "tv_show";
  title: string;
  slug: string;
  /**
   * Season folder: a fixed number, "flat" (no season dir), "file" (the
   * filename's own season), or [firstEpisode, season] breaks for shows
   * numbered continuously across seasons, ascending by firstEpisode.
   */
  season: number | "flat" | "file" | [number, number][];
  /** Season when the filename has none and `season` is "file". */
  defaultSeason?: number;
};

export const SPECIAL_CASES: SpecialCase[] = [
  // DKB numbers JoJo parts as seasons (Part 7 = S06); Steel Ball Run is its
  // own slug with no season breaks.
  {
    match: /steel ball run/i,
    kind: "anime",
    title: "Steel Ball Run JoJo no Kimyou na Bouken",
    slug: "steel-ball-run-jojo-no-kimyou-na-bouken",
    season: "flat",
  },
  // Season 4 of Tokyo Revengers under its arc title.
  {
    match: /tokyo revengers.*santen sensou/i,
    kind: "anime",
    title: "Tokyo Revengers",
    slug: "tokyo-revengers",
    season: 4,
  },
  // SubsPlease numbers Kusuriya continuously: S1 1-24, S2 25-48, S3 49+.
  {
    match: /kusuriya no hitorigoto/i,
    kind: "anime",
    title: "Kusuriya no Hitorigoto",
    slug: "kusuriya-no-hitorigoto",
    season: [[1, 1], [25, 2], [49, 3]],
  },
  // Judas titled season 2 just "Cyberpunk".
  {
    match: /\bcyberpunk\b/i,
    kind: "anime",
    title: "Cyberpunk Edgerunners",
    slug: "cyberpunk-edgerunners",
    season: "file",
    defaultSeason: 1,
  },
  // Season 1 releases carry no season marker; S2+ say "S2".
  {
    match: /hotel inhumans/i,
    kind: "anime",
    title: "Hotel Inhumans",
    slug: "hotel-inhumans",
    season: "file",
    defaultSeason: 1,
  },
  // S1 was released as Special.Ops.Lioness, S2+ as Lioness.2023.
  {
    match: /^(special[ .]ops[ .])?lioness([ .]2023)?[ .]s\d/i,
    kind: "tv_show",
    title: "Lioness",
    slug: "lioness",
    season: "file",
  },
];

/**
 * Resolve a filename against SPECIAL_CASES. Season and episode come from the
 * regex parser (with any trailing " S2" on the title promoted to a season).
 */
export function specialCase(
  filename: string,
  mediaRoot: string,
): {
  mediaType: "anime" | "tv_show";
  title: string;
  season?: number;
  episode?: number;
  destPath: string;
} | null {
  const rule = SPECIAL_CASES.find((r) => r.match.test(filename));
  if (!rule) return null;
  const parsed = regexParseFilename(filename);
  const trailing = parsed ? extractTrailingSeason(parsed.show) : null;
  const fileSeason = trailing?.season ??
    (parsed?.season ? parseInt(parsed.season) : undefined);
  const episode = parsed?.episode ? parseInt(parsed.episode) : undefined;

  let season: number | undefined;
  if (rule.season === "flat") season = undefined;
  else if (rule.season === "file") season = fileSeason ?? rule.defaultSeason;
  else if (typeof rule.season === "number") season = rule.season;
  else {
    if (episode == null) return null;
    for (const [first, s] of rule.season) if (episode >= first) season = s;
  }
  if (rule.kind === "tv_show" && season == null) return null;

  const root = mediaRoot.replace(/\/$/, "");
  const base = rule.kind === "anime"
    ? `${root}/video/anime/completed/${rule.slug}`
    : `${root}/video/shows/${rule.slug}`;
  return {
    mediaType: rule.kind,
    title: rule.title,
    season,
    episode,
    destPath: season != null ? `${base}/s${season}` : base,
  };
}

// --- Scene route (@keeb/scenerules parse results) ---

/**
 * The fields the scene route reads from an @keeb/scenerules `parse` item.
 * Structural, so the organizer doesn't depend on that extension's code.
 */
export type SceneParse = {
  style: string;
  kind: string;
  title: string | null;
  country: string | null;
  year: number | null;
  season: number | null;
  episodes: number[];
  confidence: "high" | "medium" | "low";
};

/** Bumped when the scene route's decisions change (feeds rulesHash). */
export const SCENE_ROUTE_VERSION = 2;

/**
 * Deterministic placement from a scene-rules parse, or null to defer to the
 * LLM. Only high-confidence parses are used, and only where the answer is
 * certain:
 * - a TV episode / multi-episode / season pack of a show already in the
 *   catalog (shows/ or anime/completed/): the existing dir whose slug matches
 *   the scene title, with the country code kept and a trailing year optional
 *   (Lioness.2023 → lioness). Exactly one dir must match;
 * - the same for a show not in the catalog yet (unless `allowNew` is false):
 *   shows/<title[-country]>/sN, as long as no existing dir looks like the
 *   same show (Survivor.AU next to survivor/ defers);
 * - a feature with a production year: movies/ (flat; anime films too).
 * Specials and anything ambiguous go to the LLM and the gate.
 */
export function sceneRoute(
  p: SceneParse,
  catalogDirs: string[],
  mediaRoot: string,
  allowNew = true,
): { destPath: string; mediaType: string; title: string } | null {
  if (p.confidence !== "high" || !p.title || p.style === "fansub") return null;
  const root = mediaRoot.replace(/\/$/, "");
  if (p.kind === "feature") {
    return p.year
      ? { destPath: `${root}/video/movies`, mediaType: "movie", title: p.title }
      : null;
  }
  if (!["episode", "multi-episode", "season-pack"].includes(p.kind)) {
    return null;
  }
  if (p.season == null || p.season < 1) return null;
  const base = [p.title, p.country].filter(Boolean).join(" ");
  const variants = [
    p.year ? `${base} ${p.year}` : null,
    base,
  ].filter((v): v is string => !!v).map(slugify);
  for (const v of variants) {
    const hits = catalogDirs.filter((d) => d.split("/").pop() === v);
    if (hits.length === 1) {
      return {
        destPath: `${hits[0]}/s${p.season}`,
        mediaType: hits[0].includes("/anime/") ? "anime" : "tv_show",
        title: p.title,
      };
    }
    if (hits.length > 1) return null; // same slug in shows/ and anime/: ambiguous
  }
  // A new show. Anything in the catalog that might be it (a shared distinctive
  // word, a prefix) goes to the LLM, which gets those dirs as hints.
  const slug = slugify(base);
  if (!allowNew || !slug) return null;
  const near = similarCatalogDirs(base, catalogDirs).some((s) =>
    s.contained || s.score >= 0.5
  );
  if (near) return null;
  return {
    destPath: `${root}/video/shows/${slug}/s${p.season}`,
    mediaType: "tv_show",
    title: p.title,
  };
}

/** Ollama structured-output schema for stage 1 (filename → metadata). */
export const STAGE1_SCHEMA = {
  type: "object",
  properties: {
    media_type: { type: "string", enum: ["anime", "movie", "tv_show", "book"] },
    title: { type: "string" },
    season: { type: ["integer", "null"] },
    episode: { type: ["integer", "null"] },
    volume: { type: ["integer", "null"] },
    episode_title: { type: ["string", "null"] },
    year: { type: ["integer", "null"] },
    confidence: { type: "string", enum: ["High", "Medium", "Low"] },
  },
  required: ["media_type", "title", "season", "episode", "confidence"],
};

export type Prompts = { filename: string; anime: string; savePath: string };

// --- Catalog context (stage-2 hints and the review gate) ---

/** Show directories under shows/ and anime/completed/, as absolute paths. */
export async function listCatalogDirs(mediaRoot: string): Promise<string[]> {
  const root = mediaRoot.replace(/\/$/, "");
  const out: string[] = [];
  for (const sub of ["video/shows", "video/anime/completed"]) {
    try {
      for await (const e of Deno.readDir(`${root}/${sub}`)) {
        if (e.isDirectory) out.push(`${root}/${sub}/${e.name}`);
      }
    } catch { /* subtree missing */ }
  }
  return out.sort();
}

const STOP_TOKENS = new Set([
  "the",
  "no",
  "wa",
  "ga",
  "wo",
  "to",
  "ni",
  "de",
  "of",
  "and",
  "with",
  "an",
  "in",
  "on",
  "at",
  "for",
  "from",
  "by",
  "or",
  "vs",
  "season",
  "hen",
]);

function slugTokens(s: string): string[] {
  return s.toLowerCase().split(/[^a-z0-9]+/).filter((t) =>
    t.length > 1 && !STOP_TOKENS.has(t) && !/^s\d+$/.test(t)
  );
}

/** The show directory of a destination: strips a trailing /sN. */
export function showDirOf(destPath: string): string {
  return destPath.replace(/\/+$/, "").replace(/\/s\d+$/, "");
}

/**
 * Existing show dirs whose slug shares tokens with `text`, best first. Score
 * is the share of the candidate's tokens found in `text` (so "tokyo-revengers"
 * scores 1.0 against "Tokyo Revengers Santen Sensou-hen").
 */
export function similarCatalogDirs(
  text: string,
  catalogDirs: string[],
  limit = 5,
): { dir: string; score: number; contained: boolean }[] {
  const want = new Set(slugTokens(text));
  if (want.size === 0) return [];
  return catalogDirs
    .map((dir) => {
      const t = slugTokens(dir.split("/").pop() ?? "");
      const hit = t.filter((x) => want.has(x)).length;
      // One name is a token-prefix of the other: an arc/sequel title of the
      // same show ("tokyo-revengers" → "tokyo-revengers-santen-sensou",
      // "cyberpunk" → "cyberpunk-edgerunners"). Shared words elsewhere
      // ("wolf" in "spice-and-wolf") don't count.
      const w = slugTokens(text);
      const shorter = t.length <= w.length ? t : w;
      const longer = t.length <= w.length ? w : t;
      // A lone common word ("night", "ted") isn't enough; a distinctive one
      // ("cyberpunk") is.
      const contained = shorter.length > 0 &&
        (shorter.length >= 2 || shorter[0].length >= 6) &&
        shorter.every((x, i) => longer[i] === x);
      return { dir, score: t.length ? hit / t.length : 0, contained };
    })
    .filter((x) => x.score > 0)
    .sort((a, b) => b.score - a.score || a.dir.localeCompare(b.dir))
    .slice(0, limit);
}

export type GateContext = {
  showDir: string;
  showDirExists: boolean;
  existingSeasons: string[];
  similarDirs: string[];
  /** A new show dir next to an existing one that looks like the same show. */
  splitRisk: boolean;
};

/** What the review gate knows about a proposed destination. */
export async function gateContext(
  destPath: string,
  catalogDirs: string[],
): Promise<GateContext> {
  const showDir = showDirOf(destPath);
  const showDirExists = catalogDirs.includes(showDir);
  const existingSeasons: string[] = [];
  if (showDirExists) {
    for await (const e of Deno.readDir(showDir)) {
      if (e.isDirectory && /^s\d+$/.test(e.name)) existingSeasons.push(e.name);
    }
    existingSeasons.sort((a, b) => parseInt(a.slice(1)) - parseInt(b.slice(1)));
  }
  const similar = similarCatalogDirs(
    showDir.split("/").pop() ?? "",
    catalogDirs,
  ).filter((s) => s.dir !== showDir);
  return {
    showDir,
    showDirExists,
    existingSeasons,
    similarDirs: similar.map((s) => s.dir),
    // Same subtree only: shows/one-piece (live action) isn't a split of
    // anime/completed/one-piece.
    splitRisk: !showDirExists &&
      similar.some((s) =>
        s.contained && s.dir.slice(0, s.dir.lastIndexOf("/")) ===
          showDir.slice(0, showDir.lastIndexOf("/"))
      ),
  };
}

export type ClassifyOptions = {
  mediaRoot: string;
  ollamaUrl: string;
  ollamaModel: string;
  prompts: Prompts;
  /** Skip special cases and the fast path (benchmarks the model alone). */
  llmOnly?: boolean;
  /** Constrain stage 1 to STAGE1_SCHEMA via Ollama structured outputs. */
  structured?: boolean;
  /**
   * Existing show dirs (listCatalogDirs). When given, stage 2 is told which
   * existing directories look like this show, so it reuses one instead of
   * minting a new slug from an arc or alternate title.
   */
  catalogDirs?: string[];
  /** @keeb/scenerules parse results by filename, for the scene route. */
  scene?: Map<string, SceneParse>;
  /** Runs once before the first LLM call (e.g. an Ollama health check). */
  beforeLlm?: () => Promise<void>;
};

/**
 * Where a file goes, in order: deterministic special case → One Piece arc →
 * release-group fast path → two-stage LLM (filename → JSON → save path).
 */
export async function classifyFile(
  filename: string,
  opts: ClassifyOptions,
): Promise<{
  // deno-lint-ignore no-explicit-any
  metadata: any;
  destPath: string;
  route: "special" | "one-piece" | "fast" | "scene" | "llm";
}> {
  const { mediaRoot, ollamaUrl, ollamaModel, prompts } = opts;
  if (!opts.llmOnly) {
    const special = specialCase(filename, mediaRoot);
    if (special) {
      return {
        metadata: {
          media_type: special.mediaType,
          title: special.title,
          season: special.season ?? null,
          episode: special.episode ?? null,
          confidence: "High",
        },
        destPath: special.destPath,
        route: "special",
      };
    }
    // One Piece is foldered by TMDB season number (one-piece/sN) so Jellyfin
    // pulls the right arc name + art. New episodes are the current arc —
    // Elbaph = s23 (eps 1156+). Bump this when episodes pass the end of s23
    // (TMDB ep 1181).
    if (/one piece - \d/i.test(filename)) {
      return {
        metadata: {
          media_type: "anime",
          title: "One Piece",
          season: null,
          episode: null,
          confidence: "High",
        },
        destPath: `${mediaRoot}/video/anime/completed/one-piece/s23`,
        route: "one-piece",
      };
    }
    const fast = fastPathAnime(filename, mediaRoot);
    if (fast) {
      return {
        metadata: {
          media_type: "anime",
          title: fast.title,
          season: fast.season ?? null,
          episode: fast.episode,
          confidence: "High",
        },
        destPath: fast.destPath,
        route: "fast",
      };
    }
    const sp = opts.scene?.get(filename);
    // New show dirs only for non-anime releases; anime the fast path deferred
    // (or that needs a title remap) keeps the LLM.
    const sr = sp && opts.catalogDirs
      ? sceneRoute(
        sp,
        opts.catalogDirs,
        mediaRoot,
        !isAnimeByReleaseGroup(filename) &&
          !LLM_REMAP_TRIGGERS.some((re) => re.test(filename)),
      )
      : null;
    if (sp && sr) {
      return {
        metadata: {
          media_type: sr.mediaType,
          title: sr.title,
          season: sp.season,
          episode: sp.episodes[0] ?? null,
          year: sp.year,
          confidence: "High",
        },
        destPath: sr.destPath,
        route: "scene",
      };
    }
  }

  // Stage 1: filename → JSON metadata. Release-group anime gets the anime
  // prompt — the general prompt loses season/episode on noisy anime filenames
  // (alt titles, year-in-parens) that the fast-path defers.
  await opts.beforeLlm?.();
  const stage1Prompt = isAnimeByReleaseGroup(filename)
    ? prompts.anime
    : prompts.filename;
  const metadataJson = await ollamaGenerate(
    ollamaUrl,
    ollamaModel,
    stage1Prompt,
    filename,
    opts.structured ? STAGE1_SCHEMA : undefined,
  );
  // deno-lint-ignore no-explicit-any
  let metadata: any;
  try {
    metadata = JSON.parse(metadataJson);
  } catch {
    throw new Error(
      `LLM returned invalid JSON for "${filename}": ${
        metadataJson.slice(0, 200)
      }`,
    );
  }

  // Stage 2: JSON → save path. The canonical prompt has /home/keeb/media
  // hardcoded; replace with the configured mediaRoot.
  const pathPrompt = prompts.savePath.replaceAll("/home/keeb/media", mediaRoot);
  let stage2Input = metadataJson;
  if (opts.catalogDirs?.length) {
    const near = similarCatalogDirs(
      `${metadata?.title ?? ""} ${filename}`,
      opts.catalogDirs,
    ).filter((s) => s.score >= 0.5);
    if (near.length) {
      stage2Input +=
        `\n\nExisting catalog directories that may be this same show. ` +
        `If one is this show, use it (append /s{season} as usual) instead of a new name:\n` +
        near.map((s) => `- ${s.dir}`).join("\n");
    }
  }
  const destPath = sanitizeDestPath(
    (await ollamaGenerate(ollamaUrl, ollamaModel, pathPrompt, stage2Input))
      .trim(),
    mediaRoot,
  );
  return { metadata, destPath, route: "llm" };
}

/**
 * Last line of defense on an LLM-generated path: strip characters the slug
 * rules forbid (`!?` etc.) from every directory segment under the media root.
 */
export function sanitizeDestPath(destPath: string, mediaRoot: string): string {
  const root = mediaRoot.replace(/\/$/, "");
  if (!destPath.startsWith(root)) return destPath;
  const rest = destPath.slice(root.length).split("/").map((seg) =>
    seg.replace(/[!?:,~'"()[\]]/g, "").replace(/-+/g, "-").replace(
      /^-|-$/g,
      "",
    )
  );
  return root + rest.join("/");
}

// --- Helpers ---

/**
 * Fingerprint of everything that decides a placement: special cases, anime
 * release groups, the gate rubric, prompts and the model. A job left in
 * staging is re-planned as soon as this changes.
 */
export async function rulesHash(
  prompts: Prompts,
  ollamaModel: string,
): Promise<string> {
  const material = JSON.stringify({
    special: SPECIAL_CASES.map((r) => ({ ...r, match: String(r.match) })),
    groups: [...ANIME_RELEASE_GROUPS].sort(),
    judge: JUDGE_QUESTIONS,
    scene: SCENE_ROUTE_VERSION,
    prompts,
    ollamaModel,
  });
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(material),
  );
  return [...new Uint8Array(digest)].slice(0, 8)
    .map((b) => b.toString(16).padStart(2, "0")).join("");
}

/** The three organizer prompts, as bundled with the extension. */
// deno-lint-ignore no-explicit-any
async function loadPrompts(context: any): Promise<Prompts> {
  const read = (f: string) =>
    Deno.readTextFile(context.extensionFile(`prompts/${f}`));
  return {
    filename: await read("filename-to-json.md"),
    anime: await read("anime-filename-to-json.md"),
    savePath: await read("json-to-save-path.md"),
  };
}

/**
 * Pending jobs to plan, oldest first. A job apply left in staging is only
 * re-planned when the rules changed since (a new Special Case may place it)
 * or its retry window passed.
 */
async function selectPending(
  // deno-lint-ignore no-explicit-any
  jobs: any,
  hash: string,
  retryHours: number,
  maxJobs: number,
  // deno-lint-ignore no-explicit-any
): Promise<any[]> {
  const retryCutoff = new Date(Date.now() - retryHours * 3600_000);
  return await jobs
    .find({
      status: { $in: ["pending", "queued", null] },
      $or: [
        { held_at: { $exists: false } },
        { held_rules: { $ne: hash } },
        { held_at: { $lt: retryCutoff } },
      ],
    })
    .sort({ completed_at: 1 })
    .limit(maxJobs)
    .toArray();
}

/** The video files a staging job covers: the file itself, or a folder's videos. */
async function resolveJobFiles(
  stagingDir: string,
  name: string,
): Promise<{ path: string; name: string }[]> {
  const sourcePath = `${stagingDir.replace(/\/$/, "")}/${name}`;
  let stat: Deno.FileInfo;
  try {
    stat = await Deno.stat(sourcePath);
  } catch {
    throw new Error(`Source not found: ${sourcePath}`);
  }
  if (!stat.isDirectory) return [{ path: sourcePath, name }];
  const files: { path: string; name: string }[] = [];
  for await (const entry of walkDir(sourcePath)) {
    if (isVideoFile(entry.name)) files.push(entry);
  }
  return files.sort((a, b) => a.name.localeCompare(b.name));
}

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
