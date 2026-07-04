import { z } from "npm:zod@4";

const GlobalArgsSchema = z.object({});

const ExtractionSchema = z.object({
  source: z.string().describe("Input MKV path"),
  output: z.string().describe("Output SRT path"),
  trackIndex: z.number().describe("Subtitle stream index within the MKV"),
  language: z.string().optional().describe("Language tag of the chosen track"),
  codec: z.string().describe("Source subtitle codec (e.g. subrip, ass)"),
  status: z.enum(["extracted", "skipped"]).describe("Result status"),
  processedAt: z.iso.datetime(),
});

type Extraction = z.infer<typeof ExtractionSchema>;

// One resource per extract_recent run instead of one per .srt. Keyed on a
// stable instance name so each batch versions the same resource — the version
// history is the run history.
const RunSchema = z.object({
  cutoff: z.iso.datetime().describe("Lower bound of the organizer-job window"),
  windowMinutes: z.number().describe("Window size used for this run"),
  jobs: z.number().describe("Organized jobs considered"),
  extracted: z.number().describe("Tracks newly extracted to .srt"),
  skipped: z.number().describe(
    "Files skipped (existing .srt, overwrite=false)",
  ),
  failed: z.number().describe(
    "Files that errored (no subs, bitmap-only, etc.)",
  ),
  results: z.array(ExtractionSchema).describe("Per-file extraction results"),
  failures: z
    .array(z.object({ source: z.string(), error: z.string() }))
    .describe("Per-file failures, captured rather than aborting the batch"),
  ranAt: z.iso.datetime(),
});

interface SubtitleStream {
  index: number;
  codec_name: string;
  tags?: { language?: string; title?: string };
  disposition?: { forced?: number; hearing_impaired?: number };
}

// Text-based subtitle codecs that ffmpeg can transcode to SRT directly.
// PGS/VobSub/DVB are bitmap formats and require OCR — out of scope here.
const TEXT_CODECS = new Set([
  "subrip",
  "srt",
  "ass",
  "ssa",
  "mov_text",
  "webvtt",
  "text",
]);

async function runCmd(
  cmd: string,
  args: string[],
): Promise<{ stdout: string; stderr: string; code: number }> {
  const proc = new Deno.Command(cmd, {
    args,
    stdout: "piped",
    stderr: "piped",
  });
  const out = await proc.output();
  return {
    stdout: new TextDecoder().decode(out.stdout),
    stderr: new TextDecoder().decode(out.stderr),
    code: out.code,
  };
}

async function probeSubtitles(path: string): Promise<SubtitleStream[]> {
  const { stdout, stderr, code } = await runCmd("ffprobe", [
    "-v",
    "error",
    "-select_streams",
    "s",
    "-show_entries",
    "stream=index,codec_name:stream_tags=language,title:stream_disposition=forced,hearing_impaired",
    "-of",
    "json",
    path,
  ]);
  if (code !== 0) {
    throw new Error(`ffprobe failed (${code}): ${stderr}`);
  }
  const parsed = JSON.parse(stdout);
  return parsed.streams ?? [];
}

function pickStream(
  streams: SubtitleStream[],
  language: string,
): SubtitleStream | null {
  const text = streams.filter((s) => TEXT_CODECS.has(s.codec_name));
  if (text.length === 0) return null;

  const isClean = (s: SubtitleStream) =>
    !s.disposition?.forced && !s.disposition?.hearing_impaired;
  const langMatch = (s: SubtitleStream) =>
    (s.tags?.language ?? "").toLowerCase() === language.toLowerCase();

  return (
    text.find((s) => langMatch(s) && isClean(s)) ??
      text.find(langMatch) ??
      text.find(isClean) ??
      text[0]
  );
}

export const model = {
  type: "@keeb/mms/subtitles",
  version: "2026.07.03.1",
  globalArguments: GlobalArgsSchema,
  resources: {
    // Single-file `extract` writes one of these (deliberate, manual). The batch
    // `extract_recent` does NOT — it rolls every file into one `run` resource
    // instead of spamming one per .srt.
    extraction: {
      description: "Subtitle extraction result (single-file extract)",
      schema: ExtractionSchema,
      lifetime: "infinite" as const,
      garbageCollection: 50,
    },
    run: {
      description: "Batch subtitle-extraction run summary",
      schema: RunSchema,
      lifetime: "infinite" as const,
      garbageCollection: 50,
    },
  },
  methods: {
    extract: {
      description:
        "Extract an embedded subtitle track from an MKV to a sibling .srt file",
      arguments: z.object({
        path: z.string().describe("Path to the input MKV file"),
        language: z
          .string()
          .default("eng")
          .describe("Preferred subtitle language tag (ISO 639-2)"),
        overwrite: z
          .boolean()
          .default(false)
          .describe("Overwrite an existing .srt next to the source"),
      }),
      execute: async (
        args: { path: string; language: string; overwrite: boolean },
        // deno-lint-ignore no-explicit-any
        context: any,
      ) => {
        const result = await extractOne(
          args.path,
          args.language,
          args.overwrite,
          context,
        );
        const handle = await context.writeResource(
          "extraction",
          instanceName(result.source),
          result,
        );
        return { dataHandles: [handle] };
      },
    },
    extract_recent: {
      description:
        "Batch-extract subtitles from MKVs produced by recent organizer jobs",
      arguments: z.object({
        windowMinutes: z
          .number()
          .default(60)
          .describe(
            "Look at organizer jobs processed in the last N minutes",
          ),
        language: z
          .string()
          .default("eng")
          .describe("Preferred subtitle language tag (ISO 639-2)"),
        overwrite: z
          .boolean()
          .default(false)
          .describe("Overwrite an existing .srt next to the source"),
      }),
      execute: async (
        args: {
          windowMinutes: number;
          language: string;
          overwrite: boolean;
        },
        // deno-lint-ignore no-explicit-any
        context: any,
      ) => {
        const cutoff = new Date(
          Date.now() - args.windowMinutes * 60 * 1000,
        ).toISOString();
        const predicate = `modelName == "organizer" && specName == "job" && ` +
          `attributes.status == "processed" && ` +
          `attributes.processedAt > "${cutoff}"`;
        const records = await context.queryData(predicate);
        context.logger.info(
          `Found ${records.length} organized job(s) since ${cutoff}`,
        );

        const results: Extraction[] = [];
        const failures: { source: string; error: string }[] = [];
        const seen = new Set<string>();
        for (const rec of records) {
          // deno-lint-ignore no-explicit-any
          const attrs = (rec as any).attributes ?? {};
          const destPath: string | undefined = attrs.destPath;
          const filename: string | undefined = attrs.filename;
          if (!destPath || !filename) continue;
          if (!filename.toLowerCase().endsWith(".mkv")) continue;
          const fullPath = `${destPath.replace(/\/$/, "")}/${filename}`;
          if (seen.has(fullPath)) continue;
          seen.add(fullPath);

          try {
            const result = await extractOne(
              fullPath,
              args.language,
              args.overwrite,
              context,
            );
            results.push(result);
          } catch (err) {
            // Per-file failures (no subs, bitmap-only, missing file after
            // post-organize cleanup) shouldn't abort the batch.
            context.logger.warn(`Skip ${fullPath}: ${err}`);
            failures.push({ source: fullPath, error: String(err) });
          }
        }
        const extracted =
          results.filter((r) => r.status === "extracted").length;
        const skipped = results.filter((r) => r.status === "skipped").length;
        context.logger.info(
          `Run: ${extracted} extracted, ${skipped} skipped, ` +
            `${failures.length} failed across ${records.length} job(s)`,
        );

        // One rollup resource per run, not one per .srt.
        const handle = await context.writeResource("run", "extract-recent", {
          cutoff,
          windowMinutes: args.windowMinutes,
          jobs: records.length,
          extracted,
          skipped,
          failed: failures.length,
          results,
          failures,
          ranAt: new Date().toISOString(),
        });
        return { dataHandles: [handle] };
      },
    },
  },
};

async function extractOne(
  path: string,
  language: string,
  overwrite: boolean,
  // deno-lint-ignore no-explicit-any
  context: any,
  // deno-lint-ignore no-explicit-any
): Promise<any> {
  const stat = await Deno.stat(path);
  if (!stat.isFile) {
    throw new Error(`Not a file: ${path}`);
  }
  if (!path.toLowerCase().endsWith(".mkv")) {
    throw new Error(`Expected .mkv, got: ${path}`);
  }

  const output = path.replace(/\.mkv$/i, ".srt");

  let exists = false;
  try {
    await Deno.stat(output);
    exists = true;
  } catch (err) {
    if (!(err instanceof Deno.errors.NotFound)) throw err;
  }

  const streams = await probeSubtitles(path);
  if (streams.length === 0) {
    throw new Error(`No subtitle streams found in ${path}`);
  }
  const chosen = pickStream(streams, language);
  if (!chosen) {
    const codecs = [...new Set(streams.map((s) => s.codec_name))].join(", ");
    throw new Error(
      `No text subtitle stream in ${path} (found: ${codecs}). ` +
        `Bitmap subs (PGS/VobSub/DVB) require OCR and aren't supported.`,
    );
  }

  const trackIndex = chosen.index;
  const codec = chosen.codec_name;
  const lang = chosen.tags?.language;

  if (exists && !overwrite) {
    context.logger.info(`Skipping (exists): ${output}`);
    return {
      source: path,
      output,
      trackIndex,
      language: lang,
      codec,
      status: "skipped" as const,
      processedAt: new Date().toISOString(),
    };
  }

  context.logger.info(
    `Extracting stream ${trackIndex} (${codec}, lang=${
      lang ?? "?"
    }) → ${output}`,
  );

  const ffmpegArgs = [
    "-y",
    "-i",
    path,
    "-map",
    `0:${trackIndex}`,
    "-c:s",
    "srt",
    output,
  ];
  const { stderr, code } = await runCmd("ffmpeg", ffmpegArgs);
  if (code !== 0) {
    throw new Error(`ffmpeg failed (${code}): ${stderr}`);
  }

  return {
    source: path,
    output,
    trackIndex,
    language: lang,
    codec,
    status: "extracted" as const,
    processedAt: new Date().toISOString(),
  };
}

// Stable instance name keyed on the source path so re-extractions update the
// same resource rather than creating new instances per run.
function instanceName(path: string): string {
  return path
    .replace(/^\/+/, "")
    .replace(/[^a-zA-Z0-9]+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 200);
}
