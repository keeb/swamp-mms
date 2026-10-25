import { z } from "npm:zod@4";

// ffmpeg has to stream the WHOLE container to demux an interleaved subtitle
// track. The catalog lives on a network share, so doing that on the swamp box
// saturates its mount. Instead we run ffprobe/ffmpeg on a remote host that sits
// closer to storage, translating swamp-space paths into that host's mount.
const GlobalArgsSchema = z.object({
  remoteHost: z
    .string()
    .default("keeb@10.0.0.12")
    .describe("ssh target (user@host) that runs ffprobe/ffmpeg"),
  localPrefix: z
    .string()
    .default("/home/keeb/media/video")
    .describe("Catalog path prefix as seen by swamp"),
  remotePrefix: z
    .string()
    .default("/mnt/nami/media/video")
    .describe("Catalog path prefix as mounted on remoteHost"),
});

interface RemoteCfg {
  host: string;
  localPrefix: string;
  remotePrefix: string;
}

function remoteCfg(globalArgs: {
  remoteHost: string;
  localPrefix: string;
  remotePrefix: string;
}): RemoteCfg {
  return {
    host: globalArgs.remoteHost,
    localPrefix: globalArgs.localPrefix.replace(/\/$/, ""),
    remotePrefix: globalArgs.remotePrefix.replace(/\/$/, ""),
  };
}

// Map a swamp-space path onto the remote host's mount. Paths outside the
// catalog prefix pass through unchanged.
function toRemote(cfg: RemoteCfg, path: string): string {
  return path.startsWith(cfg.localPrefix)
    ? cfg.remotePrefix + path.slice(cfg.localPrefix.length)
    : path;
}

// Single-quote an argument for the remote login shell. ssh joins its command
// args with spaces and re-parses them remotely, so filenames with spaces,
// brackets and parens must be quoted or they get word-split.
function shQuote(s: string): string {
  return `'${s.replaceAll("'", `'\\''`)}'`;
}

// Multiplex every call in a batch over one ssh connection so we pay the
// handshake once, not per file/per command.
const SSH_OPTS = [
  "-o",
  "BatchMode=yes",
  "-o",
  "ConnectTimeout=10",
  "-o",
  "ControlMaster=auto",
  "-o",
  "ControlPath=/tmp/swamp-subs-ssh-%r@%h:%p",
  "-o",
  "ControlPersist=60",
];

async function remoteRun(
  cfg: RemoteCfg,
  argv: string[],
): Promise<{ stdout: string; stderr: string; code: number }> {
  const remoteCmd = argv.map(shQuote).join(" ");
  return await runCmd("ssh", [...SSH_OPTS, cfg.host, remoteCmd]);
}

// ffprobe/ffmpeg run on a DIFFERENT host than the one that wrote the file, so
// right after the organizer moves an MKV onto the share the remote's CIFS
// client can briefly see the final size but not-yet-coherent data (EBML/EOF
// errors). Retry the command a few times before treating a nonzero exit as a
// real failure. "No text subs" is a logic throw AFTER a successful probe, so it
// never reaches here — only genuine command failures get retried.
async function remoteRunRetry(
  cfg: RemoteCfg,
  argv: string[],
  attempts = 3,
  delayMs = 3000,
): Promise<{ stdout: string; stderr: string; code: number }> {
  let last = await remoteRun(cfg, argv);
  for (let i = 1; i < attempts && last.code !== 0; i++) {
    await new Promise((r) => setTimeout(r, delayMs));
    last = await remoteRun(cfg, argv);
  }
  return last;
}

async function remoteFileExists(cfg: RemoteCfg, path: string): Promise<boolean> {
  const { code } = await remoteRun(cfg, ["test", "-f", path]);
  return code === 0;
}

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

async function probeSubtitles(
  cfg: RemoteCfg,
  remotePath: string,
): Promise<SubtitleStream[]> {
  const { stdout, stderr, code } = await remoteRunRetry(cfg, [
    "ffprobe",
    "-v",
    "error",
    "-select_streams",
    "s",
    "-show_entries",
    "stream=index,codec_name:stream_tags=language,title:stream_disposition=forced,hearing_impaired",
    "-of",
    "json",
    remotePath,
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
  version: "2026.07.20.2",
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
        const cfg = remoteCfg(context.globalArgs);
        const result = await extractOne(
          cfg,
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
        const cfg = remoteCfg(context.globalArgs);
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
              cfg,
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

// `localPath` is a swamp-space path; every filesystem touch happens on the
// remote host under the mapped path. The resource we return records the
// swamp-space paths so the data model stays in one coordinate system.
async function extractOne(
  cfg: RemoteCfg,
  localPath: string,
  language: string,
  overwrite: boolean,
  // deno-lint-ignore no-explicit-any
  context: any,
  // deno-lint-ignore no-explicit-any
): Promise<any> {
  if (!localPath.toLowerCase().endsWith(".mkv")) {
    throw new Error(`Expected .mkv, got: ${localPath}`);
  }

  const remotePath = toRemote(cfg, localPath);
  const localOutput = localPath.replace(/\.mkv$/i, ".srt");
  const remoteOutput = toRemote(cfg, localOutput);

  const exists = await remoteFileExists(cfg, remoteOutput);

  // ffprobe fails on a missing/unreadable file, so it doubles as the source
  // existence check — no extra round trip needed.
  const streams = await probeSubtitles(cfg, remotePath);
  if (streams.length === 0) {
    throw new Error(`No subtitle streams found in ${localPath}`);
  }
  const chosen = pickStream(streams, language);
  if (!chosen) {
    const codecs = [...new Set(streams.map((s) => s.codec_name))].join(", ");
    throw new Error(
      `No text subtitle stream in ${localPath} (found: ${codecs}). ` +
        `Bitmap subs (PGS/VobSub/DVB) require OCR and aren't supported.`,
    );
  }

  const trackIndex = chosen.index;
  const codec = chosen.codec_name;
  const lang = chosen.tags?.language;

  if (exists && !overwrite) {
    context.logger.info(`Skipping (exists): ${localOutput}`);
    return {
      source: localPath,
      output: localOutput,
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
    }) on ${cfg.host} → ${localOutput}`,
  );

  const { stderr, code } = await remoteRunRetry(cfg, [
    "ffmpeg",
    "-y",
    "-i",
    remotePath,
    "-map",
    `0:${trackIndex}`,
    "-c:s",
    "srt",
    remoteOutput,
  ]);
  if (code !== 0) {
    throw new Error(`ffmpeg failed (${code}): ${stderr}`);
  }

  return {
    source: localPath,
    output: localOutput,
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
