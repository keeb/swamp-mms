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
  version: "2026.04.27.1",
  globalArguments: GlobalArgsSchema,
  resources: {
    extraction: {
      description: "Subtitle extraction result",
      schema: ExtractionSchema,
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
        const { path, language, overwrite } = args;

        const stat = await Deno.stat(path);
        if (!stat.isFile) {
          throw new Error(`Not a file: ${path}`);
        }
        if (!path.toLowerCase().endsWith(".mkv")) {
          throw new Error(`Expected .mkv, got: ${path}`);
        }

        const output = path.replace(/\.mkv$/i, ".srt");

        // Idempotent: skip if .srt already exists unless overwriting
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
          const codecs = [...new Set(streams.map((s) => s.codec_name))].join(
            ", ",
          );
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
          const handle = await context.writeResource(
            "extraction",
            instanceName(path),
            {
              source: path,
              output,
              trackIndex,
              language: lang,
              codec,
              status: "skipped" as const,
              processedAt: new Date().toISOString(),
            },
          );
          return { dataHandles: [handle] };
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

        const handle = await context.writeResource(
          "extraction",
          instanceName(path),
          {
            source: path,
            output,
            trackIndex,
            language: lang,
            codec,
            status: "extracted" as const,
            processedAt: new Date().toISOString(),
          },
        );
        return { dataHandles: [handle] };
      },
    },
  },
};

// Stable instance name keyed on the source path so re-extractions update the
// same resource rather than creating new instances per run.
function instanceName(path: string): string {
  return path
    .replace(/^\/+/, "")
    .replace(/[^a-zA-Z0-9]+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 200);
}
