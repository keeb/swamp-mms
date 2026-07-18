import { z } from "npm:zod@4";
import { ollamaGenerate } from "./_lib/ollama.ts";

// Prompt loaded at runtime from prompts/filename-to-json.prompt via context.repoDir

// --- Schemas ---

const GlobalArgsSchema = z.object({
  checkDirs: z
    .array(z.string())
    .describe("Directories to scan for existing media files"),
  ollamaUrl: z
    .string()
    .default("http://localhost:11434")
    .describe("Ollama API URL for parsing raw titles"),
  ollamaModel: z
    .string()
    .default("qwen3:14b")
    .describe("Ollama model for title parsing"),
});

export const EpisodeSchema = z.object({
  show: z.string().describe("Show name"),
  episode: z.string().describe("Episode number"),
  season: z.string().optional().describe("Season number"),
  magnet: z.string().describe("Download URI"),
  provider: z.string().describe("Source provider"),
  resolution: z.string().describe("Video resolution"),
  protocol: z.enum(["torrent", "usenet"]).optional().describe(
    "Download protocol",
  ),
  infoHash: z.string().optional().describe("Torrent info hash"),
  publishDate: z.string().optional().describe("RSS publish date"),
  rawTitle: z.string().optional().describe("Original unparsed title"),
  size: z.number().optional().describe("File size in bytes"),
});

// Actionable episodes from a single dedup invocation. Single-instance
// resource (name "current") that supersedes on each run. Mirrors the
// @keeb/mms/source `episodes` spec — downstream consumers read this via
// `data.latest("dedup", "episodes")`.
const EpisodesSchema = z.object({
  episodes: z.array(EpisodeSchema).describe(
    "Actionable episodes from this run (post disk-check, post HEVC/MeGusta dedup)",
  ),
  count: z.number().describe("Number of actionable episodes"),
  timestamp: z.string().describe("ISO timestamp when the run finished"),
});

// --- Filesystem helpers ---

export function normalize(s: string): string {
  return s.toLowerCase().replace(/[^a-z0-9]/g, "");
}

// Episode strings come from RSS titles and LLM output — schema-valid values
// can contain regex metacharacters, which would blow up the interpolated
// patterns below (e.g. episode "(" → "Unterminated group").
export function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export function slugDir(s: string): string {
  return s.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
}

async function buildFileIndex(
  dirs: string[],
  // deno-lint-ignore no-explicit-any
  logger: any,
): Promise<{ name: string; normalized: string; path: string }[]> {
  const files: { name: string; normalized: string; path: string }[] = [];

  async function crawl(dir: string) {
    try {
      for await (const entry of Deno.readDir(dir)) {
        const fullPath = `${dir}/${entry.name}`;
        if (entry.isDirectory) {
          await crawl(fullPath);
        } else {
          files.push({
            name: entry.name,
            normalized: normalize(entry.name),
            path: fullPath,
          });
        }
      }
    } catch { /* doesn't exist */ }
  }

  for (const dir of dirs) {
    await crawl(dir);
  }
  logger.info(`File index: ${files.length} files across ${dirs.length} dirs`);
  return files;
}

export function fileExistsInIndex(
  show: string,
  episode: string,
  fileIndex: { name: string; normalized: string; path: string }[],
  // deno-lint-ignore no-explicit-any
  logger: any,
): boolean {
  const epPadded = episode.replace(/^0+/, "").padStart(2, "0");
  const normalizedShow = normalize(show);
  const epPattern = new RegExp(
    `[\\s\\-_\\.E]0?${escapeRegex(epPadded)}[\\s\\.\\[\\(v\\-]`,
  );

  for (const file of fileIndex) {
    if (file.normalized.includes(normalizedShow) && epPattern.test(file.name)) {
      logger.info(`Found: ${file.path}`);
      return true;
    }
  }
  return false;
}

export function fileExistsInDest(
  destPath: string,
  season: string | undefined,
  episode: string,
  fileIndex: { name: string; normalized: string; path: string }[],
  // deno-lint-ignore no-explicit-any
  logger: any,
): boolean {
  // Match by show slug. Accept the file via either of two anchors:
  //   1. path contains /<slug>/ — the canonical organized location.
  //   2. normalized filename contains normalize(slug) — catches files
  //      sitting in flat dirs (e.g. staging/) before the organizer has
  //      moved them under their slug directory.
  // The SxxExx episode pattern then guards against cross-show false
  // positives. Without the filename fallback, anything in staging is
  // invisible to dedup and gets re-downloaded on the next discover cycle.
  const showRoot = destPath.replace(/\/s\d+\/?$/, "").replace(/\/$/, "");
  const slug = showRoot.substring(showRoot.lastIndexOf("/") + 1);
  if (!slug) return false;
  const slugMarker = `/${slug}/`;
  const normalizedSlug = normalize(slug);

  const epPadded = episode.replace(/^0+/, "").padStart(2, "0");
  const seasonPadded = season
    ? String(parseInt(season)).padStart(2, "0")
    : null;
  const sePattern = seasonPadded
    ? new RegExp(`[Ss]${seasonPadded}[Ee]${escapeRegex(epPadded)}\\b`)
    : null;
  const epPattern = new RegExp(
    `[\\s\\-_\\.E]0?${escapeRegex(epPadded)}[\\s\\.\\[\\(v\\-]`,
  );

  for (const file of fileIndex) {
    const dirAnchor = file.path.includes(slugMarker);
    const nameAnchor = file.normalized.includes(normalizedSlug);
    if (!dirAnchor && !nameAnchor) continue;
    if (sePattern?.test(file.name) || epPattern.test(file.name)) {
      logger.info(`Found: ${file.path}`);
      return true;
    }
  }
  return false;
}

// Keep one candidate per show+season+episode. Preference order:
// MeGusta > HEVC/x265 > first seen.
export function pickPreferred<
  T extends { _show: string; _season?: string; _episode: string; _rawTitle?: string },
>(handles: T[]): T[] {
  const releaseScore = (title: string) =>
    (title.includes("MeGusta") ? 10 : 0) +
    (title.includes("HEVC") || title.includes("x265") ? 5 : 0);

  const byEpisode = new Map<string, T>();
  for (const h of handles) {
    const key = slugDir(`${h._show}-s${h._season ?? "0"}-e${h._episode}`);
    const existing = byEpisode.get(key);
    if (
      !existing ||
      releaseScore(h._rawTitle ?? "") > releaseScore(existing._rawTitle ?? "")
    ) {
      byEpisode.set(key, h);
    }
  }
  return [...byEpisode.values()];
}

// --- Model ---

export const model = {
  type: "@keeb/mms/dedup",
  version: "2026.05.08.1",
  reports: ["@keeb/mms/dedup-summary"],
  globalArguments: GlobalArgsSchema,
  upgrades: [
    {
      fromVersion: "2026.03.28.1",
      toVersion: "2026.03.29.1",
      description:
        "Add Ollama integration for parsing raw titles, add ollamaUrl/ollamaModel globalArgs",
      // deno-lint-ignore no-explicit-any
      upgradeAttributes: (old: any) => ({
        ...old,
        ollamaUrl: old.ollamaUrl ?? "http://localhost:11434",
        ollamaModel: old.ollamaModel ?? "qwen3:14b",
      }),
    },
    {
      fromVersion: "2026.03.29.1",
      toVersion: "2026.04.08.1",
      description:
        "Add `batch` resource spec — single 'current' instance written per " +
        "invocation containing the actionable episodes. Lets workflow forEach " +
        "iterate this run's results without filtering by workflowRunId.",
      // deno-lint-ignore no-explicit-any
      upgradeAttributes: (old: any) => old,
    },
    {
      fromVersion: "2026.04.08.1",
      toVersion: "2026.04.14.1",
      description:
        "Drop per-episode factory resources — dedup output is pipeline " +
        "output, not a set of durable entities. The aggregate resource " +
        "(renamed from `batch` → `episodes`) is the sole output. Downstream " +
        'consumers now query `data.latest("dedup", "episodes")`. Orphaned ' +
        "`episode` and `batch` records from prior versions can be purged " +
        "with `swamp data gc`.",
      // deno-lint-ignore no-explicit-any
      upgradeAttributes: (old: any) => old,
    },
    {
      fromVersion: "2026.04.14.1",
      toVersion: "2026.05.08.1",
      description:
        "Honor optional `slug` on input episodes (added in @keeb/mms/source " +
        "2026.05.08.1). When present, override the LLM-parsed title with " +
        "the slug and synthesize destPath instead of running LLM stage 2. " +
        "Fixes mismatch where the torrent's display title and the catalog " +
        "directory diverge (e.g. Judas releases).",
      // deno-lint-ignore no-explicit-any
      upgradeAttributes: (old: any) => old,
    },
  ],
  resources: {
    episodes: {
      description:
        "All actionable episodes from the most recent invocation. Single " +
        "instance named 'current' that supersedes on each run — " +
        '`data.latest("dedup", "episodes")` always returns this run\'s ' +
        "results and only this run's results.",
      schema: EpisodesSchema,
      lifetime: "infinite" as const,
      garbageCollection: 5,
    },
  },
  methods: {
    filter: {
      description:
        "Read episode data from a source model, check local directories, output only new episodes. Parses raw titles with Ollama when show/episode fields are missing.",
      arguments: z.object({
        sourceModel: z
          .string()
          .describe("Name of the model whose episode data to filter"),
      }),
      // deno-lint-ignore no-explicit-any
      execute: async (args: { sourceModel: string }, context: any) => {
        const { checkDirs, ollamaUrl, ollamaModel } = context.globalArgs;

        // Load prompts from canonical files. Same two-stage pipeline as
        // mms_organizer: stage 1 normalizes the raw title into structured JSON,
        // stage 2 turns that JSON into the canonical destPath the organizer
        // would write to. Dedup checks disk against that destPath, so a
        // skip/new decision is consistent with where the organizer would
        // actually place the file.
        const FILENAME_TO_JSON_PROMPT = await Deno.readTextFile(
          `${context.repoDir}/prompts/filename-to-json.prompt`,
        );
        const JSON_TO_SAVE_PATH_PROMPT = await Deno.readTextFile(
          `${context.repoDir}/prompts/json-to-save-path.prompt`,
        );

        // Read the source model's latest run — a single aggregate record
        // containing every item from the most recent invocation, superseded
        // on each run. This gives us "exactly the source's most recent
        // results," not accumulated catalog state.
        //
        // The `has(attributes.episodes)` clause forces the catalog to lazy-load
        // the JSON content (the query service skips parsing unless the predicate
        // references `attributes`).
        const records = await context.queryData(
          `modelName == "${args.sourceModel}" && specName == "episodes" && has(attributes.episodes)`,
        );
        if (records.length === 0) {
          context.logger.info(
            `No episodes resource found for "${args.sourceModel}" — has it been run yet?`,
          );
          return { dataHandles: [] };
        }
        // deno-lint-ignore no-explicit-any
        const items = ((records[0].attributes.episodes as any[]) ?? []).map((
          // deno-lint-ignore no-explicit-any
          ep: any,
        ) => ({ attributes: ep }));
        if (items.length === 0) {
          context.logger.info(`No data found for "${args.sourceModel}"`);
          return { dataHandles: [] };
        }

        context.logger.info(
          `Read ${items.length} episodes from "${args.sourceModel}"`,
        );

        // Build file index once
        const fileIndex = await buildFileIndex(checkDirs, context.logger);

        // deno-lint-ignore no-explicit-any
        const handles: any[] = [];
        let checked = 0;
        let skipped = 0;
        let llmParsed = 0;

        for (const item of items) {
          const data = item.attributes;
          if (!data.magnet) continue;

          let show = data.show;
          let episode = data.episode;
          let season = data.season;
          let destPath: string | undefined;
          const isSubsPlease = data.provider === "subsplease";

          // Nyaa items: always run the LLM pipeline (filename → JSON →
          // destPath). Regex on raw release titles is whack-a-mole: every new
          // release group invents a new prefix/alias/year/parenthetical
          // convention. The LLM owns canonicalization in one place (the two
          // prompts), inheriting any special cases already encoded there.
          // SubsPlease items skip parsing because the source RSS already
          // gives clean show+episode fields.
          if (!isSubsPlease && data.rawTitle) {
            try {
              const jsonStr = await ollamaGenerate(
                ollamaUrl,
                ollamaModel,
                FILENAME_TO_JSON_PROMPT,
                data.rawTitle,
              );
              const parsed = JSON.parse(jsonStr);
              if (!parsed.title || parsed.episode == null) {
                context.logger.info(
                  `LLM stage 1: incomplete parse for "${
                    data.rawTitle.slice(0, 60)
                  }" — jsonStr=[${jsonStr}] parsed=${JSON.stringify(parsed)}`,
                );
                continue;
              }
              show = parsed.title;
              episode = String(parsed.episode);
              season = parsed.season != null
                ? String(parsed.season)
                : undefined;
              if (data.slug) {
                // Config supplied a canonical slug — skip LLM stage 2 and
                // synthesize destPath directly. fileExistsInDest only uses
                // the trailing /<slug>/sN component, so the prefix is
                // immaterial. Keeps dedup matching anchored on the same
                // slug the catalog actually uses.
                show = data.slug;
                destPath = season
                  ? `/${data.slug}/s${parseInt(season)}`
                  : `/${data.slug}`;
                llmParsed++;
                context.logger.info(
                  `LLM+slug: "${data.rawTitle.slice(0, 60)}" → ${data.slug} s${
                    season ?? "?"
                  } ep ${episode} @ ${destPath}`,
                );
              } else {
                destPath = (
                  await ollamaGenerate(
                    ollamaUrl,
                    ollamaModel,
                    JSON_TO_SAVE_PATH_PROMPT,
                    jsonStr,
                  )
                ).trim();
                llmParsed++;
                context.logger.info(
                  `LLM: "${data.rawTitle.slice(0, 60)}" → ${show} s${
                    season ?? "?"
                  } ep ${episode} @ ${destPath}`,
                );
              }
            } catch (err) {
              context.logger.info(
                `LLM: failed for "${data.rawTitle?.slice(0, 60)}": ${err}`,
              );
              continue;
            }
          }

          if (!show || !episode) continue;
          checked++;

          let exists: boolean;
          if (destPath) {
            // Nyaa items: check disk under the canonical destPath the
            // organizer would write to. Consistent with on-disk dir naming.
            exists = fileExistsInDest(
              destPath,
              season,
              episode,
              fileIndex,
              context.logger,
            );
          } else {
            // SubsPlease items: trust source-provided show name; substring
            // match against the file index.
            exists = fileExistsInIndex(
              show,
              episode,
              fileIndex,
              context.logger,
            );
          }

          if (exists) {
            skipped++;
            context.logger.info(`SKIP ${show} s${season ?? "?"} ep ${episode}`);
          } else {
            context.logger.info(`NEW  ${show} s${season ?? "?"} ep ${episode}`);
            handles.push({
              _show: show,
              _episode: episode,
              _season: season,
              _provider: data.provider ?? "unknown",
              _rawTitle: data.rawTitle ?? "",
              _data: {
                show,
                episode,
                season,
                magnet: data.magnet,
                provider: data.provider ?? "unknown",
                resolution: data.resolution ?? "unknown",
                protocol: data.protocol,
                infoHash: data.infoHash,
                publishDate: data.publishDate,
                rawTitle: data.rawTitle,
                size: data.size,
              },
            });
          }
        }

        // Deduplicate new episodes — keep one per show+season+episode (prefer HEVC/MeGusta)
        const finalEpisodes = pickPreferred(handles).map((h) => h._data);

        const handle = await context.writeResource("episodes", "current", {
          episodes: finalEpisodes,
          count: finalEpisodes.length,
          timestamp: new Date().toISOString(),
        });

        context.logger.info(
          `Checked ${checked}: ${finalEpisodes.length} new (${handles.length} before dedup), ${skipped} on disk, ${llmParsed} LLM-parsed`,
        );
        return { dataHandles: [handle] };
      },
    },
  },
};
