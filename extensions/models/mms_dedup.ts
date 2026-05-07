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

const EpisodeSchema = z.object({
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

// --- Regex title parser (fast path, avoids LLM) ---

function regexParseTitle(
  raw: string,
): { show: string; episode: string; season?: string } | null {
  // S01E06 / S1E6 / s01e06
  const seMatch = raw.match(/^(.+?)[.\s_-]+[Ss](\d{1,2})[Ee](\d{1,3})\b/);
  if (seMatch) {
    return {
      show: seMatch[1].replace(/\./g, " ").trim(),
      season: String(parseInt(seMatch[2])),
      episode: String(parseInt(seMatch[3])),
    };
  }

  // 1x06 / 01x06
  const xMatch = raw.match(/^(.+?)[.\s_-]+(\d{1,2})x(\d{1,3})\b/);
  if (xMatch) {
    return {
      show: xMatch[1].replace(/\./g, " ").trim(),
      season: String(parseInt(xMatch[2])),
      episode: String(parseInt(xMatch[3])),
    };
  }

  // Anime style: [Group] Title - 06 [1080p] or Title - 1106
  // Handles: space-dash-space, space-dash-number, various digit lengths
  const animeMatch = raw.match(/^(?:\[.*?\]\s*)?(.+?)\s+-\s*(\d{2,4})\b/);
  if (animeMatch) {
    return {
      show: animeMatch[1].trim(),
      episode: String(parseInt(animeMatch[2])),
    };
  }

  // Bare: Title 1106 [quality] or Title.1106.stuff (no dash, common in Nyaa)
  const bareMatch = raw.match(/^(?:\[.*?\]\s*)?(.+?)\s+(\d{3,4})\s*[\[\(\.]/);
  if (bareMatch) {
    return {
      show: bareMatch[1].trim(),
      episode: String(parseInt(bareMatch[2])),
    };
  }

  return null;
}

// --- Filesystem helpers ---

function normalize(s: string): string {
  return s.toLowerCase().replace(/[^a-z0-9]/g, "");
}

function slugDir(s: string): string {
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

function fileExistsInIndex(
  show: string,
  episode: string,
  fileIndex: { name: string; normalized: string; path: string }[],
  // deno-lint-ignore no-explicit-any
  logger: any,
): boolean {
  const epPadded = episode.replace(/^0+/, "").padStart(2, "0");
  const normalizedShow = normalize(show);
  const epPattern = new RegExp(`[\\s\\-_\\.E]0?${epPadded}[\\s\\.\\[\\(v\\-]`);

  for (const file of fileIndex) {
    if (file.normalized.includes(normalizedShow) && epPattern.test(file.name)) {
      logger.info(`Found: ${file.path}`);
      return true;
    }
  }
  return false;
}

// --- Model ---

export const model = {
  type: "@keeb/mms/dedup",
  version: "2026.04.14.1",
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

        // Load prompt from canonical file
        const FILENAME_TO_JSON_PROMPT = await Deno.readTextFile(
          `${context.repoDir}/prompts/filename-to-json.prompt`,
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
        let regexParsedCount = 0;

        for (const item of items) {
          const data = item.attributes;
          if (!data.magnet) continue;

          let show = data.show;
          let episode = data.episode;
          let season = data.season;
          const isSubsPlease = data.provider === "subsplease";

          // Nyaa items: always extract show/season/episode from rawTitle
          // SubsPlease items: already have show+episode, skip parsing
          if (!isSubsPlease && data.rawTitle) {
            // Try regex first
            const regexParsed = regexParseTitle(data.rawTitle);
            if (regexParsed && regexParsed.season) {
              show = regexParsed.show;
              episode = regexParsed.episode;
              season = regexParsed.season;
              regexParsedCount++;
              context.logger.info(
                `Regex: "${
                  data.rawTitle.slice(0, 60)
                }" → ${show} s${season} ep ${episode}`,
              );
            } else {
              // LLM for full extraction (show, season, episode)
              try {
                const jsonStr = await ollamaGenerate(
                  ollamaUrl,
                  ollamaModel,
                  FILENAME_TO_JSON_PROMPT,
                  data.rawTitle,
                );
                const parsed = JSON.parse(jsonStr);
                if (parsed.title && parsed.episode != null) {
                  show = parsed.title;
                  episode = String(parsed.episode);
                  season = parsed.season != null
                    ? String(parsed.season)
                    : undefined;
                  llmParsed++;
                  context.logger.info(
                    `LLM: "${data.rawTitle.slice(0, 60)}" → ${show} s${
                      season ?? "?"
                    } ep ${episode}`,
                  );
                } else {
                  context.logger.info(
                    `LLM: incomplete parse for "${data.rawTitle.slice(0, 60)}"`,
                  );
                  continue;
                }
              } catch (err) {
                context.logger.info(
                  `LLM: failed for "${data.rawTitle?.slice(0, 60)}": ${err}`,
                );
                continue;
              }
            }
          }

          if (!show || !episode) continue;
          checked++;

          let exists: boolean;
          if (isSubsPlease) {
            // Fast path: exact filename match in crawled index
            exists = fileExistsInIndex(
              show,
              episode,
              fileIndex,
              context.logger,
            );
          } else {
            // Structured path: {dir}/{show-slug}/s{season}/ for parsed Nyaa items
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
        // deno-lint-ignore no-explicit-any
        const byEpisode = new Map<string, any>();
        for (const h of handles) {
          const key = slugDir(`${h._show}-s${h._season ?? "0"}-e${h._episode}`);
          const existing = byEpisode.get(key);
          if (!existing) {
            byEpisode.set(key, h);
          } else {
            // Prefer MeGusta > HEVC > anything
            const newTitle = h._rawTitle ?? "";
            const oldTitle = existing._rawTitle ?? "";
            const newScore = (newTitle.includes("MeGusta") ? 10 : 0) +
              (newTitle.includes("HEVC") || newTitle.includes("x265") ? 5 : 0);
            const oldScore = (oldTitle.includes("MeGusta") ? 10 : 0) +
              (oldTitle.includes("HEVC") || oldTitle.includes("x265") ? 5 : 0);
            if (newScore > oldScore) {
              byEpisode.set(key, h);
            }
          }
        }

        const finalEpisodes = [...byEpisode.values()].map((h) => h._data);

        const handle = await context.writeResource("episodes", "current", {
          episodes: finalEpisodes,
          count: finalEpisodes.length,
          timestamp: new Date().toISOString(),
        });

        context.logger.info(
          `Checked ${checked}: ${finalEpisodes.length} new (${handles.length} before dedup), ${skipped} on disk, ${regexParsedCount} regex-parsed, ${llmParsed} LLM-parsed`,
        );
        return { dataHandles: [handle] };
      },
    },
  },
};
