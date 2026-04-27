/**
 * `@keeb/mms/dedup` model type — filter discovered episodes against the
 * local filesystem so only episodes that aren't already on disk are kept.
 * Uses regex title parsing first and falls back to an Ollama call only when
 * regex can't extract show/episode.
 */
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

async function fileExistsForEpisode(
  show: string,
  episode: string,
  season: string | undefined,
  dirs: string[],
  // deno-lint-ignore no-explicit-any
  logger: any,
): Promise<boolean> {
  const epPadded = episode.replace(/^0+/, "").padStart(2, "0");
  const normalizedShow = normalize(show);
  const showSlug = slugDir(show);
  const epPattern = new RegExp(`[\\s\\-_\\.E]0?${epPadded}[\\s\\.\\[\\(v\\-]`);

  for (const dir of dirs) {
    // Check organized season dirs: {dir}/{show-slug}/s{season}/*
    if (season) {
      try {
        const seasonPath = `${dir}/${showSlug}/s${season}`;
        for await (const file of Deno.readDir(seasonPath)) {
          if (file.isDirectory) continue;
          if (epPattern.test(file.name)) {
            logger.info(`Found: ${seasonPath}/${file.name}`);
            return true;
          }
        }
      } catch { /* doesn't exist */ }
    }

    // Check all season subdirs: {dir}/{show-slug}/s*/*
    try {
      const showDir = `${dir}/${showSlug}`;
      for await (const seasonEntry of Deno.readDir(showDir)) {
        if (!seasonEntry.isDirectory) continue;
        const seasonPath = `${showDir}/${seasonEntry.name}`;
        for await (const file of Deno.readDir(seasonPath)) {
          if (file.isDirectory) continue;
          if (
            epPattern.test(file.name) &&
            normalize(file.name).includes(normalizedShow)
          ) {
            logger.info(`Found: ${seasonPath}/${file.name}`);
            return true;
          }
        }
      }
    } catch { /* doesn't exist */ }

    // Check flat show dir: {dir}/{show-slug}/*
    try {
      const showDir = `${dir}/${showSlug}`;
      for await (const file of Deno.readDir(showDir)) {
        if (file.isDirectory) continue;
        if (epPattern.test(file.name)) {
          logger.info(`Found: ${showDir}/${file.name}`);
          return true;
        }
      }
    } catch { /* doesn't exist */ }

    // Check flat staging: {dir}/* — match show name + episode in filename
    try {
      for await (const file of Deno.readDir(dir)) {
        if (file.isDirectory) continue;
        if (normalizedShow && normalize(file.name).includes(normalizedShow)) {
          if (epPattern.test(file.name)) {
            logger.info(`Found in staging: ${dir}/${file.name}`);
            return true;
          }
        }
      }
    } catch { /* doesn't exist */ }
  }

  return false;
}

// --- Model ---

/** Swamp model definition for `@keeb/mms/dedup`. */
export const model = {
  type: "@keeb/mms/dedup",
  version: "2026.03.29.1",
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
  ],
  resources: {
    episode: {
      description: "Episode that does not exist locally (new, actionable)",
      schema: EpisodeSchema,
      lifetime: "infinite" as const,
      garbageCollection: 50,
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

        // Read source model data using first-class API
        const items = await context.readModelData(args.sourceModel, "episode");
        if (items.length === 0) {
          context.logger.info(`No data found for "${args.sourceModel}"`);
          return { dataHandles: [] };
        }

        context.logger.info(
          `Read ${items.length} episodes from "${args.sourceModel}"`,
        );

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

          // If no episode field, try regex first, then LLM as fallback
          if (!episode && data.rawTitle) {
            const regexParsed = regexParseTitle(data.rawTitle);
            if (regexParsed) {
              show = regexParsed.show;
              episode = regexParsed.episode;
              season = regexParsed.season;
              regexParsedCount++;
              context.logger.info(
                `Regex: "${data.rawTitle.slice(0, 60)}" → ${show} s${
                  season ?? "?"
                } ep ${episode}`,
              );
            } else {
              // Fall back to LLM for titles regex can't handle
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

          const exists = await fileExistsForEpisode(
            show,
            episode,
            season,
            checkDirs,
            context.logger,
          );

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

        // Write only the deduplicated winners
        // deno-lint-ignore no-explicit-any
        const finalHandles: any[] = [];
        for (const h of byEpisode.values()) {
          const instanceName = slugDir(
            `${h._provider}-${h._show}-s${h._season ?? "0"}-e${h._episode}`,
          ).slice(0, 80);
          const handle = await context.writeResource(
            "episode",
            instanceName,
            h._data,
          );
          finalHandles.push(handle);
        }

        context.logger.info(
          `Checked ${checked}: ${finalHandles.length} new (${handles.length} before dedup), ${skipped} on disk, ${regexParsedCount} regex-parsed, ${llmParsed} LLM-parsed`,
        );
        return { dataHandles: finalHandles };
      },
    },
  },
};
