import { z } from "npm:zod@4";

// --- Embedded filename-to-json prompt ---

const FILENAME_TO_JSON_PROMPT = `# Media Filename Parser

Parse TV show, anime, and movie filenames into structured data.

## Input Format
Filename (with or without extension)

## Output Format
Return JSON with these fields:
- \`media_type\`: "tv_show", "anime", "movie", or "book"
- \`title\`: Clean title (spaces, proper capitalization). This is the SHOW NAME ONLY — never include episode titles, quality info, or release group.
- \`season\`: Season number (integer, null for movies/books)
- \`episode\`: Episode number (integer, null for movies/books)
- \`episode_title\`: Episode title if present (null if not found)
- \`year\`: Release year if present (integer, null if not found)
- \`confidence\`: High/Medium/Low based on pattern match quality

## Detection Rules
**Movie**: Single file, often has year, no season/episode markers
**Anime**: Japanese titles, episode ranges like "01-12", "OVA", "Special"
**TV Show**: Standard S##E## or #x## patterns, Western show names
**Book**: Comic book archives (.cbz, .cbr), manga, graphic novels, often numbered volumes

## Common Patterns
- TV: \`Show.Name.S01E01.Episode.Title.ext\` → title is "Show Name", episode_title is "Episode Title"
- Anime: \`Anime Title - 01 [1080p].mkv\` or \`Anime.Title.E01.ext\`
- Movie: \`Movie Title (2023) 1080p.mp4\`

## Special Cases (Anime Season Detection)
These rules OVERRIDE the default season detection:
- "Jujutsu Kaisen - Shimetsu Kaiyuu" or "Jujutsu Kaisen: Shimetsu Kaiyuu" → season: 3
- "Jujutsu Kaisen 2nd Season" → season: 2
- Any anime with "2nd Season" in the title → season: 2
- Any anime with "3rd Season" in the title → season: 3

## Examples
Input: \`The.Sopranos.S01E01.Pilot.avi\`
Output: \`{"media_type": "tv_show", "title": "The Sopranos", "season": 1, "episode": 1, "episode_title": "Pilot", "year": null, "confidence": "High"}\`

Input: \`Star.Trek.Starfleet.Academy.S01E09.300th.Night.1080p.AMZN.WEB-DL.DDP.5.1.H.264-NTb\`
Output: \`{"media_type": "tv_show", "title": "Star Trek Starfleet Academy", "season": 1, "episode": 9, "episode_title": "300th Night", "year": null, "confidence": "High"}\`

Input: \`Scrubs.2026.S01E06.My.V.I.P.1080p.HEVC.x265-MeGusta\`
Output: \`{"media_type": "tv_show", "title": "Scrubs 2026", "season": 1, "episode": 6, "episode_title": "My V I P", "year": null, "confidence": "High"}\`

Input: \`[SubsPlease] Dandadan - 01 (1080p) [2AB10B14].mkv\`
Output: \`{"media_type": "anime", "title": "Dandadan", "season": null, "episode": 1, "episode_title": null, "year": null, "confidence": "High"}\`

Input: \`[Erai-raws] Jujutsu Kaisen: Shimetsu Kaiyuu - Zenpen - 05 [1080p]\`
Output: \`{"media_type": "anime", "title": "Jujutsu Kaisen", "season": 3, "episode": 5, "episode_title": null, "year": null, "confidence": "High"}\`

Parse the filename and return only the JSON response.`;

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

// --- Ollama ---

async function ollamaGenerate(
  ollamaUrl: string,
  model: string,
  prompt: string,
  input: string,
): Promise<string> {
  const resp = await fetch(`${ollamaUrl}/api/generate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      model,
      prompt: `${prompt}\n\n${input}`,
      stream: false,
      think: false,
      options: { num_predict: 512 },
    }),
  });
  if (!resp.ok) {
    throw new Error(`Ollama error (${resp.status}): ${await resp.text()}`);
  }
  const json = await resp.json();
  let raw = (json.response ?? "").trim();
  if (raw.startsWith("```")) {
    raw = raw.replace(/^```(?:json)?\n?/, "").replace(/\n?```$/, "");
  }
  return raw;
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
  logger,
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

// --- Data reading helpers ---

async function readLatestData(
  dataDir: string,
): Promise<{ name: string; data }[]> {
  const results: { name: string; data }[] = [];
  try {
    for await (const entry of Deno.readDir(dataDir)) {
      if (!entry.isDirectory) continue;
      if (entry.name.startsWith("report-")) continue;

      const latestPath = `${dataDir}/${entry.name}/latest`;
      let version: string;
      try {
        version = (await Deno.readTextFile(latestPath)).trim();
      } catch {
        continue;
      }

      const rawPath = `${dataDir}/${entry.name}/${version}/raw`;
      try {
        const content = await Deno.readTextFile(rawPath);
        results.push({ name: entry.name, data: JSON.parse(content) });
      } catch {
        continue;
      }
    }
  } catch { /* dataDir doesn't exist */ }
  return results;
}

async function resolveModelByName(
  repoDir: string,
  name: string,
): Promise<{ type: string; id: string } | null> {
  for await (const entry of walkYaml(`${repoDir}/models`)) {
    const content = await Deno.readTextFile(entry);
    const nameMatch = content.match(/^name:\s*(.+)$/m);
    const idMatch = content.match(/^id:\s*(.+)$/m);
    const typeMatch = content.match(/^type:\s*'?(.+?)'?$/m);
    if (nameMatch && idMatch && typeMatch) {
      const defName = nameMatch[1].trim().replace(/^['"]|['"]$/g, "");
      if (defName === name) {
        return {
          type: typeMatch[1].trim().replace(/^['"]|['"]$/g, ""),
          id: idMatch[1].trim().replace(/^['"]|['"]$/g, ""),
        };
      }
    }
  }
  return null;
}

async function* walkYaml(dir: string): AsyncGenerator<string> {
  try {
    for await (const entry of Deno.readDir(dir)) {
      const path = `${dir}/${entry.name}`;
      if (entry.isDirectory) yield* walkYaml(path);
      else if (entry.name.endsWith(".yaml")) yield path;
    }
  } catch { /* directory doesn't exist */ }
}

// --- Model ---

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
      upgradeAttributes: (old) => ({
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
      execute: async (args: { sourceModel: string }, context) => {
        const { checkDirs, ollamaUrl, ollamaModel } = context.globalArgs;

        // Resolve source model
        const resolved = await resolveModelByName(
          context.repoDir,
          args.sourceModel,
        );
        if (!resolved) {
          throw new Error(
            `Could not find model definition for "${args.sourceModel}"`,
          );
        }
        context.logger.info(
          `Resolved "${args.sourceModel}" → type=${resolved.type}, id=${resolved.id}`,
        );

        const dataDir =
          `${context.repoDir}/.swamp/data/${resolved.type}/${resolved.id}`;
        const items = await readLatestData(dataDir);
        if (items.length === 0) {
          context.logger.info(`No data found for "${args.sourceModel}"`);
          return { dataHandles: [] };
        }

        const handles = [];
        let checked = 0;
        let skipped = 0;
        let llmParsed = 0;

        for (const { name: _name, data } of items) {
          if (!data.magnet) continue;

          let show = data.show;
          let episode = data.episode;
          let season = data.season;
          const needsLlm = !episode;

          // If no episode field, try LLM parsing on the raw title
          if (needsLlm && data.rawTitle) {
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
        const byEpisode = new Map<string, unknown>();
        for (const h of handles) {
          const key = `${h._show}-s${h._season ?? "0"}-e${h._episode}`;
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
        const finalHandles = [];
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
          `Checked ${checked}: ${finalHandles.length} new (${handles.length} before dedup), ${skipped} on disk, ${llmParsed} LLM-parsed`,
        );
        return { dataHandles: finalHandles };
      },
    },
  },
};
