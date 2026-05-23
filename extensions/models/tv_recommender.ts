// tv_recommender — packages enriched RT shows + a persona doc into per-show
// prompts that downstream Claude steps consume, then aggregates the resulting
// verdicts back into a single ranked recommendation set.
//
// The model intentionally does no LLM calls of its own; it produces structured
// inputs for `@keeb/anthropic/claude.generate` (one call per show via workflow
// forEach) and consumes that model's `result` resources to render the verdict
// summary.

import { z } from "npm:zod@4";

const GlobalArgs = z.object({
  personaPath: z.string().optional().describe(
    "Default persona markdown path. Relative paths resolve under the consumer's repo root. If unset, the bundled `personas/host.md` is used.",
  ),
});

const CollectArgs = z.object({
  browseSort: z.enum(["newest", "popular"]).describe(
    "Which RT browse list to evaluate (matches rottentomatoes browseSort tag).",
  ),
  limit: z.number().int().positive().default(50).describe(
    "Maximum number of shows to package for evaluation.",
  ),
  personaPath: z.string().optional().describe(
    "Persona markdown path for this run. Overrides the model's personaPath global arg. Relative paths resolve under the consumer's repo root.",
  ),
  bundledPersona: z.string().optional().describe(
    "Name of a persona bundled with @keeb/mms (file in `personas/`, without the .md suffix). Use 'host' for the default. Wins over personaPath only when no path is given.",
  ),
});

const AggregateArgs = z.object({
  collectVersion: z.number().int().positive().optional().describe(
    "Version of tv_recommender's `pack` resource to aggregate against. Defaults to latest.",
  ),
});

const PackSchema = z.object({
  browseSort: z.string(),
  collectedAt: z.string(),
  personaSource: z.string().describe(
    "Origin label, e.g. 'repo:design/adversary/host.md' or 'bundled:host'.",
  ),
  persona: z.string(),
  promptInstructions: z.string(),
  shows: z.array(z.object({
    tmdbId: z.string(),
    title: z.string(),
    showJson: z.string(),
  })),
});

const RecommendationSchema = z.object({
  browseSort: z.string(),
  aggregatedAt: z.string(),
  total: z.number(),
  watchCount: z.number(),
  skipCount: z.number(),
  unparsedCount: z.number(),
  verdicts: z.array(z.object({
    tmdbId: z.string(),
    title: z.string(),
    year: z.string().nullable(),
    networks: z.array(z.string()).default([]),
    genres: z.array(z.string()).default([]),
    tomatoScore: z.number().nullable(),
    popcornScore: z.number().nullable(),
    verdict: z.enum(["Watch", "Skip", "Unparsed"]),
    lane: z.string().nullable(),
    score: z.number().nullable(),
    reason: z.string(),
    rawResponse: z.string(),
  })),
});

const PROMPT_INSTRUCTIONS = [
  "You are evaluating a single TV show against the persona above.",
  "",
  "Return ONLY a JSON object on a single line, no prose, no code fences. Schema:",
  '{"tmdbId":"<echo the tmdbId from the Show block verbatim>","verdict":"Watch"|"Skip","lane":"prestige drama"|"atmospheric horror / mystery"|"high-concept / weird"|"mainstream event"|null,"score":<integer 0-100, your confidence in the verdict>,"reason":"<one sentence, plain language>"}',
  "",
  "Use Skip when nothing in the show fits the persona's lanes. Use Watch only when the persona would actually queue it.",
].join("\n");

function tryParseVerdict(raw: string): {
  verdict: "Watch" | "Skip" | "Unparsed";
  lane: string | null;
  score: number | null;
  reason: string;
} {
  // Pull the first {...} block out of the response; LLMs occasionally wrap
  // it in prose despite the instructions.
  const match = raw.match(/\{[\s\S]*\}/);
  if (!match) {
    return {
      verdict: "Unparsed",
      lane: null,
      score: null,
      reason: raw.slice(0, 200),
    };
  }
  try {
    const parsed = JSON.parse(match[0]);
    const verdict = parsed.verdict === "Watch" || parsed.verdict === "Skip"
      ? parsed.verdict
      : "Unparsed";
    return {
      verdict,
      lane: typeof parsed.lane === "string" ? parsed.lane : null,
      score: typeof parsed.score === "number" ? parsed.score : null,
      reason: typeof parsed.reason === "string"
        ? parsed.reason
        : raw.slice(0, 200),
    };
  } catch {
    return {
      verdict: "Unparsed",
      lane: null,
      score: null,
      reason: raw.slice(0, 200),
    };
  }
}

export const model = {
  type: "@keeb/mms/tv-recommender",
  version: "2026.05.22.1",
  reports: ["@keeb/mms/tv-recommendations"],
  globalArguments: GlobalArgs,
  resources: {
    pack: {
      description:
        "Per-show prompt material: persona text plus per-show JSON, ready for a forEach Claude call.",
      schema: PackSchema,
      lifetime: "infinite",
      garbageCollection: 10,
    },
    recommendations: {
      description:
        "Aggregated verdicts: one row per show with parsed Watch/Skip + reasoning.",
      schema: RecommendationSchema,
      lifetime: "infinite",
      garbageCollection: 10,
    },
  },
  methods: {
    collect: {
      description:
        "Read rottentomatoes shows for a given browseSort and the persona markdown; write a single `pack` resource the workflow forEach can drive Claude with.",
      arguments: CollectArgs,
      execute: async (args, context) => {
        // Persona resolution order (first wins):
        //   1. method input personaPath  (consumer-repo path)
        //   2. global personaPath        (consumer-repo path)
        //   3. method input bundledPersona ("host" → personas/host.md inside @keeb/mms)
        //   4. default bundled persona "host"
        const repoRelPath = args.personaPath ?? context.globalArgs.personaPath;
        let personaAbs: string;
        let personaLabel: string;
        if (repoRelPath) {
          personaAbs = `${context.repoDir}/${repoRelPath}`;
          personaLabel = `repo:${repoRelPath}`;
        } else {
          const bundleName = args.bundledPersona ?? "host";
          personaAbs = context.extensionFile(`personas/${bundleName}.md`);
          personaLabel = `bundled:${bundleName}`;
        }

        let persona: string;
        try {
          persona = await Deno.readTextFile(personaAbs);
        } catch (err) {
          throw new Error(
            `Could not read persona file at ${personaAbs} (${personaLabel}): ${
              (err as Error).message
            }`,
          );
        }

        context.logger.info(
          `[tv_recommender/collect] persona=${personaLabel} bytes=${persona.length}`,
        );

        // queryData() filters via the local sqlite catalog, which does not
        // see rottentomatoes records on this datastore. Bypass it: resolve
        // the rottentomatoes definition by name globally (which also yields
        // a ModelType object we can hand to dataRepository).
        const found = await context.definitionRepository.findByNameGlobal(
          "rottentomatoes",
        );
        if (!found) {
          throw new Error(
            "No definition named 'rottentomatoes' found; create the instance and run browse first.",
          );
        }
        const rtType = found.type;
        const rtDef = found.definition;
        const allData = await context.dataRepository.findAllForModel(
          rtType,
          rtDef.id,
        );

        type ShowAttrs = {
          tmdbId?: number;
          rtTitle?: string;
          tmdbName?: string;
          firstAirDate?: string | null;
          tomatoScore?: number | null;
          popcornScore?: number | null;
          genres?: string[];
          networks?: string[];
          status?: string | null;
          numberOfSeasons?: number | null;
          numberOfEpisodes?: number | null;
          overview?: string | null;
          tmdbPopularity?: number | null;
          browseSort?: string;
        };

        const enriched: { attrs: ShowAttrs; popularity: number }[] = [];
        for (const d of allData) {
          if (d.tags.specName !== "show") continue;
          const bytes = await context.dataRepository.getContent(
            rtType,
            String(rtDef.id),
            d.name,
          );
          if (!bytes) continue;
          let parsed: ShowAttrs;
          try {
            parsed = JSON.parse(new TextDecoder().decode(bytes));
          } catch {
            continue;
          }
          if (parsed.browseSort !== args.browseSort) continue;
          if (parsed.tmdbId == null) continue;
          enriched.push({
            attrs: parsed,
            popularity: parsed.tmdbPopularity ?? 0,
          });
        }

        context.logger.info(
          `[tv_recommender/collect] found ${enriched.length} rottentomatoes show(s) for browseSort=${args.browseSort}`,
        );

        enriched.sort((a, b) => b.popularity - a.popularity);
        const sorted = enriched.slice(0, args.limit);

        const shows = sorted.map(({ attrs: a }) => {
          const showPayload = {
            tmdbId: String(a.tmdbId),
            title: a.tmdbName ?? a.rtTitle,
            year: a.firstAirDate ?? null,
            tomatoScore: a.tomatoScore ?? null,
            popcornScore: a.popcornScore ?? null,
            genres: a.genres ?? [],
            networks: a.networks ?? [],
            status: a.status ?? null,
            numberOfSeasons: a.numberOfSeasons ?? null,
            numberOfEpisodes: a.numberOfEpisodes ?? null,
            overview: a.overview ?? null,
          };
          return {
            tmdbId: String(a.tmdbId),
            title: showPayload.title ?? "unknown",
            showJson: JSON.stringify(showPayload),
          };
        });

        const handle = await context.writeResource("pack", "pack", {
          browseSort: args.browseSort,
          collectedAt: new Date().toISOString(),
          personaSource: personaLabel,
          persona,
          promptInstructions: PROMPT_INSTRUCTIONS,
          shows,
        });

        context.logger.info(
          `[tv_recommender/collect] packed ${shows.length} show(s) into pack resource`,
        );

        return { dataHandles: [handle] };
      },
    },
    aggregate: {
      description:
        "Join the per-show Claude verdicts with the original RT metadata into a single ranked recommendation resource for the report.",
      arguments: AggregateArgs,
      execute: async (args, context) => {
        // Pull the latest pack written by this tv_recommender instance.
        const ownData = await context.dataRepository.findAllForModel(
          context.modelType,
          context.modelId,
        );
        type DataRow = {
          name: string;
          version: number;
          tags: Record<string, string>;
        };
        const packRow = (ownData as unknown as DataRow[])
          .filter((d) => d.tags.specName === "pack")
          .sort((a, b) => b.version - a.version)[0];
        if (!packRow) {
          throw new Error(
            `No tv-recommender pack found for ${context.definition.name}; run collect first.`,
          );
        }
        const packBytes = await context.dataRepository.getContent(
          context.modelType,
          context.modelId,
          packRow.name,
          packRow.version,
        );
        if (!packBytes) {
          throw new Error(
            `tv-recommender pack found but content unreadable (${packRow.name} v${packRow.version})`,
          );
        }
        const packContent = JSON.parse(new TextDecoder().decode(packBytes));
        const packShows = packContent.shows as Array<
          { tmdbId: string; title: string; showJson: string }
        >;
        const browseSort = packContent.browseSort as string;

        const showMeta = new Map<
          string,
          {
            // deno-lint-ignore no-explicit-any
            year: any;
            networks: string[];
            genres: string[];
            tomatoScore: number | null;
            popcornScore: number | null;
          }
        >();
        for (const s of packShows) {
          try {
            const parsed = JSON.parse(s.showJson);
            showMeta.set(s.tmdbId, {
              year: parsed.year ?? null,
              networks: parsed.networks ?? [],
              genres: parsed.genres ?? [],
              tomatoScore: parsed.tomatoScore ?? null,
              popcornScore: parsed.popcornScore ?? null,
            });
          } catch {
            // skip malformed entries silently; the pack writer enforces JSON
          }
        }

        // Claude (@keeb/anthropic/claude) writes every generate() to the
        // SAME data name ("result"), one version per call. Walk all versions
        // of host-evaluator's "result" record written after the pack was
        // collected, and parse the tmdbId out of each response to bind it
        // back to the originating show.
        const evaluator = await context.definitionRepository.findByNameGlobal(
          "host-evaluator",
        );
        if (!evaluator) {
          throw new Error(
            "No definition named 'host-evaluator' found; create the @keeb/anthropic/claude instance first.",
          );
        }
        const claudeType = evaluator.type;
        const claudeId = String(evaluator.definition.id);
        const versions = await context.dataRepository.listVersions(
          claudeType,
          claudeId,
          "result",
        );

        const packCollectedAt = new Date(packContent.collectedAt);
        const recentVersions = versions.slice().sort((a, b) => b - a);

        context.logger.info(
          `[tv_recommender/aggregate] pack=${packShows.length} claude-total-versions=${versions.length} pack-collectedAt=${packContent.collectedAt}`,
        );

        const responsesByTmdb = new Map<string, string>();
        let inspectedAfterPack = 0;
        for (const version of recentVersions) {
          const bytes = await context.dataRepository.getContent(
            claudeType,
            claudeId,
            "result",
            version,
          );
          if (!bytes) continue;
          let payload: { response?: string; timestamp?: string };
          try {
            payload = JSON.parse(new TextDecoder().decode(bytes));
          } catch {
            continue;
          }
          if (payload.timestamp) {
            const t = new Date(payload.timestamp);
            if (t < packCollectedAt) break;
            inspectedAfterPack++;
          }
          const response = typeof payload.response === "string"
            ? payload.response
            : "";
          if (!response) continue;
          const match = response.match(/"tmdbId"\s*:\s*"?([^",}\s]+)"?/);
          if (!match) continue;
          if (!responsesByTmdb.has(match[1])) {
            responsesByTmdb.set(match[1], response);
          }
        }

        context.logger.info(
          `[tv_recommender/aggregate] inspected ${inspectedAfterPack} claude version(s) created after the pack; matched ${responsesByTmdb.size} verdict(s) by tmdbId`,
        );

        const verdicts = packShows.map((s) => {
          const meta = showMeta.get(s.tmdbId) ?? {
            year: null,
            networks: [],
            genres: [],
            tomatoScore: null,
            popcornScore: null,
          };
          const raw = responsesByTmdb.get(s.tmdbId) ?? "";
          const parsed = raw ? tryParseVerdict(raw) : {
            verdict: "Unparsed" as const,
            lane: null,
            score: null,
            reason: "no claude response for this show",
          };
          return {
            tmdbId: s.tmdbId,
            title: s.title,
            year: meta.year,
            networks: meta.networks,
            genres: meta.genres,
            tomatoScore: meta.tomatoScore,
            popcornScore: meta.popcornScore,
            verdict: parsed.verdict,
            lane: parsed.lane,
            score: parsed.score,
            reason: parsed.reason,
            rawResponse: raw,
          };
        });

        const watchCount = verdicts.filter((v) => v.verdict === "Watch").length;
        const skipCount = verdicts.filter((v) => v.verdict === "Skip").length;
        const unparsedCount =
          verdicts.filter((v) => v.verdict === "Unparsed").length;

        const handle = await context.writeResource(
          "recommendations",
          "recommendations",
          {
            browseSort,
            aggregatedAt: new Date().toISOString(),
            total: verdicts.length,
            watchCount,
            skipCount,
            unparsedCount,
            verdicts,
          },
        );

        context.logger.info(
          `[tv_recommender/aggregate] watch=${watchCount} skip=${skipCount} unparsed=${unparsedCount}`,
        );
        // Silence unused-arg warning for the optional version selector.
        void args;
        return { dataHandles: [handle] };
      },
    },
  },
};
