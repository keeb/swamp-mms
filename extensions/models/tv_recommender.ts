// tv_recommender — scores enriched RT shows against a persona's lanes and
// writes a single ranked recommendation set for the tv-recommendations report.
//
// `evaluate` is the supported path: one step, no LLM, no API key. RT + TMDB
// already give us genres, networks, scores and season counts as structured
// fields, and the persona's lanes and auto-skips are genre rules, so the whole
// verdict is a deterministic function of data we already hold. The rule tables
// live next to `evaluateShow` below.
//
// `collect` + `aggregate` are the older LLM path, kept for reference: collect
// packages per-show prompts for `@keeb/anthropic/claude.generate` (one call per
// show via workflow forEach) and aggregate reads that model's `result`
// versions back. That path needs a paid API key and binds verdicts to shows by
// walking versions of a single shared data name, which is fragile under
// concurrency. Prefer `evaluate`.
//
// `pack_typesafe` + `aggregate_typesafe` run side by side with `evaluate`, and
// split the work: the typesafe evaluator (a cheap classifier) answers only what
// the metadata can't — short yes/no rubric statements about each overview,
// read from the persona's frontmatter — and the verdict stays deterministic
// here. pack_typesafe packages the browse as `{name, state}` items plus the
// rubric as noul `questions` for `@swamp/typesafe-ai.ask_batch`; shows already
// in the catalogue are auto-included and never asked. aggregate_typesafe joins
// each item's `evaluation-<name>` answers and runs them through the same rule
// table `evaluate` uses (see `evaluateShow`). Binding is by resource name.

import { z } from "npm:zod@4";
import { parse as parseYaml } from "npm:yaml@2.8.1";

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

const SinceArg = z.string().optional().describe(
  "ISO timestamp; only shows the rottentomatoes model wrote at or after it are considered. Pass the workflow's run.startedAt to evaluate just the browse that run scraped.",
);

const PackTypesafeArgs = z.object({
  browseSort: z.enum(["newest", "popular"]).describe(
    "Which RT browse list to evaluate (matches rottentomatoes browseSort tag).",
  ),
  limit: z.number().int().positive().default(50).describe(
    "Maximum number of shows to package, taken by descending TMDB popularity.",
  ),
  since: SinceArg,
  personaPath: z.string().optional().describe(
    "Persona markdown path for this run. Overrides the model's personaPath global arg. Relative paths resolve under the consumer's repo root.",
  ),
  catalogModel: z.string().default("jellyfin").describe(
    "Model name to read the existing catalogue from. Shows already in it are auto-included and not sent for evaluation. Set empty to disable.",
  ),
});

const AggregateTypesafeArgs = z.object({
  evaluatorModel: z.string().default("jev").describe(
    "Name of the @swamp/typesafe-ai instance whose ask_batch answered the pack's items.",
  ),
  minScore: z.number().int().min(0).max(100).default(70).describe(
    "Blended RT score an in-lane show must clear to earn Watch — the same bar as evaluate.",
  ),
});

const PackCatalogueArgs = z.object({
  library: z.string().default("Shows").describe(
    "Catalogue library display name whose series become the calibration set — the same value passed to the catalogue model's `series` method.",
  ),
  catalogModel: z.string().default("jellyfin").describe(
    "Model name holding the catalogue's `series` resources.",
  ),
  personaPath: z.string().optional().describe(
    "Persona markdown path whose rubric is being calibrated. Overrides the model's personaPath global arg.",
  ),
});

const CalibrateArgs = z.object({
  evaluatorModel: z.string().default("jev").describe(
    "Name of the @swamp/typesafe-ai instance whose ask_batch answered the catalogue-pack.",
  ),
});

// The persona's machine-readable half, from its YAML frontmatter. Each entry
// is one short statement the classifier scores 0–1 against a show; the role it
// sits under decides what the probability does in `evaluateShow`:
//   hook    — any at/over threshold puts a show in lane even without a lane genre
//   offLane — at/over threshold and above the strongest hook → Skip
//   veto    — at/over threshold → Skip, always
//
// `vetoThreshold` lets the hard line demand more certainty than the soft
// roles. `offLaneGenres` names the TMDB genre that must corroborate an
// off-lane statement before it can override a show's lane genre (a cheap
// classifier calls dark dramas "comedy"; TMDB's Comedy tag keeps it honest).
const RubricSchema = z.object({
  threshold: z.number().min(0).max(1).default(0.5),
  vetoThreshold: z.number().min(0).max(1).optional(),
  hook: z.record(z.string(), z.string()).default({}),
  offLane: z.record(z.string(), z.string()).default({}),
  offLaneGenres: z.record(z.string(), z.string()).default({}),
  veto: z.record(z.string(), z.string()).default({}),
});

type Rubric = z.infer<typeof RubricSchema>;

const EvaluateArgs = z.object({
  browseSort: z.enum(["newest", "popular"]).describe(
    "Which RT browse list to evaluate (matches rottentomatoes browseSort tag).",
  ),
  limit: z.number().int().positive().default(50).describe(
    "Maximum number of shows to evaluate, taken by descending TMDB popularity.",
  ),
  minScore: z.number().int().min(0).max(100).default(70).describe(
    "Blended RT score a lane-matching show must clear to earn Watch. Below this it is in-lane but under the bar.",
  ),
  catalogModel: z.string().default("jellyfin").describe(
    "Model name to read the existing catalogue from, for the persona's 'new season of something I already watch' auto-include. Set empty to disable the lookup.",
  ),
  since: SinceArg,
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

const TypesafePackSchema = z.object({
  browseSort: z.string(),
  collectedAt: z.string(),
  personaSource: z.string(),
  questions: z.record(
    z.string(),
    z.object({ type: z.literal("noul"), instructions: z.string() }),
  ).describe(
    "The persona's rubric as ask_batch questions, one noul statement per id.",
  ),
  rubric: RubricSchema.describe(
    "The rubric the questions came from, so aggregate scores with the same roles and threshold.",
  ),
  items: z.array(z.object({
    name: z.string(),
    state: z.record(z.string(), z.unknown()),
  })).describe(
    "Shows to evaluate, as ask_batch items. Catalogue shows are excluded.",
  ),
  shows: z.array(z.object({
    tmdbId: z.string(),
    name: z.string(),
    title: z.string(),
    year: z.string().nullable(),
    networks: z.array(z.string()),
    genres: z.array(z.string()),
    tomatoScore: z.number().nullable(),
    popcornScore: z.number().nullable(),
    numberOfSeasons: z.number().nullable(),
    inCatalogue: z.boolean(),
  })),
});

// One calibration run: how the persona's rubric + lane rules judge shows the
// group already chose. Every catalogue show is a known Watch, so `recall` is
// the headline and `misses` are what to tune against. Versions of this
// resource are the optimizer's history — compare them across rubric edits.
const CalibrationSchema = z.object({
  personaSource: z.string(),
  library: z.string(),
  packCollectedAt: z.string(),
  calibratedAt: z.string(),
  rubric: RubricSchema,
  total: z.number().describe("Catalogue shows answered by the evaluator."),
  unanswered: z.number().describe("Pack items with no answer since the pack was built."),
  watchCount: z.number(),
  recall: z.number().describe("watchCount / total — share of known watches the rubric recovers."),
  byRule: z.record(z.string(), z.number()).describe("Verdict count per deciding rule."),
  byLane: z.record(z.string(), z.number()).describe("Watch count per lane, including hook: lanes."),
  statements: z.array(z.object({
    id: z.string(),
    role: z.enum(["hook", "offLane", "veto"]),
    statement: z.string(),
    meanP: z.number(),
    fireRate: z.number().describe("Share of shows at/over threshold."),
    decided: z.number().describe("Shows whose verdict this statement decided."),
  })),
  misses: z.array(z.object({
    title: z.string(),
    year: z.string().nullable(),
    genres: z.array(z.string()),
    rule: z.string(),
    reason: z.string(),
    themes: z.record(z.string(), z.number()),
  })).describe("Catalogue shows the rubric would Skip, grouped by rule."),
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
    confidence: z.number().nullable().default(null).describe(
      "Evaluator certainty in the verdict, 0-1. Null for rule-based verdicts.",
    ),
    themes: z.record(z.string(), z.number()).optional().describe(
      "Rubric answers the verdict was built from: statement id → P(yes), 0-1.",
    ),
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

// ── Deterministic rules ─────────────────────────────────────────────────────
//
// These encode `personas/host.md` as code. Prose can't drive a classifier, so
// the markdown stays the human-readable spec and this block is the executable
// one — if you change the lanes or auto-skips there, change them here too.
// Everything below reads structured TMDB fields only; nothing inspects prose.

// Genres that are never in lane, per the persona's auto-skip list.
const SKIP_GENRES = ["Kids", "News", "Talk", "Soap", "Family"];

// Genres the persona only accepts when it's already a known quantity —
// "reality with stakes / shows the group already loves", not vanity
// competitions, and story documentaries rather than talking-head ones. We
// can't tell those apart from metadata, so they qualify only via the
// catalogue auto-include below.
const CATALOGUE_ONLY_GENRES = ["Reality", "Documentary"];

// Lane assignment, most specific first — first match wins.
//
// Deliberately NOT keyed on "Drama": TMDB hangs it on nearly every scripted
// show, so it admits college romances and workplace comedies into the prestige
// lane. Prestige here means the crime/politics spine the persona's examples
// share (The Wire, Sopranos, Fargo, True Detective), so it requires Crime or
// War & Politics and treats a bare Drama tag as no signal at all.
//
// host.md's fourth lane, "mainstream event", has no genre expression — it's a
// cultural-moment judgement, not a property of the metadata — so it is not
// represented here. The catalogue auto-include covers the returning half of
// it; net-new event shows are the one thing this classifier won't surface.
const LANE_RULES: { lane: string; genres: string[] }[] = [
  { lane: "atmospheric horror / mystery", genres: ["Mystery", "Horror"] },
  { lane: "high-concept / weird", genres: ["Sci-Fi & Fantasy"] },
  { lane: "prestige drama", genres: ["Crime", "War & Politics"] },
];

// A lone meter is one confirmation, not two, and must not rank as though it
// were both. Taking a single number at face value lets a critics-only 92
// outrank a 98/86 that critics and audiences independently stood behind — the
// corroborated show is the safer watch even though its blend is lower.
//
// So a single meter is regressed toward a neutral prior: it keeps most of its
// signal but stops impersonating a fully confirmed score. A critics-only 92
// lands at 84; a critics-only 100 still lands at 90.
const SINGLE_METER_PRIOR = 60;
const SINGLE_METER_SHRINK = 0.25;

// Blend the two RT meters into one rankable number. Audience agreement is the
// scarcer signal — critics reliably over-reward prestige-shaped shows — so
// popcorn carries slightly more weight when both are present.
function blendScore(
  tomato: number | null,
  popcorn: number | null,
): { score: number | null; corroborated: boolean } {
  if (tomato != null && popcorn != null) {
    return {
      score: Math.round(tomato * 0.45 + popcorn * 0.55),
      corroborated: true,
    };
  }
  const lone = tomato ?? popcorn ?? null;
  if (lone == null) return { score: null, corroborated: false };
  return {
    score: Math.round(
      lone * (1 - SINGLE_METER_SHRINK) +
        SINGLE_METER_PRIOR * SINGLE_METER_SHRINK,
    ),
    corroborated: false,
  };
}

function assignLane(genres: string[]): string | null {
  for (const rule of LANE_RULES) {
    if (rule.genres.some((g) => genres.includes(g))) return rule.lane;
  }
  return null;
}

type RuleVerdict = {
  verdict: "Watch" | "Skip";
  lane: string | null;
  score: number | null;
  reason: string;
  rule: string;
  // The rubric statement that decided the verdict, when one did.
  decidedBy?: string;
};

// The strongest answer in one rubric role, or null when the role is empty.
type Strongest = { id: string; p: number } | null;

// What the rubric answers contribute to a verdict. Absent for `evaluate`,
// which judges on metadata alone.
type RubricSignal = {
  threshold: number;
  vetoThreshold: number;
  veto: Strongest;
  // Off-lane candidates, strongest first, each with the TMDB genre (if any)
  // that must corroborate it against a lane genre.
  offLane: { id: string; p: number; genre?: string }[];
  hook: Strongest;
};

function strongest(
  ids: string[],
  themes: Record<string, number>,
): Strongest {
  let best: Strongest = null;
  for (const id of ids) {
    const p = themes[id];
    if (p === undefined) continue;
    if (!best || p > best.p) best = { id, p };
  }
  return best;
}

function rubricSignal(
  rubric: Rubric,
  themes: Record<string, number>,
): RubricSignal {
  return {
    threshold: rubric.threshold,
    vetoThreshold: rubric.vetoThreshold ?? rubric.threshold,
    veto: strongest(Object.keys(rubric.veto), themes),
    offLane: Object.keys(rubric.offLane)
      .filter((id) => themes[id] !== undefined)
      .map((id) => ({ id, p: themes[id], genre: rubric.offLaneGenres[id] }))
      .sort((a, b) => b.p - a.p),
    hook: strongest(Object.keys(rubric.hook), themes),
  };
}

const pct = (p: number) => `${Math.round(p * 100)}%`;

function evaluateShow(
  show: {
    genres: string[];
    tomatoScore: number | null;
    popcornScore: number | null;
    numberOfSeasons: number | null;
  },
  inCatalogue: boolean,
  // null judges lane fit alone, with no RT quality bar.
  minScore: number | null,
  rubric?: RubricSignal,
): RuleVerdict {
  const { score, corroborated } = blendScore(
    show.tomatoScore,
    show.popcornScore,
  );
  // A strong hook in the overview places a show the genre tags miss (a bare
  // "Drama" that is really a thriller).
  const lane = assignLane(show.genres) ??
    (rubric?.hook && rubric.hook.p >= rubric.threshold
      ? `hook: ${rubric.hook.id}`
      : null);
  // Name the weaker evidence in the verdict so a discounted score is legible
  // in the report rather than looking like a plain low rating.
  const meterNote = corroborated
    ? ""
    : show.tomatoScore != null
    ? " (critics only, no audience score — discounted)"
    : " (audience only, no critic score — discounted)";

  // Auto-include beats every other rule, exactly as the persona states: a new
  // season of something already being watched skips the lane check entirely.
  if (inCatalogue) {
    return {
      verdict: "Watch",
      lane: lane ?? "returning",
      score,
      reason: show.numberOfSeasons && show.numberOfSeasons > 1
        ? `Already in the catalogue and now on season ${show.numberOfSeasons} — auto-include.`
        : "Already in the catalogue — auto-include.",
      rule: "catalogue-auto-include",
    };
  }

  // A veto is the persona's hard line — nothing below can rescue the show.
  if (rubric?.veto && rubric.veto.p >= rubric.vetoThreshold) {
    return {
      verdict: "Skip",
      lane: null,
      score,
      reason: `Trips the ${rubric.veto.id} veto (${pct(rubric.veto.p)}).`,
      rule: "rubric-veto",
      decidedBy: rubric.veto.id,
    };
  }

  // Off-lane only wins when it outweighs every hook (a comedy that is also
  // plainly a thriller stays in play), and against a lane genre only when
  // TMDB's own tag corroborates it.
  const genreLane = assignLane(show.genres);
  const offLane = rubric?.offLane.find((o) =>
    o.p >= rubric.threshold && o.p > (rubric.hook?.p ?? 0) &&
    (!genreLane || !o.genre || show.genres.includes(o.genre))
  );
  if (rubric && offLane) {
    return {
      verdict: "Skip",
      lane: null,
      score,
      reason: `Reads as ${offLane.id} (${pct(offLane.p)})${
        rubric.hook ? `, over its strongest hook ${rubric.hook.id} (${pct(rubric.hook.p)})` : ""
      }.`,
      rule: "rubric-off-lane",
      decidedBy: offLane.id,
    };
  }

  const skipGenre = show.genres.find((g) => SKIP_GENRES.includes(g));
  if (skipGenre) {
    return {
      verdict: "Skip",
      lane: null,
      score,
      reason: `${skipGenre} is on the persona's auto-skip list.`,
      rule: "auto-skip-genre",
    };
  }

  // Reality/documentary are gated only when they're the whole show; a tag
  // riding along a real lane genre (The X-Files, Vikings) doesn't gate.
  const gatedGenre = show.genres.find((g) => CATALOGUE_ONLY_GENRES.includes(g));
  if (gatedGenre && !assignLane(show.genres)) {
    return {
      verdict: "Skip",
      lane: null,
      score,
      reason:
        `${gatedGenre} only qualifies when the group already watches it, and it is not in the catalogue.`,
      rule: "catalogue-gated-genre",
    };
  }

  if (!lane) {
    return {
      verdict: "Skip",
      lane: null,
      score,
      reason: show.genres.length
        ? `No lane fits ${show.genres.join(", ")}.`
        : "No genre data to place it in a lane.",
      rule: "no-lane",
    };
  }

  // Fit-only mode (calibrate): the show has no RT scores to gate on, so being
  // in lane is the whole verdict.
  if (minScore === null) {
    return {
      verdict: "Watch",
      lane,
      score,
      reason: `Fits ${lane}.`,
      rule: "lane-fit",
    };
  }

  if (score == null) {
    return {
      verdict: "Skip",
      lane,
      score: null,
      reason: `Fits ${lane} but has no RT score yet to judge it on.`,
      rule: "unscored",
    };
  }

  if (score < minScore) {
    return {
      verdict: "Skip",
      lane,
      score,
      reason:
        `Fits ${lane} but scores ${score}${meterNote}, under the ${minScore} bar.`,
      rule: "below-bar",
    };
  }

  return {
    verdict: "Watch",
    lane,
    score,
    reason: `Fits ${lane} and scores ${score}${meterNote}.`,
    rule: corroborated ? "lane-match" : "lane-match-uncorroborated",
  };
}

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

// Read rottentomatoes `show` resources for one browse list, most popular first.
//
// queryData() filters via the local sqlite catalog, which does not see
// rottentomatoes records on this datastore. Bypass it: resolve the definition
// by name globally, which also yields a ModelType we can hand to
// dataRepository.
async function readRtShows(
  // deno-lint-ignore no-explicit-any
  context: any,
  browseSort: string,
  limit: number,
  since?: string,
) {
  const sinceMs = since ? Date.parse(since) : undefined;
  if (since && Number.isNaN(sinceMs)) {
    throw new Error(`since is not a valid timestamp: ${since}`);
  }
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

  const enriched: { attrs: ShowAttrs; popularity: number }[] = [];
  for (const d of allData) {
    if (d.tags.specName !== "show") continue;
    // Every browse rewrites the shows it saw, so a show's latest write time
    // says whether it was on the list this run scraped.
    if (sinceMs !== undefined) {
      const written = Date.parse(String(d.createdAt));
      if (Number.isNaN(written)) {
        throw new Error(
          `rottentomatoes data ${d.name} has no readable createdAt; cannot apply since`,
        );
      }
      if (written < sinceMs) continue;
    }
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
    if (parsed.browseSort !== browseSort) continue;
    if (parsed.tmdbId == null) continue;
    enriched.push({ attrs: parsed, popularity: parsed.tmdbPopularity ?? 0 });
  }

  context.logger.info(
    `[tv_recommender] found ${enriched.length} rottentomatoes show(s) for browseSort=${browseSort}${
      since ? ` written since ${since}` : ""
    }`,
  );

  enriched.sort((a, b) => b.popularity - a.popularity);
  return { all: enriched.length, shows: enriched.slice(0, limit) };
}

// Collect every TMDB id present in the catalogue model's `series` resources.
// Jellyfin records providerIds.Tmdb per series, so this is an exact join —
// no title normalisation, no fuzzy matching.
// deno-lint-ignore no-explicit-any
async function readCatalogueTmdbIds(context: any, modelName: string) {
  const ids = new Set<string>();
  if (!modelName) return ids;
  const found = await context.definitionRepository.findByNameGlobal(modelName);
  if (!found) {
    context.logger.info(
      `[tv_recommender] catalogue model '${modelName}' not found; auto-include disabled for this run`,
    );
    return ids;
  }
  const rows = await context.dataRepository.findAllForModel(
    found.type,
    found.definition.id,
  );
  for (const d of rows) {
    if (d.tags.specName !== "series") continue;
    const bytes = await context.dataRepository.getContent(
      found.type,
      String(found.definition.id),
      d.name,
    );
    if (!bytes) continue;
    try {
      const parsed = JSON.parse(new TextDecoder().decode(bytes));
      for (const item of parsed.items ?? []) {
        const tmdb = item?.providerIds?.Tmdb;
        if (tmdb) ids.add(String(tmdb));
      }
    } catch {
      continue;
    }
  }
  return ids;
}

// Persona resolution order (first wins):
//   1. method input personaPath  (consumer-repo path)
//   2. global personaPath        (consumer-repo path)
//   3. method input bundledPersona ("host" → personas/host.md inside @keeb/mms)
//   4. default bundled persona "host"
//
// The persona may open with YAML frontmatter carrying a `rubric:` block; it is
// split off here, so `persona` is always just the prose.
async function readPersona(
  // deno-lint-ignore no-explicit-any
  context: any,
  personaPath?: string,
  bundledPersona?: string,
): Promise<{ persona: string; personaLabel: string; rubric: Rubric | null }> {
  const repoRelPath = personaPath ?? context.globalArgs.personaPath;
  let personaAbs: string;
  let personaLabel: string;
  if (repoRelPath) {
    personaAbs = `${context.repoDir}/${repoRelPath}`;
    personaLabel = `repo:${repoRelPath}`;
  } else {
    const bundleName = bundledPersona ?? "host";
    personaAbs = context.extensionFile(`personas/${bundleName}.md`);
    personaLabel = `bundled:${bundleName}`;
  }
  let text: string;
  try {
    text = await Deno.readTextFile(personaAbs);
  } catch (err) {
    throw new Error(
      `Could not read persona file at ${personaAbs} (${personaLabel}): ${
        (err as Error).message
      }`,
    );
  }
  const fm = text.match(/^---\r?\n([\s\S]*?)\r?\n---\r?\n?/);
  if (!fm) return { persona: text, personaLabel, rubric: null };
  // deno-lint-ignore no-explicit-any
  const meta: any = parseYaml(fm[1]) ?? {};
  const rubric = meta.rubric === undefined ? null : RubricSchema.parse(meta.rubric);
  if (rubric) {
    const ids = [
      ...Object.keys(rubric.hook),
      ...Object.keys(rubric.offLane),
      ...Object.keys(rubric.veto),
    ];
    const dup = ids.find((id, i) => ids.indexOf(id) !== i);
    if (dup) {
      throw new Error(
        `Persona ${personaLabel} rubric uses id '${dup}' in more than one role`,
      );
    }
  }
  return { persona: text.slice(fm[0].length), personaLabel, rubric };
}

// The persona's rubric, required — the typesafe path asks the rubric, never
// the prose.
async function readRubric(
  // deno-lint-ignore no-explicit-any
  context: any,
  personaPath?: string,
): Promise<{ personaLabel: string; rubric: Rubric; questions: Questions }> {
  const { personaLabel, rubric } = await readPersona(context, personaPath);
  if (!rubric) {
    throw new Error(
      `Persona ${personaLabel} has no rubric frontmatter; the typesafe path asks the rubric, not the prose.`,
    );
  }
  const questions = Object.fromEntries(
    [rubric.hook, rubric.offLane, rubric.veto].flatMap((role) =>
      Object.entries(role).map(([id, statement]) => [
        id,
        { type: "noul" as const, instructions: statement },
      ])
    ),
  );
  return { personaLabel, rubric, questions };
}

type Questions = z.infer<typeof TypesafePackSchema>["questions"];

// The evaluator's rubric answers for one pack item, as id → P(yes). Null when
// there is no answer written since the pack was built — an older one belongs
// to an earlier run.
async function readThemes(
  // deno-lint-ignore no-explicit-any
  context: any,
  // deno-lint-ignore no-explicit-any
  evaluator: any,
  evaluatorName: string,
  itemName: string,
  questions: Questions,
  collectedAt: number,
): Promise<Record<string, number> | null> {
  const bytes = await context.dataRepository.getContent(
    evaluator.type,
    String(evaluator.definition.id),
    `evaluation-${itemName}`,
  );
  // deno-lint-ignore no-explicit-any
  const evaluation: any = bytes
    ? JSON.parse(new TextDecoder().decode(bytes))
    : null;
  if (!evaluation || Date.parse(evaluation.evaluatedAt) < collectedAt) {
    return null;
  }
  const themes: Record<string, number> = {};
  for (const id of Object.keys(questions)) {
    const answer = evaluation.answers?.[id];
    if (answer?.type !== "noul") {
      throw new Error(
        `${evaluatorName} evaluation-${itemName} has no noul answer for rubric id '${id}'`,
      );
    }
    themes[id] = Math.round(answer.noul * 1000) / 1000;
  }
  return themes;
}

// deno-lint-ignore no-explicit-any
async function findEvaluator(context: any, name: string) {
  const evaluator = await context.definitionRepository.findByNameGlobal(name);
  if (!evaluator) {
    throw new Error(
      `No definition named '${name}' found; create the @swamp/typesafe-ai instance first.`,
    );
  }
  return evaluator;
}

// deno-lint-ignore no-explicit-any
async function readOwnPack(context: any, instance: string, producer: string) {
  const bytes = await context.dataRepository.getContent(
    context.modelType,
    context.modelId,
    instance,
  );
  if (!bytes) {
    throw new Error(
      `No ${instance} found for ${context.definition.name}; run ${producer} first.`,
    );
  }
  return TypesafePackSchema.parse(JSON.parse(new TextDecoder().decode(bytes)));
}

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
  version: "2026.09.25.1",
  reports: ["@keeb/mms/tv-recommendations"],
  globalArguments: GlobalArgs,
  upgrades: [
    {
      toVersion: "2026.09.16.1",
      description:
        "Add pack_typesafe/aggregate_typesafe and optional since on evaluate; no globalArgs change",
      upgradeAttributes: (old: Record<string, unknown>) => old,
    },
    {
      toVersion: "2026.09.25.1",
      description:
        "typesafe path asks the persona's frontmatter rubric via ask_batch and scores it with the evaluate rules; add pack_catalogue/calibrate rubric calibration against the catalogue; no globalArgs change",
      upgradeAttributes: (old: Record<string, unknown>) => old,
    },
  ],
  resources: {
    "typesafe-pack": {
      description:
        "Current RT browse as @swamp/typesafe-ai.choice_batch input: persona instructions, yes/no criteria, one item per show.",
      schema: TypesafePackSchema,
      lifetime: "infinite",
      garbageCollection: 10,
    },
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
    "rubric-calibration": {
      description:
        "How the persona rubric + lane rules judge the existing catalogue: recall, per-statement stats, and misses. Versions are the rubric optimizer's history.",
      schema: CalibrationSchema,
      lifetime: "infinite",
      garbageCollection: 50,
    },
  },
  methods: {
    evaluate: {
      description:
        "Score rottentomatoes shows against the host persona's lanes using structured TMDB fields only, and write the ranked `recommendations` resource. No LLM, no API key, one step.",
      arguments: EvaluateArgs,
      execute: async (args, context) => {
        const { shows: sorted } = await readRtShows(
          context,
          args.browseSort,
          args.limit,
          args.since,
        );
        const catalogue = await readCatalogueTmdbIds(
          context,
          args.catalogModel,
        );
        context.logger.info(
          `[tv_recommender/evaluate] catalogue tmdbIds=${catalogue.size} minScore=${args.minScore}`,
        );

        const verdicts = sorted.map(({ attrs: a }) => {
          const tmdbId = String(a.tmdbId);
          const genres = a.genres ?? [];
          const ruled = evaluateShow(
            {
              genres,
              tomatoScore: a.tomatoScore ?? null,
              popcornScore: a.popcornScore ?? null,
              numberOfSeasons: a.numberOfSeasons ?? null,
            },
            catalogue.has(tmdbId),
            args.minScore,
          );
          return {
            tmdbId,
            title: a.tmdbName ?? a.rtTitle ?? "unknown",
            year: a.firstAirDate ?? null,
            networks: a.networks ?? [],
            genres,
            tomatoScore: a.tomatoScore ?? null,
            popcornScore: a.popcornScore ?? null,
            verdict: ruled.verdict,
            lane: ruled.lane,
            score: ruled.score,
            reason: ruled.reason,
            rawResponse: `rule:${ruled.rule}`,
          };
        });

        // Rank Watch above Skip, then by blended score descending, so the
        // report's first rows are the actual picks.
        verdicts.sort((a, b) => {
          if (a.verdict !== b.verdict) return a.verdict === "Watch" ? -1 : 1;
          return (b.score ?? -1) - (a.score ?? -1);
        });

        const watchCount = verdicts.filter((v) => v.verdict === "Watch").length;
        const skipCount = verdicts.filter((v) => v.verdict === "Skip").length;

        const handle = await context.writeResource(
          "recommendations",
          "recommendations",
          {
            browseSort: args.browseSort,
            aggregatedAt: new Date().toISOString(),
            total: verdicts.length,
            watchCount,
            skipCount,
            unparsedCount: 0,
            verdicts,
          },
        );

        context.logger.info(
          `[tv_recommender/evaluate] watch=${watchCount} skip=${skipCount} total=${verdicts.length}`,
        );
        return { dataHandles: [handle] };
      },
    },
    collect: {
      description:
        "LEGACY LLM PATH — superseded by `evaluate`. Read rottentomatoes shows for a given browseSort and the persona markdown; write a single `pack` resource the workflow forEach can drive Claude with. Requires an @keeb/anthropic/claude instance named host-evaluator.",
      arguments: CollectArgs,
      execute: async (args, context) => {
        const { persona, personaLabel } = await readPersona(
          context,
          args.personaPath,
          args.bundledPersona,
        );

        context.logger.info(
          `[tv_recommender/collect] persona=${personaLabel} bytes=${persona.length}`,
        );

        const { shows: sorted } = await readRtShows(
          context,
          args.browseSort,
          args.limit,
        );

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
        "LEGACY LLM PATH — superseded by `evaluate`. Join the per-show Claude verdicts with the original RT metadata into a single ranked recommendation resource for the report.",
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
    pack_typesafe: {
      description:
        "Package the current RT browse for @swamp/typesafe-ai.choice_batch: persona + watch question as instructions, one {name, state} item per show. Catalogue shows are auto-included and left out of items.",
      arguments: PackTypesafeArgs,
      execute: async (args, context) => {
        const { personaLabel, rubric, questions } = await readRubric(
          context,
          args.personaPath,
        );
        const { shows: sorted } = await readRtShows(
          context,
          args.browseSort,
          args.limit,
          args.since,
        );
        // An empty browse means the scrape or the since window is wrong, not
        // that there is nothing to watch — fail rather than pack nothing.
        if (sorted.length === 0) {
          throw new Error(
            `No rottentomatoes shows for browseSort=${args.browseSort}${
              args.since ? ` written since ${args.since}` : ""
            }; did browse run?`,
          );
        }
        const catalogue = await readCatalogueTmdbIds(
          context,
          args.catalogModel,
        );

        const shows = sorted.map(({ attrs: a }) => {
          const tmdbId = String(a.tmdbId);
          return {
            tmdbId,
            name: `rt-${tmdbId}`,
            title: a.tmdbName ?? a.rtTitle ?? "unknown",
            year: a.firstAirDate ?? null,
            networks: a.networks ?? [],
            genres: a.genres ?? [],
            tomatoScore: a.tomatoScore ?? null,
            popcornScore: a.popcornScore ?? null,
            numberOfSeasons: a.numberOfSeasons ?? null,
            inCatalogue: catalogue.has(tmdbId),
            status: a.status ?? null,
            overview: a.overview ?? null,
          };
        });

        const items = shows.filter((s) => !s.inCatalogue).map((s) => ({
          name: s.name,
          state: {
            title: s.title,
            firstAired: s.year,
            genres: s.genres,
            networks: s.networks,
            tomatometer: s.tomatoScore,
            popcornmeter: s.popcornScore,
            seasons: s.numberOfSeasons,
            status: s.status,
            overview: s.overview,
          },
        }));

        const handle = await context.writeResource(
          "typesafe-pack",
          "typesafe-pack",
          {
            browseSort: args.browseSort,
            collectedAt: new Date().toISOString(),
            personaSource: personaLabel,
            questions,
            rubric,
            items,
            shows: shows.map(({ status: _s, overview: _o, ...rest }) => rest),
          },
        );

        context.logger.info(
          `[tv_recommender/pack_typesafe] persona=${personaLabel} questions=${
            Object.keys(questions).length
          } shows=${shows.length} items=${items.length} auto-included=${
            shows.length - items.length
          }`,
        );
        return { dataHandles: [handle] };
      },
    },
    aggregate_typesafe: {
      description:
        "Join the evaluator's evaluation-<name> rubric answers onto the latest typesafe-pack, run them through the persona rules, and write the ranked `recommendations-typesafe` instance for the report.",
      arguments: AggregateTypesafeArgs,
      execute: async (args, context) => {
        const pack = await readOwnPack(context, "typesafe-pack", "pack_typesafe");
        const evaluator = await findEvaluator(context, args.evaluatorModel);
        const collectedAt = Date.parse(pack.collectedAt);

        let missing = 0;
        const verdicts = [];
        for (const s of pack.shows) {
          const base = {
            tmdbId: s.tmdbId,
            title: s.title,
            year: s.year,
            networks: s.networks,
            genres: s.genres,
            tomatoScore: s.tomatoScore,
            popcornScore: s.popcornScore,
            lane: null,
          };
          if (s.inCatalogue) {
            verdicts.push({
              ...base,
              verdict: "Watch" as const,
              score: 100,
              confidence: null,
              reason: "Already in the catalogue — auto-include.",
              rawResponse: "rule:catalogue-auto-include",
            });
            continue;
          }
          const themes = await readThemes(
            context,
            evaluator,
            args.evaluatorModel,
            s.name,
            pack.questions,
            collectedAt,
          );
          if (!themes) {
            missing++;
            verdicts.push({
              ...base,
              verdict: "Unparsed" as const,
              score: null,
              confidence: null,
              reason:
                `No ${args.evaluatorModel} answer since the pack was built.`,
              rawResponse: "",
            });
            continue;
          }
          const ruled = evaluateShow(
            s,
            false,
            args.minScore,
            rubricSignal(pack.rubric, themes),
          );
          verdicts.push({
            ...base,
            lane: ruled.lane,
            verdict: ruled.verdict,
            score: ruled.score,
            confidence: null,
            themes,
            reason: ruled.reason,
            rawResponse: `rubric:${ruled.rule}`,
          });
        }

        // Watch first, then by P(yes) — the report's first rows are the picks.
        verdicts.sort((a, b) => {
          if (a.verdict !== b.verdict) return a.verdict === "Watch" ? -1 : 1;
          return (b.score ?? -1) - (a.score ?? -1);
        });

        const watchCount = verdicts.filter((v) => v.verdict === "Watch").length;
        const skipCount = verdicts.filter((v) => v.verdict === "Skip").length;

        const handle = await context.writeResource(
          "recommendations",
          "recommendations-typesafe",
          {
            browseSort: pack.browseSort,
            aggregatedAt: new Date().toISOString(),
            total: verdicts.length,
            watchCount,
            skipCount,
            unparsedCount: missing,
            verdicts,
          },
        );

        context.logger.info(
          `[tv_recommender/aggregate_typesafe] watch=${watchCount} skip=${skipCount} missing=${missing}`,
        );
        return { dataHandles: [handle] };
      },
    },
    pack_catalogue: {
      description:
        "Package every series in a catalogue library as ask_batch items with the persona rubric, for calibrate to measure the rubric against shows the group already chose. Writes `catalogue-pack`.",
      arguments: PackCatalogueArgs,
      execute: async (args, context) => {
        const { personaLabel, rubric, questions } = await readRubric(
          context,
          args.personaPath,
        );
        const found = await context.definitionRepository.findByNameGlobal(
          args.catalogModel,
        );
        if (!found) {
          throw new Error(`No definition named '${args.catalogModel}' found.`);
        }
        // The catalogue's series method takes the library's display name
        // ("Shows", "weeb shit") and writes it as a slug ("shows", "weeb-shit").
        const instance = args.library.toLowerCase().trim().replace(/\s+/g, "-");
        const bytes = await context.dataRepository.getContent(
          found.type,
          String(found.definition.id),
          instance,
        );
        if (!bytes) {
          throw new Error(
            `${args.catalogModel} has no '${args.library}' series data; run ${args.catalogModel} series --input library=${args.library} first.`,
          );
        }
        // deno-lint-ignore no-explicit-any
        const series: any[] = JSON.parse(new TextDecoder().decode(bytes)).items ?? [];
        if (series.length === 0) {
          throw new Error(`${args.catalogModel} '${args.library}' has no series.`);
        }

        // One item per show, keyed on TMDB id where Jellyfin matched one.
        const byName = new Map<string, {
          show: z.infer<typeof TypesafePackSchema>["shows"][number];
          state: Record<string, unknown>;
        }>();
        for (const s of series) {
          const id = String(s.providerIds?.Tmdb ?? s.id);
          const name = `cat-${id}`.toLowerCase().replace(/[^a-z0-9_-]/g, "-").slice(0, 63);
          if (byName.has(name)) continue;
          const year = s.productionYear != null ? String(s.productionYear) : null;
          const genres: string[] = s.genres ?? [];
          const networks: string[] = s.studios ?? [];
          byName.set(name, {
            show: {
              tmdbId: id,
              name,
              title: s.name ?? "unknown",
              year,
              networks,
              genres,
              tomatoScore: null,
              popcornScore: null,
              numberOfSeasons: null,
              inCatalogue: false,
            },
            state: {
              title: s.name ?? "unknown",
              firstAired: year,
              genres,
              networks,
              status: s.status ?? null,
              overview: s.overview ?? null,
            },
          });
        }
        const entries = [...byName.values()];

        const handle = await context.writeResource(
          "typesafe-pack",
          "catalogue-pack",
          {
            browseSort: `catalogue:${args.library}`,
            collectedAt: new Date().toISOString(),
            personaSource: personaLabel,
            questions,
            rubric,
            items: entries.map((e) => ({ name: e.show.name, state: e.state })),
            shows: entries.map((e) => e.show),
          },
        );
        context.logger.info(
          `[tv_recommender/pack_catalogue] persona=${personaLabel} library=${args.library} series=${series.length} items=${entries.length} questions=${
            Object.keys(questions).length
          }`,
        );
        return { dataHandles: [handle] };
      },
    },
    calibrate: {
      description:
        "Score the evaluator's rubric answers for the catalogue-pack with the persona rules (lane fit only, no RT bar) and write `rubric-calibration`: recall on shows the group already chose, per-statement stats, and the misses to tune against.",
      arguments: CalibrateArgs,
      execute: async (args, context) => {
        const pack = await readOwnPack(context, "catalogue-pack", "pack_catalogue");
        const evaluator = await findEvaluator(context, args.evaluatorModel);
        const collectedAt = Date.parse(pack.collectedAt);

        const roleOf: Record<string, "hook" | "offLane" | "veto"> = {};
        for (const role of ["hook", "offLane", "veto"] as const) {
          for (const id of Object.keys(pack.rubric[role])) roleOf[id] = role;
        }
        const pSum: Record<string, number> = {};
        const fires: Record<string, number> = {};
        const decided: Record<string, number> = {};
        const byRule: Record<string, number> = {};
        const byLane: Record<string, number> = {};
        const misses = [];
        let total = 0;
        let unanswered = 0;
        let watchCount = 0;

        for (const s of pack.shows) {
          const themes = await readThemes(
            context,
            evaluator,
            args.evaluatorModel,
            s.name,
            pack.questions,
            collectedAt,
          );
          if (!themes) {
            unanswered++;
            continue;
          }
          total++;
          for (const [id, p] of Object.entries(themes)) {
            pSum[id] = (pSum[id] ?? 0) + p;
            if (p >= pack.rubric.threshold) fires[id] = (fires[id] ?? 0) + 1;
          }
          const signal = rubricSignal(pack.rubric, themes);
          const ruled = evaluateShow(s, false, null, signal);
          byRule[ruled.rule] = (byRule[ruled.rule] ?? 0) + 1;

          // Credit the statement that decided the verdict, if one did.
          const decider = ruled.decidedBy ??
            (ruled.lane?.startsWith("hook: ")
              ? ruled.lane.slice("hook: ".length)
              : undefined);
          if (decider) decided[decider] = (decided[decider] ?? 0) + 1;

          if (ruled.verdict === "Watch") {
            watchCount++;
            const lane = ruled.lane ?? "none";
            byLane[lane] = (byLane[lane] ?? 0) + 1;
          } else {
            misses.push({
              title: s.title,
              year: s.year,
              genres: s.genres,
              rule: ruled.rule,
              reason: ruled.reason,
              themes,
            });
          }
        }
        if (total === 0) {
          throw new Error(
            `No ${args.evaluatorModel} answers since catalogue-pack was built; run ${args.evaluatorModel} ask_batch on it first.`,
          );
        }

        misses.sort((a, b) => a.rule.localeCompare(b.rule) || a.title.localeCompare(b.title));
        const round = (n: number) => Math.round(n * 1000) / 1000;
        const statements = Object.keys(pack.questions).map((id) => ({
          id,
          role: roleOf[id],
          statement: pack.questions[id].instructions,
          meanP: round((pSum[id] ?? 0) / total),
          fireRate: round((fires[id] ?? 0) / total),
          decided: decided[id] ?? 0,
        }));
        const recall = round(watchCount / total);

        const handle = await context.writeResource(
          "rubric-calibration",
          "rubric-calibration",
          {
            personaSource: pack.personaSource,
            library: pack.browseSort.replace(/^catalogue:/, ""),
            packCollectedAt: pack.collectedAt,
            calibratedAt: new Date().toISOString(),
            rubric: pack.rubric,
            total,
            unanswered,
            watchCount,
            recall,
            byRule,
            byLane,
            statements,
            misses,
          },
        );
        context.logger.info(
          `[tv_recommender/calibrate] recall=${recall} (${watchCount}/${total}) unanswered=${unanswered} misses=${misses.length}`,
        );
        return { dataHandles: [handle] };
      },
    },
  },
};
