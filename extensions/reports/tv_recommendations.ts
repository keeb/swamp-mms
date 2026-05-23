export const report = {
  name: "@keeb/mms/tv-recommendations",
  description:
    "Render the host's Watch/Skip verdicts for a tv_recommender.aggregate run — sorted by confidence within Watch, then Skip, then Unparsed.",
  scope: "method" as const,
  labels: ["recommendations", "host"],
  // deno-lint-ignore no-explicit-any
  execute: async (context: any) => {
    const modelName = context.definition.name;
    const method = context.methodName;
    const status = context.executionStatus;
    const modelType = context.modelType;
    const modelId = context.definition.id;

    if (method !== "aggregate") {
      return {
        markdown:
          `# TV Recommendations\n\nReport scoped to aggregate; skipped for ${modelName}.${method}.\n`,
        json: { status, skipped: true },
      };
    }

    const handle = (context.dataHandles ?? []).find(
      // deno-lint-ignore no-explicit-any
      (h: any) => h?.metadata?.tags?.specName === "recommendations",
    );

    if (!handle) {
      return {
        markdown:
          `# TV Recommendations: ${modelName}.${method} (${status})\n\nNo recommendations resource produced.\n`,
        json: { status, total: 0 },
      };
    }

    const raw = await context.dataRepository.getContent(
      modelType,
      modelId,
      handle.name,
      handle.version,
    );
    if (!raw) {
      return {
        markdown:
          `# TV Recommendations: ${modelName}.${method} (${status})\n\nrecommendations content unavailable.\n`,
        json: { status, total: 0 },
      };
    }

    const rec = JSON.parse(new TextDecoder().decode(raw));
    type Verdict = {
      tmdbId: string;
      title: string;
      year: string | null;
      networks: string[];
      genres: string[];
      tomatoScore: number | null;
      popcornScore: number | null;
      verdict: "Watch" | "Skip" | "Unparsed";
      lane: string | null;
      score: number | null;
      reason: string;
      rawResponse: string;
    };
    const verdicts = (rec.verdicts ?? []) as Verdict[];

    const rankKey = (v: Verdict) => {
      const verdictRank = v.verdict === "Watch"
        ? 0
        : v.verdict === "Skip"
        ? 1
        : 2;
      const scoreRank = -(v.score ?? -1); // higher score first
      return [verdictRank, scoreRank];
    };
    const sorted = verdicts.slice().sort((a, b) => {
      const ra = rankKey(a);
      const rb = rankKey(b);
      for (let i = 0; i < ra.length; i++) {
        if (ra[i] !== rb[i]) return ra[i] - rb[i];
      }
      return a.title.localeCompare(b.title);
    });

    // Escape markdown table cell content: pipes and newlines break rows.
    const cell = (s: string) =>
      s.replace(/\|/g, "\\|").replace(/\r?\n/g, " ").trim();
    const yr = (v: Verdict) => v.year ? String(v.year).slice(0, 4) : "—";
    const sc = (v: Verdict) => v.score !== null ? String(v.score) : "—";
    const laneShort = (v: Verdict) => {
      if (!v.lane) return "—";
      const map: Record<string, string> = {
        "prestige drama": "prestige",
        "atmospheric horror / mystery": "horror/mystery",
        "high-concept / weird": "high-concept",
        "mainstream event": "mainstream",
      };
      return map[v.lane] ?? v.lane;
    };

    const lines: string[] = [
      `# TV Recommendations (${rec.browseSort})`,
      "",
      `${rec.watchCount} Watch · ${rec.skipCount} Skip · ${rec.unparsedCount} Unparsed · ${rec.total} total`,
      "",
      "| Verdict | Score | Title | Year | Lane |",
      "|---|---:|---|---|---|",
    ];
    for (const v of sorted) {
      lines.push(
        "| " + [
          v.verdict,
          sc(v),
          cell(v.title),
          yr(v),
          laneShort(v),
        ].join(" | ") + " |",
      );
    }
    lines.push("");
    lines.push("## Reasoning");
    lines.push("");
    for (const v of sorted) {
      const tag = v.verdict === "Watch"
        ? "✓"
        : v.verdict === "Skip"
        ? "✗"
        : "?";
      lines.push(`- ${tag} **${v.title}** (${yr(v)}) — ${v.reason}`);
    }
    lines.push("");
    if (rec.unparsedCount > 0) {
      lines.push(
        `_${rec.unparsedCount} response(s) could not be parsed as JSON; raw payloads are on the recommendations resource._`,
      );
      lines.push("");
    }

    return {
      markdown: lines.join("\n"),
      json: {
        status,
        browseSort: rec.browseSort,
        total: rec.total,
        watch: rec.watchCount,
        skip: rec.skipCount,
        unparsed: rec.unparsedCount,
        verdicts: sorted.map((v) => ({
          tmdbId: v.tmdbId,
          title: v.title,
          year: v.year,
          verdict: v.verdict,
          lane: v.lane,
          score: v.score,
          reason: v.reason,
        })),
      },
    };
  },
};
