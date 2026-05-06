export const report = {
  name: "@keeb/mms/discovery-summary",
  description: "Summarize discovered content from source searches",
  scope: "method" as const,
  labels: ["discovery"],
  // deno-lint-ignore no-explicit-any
  execute: async (context: any) => {
    const handles = context.dataHandles ?? [];
    const modelName = context.definition.name;
    const method = context.methodName;
    const status = context.executionStatus;
    const modelType = context.modelType;
    const modelId = context.definition.id;

    // The source model writes a single `episodes` resource whose attributes
    // contain the full run's episodes array.
    const items: {
      show: string;
      episode?: string;
      provider: string;
      resolution: string;
      rawTitle?: string;
    }[] = [];

    for (const handle of handles) {
      const content = await context.dataRepository.getContent(
        modelType,
        modelId,
        handle.name,
      );
      if (!content) continue;
      try {
        const data = JSON.parse(new TextDecoder().decode(content));
        // deno-lint-ignore no-explicit-any
        for (const ep of (data.episodes ?? []) as any[]) {
          items.push({
            show: ep.show ?? "unknown",
            episode: ep.episode,
            provider: ep.provider ?? "unknown",
            resolution: ep.resolution ?? "?",
            rawTitle: ep.rawTitle,
          });
        }
      } catch {
        /* skip */
      }
    }

    // Split into structured (have episode) and raw (nyaa, no episode)
    const structured = items.filter((i) => i.episode);
    const raw = items.filter((i) => !i.episode);

    // Group structured by show
    const byShow = new Map<string, typeof structured>();
    for (const ep of structured) {
      if (!byShow.has(ep.show)) byShow.set(ep.show, []);
      byShow.get(ep.show)!.push(ep);
    }
    for (const eps of byShow.values()) {
      eps.sort(
        (a, b) => (parseInt(a.episode!) || 0) - (parseInt(b.episode!) || 0),
      );
    }

    // Build markdown
    let md = `# Discovery: ${modelName}.${method} (${status})\n\n`;
    md +=
      `**${items.length} items** (${structured.length} parsed, ${raw.length} raw)\n\n`;

    for (const [show, eps] of byShow) {
      const provider = eps[0].provider;
      const res = eps[0].resolution;
      const epNums = eps.map((e) => e.episode).join(", ");
      md += `### ${show}\n`;
      md += `- **Provider**: ${provider} @ ${res}p\n`;
      md += `- **Episodes**: ${epNums}\n`;
      md += `- **Count**: ${eps.length}\n\n`;
    }

    if (raw.length > 0) {
      md += `### Raw (unparsed — needs LLM)\n`;
      // Group by provider
      const byProvider = new Map<string, string[]>();
      for (const r of raw) {
        const p = r.provider;
        if (!byProvider.has(p)) byProvider.set(p, []);
        byProvider.get(p)!.push(r.rawTitle ?? r.show);
      }
      for (const [provider, titles] of byProvider) {
        md += `- **${provider}**: ${titles.length} items\n`;
        for (const t of titles.slice(0, 10)) {
          md += `  - ${t}\n`;
        }
        if (titles.length > 10) {
          md += `  - _(${titles.length - 10} more)_\n`;
        }
      }
      md += "\n";
    }

    return {
      markdown: md,
      json: {
        status,
        totalItems: items.length,
        parsedEpisodes: structured.length,
        rawItems: raw.length,
        shows: Object.fromEntries(
          [...byShow].map(([show, eps]) => [
            show,
            {
              provider: eps[0].provider,
              resolution: eps[0].resolution,
              count: eps.length,
              episodes: eps.map((e) => e.episode),
            },
          ]),
        ),
      },
    };
  },
};
