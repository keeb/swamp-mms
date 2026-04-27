/**
 * `@keeb/mms/dedup-summary` report — summarize the output of a dedup filter
 * run: count of new episodes grouped by show.
 */

/** Swamp report definition for `@keeb/mms/dedup-summary`. */
export const report = {
  name: "@keeb/mms/dedup-summary",
  description: "Summarize dedup filter results: what's new vs what was skipped",
  scope: "method" as const,
  labels: ["discovery", "dedup"],
  // deno-lint-ignore no-explicit-any
  execute: async (context: any) => {
    const handles = context.dataHandles ?? [];
    const modelName = context.definition.name;
    const method = context.methodName;
    const status = context.executionStatus;
    const modelType = context.modelType;
    const modelId = context.definition.id;

    const newEpisodes: { show: string; episode: string; provider: string }[] =
      [];
    for (const handle of handles) {
      const content = await context.dataRepository.getContent(
        modelType,
        modelId,
        handle.name,
      );
      if (!content) continue;
      try {
        const data = JSON.parse(new TextDecoder().decode(content));
        if (data.show && data.episode) {
          newEpisodes.push({
            show: data.show,
            episode: data.episode,
            provider: data.provider ?? "unknown",
          });
        }
      } catch { /* skip */ }
    }

    // Group by show
    const byShow = new Map<string, typeof newEpisodes>();
    for (const ep of newEpisodes) {
      if (!byShow.has(ep.show)) byShow.set(ep.show, []);
      byShow.get(ep.show)!.push(ep);
    }
    for (const eps of byShow.values()) {
      eps.sort((a, b) =>
        (parseInt(a.episode) || 0) - (parseInt(b.episode) || 0)
      );
    }

    let md = `# Dedup: ${modelName}.${method} (${status})\n\n`;

    if (newEpisodes.length === 0) {
      md += `**Nothing new.** All episodes already exist locally.\n`;
    } else {
      md += `**${newEpisodes.length} new episode(s)** to download:\n\n`;
      for (const [show, eps] of byShow) {
        const epNums = eps.map((e) => e.episode).join(", ");
        md += `- **${show}**: ${epNums}\n`;
      }
    }

    return {
      markdown: md,
      json: {
        status,
        newEpisodes: newEpisodes.length,
        shows: Object.fromEntries(
          [...byShow].map(([show, eps]) => [
            show,
            eps.map((e) => e.episode),
          ]),
        ),
      },
    };
  },
};
