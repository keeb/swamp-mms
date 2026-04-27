/**
 * `@keeb/mms/download-status` report — render the current Transmission and
 * SABnzbd download state as a markdown list grouped by status.
 */

/** Swamp report definition for `@keeb/mms/download-status`. */
export const report = {
  name: "@keeb/mms/download-status",
  description: "Show active downloads with name, progress, and state",
  scope: "method",
  labels: ["status"],
  // deno-lint-ignore no-explicit-any
  execute: async (context: any) => {
    const handles = context.dataHandles || [];

    if (handles.length === 0) {
      return {
        markdown: "# Downloads\n\nNo active downloads.",
        json: { total: 0, downloads: [] },
      };
    }

    const modelType = context.modelType;
    const modelId = context.definition.id;
    // deno-lint-ignore no-explicit-any
    const downloads: any[] = [];
    for (const h of handles) {
      const raw = await context.dataRepository.getContent(
        modelType,
        modelId,
        h.name,
      );
      if (!raw) continue;
      const c = JSON.parse(new TextDecoder().decode(raw));
      downloads.push({
        name: c.name,
        status: c.status,
        progress: c.progress ?? null,
        backend: c.backend,
      });
    }

    downloads.sort((a, b) => {
      const order: Record<string, number> = {
        downloading: 0,
        added: 1,
        seeding: 2,
        completed: 3,
        error: -1,
      };
      return (order[a.status] ?? 99) - (order[b.status] ?? 99);
    });

    const lines = ["# Downloads", "", `**${downloads.length}** active`, ""];
    // deno-lint-ignore no-explicit-any
    const byStatus: Record<string, any[]> = {};
    for (const d of downloads) {
      const s = d.status;
      if (!byStatus[s]) byStatus[s] = [];
      byStatus[s].push(d);
    }

    for (const [status, items] of Object.entries(byStatus)) {
      lines.push(`### ${status} (${items.length})`);
      for (const d of items) {
        const pct = d.progress != null
          ? ` ${Math.round(d.progress * 100)}%`
          : "";
        lines.push(`- ${d.name}${pct}`);
      }
      lines.push("");
    }

    return {
      markdown: lines.join("\n"),
      json: {
        total: downloads.length,
        byStatus: Object.fromEntries(
          Object.entries(byStatus).map(([k, v]) => [k, v.length]),
        ),
        downloads,
      },
    };
  },
};
