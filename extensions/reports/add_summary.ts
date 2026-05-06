export const report = {
  name: "@keeb/mms/add-summary",
  description:
    "Summarize an add / add_from_source invocation — lists torrents/NZBs accepted and items that failed to add",
  scope: "method" as const,
  labels: ["downloader", "add"],
  // deno-lint-ignore no-explicit-any
  execute: async (context: any) => {
    const modelName = context.definition.name;
    const method = context.methodName;
    const status = context.executionStatus;
    const modelType = context.modelType;
    const modelId = context.definition.id;

    const handle = (context.dataHandles ?? []).find(
      // deno-lint-ignore no-explicit-any
      (h: any) => h?.metadata?.tags?.specName === "add_batch",
    );

    if (!handle) {
      return {
        markdown:
          `# Add Summary: ${modelName}.${method} (${status})\n\nNo add_batch resource produced.\n`,
        json: { status, added: 0, failed: 0 },
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
          `# Add Summary: ${modelName}.${method} (${status})\n\nadd_batch content unavailable.\n`,
        json: { status, added: 0, failed: 0 },
      };
    }

    const batch = JSON.parse(new TextDecoder().decode(raw));
    // deno-lint-ignore no-explicit-any
    const added: any[] = batch.added ?? [];
    // deno-lint-ignore no-explicit-any
    const failed: any[] = batch.failed ?? [];
    const source: string = batch.source ?? "unknown";

    const lines = [
      `# Add Summary: ${modelName}.${method} (${status})`,
      "",
      `**${added.length} added**, ${failed.length} failed — source: \`${source}\``,
      "",
    ];

    if (added.length > 0) {
      lines.push(`## Added (${added.length})`);
      for (const a of added) {
        const proto = a.protocol ? ` _${a.protocol}_` : "";
        const id = a.id ? ` (id ${a.id})` : "";
        lines.push(`- ${a.name}${proto}${id}`);
      }
      lines.push("");
    }

    if (failed.length > 0) {
      lines.push(`## Failed (${failed.length})`);
      for (const f of failed) {
        const label = f.show
          ? `${f.show}${f.episode ? ` — ${f.episode}` : ""}`
          : (f.uri ?? "<unknown>");
        lines.push(`- ${label}: ${f.reason}`);
      }
      lines.push("");
    }

    return {
      markdown: lines.join("\n"),
      json: {
        status,
        source,
        added: added.length,
        failed: failed.length,
        // deno-lint-ignore no-explicit-any
        addedItems: added.map((a: any) => ({
          id: a.id,
          name: a.name,
          protocol: a.protocol,
          backend: a.backend,
        })),
        // deno-lint-ignore no-explicit-any
        failedItems: failed.map((f: any) => ({
          show: f.show,
          episode: f.episode,
          uri: f.uri,
          reason: f.reason,
        })),
      },
    };
  },
};
