import { z } from "npm:zod@4";

// --- Transmission RPC helpers ---

async function transmissionRpc(
  url: string,
  method: string,
  args: Record<string, unknown>,
  auth?: { user: string; password: string },
  sessionId?: string,
): Promise<{ result: Record<string, unknown>; sessionId: string }> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };
  if (sessionId) headers["X-Transmission-Session-Id"] = sessionId;
  if (auth) {
    headers["Authorization"] = "Basic " + btoa(`${auth.user}:${auth.password}`);
  }

  const resp = await fetch(`${url}/transmission/rpc`, {
    method: "POST",
    headers,
    body: JSON.stringify({ method, arguments: args }),
  });

  if (resp.status === 409) {
    const newSessionId = resp.headers.get("X-Transmission-Session-Id");
    if (!newSessionId) throw new Error("Transmission 409 without session id");
    return transmissionRpc(url, method, args, auth, newSessionId);
  }

  if (!resp.ok) {
    const body = await resp.text();
    throw new Error(
      `Transmission RPC ${method} failed (${resp.status}): ${body}`,
    );
  }

  const json = await resp.json();
  return {
    result: json as Record<string, unknown>,
    sessionId: sessionId ?? resp.headers.get("X-Transmission-Session-Id") ?? "",
  };
}

// --- Schemas ---

const GlobalArgsSchema = z.object({
  transmissionUrl: z
    .string()
    .optional()
    .describe("Base URL for Transmission RPC (e.g. http://host:9091)"),
  transmissionUser: z.string().optional().describe("Transmission RPC username"),
  transmissionPassword: z
    .string()
    .optional()
    .meta({ sensitive: true })
    .describe("Transmission RPC password"),
  sabnzbdUrl: z
    .string()
    .optional()
    .describe("Base URL for SABnzbd API (e.g. http://host:8080)"),
  sabnzbdApiKey: z
    .string()
    .optional()
    .meta({ sensitive: true })
    .describe("SABnzbd API key"),
});

const DownloadSchema = z.object({
  id: z.string().describe("Backend-specific download identifier"),
  name: z.string().describe("Download name / torrent name"),
  uri: z.string().describe("Original magnet or NZB URI"),
  protocol: z.enum(["torrent", "usenet"]).describe("Download protocol"),
  status: z.string().describe(
    "Current status (added, downloading, seeding, completed, error)",
  ),
  backend: z.string().describe("Backend that handles this download"),
  addedAt: z.iso.datetime().describe("When the download was added"),
  completedAt: z.iso.datetime().optional().describe(
    "When the download completed",
  ),
  progress: z.number().optional().describe("Download progress 0.0 to 1.0"),
});

const AddBatchItemSchema = z.object({
  id: z.string().describe("Backend-assigned download id"),
  name: z.string().describe("Torrent / NZB name as reported by backend"),
  uri: z.string().describe("Original magnet or NZB URI"),
  protocol: z.enum(["torrent", "usenet"]).describe("Download protocol"),
  backend: z.string().describe("Backend that accepted the download"),
});

const AddBatchFailureSchema = z.object({
  uri: z.string().optional().describe("URI that failed to add, when known"),
  show: z.string().optional().describe("Show name from the source episode"),
  episode: z.string().optional().describe("Episode identifier from the source"),
  reason: z.string().describe("Human-readable failure reason"),
});

const AddBatchSchema = z.object({
  source: z.string().describe(
    "Origin of this add batch (e.g. 'add' for single uri, or upstream model name for add_from_source)",
  ),
  added: z.array(AddBatchItemSchema).describe("Downloads accepted by backends"),
  failed: z.array(AddBatchFailureSchema).describe("Items that failed to add"),
  count: z.number().describe("Total items attempted"),
  timestamp: z.iso.datetime().describe("When this add invocation completed"),
});

// --- Model ---

export const model = {
  type: "@keeb/mms/downloader",
  version: "2026.04.21.1",
  upgrades: [
    {
      fromVersion: "2026.03.28.1",
      toVersion: "2026.04.02.3",
      description: "Add remove method for selective torrent deletion",
      upgradeAttributes: (old: Record<string, unknown>) => old,
    },
    {
      fromVersion: "2026.04.02.3",
      toVersion: "2026.04.11.1",
      description:
        "Add add_from_source fan-out method — reads a source model's " +
        "batch/current and adds every episode in one execution. Replaces " +
        "workflow-level forEach over data.* (which can't await Promises " +
        "in forEach.in expansion).",
      upgradeAttributes: (old: Record<string, unknown>) => old,
    },
    {
      fromVersion: "2026.04.11.1",
      toVersion: "2026.04.14.1",
      description:
        "Update add_from_source to read the source model's `episodes` " +
        "resource (renamed from `batch`, now the sole output). Upstream " +
        "source/dedup models no longer write per-episode factory resources.",
      upgradeAttributes: (old: Record<string, unknown>) => old,
    },
    {
      fromVersion: "2026.04.14.1",
      toVersion: "2026.04.21.1",
      description:
        "add and add_from_source now write a single aggregate add_batch " +
        "resource per invocation instead of one download record per " +
        "episode. Per-torrent tracking is owned exclusively by the status " +
        "method. Eliminates the factory-per-episode orphan-data pattern.",
      upgradeAttributes: (old: Record<string, unknown>) => old,
    },
  ],
  reports: ["@keeb/mms/download-status", "@keeb/mms/add-summary"],
  globalArguments: GlobalArgsSchema,
  resources: {
    download: {
      description: "Tracked download state",
      schema: DownloadSchema,
      lifetime: "infinite" as const,
      garbageCollection: 20,
    },
    add_batch: {
      description:
        "Per-invocation summary of an add call — added/failed items and counts. Aggregate shape; does not track per-torrent lifecycle (see download spec for that).",
      schema: AddBatchSchema,
      lifetime: "infinite" as const,
      garbageCollection: 20,
    },
  },
  methods: {
    add: {
      description:
        "Add a download to the appropriate backend (Transmission for torrents, SABnzbd for usenet)",
      arguments: z.object({
        uri: z.string().describe("Magnet URI or NZB URL"),
        protocol: z
          .enum(["torrent", "usenet"])
          .describe("Download protocol"),
        downloadDir: z
          .string()
          .optional()
          .describe("Override download directory"),
      }),
      execute: async (
        args: {
          uri: string;
          protocol: "torrent" | "usenet";
          downloadDir?: string;
        },
        // deno-lint-ignore no-explicit-any
        context: any,
      ) => {
        const now = new Date().toISOString();
        // deno-lint-ignore no-explicit-any
        const added: any[] = [];
        // deno-lint-ignore no-explicit-any
        const failed: any[] = [];

        if (args.protocol === "torrent") {
          const { transmissionUrl, transmissionUser, transmissionPassword } =
            context.globalArgs;
          if (!transmissionUrl) {
            throw new Error(
              "transmissionUrl is required for torrent downloads",
            );
          }

          const rpcArgs: Record<string, unknown> = { filename: args.uri };
          if (args.downloadDir) rpcArgs["download-dir"] = args.downloadDir;

          const auth = transmissionUser && transmissionPassword
            ? { user: transmissionUser, password: transmissionPassword }
            : undefined;

          try {
            const { result } = await transmissionRpc(
              transmissionUrl,
              "torrent-add",
              rpcArgs,
              auth,
            );
            // deno-lint-ignore no-explicit-any
            const t = (result as any).arguments?.["torrent-added"] ??
              // deno-lint-ignore no-explicit-any
              (result as any).arguments?.["torrent-duplicate"];
            added.push({
              id: String(t?.id ?? t?.hashString ?? "unknown"),
              name: t?.name ?? args.uri.slice(0, 80),
              uri: args.uri,
              protocol: "torrent",
              backend: "transmission",
            });
          } catch (err) {
            failed.push({ uri: args.uri, reason: String(err) });
          }
        } else if (args.protocol === "usenet") {
          const { sabnzbdUrl, sabnzbdApiKey } = context.globalArgs;
          if (!sabnzbdUrl || !sabnzbdApiKey) {
            throw new Error(
              "sabnzbdUrl and sabnzbdApiKey are required for usenet downloads",
            );
          }

          const url = new URL(`${sabnzbdUrl}/api`);
          url.searchParams.set("mode", "addurl");
          url.searchParams.set("name", args.uri);
          url.searchParams.set("apikey", sabnzbdApiKey);
          url.searchParams.set("output", "json");

          try {
            const resp = await fetch(url.toString());
            if (!resp.ok) {
              throw new Error(
                `SABnzbd addurl failed (${resp.status}): ${await resp.text()}`,
              );
            }
            // deno-lint-ignore no-explicit-any
            const json: any = await resp.json();
            const nzoId = json.nzo_ids?.[0] ?? "unknown";
            added.push({
              id: nzoId,
              name: args.uri.split("/").pop() ?? args.uri.slice(0, 80),
              uri: args.uri,
              protocol: "usenet",
              backend: "sabnzbd",
            });
          } catch (err) {
            failed.push({ uri: args.uri, reason: String(err) });
          }
        } else {
          throw new Error(`Unsupported protocol: ${args.protocol}`);
        }

        const handle = await context.writeResource("add_batch", "current", {
          source: "add",
          added,
          failed,
          count: added.length + failed.length,
          timestamp: now,
        });
        return { dataHandles: [handle] };
      },
    },

    add_from_source: {
      description:
        "Read a source model's `episodes` resource and add every item in its episodes array as a torrent. Fan-out alternative to workflow-level forEach.",
      arguments: z.object({
        sourceModel: z
          .string()
          .describe(
            "Name of the upstream model whose episodes resource to read (e.g. 'dedup')",
          ),
      }),
      execute: async (
        args: { sourceModel: string },
        // deno-lint-ignore no-explicit-any
        context: any,
      ) => {
        const { transmissionUrl, transmissionUser, transmissionPassword } =
          context.globalArgs;
        if (!transmissionUrl) {
          throw new Error(
            "transmissionUrl is required for torrent downloads",
          );
        }

        const records = await context.queryData(
          `modelName == "${args.sourceModel}" && specName == "episodes" && has(attributes.episodes)`,
        );
        if (records.length === 0) {
          context.logger.info(
            `No episodes resource found for "${args.sourceModel}" — has it been run yet?`,
          );
          const handle = await context.writeResource("add_batch", "current", {
            source: args.sourceModel,
            added: [],
            failed: [],
            count: 0,
            timestamp: new Date().toISOString(),
          });
          return { dataHandles: [handle] };
        }
        // deno-lint-ignore no-explicit-any
        const episodes: any[] = records[0].attributes.episodes ?? [];
        if (episodes.length === 0) {
          context.logger.info(
            `Source "${args.sourceModel}" batch is empty`,
          );
          const handle = await context.writeResource("add_batch", "current", {
            source: args.sourceModel,
            added: [],
            failed: [],
            count: 0,
            timestamp: new Date().toISOString(),
          });
          return { dataHandles: [handle] };
        }

        context.logger.info(
          `Adding ${episodes.length} torrents from "${args.sourceModel}"`,
        );

        const auth = transmissionUser && transmissionPassword
          ? { user: transmissionUser, password: transmissionPassword }
          : undefined;

        // deno-lint-ignore no-explicit-any
        const added: any[] = [];
        // deno-lint-ignore no-explicit-any
        const failed: any[] = [];

        for (const ep of episodes) {
          if (!ep.magnet) {
            context.logger.warn(
              `SKIP ${ep.show ?? "unknown"} ${ep.episode ?? ""}: no magnet`,
            );
            failed.push({
              show: ep.show,
              episode: ep.episode,
              reason: "no magnet",
            });
            continue;
          }

          try {
            const { result } = await transmissionRpc(
              transmissionUrl,
              "torrent-add",
              { filename: ep.magnet },
              auth,
            );

            // deno-lint-ignore no-explicit-any
            const t = (result as any).arguments?.["torrent-added"] ??
              // deno-lint-ignore no-explicit-any
              (result as any).arguments?.["torrent-duplicate"];
            const torrentName = t?.name ?? ep.rawTitle ??
              `${ep.show}-${ep.episode}`;
            const torrentId = String(
              t?.id ?? t?.hashString ?? "unknown",
            );
            added.push({
              id: torrentId,
              name: torrentName,
              uri: ep.magnet,
              protocol: "torrent",
              backend: "transmission",
            });
            context.logger.info(
              `  [${added.length}/${episodes.length}] ${torrentName}`,
            );
          } catch (err) {
            failed.push({
              uri: ep.magnet,
              show: ep.show,
              episode: ep.episode,
              reason: String(err),
            });
            context.logger.error(
              `  FAIL ${ep.show ?? "?"} ${ep.episode ?? "?"}: ${err}`,
            );
          }
        }

        context.logger.info(
          `Done: ${added.length} added, ${failed.length} failed, ${episodes.length} total`,
        );

        const handle = await context.writeResource("add_batch", "current", {
          source: args.sourceModel,
          added,
          failed,
          count: episodes.length,
          timestamp: new Date().toISOString(),
        });
        return { dataHandles: [handle] };
      },
    },

    status: {
      description:
        "Query all backends for current download status (factory: one resource per download)",
      arguments: z.object({}),
      // deno-lint-ignore no-explicit-any
      execute: async (_args: Record<string, never>, context: any) => {
        // deno-lint-ignore no-explicit-any
        const handles: any[] = [];
        const now = new Date().toISOString();
        const {
          transmissionUrl,
          transmissionUser,
          transmissionPassword,
          sabnzbdUrl,
          sabnzbdApiKey,
        } = context.globalArgs;

        // --- Transmission ---
        if (transmissionUrl) {
          const auth = transmissionUser && transmissionPassword
            ? { user: transmissionUser, password: transmissionPassword }
            : undefined;

          const { result } = await transmissionRpc(
            transmissionUrl,
            "torrent-get",
            {
              fields: [
                "id",
                "name",
                "status",
                "percentDone",
                "magnetLink",
                "doneDate",
                "downloadDir",
              ],
            },
            auth,
          );

          // deno-lint-ignore no-explicit-any
          const torrents: any[] = (result as any).arguments?.torrents ?? [];

          for (const t of torrents) {
            const statusMap: Record<number, string> = {
              0: "stopped",
              1: "check_wait",
              2: "checking",
              3: "download_wait",
              4: "downloading",
              5: "seed_wait",
              6: "seeding",
            };
            const status = statusMap[t.status] ?? "unknown";
            const idSuffix = `-${t.id}`;
            const instanceName =
              slugify(t.name).slice(0, 80 - idSuffix.length) + idSuffix;

            const handle = await context.writeResource(
              "download",
              instanceName,
              {
                id: String(t.id),
                name: t.name,
                uri: t.magnetLink ?? "",
                protocol: "torrent" as const,
                status,
                backend: "transmission",
                addedAt: now,
                completedAt: t.doneDate && t.doneDate > 0
                  ? new Date(t.doneDate * 1000).toISOString()
                  : undefined,
                progress: t.percentDone,
              },
            );
            handles.push(handle);
          }
        }

        // --- SABnzbd ---
        if (sabnzbdUrl && sabnzbdApiKey) {
          const url = new URL(`${sabnzbdUrl}/api`);
          url.searchParams.set("mode", "queue");
          url.searchParams.set("apikey", sabnzbdApiKey);
          url.searchParams.set("output", "json");

          const resp = await fetch(url.toString());
          if (resp.ok) {
            // deno-lint-ignore no-explicit-any
            const json: any = await resp.json();
            // deno-lint-ignore no-explicit-any
            const slots: any[] = json.queue?.slots ?? [];

            for (const s of slots) {
              const instanceName = `usenet-${
                slugify(s.nzo_id ?? s.filename ?? "unknown")
              }`;
              const handle = await context.writeResource(
                "download",
                instanceName,
                {
                  id: s.nzo_id ?? "unknown",
                  name: s.filename ?? "unknown",
                  uri: "",
                  protocol: "usenet" as const,
                  status: s.status?.toLowerCase() ?? "unknown",
                  backend: "sabnzbd",
                  addedAt: now,
                  progress: parseFloat(s.percentage ?? "0") / 100,
                },
              );
              handles.push(handle);
            }
          }
        }

        return { dataHandles: handles };
      },
    },

    remove: {
      description: "Remove specific torrents from Transmission by name filter",
      arguments: z.object({
        filter: z
          .string()
          .describe(
            "Substring to match against torrent names (case-insensitive)",
          ),
        deleteData: z
          .boolean()
          .default(true)
          .describe("Also delete downloaded files from disk"),
      }),
      execute: async (
        args: { filter: string; deleteData: boolean },
        // deno-lint-ignore no-explicit-any
        context: any,
      ) => {
        const { transmissionUrl, transmissionUser, transmissionPassword } =
          context.globalArgs;
        if (!transmissionUrl) {
          throw new Error("transmissionUrl is required");
        }

        const auth = transmissionUser && transmissionPassword
          ? { user: transmissionUser, password: transmissionPassword }
          : undefined;

        const { result, sessionId } = await transmissionRpc(
          transmissionUrl,
          "torrent-get",
          { fields: ["id", "name", "status", "percentDone"] },
          auth,
        );

        // deno-lint-ignore no-explicit-any
        const torrents: any[] = (result as any).arguments?.torrents ?? [];
        const filterLower = args.filter.toLowerCase();

        const idsToRemove: number[] = [];
        const namesRemoved: string[] = [];

        for (const t of torrents) {
          const name: string = (t.name ?? "").toLowerCase();
          if (!name.includes(filterLower)) continue;
          idsToRemove.push(t.id);
          namesRemoved.push(t.name);
        }

        if (idsToRemove.length === 0) {
          context.logger.info(
            `No torrents matched filter "${args.filter}"`,
          );
          const handle = await context.writeResource(
            "download",
            "remove-result",
            {
              id: "remove",
              name: `No torrents matched "${args.filter}"`,
              uri: "",
              protocol: "torrent" as const,
              status: "cleaned",
              backend: "transmission",
              addedAt: new Date().toISOString(),
            },
          );
          return { dataHandles: [handle] };
        }

        await transmissionRpc(
          transmissionUrl,
          "torrent-remove",
          { ids: idsToRemove, "delete-local-data": args.deleteData },
          auth,
          sessionId,
        );

        context.logger.info(
          `Removed ${idsToRemove.length} torrents matching "${args.filter}"`,
        );
        for (const n of namesRemoved) {
          context.logger.info(`  removed: ${n}`);
        }

        const handle = await context.writeResource(
          "download",
          "remove-result",
          {
            id: "remove",
            name:
              `Removed ${idsToRemove.length} torrents matching "${args.filter}"`,
            uri: "",
            protocol: "torrent" as const,
            status: "cleaned",
            backend: "transmission",
            addedAt: new Date().toISOString(),
          },
        );
        return { dataHandles: [handle] };
      },
    },

    clean: {
      description: "Remove completed downloads from backends",
      arguments: z.object({
        completedOnly: z
          .boolean()
          .default(true)
          .describe("Only remove completed downloads (true) or all (false)"),
        deleteData: z
          .boolean()
          .default(false)
          .describe("Also delete downloaded files from disk"),
      }),
      execute: async (
        args: { completedOnly: boolean; deleteData: boolean },
        // deno-lint-ignore no-explicit-any
        context: any,
      ) => {
        const {
          transmissionUrl,
          transmissionUser,
          transmissionPassword,
          sabnzbdUrl,
          sabnzbdApiKey,
        } = context.globalArgs;

        let removedCount = 0;

        // --- Transmission ---
        if (transmissionUrl) {
          const auth = transmissionUser && transmissionPassword
            ? { user: transmissionUser, password: transmissionPassword }
            : undefined;

          const { result, sessionId } = await transmissionRpc(
            transmissionUrl,
            "torrent-get",
            { fields: ["id", "name", "status", "percentDone"] },
            auth,
          );

          // deno-lint-ignore no-explicit-any
          const torrents: any[] = (result as any).arguments?.torrents ?? [];

          const idsToRemove: number[] = [];
          for (const t of torrents) {
            if (args.completedOnly) {
              // status 0 = stopped (completed), or percentDone == 1
              if (t.status === 0 || t.percentDone === 1) {
                idsToRemove.push(t.id);
              }
            } else {
              idsToRemove.push(t.id);
            }
          }

          if (idsToRemove.length > 0) {
            await transmissionRpc(
              transmissionUrl,
              "torrent-remove",
              { ids: idsToRemove, "delete-local-data": args.deleteData },
              auth,
              sessionId,
            );
            removedCount += idsToRemove.length;
            context.logger.info(
              `Removed ${idsToRemove.length} torrents from Transmission`,
            );
          }
        }

        // --- SABnzbd history cleanup ---
        if (sabnzbdUrl && sabnzbdApiKey) {
          const url = new URL(`${sabnzbdUrl}/api`);
          url.searchParams.set("mode", "history");
          url.searchParams.set("name", "delete");
          url.searchParams.set("value", "completed");
          url.searchParams.set("apikey", sabnzbdApiKey);
          url.searchParams.set("output", "json");

          const resp = await fetch(url.toString());
          if (resp.ok) {
            context.logger.info("Cleared SABnzbd completed history");
            removedCount++;
          }
        }

        const handle = await context.writeResource(
          "download",
          "cleanup-result",
          {
            id: "cleanup",
            name: `Cleaned ${removedCount} downloads`,
            uri: "",
            protocol: "torrent" as const,
            status: "cleaned",
            backend: "all",
            addedAt: new Date().toISOString(),
          },
        );
        return { dataHandles: [handle] };
      },
    },
  },
};

// --- Utilities ---

function slugify(s: string): string {
  return s
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
}
