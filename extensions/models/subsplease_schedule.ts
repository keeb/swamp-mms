import { z } from "npm:zod@4";

const GlobalArgsSchema = z.object({});

const ShowSchema = z.object({
  title: z.string(),
  page: z.string(),
  imageUrl: z.string(),
  day: z.string(),
  time: z.string(),
});

function slugify(title: string) {
  return title
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

interface ScheduleEntry {
  title: string;
  page?: string;
  image_url?: string;
  time?: string;
}

export const model = {
  type: "@keeb/subsplease-schedule",
  version: "2026.04.11.1",
  globalArguments: GlobalArgsSchema,
  resources: {
    show: {
      description: "A show on the SubsPlease weekly schedule",
      schema: ShowSchema,
      lifetime: "infinite" as const,
      garbageCollection: 3,
    },
  },
  methods: {
    fetch: {
      description:
        "Fetch the SubsPlease weekly schedule, one resource per scheduled show",
      arguments: z.object({
        tz: z.string().default("UTC").describe("Timezone for schedule times"),
      }),
      // deno-lint-ignore no-explicit-any
      execute: async (args: any, context: any) => {
        const url = `https://subsplease.org/api/?f=schedule&tz=${
          encodeURIComponent(args.tz)
        }`;
        context.logger.info(`Fetching ${url}`);

        const res = await fetch(url);
        if (!res.ok) {
          throw new Error(
            `SubsPlease API error: ${res.status} ${res.statusText}`,
          );
        }

        const json = await res.json();
        const schedule = json.schedule as Record<string, ScheduleEntry[]>;
        if (!schedule || typeof schedule !== "object") {
          throw new Error(
            "SubsPlease schedule response missing schedule field",
          );
        }

        const handles = [];
        const seen = new Set<string>();

        for (const [day, shows] of Object.entries(schedule)) {
          for (const show of shows) {
            const slug = show.page && show.page.length > 0
              ? show.page
              : slugify(show.title);
            if (seen.has(slug)) continue;
            seen.add(slug);

            const handle = await context.writeResource("show", slug, {
              title: show.title,
              page: show.page ?? "",
              imageUrl: show.image_url ?? "",
              day,
              time: show.time ?? "",
            });
            handles.push(handle);
          }
        }

        context.logger.info(
          `Wrote ${handles.length} shows from SubsPlease schedule`,
        );

        return { dataHandles: handles };
      },
    },
  },
};
