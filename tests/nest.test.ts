import { describe, expect, it } from "vitest";
import { GraphRegistry, defineGraph, defineState } from "@langgraph-toolkit/core";
import { GraphService, LangGraphModule } from "../src/index.js";

function makeRegistry(): GraphRegistry {
  const registry = new GraphRegistry();
  registry.register(
    defineGraph({
      name: "ping",
      state: defineState({ done: false }),
      nodes: {
        finish: async () => ({ done: true }),
      },
    }),
  );
  return registry;
}

describe("adapter-nestjs", () => {
  it("wraps the registry with typed service methods", async () => {
    const service = new GraphService(makeRegistry());
    expect(service.list()).toEqual(["ping"]);
    expect(service.has("ping")).toBe(true);

    const result = await service.run("ping", {});
    expect(result.state.done).toBe(true);

    const events = [];
    for await (const event of service.streamAsObservable("ping", {})) events.push(event.type);
    expect(events).toEqual(expect.arrayContaining(["node_start", "node_end", "edge"]));
  });

  it("returns a Nest-compatible dynamic module without owning framework wiring", () => {
    const dynamicModule = LangGraphModule.forRoot({ runtime: makeRegistry(), global: true });
    expect(dynamicModule.global).toBe(true);
    expect(dynamicModule.exports).toContain(GraphService);
    expect(dynamicModule.providers?.[0]).toMatchObject({ provide: GraphService });
  });
});
