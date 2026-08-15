import { firstValueFrom, toArray } from "rxjs";
import { describe, expect, it } from "vitest";
import { defineGraph, defineState } from "@langgraph-toolkit/core/legacy";
import { GraphRegistry } from "@langgraph-toolkit/core/runtime";
import {
  BoundGraphService,
  createNestJSAdapter,
  GraphHttpExceptionFilter,
  GraphService,
  LangGraphModule,
} from "../dist/index.js";

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

    const events = await firstValueFrom(service.streamAsObservable("ping", {}).pipe(toArray()));
    expect(events.map((event) => event.type)).toEqual(expect.arrayContaining(["node_start", "node_end", "edge"]));

    const bound = service.bind<{ done: boolean }, Record<string, never>>("ping");
    expect(bound).toBeInstanceOf(BoundGraphService);
    expect((await bound.run({})).state.done).toBe(true);
  });

  it("returns a Nest-compatible dynamic module without owning framework wiring", () => {
    const dynamicModule = LangGraphModule.forRoot({ runtime: makeRegistry(), global: true });
    expect(dynamicModule.global).toBe(true);
    expect(dynamicModule.exports).toContain(GraphService);
    expect(dynamicModule.providers?.[0]).toMatchObject({ provide: GraphService });
  });

  it("exposes a typed HTTP filter for toolkit errors", () => {
    expect(GraphHttpExceptionFilter).toBeDefined();
  });

  it("creates a module and service from an existing registry", () => {
    const adapter = createNestJSAdapter(makeRegistry());
    expect(adapter.service.list()).toEqual(["ping"]);
    expect(adapter.module.module).toBe(LangGraphModule);
  });
});
