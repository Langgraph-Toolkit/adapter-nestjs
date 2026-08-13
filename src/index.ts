/**
 * @langgraph-toolkit/adapter-nestjs
 *
 * NestJS binding: a DynamicModule that registers a generic GraphService
 * wrapping the core GraphRegistry. Controllers can expose graphs with Nest's
 * built-in decorators while keeping application contracts at the call site.
 *
 * Install: npm install @nestjs/common @nestjs/core @langgraph-toolkit/adapter-nestjs
 */
import type {
  DefaultGraphContracts,
  GraphContracts,
  GraphRegistry,
  JsonObject,
  RunOptions,
  RunResult,
  StepEvent,
  ToolkitRuntime,
} from "@langgraph-toolkit/core";
import { GraphRuntimeError } from "@langgraph-toolkit/core";

/** Nest-injectable wrapper around GraphRegistry with explicit generic methods. */
export class GraphService {
  constructor(private readonly registry: GraphRegistry) {}

  has(name: string): boolean {
    return this.registry.has(name);
  }

  list(): string[] {
    return this.registry.list();
  }

  run<
    TState extends object,
    TInput extends object,
    TOutput extends object = TState,
    C extends GraphContracts = DefaultGraphContracts,
    TVariables extends JsonObject = JsonObject,
    TGlobal extends JsonObject = JsonObject,
  >(
    name: string,
    input: TInput,
    opts?: RunOptions<C, TVariables, TGlobal>,
  ): Promise<RunResult<TState, TOutput, C["interrupt"], TVariables>> {
    return this.registry.run<TState, TInput, TOutput, C, TVariables, TGlobal>(name, input, opts);
  }

  stream<
    TState extends object,
    TInput extends object,
    TOutput extends object = TState,
    C extends GraphContracts = DefaultGraphContracts,
    TVariables extends JsonObject = JsonObject,
    TGlobal extends JsonObject = JsonObject,
  >(
    name: string,
    input: TInput,
    opts?: RunOptions<C, TVariables, TGlobal>,
  ): AsyncIterable<StepEvent<TState, C>> {
    return this.registry.stream<TState, TInput, TOutput, C, TVariables, TGlobal>(name, input, opts);
  }

  /** Observable bridge boundary for Nest @Sse(): async iterable remains typed. */
  streamAsObservable<
    TState extends object,
    TInput extends object,
    TOutput extends object = TState,
    C extends GraphContracts = DefaultGraphContracts,
    TVariables extends JsonObject = JsonObject,
    TGlobal extends JsonObject = JsonObject,
  >(
    name: string,
    input: TInput,
    opts?: RunOptions<C, TVariables, TGlobal>,
  ): AsyncIterable<StepEvent<TState, C>> {
    return {
      [Symbol.asyncIterator]: () => this.stream<TState, TInput, TOutput, C, TVariables, TGlobal>(name, input, opts)[Symbol.asyncIterator](),
    };
  }
}

/** Options for LangGraphModule.forRoot(). */
export interface LangGraphModuleOptions {
  /** Runtime facade holding the graphs exposed to controllers. */
  runtime?: ToolkitRuntime;
  /** Backward-compatible registry option. */
  graphs?: GraphRegistry;
  /** Optional global provider flag. */
  global?: boolean;
}

/** DynamicModule-shaped factory independent of the exact Nest version. */
export class LangGraphModule {
  static forRoot(options: LangGraphModuleOptions) {
    const graphs = options.runtime ?? options.graphs;
    if (graphs === undefined) throw new GraphRuntimeError("LangGraphModule.forRoot requires runtime or graphs.");
    return {
      module: LangGraphModule as never,
      global: options.global ?? false,
      providers: [{ provide: GraphService, useValue: new GraphService(graphs) }],
      exports: [GraphService],
    };
  }
}

export { GraphRuntimeError };
