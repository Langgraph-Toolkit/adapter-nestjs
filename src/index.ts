/**
 * @langgraph-toolkit/adapter-nestjs
 *
 * NestJS binding: a DynamicModule that registers a generic GraphService
 * wrapping the core GraphRegistry. Controllers can expose graphs with Nest's
 * built-in decorators while keeping application contracts at the call site.
 *
 * Install: npm install @nestjs/common @nestjs/core @langgraph-toolkit/adapter-nestjs
 */
import { Observable } from "rxjs";
import { ArgumentsHost, Catch, ExceptionFilter } from "@nestjs/common";
import type {
  DynamicModule,
  ModuleMetadata,
  Type,
} from "@nestjs/common";
import type {
  CompiledGraph,
  Checkpoint,
  GraphDefinition,
  DefaultGraphContracts,
  GraphContracts,
  JsonObject,
  JsonValue,
  RunOptions,
  RunResult,
  StepEvent,
} from "@langgraph-toolkit/core";
import { GraphRegistry } from "@langgraph-toolkit/core/runtime";
import {
  createGraphLifecycle,
  ToolkitRuntime,
  type GraphForkRequest,
  type GraphInvokeRequest,
  type GraphLifecycle,
  type GraphReplayRequest,
  type GraphResumeRequest,
} from "@langgraph-toolkit/core/runtime";
import { GraphRuntimeError, ToolkitError } from "@langgraph-toolkit/core";

/** JSON-safe error payload emitted by Nest SSE streams. */
export interface GraphSseError {
  readonly message: string;
  readonly code: string;
}

/** A Nest-compatible SSE message carrying one graph step or a serialized error. */
export type GraphSseMessage<TEvent extends object = JsonObject> =
  | { readonly type: string; readonly data: TEvent }
  | { readonly type: "error"; readonly data: GraphSseError };

/** JSON response envelope used by the built-in Nest graph exception filter. */
export interface GraphHttpErrorResponse {
  readonly error: GraphSseError;
}

/** Minimal response surface required by the framework-agnostic HTTP filter. */
export interface GraphHttpResponse {
  status(code: number): GraphHttpResponse;
  json(body: GraphHttpErrorResponse): GraphHttpResponse;
}

/** Map a toolkit error to a stable HTTP status without coupling the adapter to Express. */
export function graphErrorStatus(error: ToolkitError): number {
  if (error.code === "PERMISSION_DENIED") return 403;
  if (error.code === "CANCELLED") return 499;
  if (error.code === "TOKEN_BUDGET_EXCEEDED") return 429;
  if (error.code === "SAFETY_LIMIT_EXCEEDED") return 408;
  return 500;
}

/** Serialize graph failures for regular Nest HTTP routes. */
@Catch(ToolkitError)
export class GraphHttpExceptionFilter implements ExceptionFilter<ToolkitError> {
  catch(error: ToolkitError, host: ArgumentsHost): void {
    const response = host.switchToHttp().getResponse<GraphHttpResponse>();
    response.status(graphErrorStatus(error)).json({
      error: { message: error.message, code: error.code },
    });
  }
}

/** A graph-bound facade that infers the same state/input/output contract across HTTP methods. */
export class BoundGraphService<
  TState extends object,
  TInput extends object,
  TOutput extends object = TState,
  C extends GraphContracts = DefaultGraphContracts,
  TVariables extends JsonObject = JsonObject,
  TGlobal extends JsonObject = JsonObject,
> {
  constructor(
    private readonly service: GraphService,
    private readonly name: string,
  ) {}

  run(
    input: TInput,
    opts?: RunOptions<C, TVariables, TGlobal>,
  ): Promise<RunResult<TState, TOutput, C["interrupt"], TVariables>> {
    return this.service.run<TState, TInput, TOutput, C, TVariables, TGlobal>(this.name, input, opts);
  }

  stream(input: TInput, opts?: RunOptions<C, TVariables, TGlobal>): AsyncIterable<StepEvent<TState, C>> {
    return this.service.stream<TState, TInput, TOutput, C, TVariables, TGlobal>(this.name, input, opts);
  }

  streamAsObservable(input: TInput, opts?: RunOptions<C, TVariables, TGlobal>): Observable<StepEvent<TState, C>> {
    return this.service.streamAsObservable<TState, TInput, TOutput, C, TVariables, TGlobal>(this.name, input, opts);
  }

  streamSse(input: TInput, opts?: RunOptions<C, TVariables, TGlobal>): Observable<GraphSseMessage<StepEvent<TState, C>>> {
    return this.service.streamSse<TState, TInput, TOutput, C, TVariables, TGlobal>(this.name, input, opts);
  }
}

/** Nest-injectable wrapper around GraphRegistry with explicit generic methods. */
export class GraphService {
  readonly lifecycle: GraphLifecycle;

  constructor(
    private readonly registry: GraphRegistry,
    private readonly closeHook?: () => Promise<void>,
  ) {
    this.lifecycle = createGraphLifecycle(registry);
  }

  async onModuleDestroy(): Promise<void> {
    await this.closeHook?.();
  }

  has(name: string): boolean {
    return this.registry.has(name);
  }

  list(): string[] {
    return this.registry.list();
  }

  /** Canonical invoke lifecycle used by Nest controllers. */
  invoke(name: string, request: GraphInvokeRequest): Promise<RunResult<JsonObject>> {
    return this.lifecycle.invoke(name, request);
  }

  /** Resume an interrupted graph from its most recent checkpoint. */
  resume(name: string, request: GraphResumeRequest): Promise<RunResult<JsonObject>> {
    return this.lifecycle.resume(name, request);
  }

  /** Abort an in-flight graph operation identified by graph and thread. */
  cancel(name: string, threadId: string): boolean {
    return this.lifecycle.cancel(name, threadId);
  }

  /** Read the latest state checkpoint for a thread. */
  state(name: string, threadId: string): Promise<Checkpoint<JsonObject> | null> {
    return this.lifecycle.state(name, threadId);
  }

  /** Read retained checkpoints in chronological order for a thread. */
  history(name: string, threadId: string): Promise<readonly Checkpoint<JsonObject>[]> {
    return this.lifecycle.history(name, threadId);
  }

  /** Run from an explicit retained checkpoint for deterministic replay. */
  replay(name: string, request: GraphReplayRequest): Promise<RunResult<JsonObject>> {
    return this.lifecycle.replay(name, request);
  }

  /** Copy one retained checkpoint into a new branch thread. */
  fork(name: string, request: GraphForkRequest): Promise<Checkpoint<JsonObject>> {
    return this.lifecycle.fork(name, request);
  }

  /** Bind one graph name once so controllers can reuse a fully typed facade. */
  bind<
    TState extends object,
    TInput extends object,
    TOutput extends object = TState,
    C extends GraphContracts = DefaultGraphContracts,
    TVariables extends JsonObject = JsonObject,
    TGlobal extends JsonObject = JsonObject,
  >(name: string): BoundGraphService<TState, TInput, TOutput, C, TVariables, TGlobal> {
    return new BoundGraphService<TState, TInput, TOutput, C, TVariables, TGlobal>(this, name);
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

  /** RxJS bridge for Nest @Sse(), with cancellation propagated to graph execution. */
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
  ): Observable<StepEvent<TState, C>> {
    return new Observable<StepEvent<TState, C>>((subscriber) => {
      const controller = new AbortController();
      const sourceSignal = opts?.signal;
      const abort = (): void => controller.abort();
      if (sourceSignal?.aborted === true) controller.abort();
      else sourceSignal?.addEventListener("abort", abort, { once: true });
      void (async () => {
        try {
          for await (const event of this.stream<TState, TInput, TOutput, C, TVariables, TGlobal>(name, input, { ...opts, signal: controller.signal })) {
            subscriber.next(event);
          }
          subscriber.complete();
        } catch (error) {
          subscriber.error(error);
        } finally {
          sourceSignal?.removeEventListener("abort", abort);
        }
      })();
      return () => controller.abort();
    });
  }

  /** Nest @Sse() friendly event envelope; no manual headers or JSON serialization required. */
  streamSse<
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
  ): Observable<GraphSseMessage<StepEvent<TState, C>>> {
    return new Observable<GraphSseMessage<StepEvent<TState, C>>>((subscriber) => {
      const stream = this.streamAsObservable<TState, TInput, TOutput, C, TVariables, TGlobal>(name, input, opts);
      const subscription = stream.subscribe({
        next: (event: StepEvent<TState, C>) => subscriber.next({ type: event.type, data: event }),
        error: (error: object) => {
          subscriber.next({ type: "error", data: serializeGraphError(error) });
          subscriber.complete();
        },
        complete: () => subscriber.complete(),
      });
      return () => subscription.unsubscribe();
    });
  }
}

function serializeGraphError(error: object): GraphSseError {
  if (error instanceof GraphRuntimeError) return { message: error.message, code: error.code };
  if (error instanceof Error) return { message: error.message, code: "GRAPH_RUNTIME_ERROR" };
  return { message: "Graph stream failed.", code: "GRAPH_RUNTIME_ERROR" };
}

/** Options for LangGraphModule.forRoot(). */
export interface LangGraphModuleOptions {
  /** Runtime facade holding the graphs exposed to controllers. */
  runtime?: ToolkitRuntime;
  /** Backward-compatible registry option. */
  graphs?: GraphRegistry;
  /** Optional global provider flag. */
  global?: boolean;
  /** Optional resource cleanup invoked when Nest destroys the application. */
  close?: () => Promise<void>;
}

/** Zero-config options for createNestJSAdapter(). */
export interface NestJSAdapterOptions extends Pick<LangGraphModuleOptions, "global" | "close"> {}

/** NestJS resource returned by createNestJSAdapter(). */
export interface NestJSAdapter<TGraph extends object = object> {
  readonly graph: TGraph;
  readonly runtime: GraphRegistry;
  /** Canonical graph lifecycle for controllers that expose HTTP routes. */
  readonly lifecycle: GraphLifecycle;
  readonly service: GraphService;
  readonly module: DynamicModule;
}

type LangGraphInjectionToken = string | symbol | Type<object>;

/** Application resource returned by an async factory, including optional cleanup. */
export interface LangGraphApplication {
  readonly runtime: ToolkitRuntime | GraphRegistry;
  readonly close?: () => Promise<void>;
}

type LangGraphFactoryResult = ToolkitRuntime | GraphRegistry | LangGraphApplication;

function normalizeApplication(
  value: LangGraphFactoryResult,
  close?: () => Promise<void>,
): LangGraphApplication {
  if ("runtime" in value) return { runtime: value.runtime, close: value.close ?? close };
  return { runtime: value, close };
}

/** Async runtime factory for credentials, databases, or environment-backed resources. */
export interface LangGraphModuleAsyncOptions extends Pick<ModuleMetadata, "imports"> {
  readonly global?: boolean;
  readonly inject?: readonly LangGraphInjectionToken[];
  readonly useFactory: (...dependencies: readonly object[]) => LangGraphFactoryResult | Promise<LangGraphFactoryResult>;
  readonly close?: () => Promise<void>;
}

/** A stable token for applications that need to inject the runtime directly. */
export const LANGGRAPH_RUNTIME = "LANGGRAPH_RUNTIME";
/** A stable token for applications that need to inject the complete runtime resource. */
export const LANGGRAPH_APPLICATION = "LANGGRAPH_APPLICATION";

/** DynamicModule-shaped factory independent of the exact Nest version. */
export class LangGraphModule {
  static forRoot(options: LangGraphModuleOptions): DynamicModule {
    const graphs = options.runtime ?? options.graphs;
    if (graphs === undefined) throw new GraphRuntimeError("LangGraphModule.forRoot requires runtime or graphs.");
    return {
      module: LangGraphModule,
      global: options.global ?? false,
      providers: [{ provide: GraphService, useFactory: () => new GraphService(graphs, options.close) }],
      exports: [GraphService],
    };
  }

  static forRootAsync(options: LangGraphModuleAsyncOptions): DynamicModule {
    return {
      module: LangGraphModule,
      global: options.global ?? false,
      imports: options.imports,
      providers: [
        {
          provide: LANGGRAPH_APPLICATION,
          inject: options.inject === undefined ? undefined : [...options.inject],
          useFactory: async (...dependencies: readonly object[]): Promise<LangGraphApplication> =>
            normalizeApplication(await options.useFactory(...dependencies), options.close),
        },
        {
          provide: LANGGRAPH_RUNTIME,
          inject: [LANGGRAPH_APPLICATION],
          useFactory: (application: LangGraphApplication) => application.runtime,
        },
        {
          provide: GraphService,
          inject: [LANGGRAPH_APPLICATION],
          useFactory: (application: LangGraphApplication) => new GraphService(application.runtime, application.close),
        },
      ],
      exports: [GraphService, LANGGRAPH_RUNTIME, LANGGRAPH_APPLICATION],
    };
  }
}

/** Create a Nest DynamicModule and typed service from one graph resource. */
export function createNestJSAdapter<TGraph extends object>(graph: TGraph, options: NestJSAdapterOptions = {}): NestJSAdapter<TGraph> {
  const runtime = normalizeGraph(graph);
  const service = new GraphService(runtime, options.close);
  return {
    graph,
    runtime,
    lifecycle: service.lifecycle,
    service,
    module: LangGraphModule.forRoot({ graphs: runtime, global: options.global, close: options.close }),
  };
}

function normalizeGraph<TGraph extends object>(graph: TGraph): GraphRegistry {
  if (graph instanceof ToolkitRuntime) return graph;
  const runtime = new GraphRegistry();
  const source = graph as object;
  const collection = source as { readonly list?: () => string[]; readonly get?: (name: string) => CompiledGraph<object> | undefined };
  if (typeof collection.list === "function" && typeof collection.get === "function") {
    for (const name of collection.list()) {
      const compiled = collection.get(name);
      if (compiled && !runtime.has(compiled.name)) runtime.add(compiled);
    }
    return runtime;
  }
  const executable = source as { readonly name?: string; readonly definition?: GraphDefinition<object>; readonly run?: (input: object) => Promise<object>; readonly stream?: (input: object) => AsyncIterable<object> };
  if (typeof executable.name === "string" && executable.definition !== undefined && typeof executable.run === "function" && typeof executable.stream === "function") {
    runtime.add(graph as CompiledGraph<object>);
    return runtime;
  }
  const builder = source as { readonly build?: () => CompiledGraph<object> };
  if (typeof builder.build === "function") {
    runtime.add(builder.build());
    return runtime;
  }
  throw new GraphRuntimeError("createNestJSAdapter requires a compiled graph, graph builder, runtime, or registry.");
}

export { GraphRuntimeError };
