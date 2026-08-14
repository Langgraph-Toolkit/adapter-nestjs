# @langgraph-toolkit/adapter-nestjs

**Let NestJS own dependency injection while the graph remains framework-neutral.** The module binds a compiled registry to NestJS lifecycle and services. The graph resource still owns state, MCP, checkpoint, actor, provider, and policy defaults.

## Install

```bash
npm install @nestjs/common @nestjs/core @langgraph-toolkit/core @langgraph-toolkit/adapter-nestjs
```

## Minimal module setup

```ts
import { Module } from "@nestjs/common";
import { LangGraphModule } from "@langgraph-toolkit/adapter-nestjs";
import { runtime } from "./database-chat/resource.js";

@Module({
  imports: [LangGraphModule.forRoot({ runtime })],
})
export class AppModule {}
```

For applications whose graph resource needs asynchronous MCP credentials or provider inference, let NestJS own resource creation and cleanup with `forRootAsync`. The factory may return either a runtime or `{ runtime, close }`; the adapter registers the runtime and invokes `close` during module shutdown.

```ts
@Module({
  imports: [
    LangGraphModule.forRootAsync({
      useFactory: async () => createChatApplication(),
    }),
  ],
})
export class AppModule {}
```

`GraphService` provides `has`, `list`, `run`, and `stream` methods for controllers. Use `bind(name)` once in a controller to retain the graph's state/input/output generics without repeating the graph name and generic list on every route. `streamSse` returns a typed Nest `Observable` envelope and serializes toolkit errors, so controllers do not need to set headers, stringify events, or map runtime errors manually.

```ts
@Controller("chat")
@UseFilters(GraphHttpExceptionFilter)
export class ChatController {
  private readonly chat = this.graphs.bind<ChatState, ChatInput>("chat");

  @Post("run")
  run(@Body() input: ChatInput) {
    return this.chat.run(input);
  }

  @Sse("stream")
  stream(@Query() input: ChatInput) {
    return this.chat.streamSse(input);
  }

  constructor(private readonly graphs: GraphService) {}
}
```

## One resource, many framework bindings

| Layer | NestJS owns | Resource owns |
|---|---|---|
| Lifecycle | Module registration and dependency injection | Graph construction and runtime defaults |
| Transport | Controller request and response | Typed output and event semantics |
| Security | Host authentication integration | Actor, tier, policy, and approval contracts |
| Persistence | Application connection lifecycle | Checkpoint contract and thread semantics |

The resource can be mounted by Express, Fastify, StruxJS, or a worker without changing its graph source.

## Public API and development

The package exports `LangGraphModule`, `LangGraphModuleOptions`, `LangGraphModuleAsyncOptions`, `LangGraphApplication`, `GraphService`, `BoundGraphService`, `GraphHttpExceptionFilter`, and `GraphRuntimeError`. It does not install Express, Fastify, MCP, or community providers.

```bash
npm install
npm run build
npm test
```

See `examples/projects/nest` for a full Nest CLI project with controller, module, bootstrap, `.env.example`, and database-chat tests.

## License

MIT
