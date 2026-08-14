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

`GraphService` provides `has`, `list`, `run`, and `stream` methods for controllers. A controller should pass business input and a thread identifier for resume, not reconstruct graph infrastructure on every request.

## One resource, many framework bindings

| Layer | NestJS owns | Resource owns |
|---|---|---|
| Lifecycle | Module registration and dependency injection | Graph construction and runtime defaults |
| Transport | Controller request and response | Typed output and event semantics |
| Security | Host authentication integration | Actor, tier, policy, and approval contracts |
| Persistence | Application connection lifecycle | Checkpoint contract and thread semantics |

The resource can be mounted by Express, Fastify, StruxJS, or a worker without changing its graph source.

## Public API and development

The package exports `LangGraphModule`, `LangGraphModuleOptions`, `GraphService`, and `GraphRuntimeError`. It does not install Express, Fastify, MCP, or community providers.

```bash
npm install
npm run build
npm test
```

See `examples/projects/nest` for a full Nest CLI project with controller, module, bootstrap, `.env.example`, and database-chat tests.

## License

MIT
