# @langgraph-toolkit/adapter-nestjs

**Let NestJS own dependency injection while the graph remains framework-neutral.** The adapter binds a graph resource to NestJS lifecycle and services without moving state, MCP, checkpoint, actor, provider, or policy defaults into controllers.

## Install

```bash
npm install @nestjs/common @nestjs/core @langgraph-toolkit/core @langgraph-toolkit/adapter-nestjs
```

## Zero-config factory

```ts
import { createNestJSAdapter } from "@langgraph-toolkit/adapter-nestjs";
import { resource } from "./resource.js";

const adapter = createNestJSAdapter(resource.runtime);

const moduleOptions = adapter.module;
const graphs = adapter.service;
```

The factory returns `{ graph, runtime, module, service }`. `module` is a Nest-compatible dynamic module and `service` exposes typed `has`, `list`, `run`, `stream`, and `bind` methods. Use `LangGraphModule.forRoot()` or `forRootAsync()` directly when the application needs full Nest module composition.

```ts
import { Module } from "@nestjs/common";
import { LangGraphModule } from "@langgraph-toolkit/adapter-nestjs";

@Module({
  imports: [LangGraphModule.forRoot({ runtime: resource.runtime, global: true })],
})
export class AppModule {}
```

`GraphHttpExceptionFilter` and `streamSse` keep controller code focused on request input. A controller does not need to stringify events or reconstruct MCP and graph state.

## Development

```bash
npm install
npm run build
npm test
```

## License

MIT
