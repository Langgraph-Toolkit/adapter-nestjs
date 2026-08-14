# @langgraph-toolkit/adapter-nestjs

NestJS module and service adapter for Langgraph-Toolkit. The module owns framework lifecycle and dependency injection while the graph resource owns state, MCP, checkpoint, actor, and policy defaults.

## Install

```bash
npm install @nestjs/common @nestjs/core @langgraph-toolkit/core @langgraph-toolkit/adapter-nestjs
```

## Module setup

```ts
import { Module } from "@nestjs/common";
import { LangGraphModule } from "@langgraph-toolkit/adapter-nestjs";
import { runtime } from "./database-chat/resource.js";

@Module({
  imports: [LangGraphModule.forRoot({ runtime })],
})
export class AppModule {}
```

`GraphService` provides `has`, `list`, `run`, and `stream` methods for controllers. Keep controller methods thin and pass only request input plus a thread identifier when resuming a graph.

## Public API

The package exports `LangGraphModule`, `LangGraphModuleOptions`, `GraphService`, and `GraphRuntimeError`. The module does not install Express, Fastify, MCP, or community providers.

## Development

```bash
npm install
npm run build
npm test
```

See `examples/projects/nest` for a full Nest CLI project with controller, module, bootstrap, `.env.example`, and database-chat tests.

## License

MIT
