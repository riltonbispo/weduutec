import { buildApp } from './app.js';
import { loadEnv } from './config/env.js';

try {
  const env = loadEnv();
  const app = buildApp({ logLevel: env.LOG_LEVEL });

  await app.listen({ port: env.PORT, host: '0.0.0.0' });
} catch (error) {
  const message = error instanceof Error ? error.message : 'Unknown startup error';
  console.error(`Failed to start server: ${message}`);
  process.exitCode = 1;
}
