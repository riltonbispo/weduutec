import { buildApp } from './app.js';
import { loadEnv } from './config/env.js';

const env = loadEnv();
const app = buildApp();

try {
  await app.listen({ port: env.PORT, host: '0.0.0.0' });
} catch (error) {
  app.log.error({ err: error }, 'failed to start server');
  process.exit(1);
}
