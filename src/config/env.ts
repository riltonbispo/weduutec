import { z } from 'zod';

const envSchema = z.object({
  PORT: z.coerce.number().int().min(1).max(65_535).default(4000),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent']).default('info'),
  REDIS_URL: z.string().url().default('redis://127.0.0.1:6379'),
  REDIS_COMMAND_TIMEOUT_MS: z.coerce.number().int().positive().default(175),
  REDIS_CONNECT_TIMEOUT_MS: z.coerce.number().int().positive().default(75),
  ENRICH_TIMEOUT_MS: z.coerce.number().int().positive().default(5_000),
  ENRICH_MAX_ATTEMPTS: z.coerce.number().int().positive().default(5),
  ENRICH_BACKOFF_BASE_MS: z.coerce.number().int().positive().default(500),
  ENRICH_BACKOFF_MAX_MS: z.coerce.number().int().positive().default(8_000),
  ENRICH_RATE_LIMIT_MAX_WAITS: z.coerce.number().int().positive().default(20),
  WEDUU_BASE_URL: z.string().url().optional(),
  WEDUU_CID: z.string().min(1).optional(),
  WEDUU_TOKEN: z.string().min(1).optional(),
});

const workerEnvSchema = envSchema.extend({
  WEDUU_BASE_URL: z.string().url(),
  WEDUU_CID: z.string().min(1),
  WEDUU_TOKEN: z.string().min(1),
});

export type Env = z.infer<typeof envSchema>;
export type WorkerEnv = z.infer<typeof workerEnvSchema>;

function formatEnvError(error: z.ZodError): Error {
  const details = error.issues
    .map((issue) => `${issue.path.join('.') || 'environment'}: ${issue.message}`)
    .join('; ');

  return new Error(`Invalid environment variables: ${details}`);
}

export function loadEnv(source: NodeJS.ProcessEnv = process.env): Env {
  const result = envSchema.safeParse(source);

  if (!result.success) {
    throw formatEnvError(result.error);
  }

  return result.data;
}

export function loadWorkerEnv(source: NodeJS.ProcessEnv = process.env): WorkerEnv {
  const result = workerEnvSchema.safeParse(source);

  if (!result.success) {
    throw formatEnvError(result.error);
  }

  return result.data;
}
