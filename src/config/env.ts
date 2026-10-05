import { z } from 'zod';

const envShape = {
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
  CALLBACK_MAX_ATTEMPTS: z.coerce.number().int().positive().default(5),
  CALLBACK_TIMEOUT_MS: z.coerce.number().int().positive().default(10_000),
  CALLBACK_LEASE_MS: z.coerce.number().int().positive().default(30_000),
  CALLBACK_BACKOFF_BASE_MS: z.coerce.number().int().positive().default(1_000),
  SWEEP_INTERVAL_MS: z.coerce.number().int().positive().default(2_000),
  RUN_STALL_TIMEOUT_MS: z.coerce.number().int().positive().default(60_000),
  WEDUU_BASE_URL: z.string().url().optional(),
  WEDUU_CID: z.string().min(1).optional(),
  WEDUU_TOKEN: z.string().min(1).optional(),
};

function validateCallbackLease<Schema extends z.ZodRawShape>(schema: z.ZodObject<Schema>) {
  return schema.superRefine((env, context) => {
    const lease = Number(env.CALLBACK_LEASE_MS);
    const timeout = Number(env.CALLBACK_TIMEOUT_MS);
    if (lease <= timeout) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['CALLBACK_LEASE_MS'],
        message: 'must be greater than CALLBACK_TIMEOUT_MS',
      });
    }
  });
}

const envSchema = validateCallbackLease(z.object(envShape));

const workerEnvSchema = validateCallbackLease(
  z.object({
    ...envShape,
    WEDUU_BASE_URL: z.string().url(),
    WEDUU_CID: z.string().min(1),
    WEDUU_TOKEN: z.string().min(1),
  }),
);

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
