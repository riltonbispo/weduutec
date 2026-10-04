import { z } from 'zod';

const portSchema = z.coerce.number().int().min(1).max(65_535).default(3000);

const envSchema = z.object({
  PORT: portSchema,
  PLATFORM_BASE_URL: z
    .string()
    .url()
    .default('https://dev-wdu-ped-test-1014944555984.us-central1.run.app'),
  CID: z.string().min(1).optional(),
  TOKEN: z.string().min(1).optional(),
});

export type Env = z.infer<typeof envSchema>;

export function loadEnv(source: NodeJS.ProcessEnv = process.env): Env {
  return envSchema.parse(source);
}
