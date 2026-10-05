import { z } from 'zod';

const envSchema = z.object({
  WEDUU_BASE_URL: z.string().url(),
  WEBHOOK_URL: z.string().url(),
  REGISTER_NAME: z.string().min(1).default('Weduu technical challenge'),
});

const responseSchema = z.object({ cid: z.string().min(1), token: z.string().min(1) });
const env = envSchema.parse({
  ...process.env,
  WEBHOOK_URL: process.argv[2] ?? process.env.WEBHOOK_URL,
  REGISTER_NAME: process.argv[3] ?? process.env.REGISTER_NAME,
});
const baseUrl = env.WEDUU_BASE_URL.endsWith('/') ? env.WEDUU_BASE_URL : `${env.WEDUU_BASE_URL}/`;
const response = await fetch(new URL('register', baseUrl), {
  method: 'POST',
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify({ name: env.REGISTER_NAME, webhook: env.WEBHOOK_URL }),
});
if (!response.ok) throw new Error(`Register failed with HTTP ${String(response.status)}`);
const credentials = responseSchema.parse(await response.json());
console.log(`WEDUU_CID=${credentials.cid}`);
console.log(`WEDUU_TOKEN=${credentials.token}`);
