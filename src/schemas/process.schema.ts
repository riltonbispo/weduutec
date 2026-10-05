import { z } from 'zod';

const nonBlankString = z.string().refine((value) => value.trim().length > 0, {
  message: 'must not be empty',
});

export const processBodySchema = z.object({
  run_id: nonBlankString,
  seq: z.number().int().min(0),
  sku: nonBlankString,
});

export type ProcessBody = z.infer<typeof processBodySchema>;
