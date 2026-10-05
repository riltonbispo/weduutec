import { z } from 'zod';

export const checkBodySchema = z.object({
  token: z.string().refine((token) => token.trim().length > 0, {
    message: 'token must not be empty',
  }),
});
