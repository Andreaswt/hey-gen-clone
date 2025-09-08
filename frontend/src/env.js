import { createEnv } from "@t3-oss/env-nextjs";
import { z } from "zod";

export const env = createEnv({
  /**
   * Specify your server-side environment variables schema here. This way you can ensure the app
   * isn't built with invalid env vars.
   */
  server: {
    DATABASE_URL: z.string().url(),
    NODE_ENV: z
      .enum(["development", "test", "production"])
      .default("development"),
    MODAL_KEY: z.string(),
    MODAL_SECRET: z.string(),
    AWS_ACCESS_KEY_ID: z.string(),
    AWS_SECRET_ACCESS_KEY_ID: z.string(),
    AWS_REGION: z.string(),
    S3_BUCKET_NAME: z.string(),
    TEXT_TO_SPEECH_ENDPOINT: z.string().url(),
    PHOTO_TO_VIDEO_ENDPOINT: z.string().url(),
    FILE_TO_S3_ENDPOINT: z.string().url(),
    POLAR_ACCESS_TOKEN: z.string(),
    POLAR_WEBHOOK_SECRET: z.string(),
    FAL_KEY: z.string(),
  },

  /**
   * Specify your client-side environment variables schema here. This way you can ensure the app
   * isn't built with invalid env vars. To expose them to the client, prefix them with
   * `NEXT_PUBLIC_`.
   */
  client: {
    // NEXT_PUBLIC_CLIENTVAR: z.string(),
    NEXT_PUBLIC_SMALL_CREDIT_PACK_ID: z.string(),
    NEXT_PUBLIC_MEDIUM_CREDIT_PACK_ID: z.string(),
    NEXT_PUBLIC_LARGE_CREDIT_PACK_ID: z.string(),
  },

  /**
   * You can't destruct `process.env` as a regular object in the Next.js edge runtimes (e.g.
   * middlewares) or client-side so we need to destruct manually.
   */
  runtimeEnv: {
    DATABASE_URL: process.env.DATABASE_URL,
    NODE_ENV: process.env.NODE_ENV,
    MODAL_KEY: process.env.MODAL_KEY,
    MODAL_SECRET: process.env.MODAL_SECRET,
    AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY_ID: process.env.AWS_SECRET_ACCESS_KEY_ID,
    AWS_REGION: process.env.AWS_REGION,
    S3_BUCKET_NAME: process.env.S3_BUCKET_NAME,
    TEXT_TO_SPEECH_ENDPOINT: process.env.TEXT_TO_SPEECH_ENDPOINT,
    PHOTO_TO_VIDEO_ENDPOINT: process.env.PHOTO_TO_VIDEO_ENDPOINT,
    FILE_TO_S3_ENDPOINT: process.env.FILE_TO_S3_ENDPOINT,
    POLAR_ACCESS_TOKEN: process.env.POLAR_ACCESS_TOKEN,
    NEXT_PUBLIC_SMALL_CREDIT_PACK_ID:
      process.env.NEXT_PUBLIC_SMALL_CREDIT_PACK_ID,
    NEXT_PUBLIC_MEDIUM_CREDIT_PACK_ID:
      process.env.NEXT_PUBLIC_MEDIUM_CREDIT_PACK_ID,
    NEXT_PUBLIC_LARGE_CREDIT_PACK_ID:
      process.env.NEXT_PUBLIC_LARGE_CREDIT_PACK_ID,
    POLAR_WEBHOOK_SECRET: process.env.POLAR_WEBHOOK_SECRET,
    FAL_KEY: process.env.FAL_KEY,
    // NEXT_PUBLIC_CLIENTVAR: process.env.NEXT_PUBLIC_CLIENTVAR,
  },
  /**
   * Run `build` or `dev` with `SKIP_ENV_VALIDATION` to skip env validation. This is especially
   * useful for Docker builds.
   */
  skipValidation: !!process.env.SKIP_ENV_VALIDATION,
  /**
   * Makes it so that empty strings are treated as undefined. `SOME_VAR: z.string()` and
   * `SOME_VAR=''` will throw an error.
   */
  emptyStringAsUndefined: true,
});
