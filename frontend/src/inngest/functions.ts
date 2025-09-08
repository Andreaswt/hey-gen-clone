import { db } from "~/server/db";
import { inngest } from "./client";
import { env } from "~/env";
import { getPresignedUrl } from "~/lib/s3";
import { fal } from "@fal-ai/client";

// Configure FAL client
fal.config({
  credentials: env.FAL_KEY,
});

export const photoToVideo = inngest.createFunction(
  {
    id: "photo-to-video",
    concurrency: {
      limit: 5,
      key: "event.data.userId",
    },
    onFailure: async ({ event, error }) => {
      await db.photoToVideoGeneration.update({
        where: {
          id: (event?.data?.event.data as { photoToVideoId: string })
            .photoToVideoId,
        },
        data: {
          status: "failed",
        },
      });
    },
  },
  { event: "photo-to-video-event" },
  async ({ event, step }) => {
    const { photoToVideoId } = event.data as {
      photoToVideoId: string;
      userId: string;
    };

    const photoToVideo = await step.run("get-photo-to-video", async () => {
      return await db.photoToVideoGeneration.findUniqueOrThrow({
        where: {
          id: photoToVideoId,
        },
        include: {
          user: {
            select: {
              id: true,
              credits: true,
            },
          },
        },
      });
    });

    if (photoToVideo.user.credits > 0) {
      await step.run("set-status-processing", async () => {
        return await db.photoToVideoGeneration.update({
          where: { id: photoToVideo.id },
          data: {
            status: "processing",
          },
        });
      });

      if (photoToVideo.drivingAudioS3Key === null) {
        const ttsResponse = await step.fetch(env.TEXT_TO_SPEECH_ENDPOINT, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Modal-Key": env.MODAL_KEY,
            "Modal-Secret": env.MODAL_SECRET,
          },
          body: JSON.stringify({
            text: photoToVideo.script,
            voice_S3_key: photoToVideo.voiceS3Key,
          }),
        });
        const data = (await ttsResponse.json()) as { s3_key: string };
        photoToVideo.drivingAudioS3Key = data.s3_key;
      }

      if (photoToVideo.experimentalModel) {
        const [photoUrl, audioUrl] = await step.run(
          "create-presigned-urls",
          async () => {
            const photo = await getPresignedUrl(photoToVideo.photoS3Key!);
            const audio = await getPresignedUrl(
              photoToVideo.drivingAudioS3Key!,
            );
            return [photo, audio];
          },
        );

        if (!photoUrl || !audioUrl)
          throw new Error(
            "Missing photo or audio URL: " + photoUrl + ", " + audioUrl,
          );

        const { request_id } = await step.run("submit-fal-avatar", async () => {
          return await fal.queue.submit("fal-ai/ai-avatar", {
            input: {
              image_url: photoUrl,
              audio_url: audioUrl,
              prompt: photoToVideo.script || "A person talking naturally",
              num_frames: 145,
              resolution: "720p",
              seed: 42,
              acceleration: "regular",
            },
            webhookUrl: `https://${process.env.VERCEL_URL}/api/fal`,
          });
        });

        await step.run("update-db-with-fal-request-id", async () => {
          return await db.photoToVideoGeneration.update({
            where: { id: photoToVideo.id },
            data: {
              falJobId: request_id,
            },
          });
        });
      } else {
        const videoResponse = await step.fetch(env.PHOTO_TO_VIDEO_ENDPOINT, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Modal-Key": env.MODAL_KEY,
            "Modal-Secret": env.MODAL_SECRET,
          },
          body: JSON.stringify({
            transcript: photoToVideo.script,
            photo_s3_key: photoToVideo.photoS3Key,
            audio_s3_key: photoToVideo.drivingAudioS3Key,
          }),
        });

        const data = (await videoResponse.json()) as { video_s3_key: string };
        const videoS3Key = data.video_s3_key;

        await step.run("update-db-with-video", async () => {
          return await db.photoToVideoGeneration.update({
            where: { id: photoToVideo.id },
            data: {
              videoS3Key: videoS3Key,
              status: "completed",
            },
          });
        });
      }

      await step.run("deduct-credits", async () => {
        return await db.user.update({
          where: {
            id: photoToVideo.user.id,
          },
          data: { credits: { decrement: 1 } },
        });
      });
    } else {
      await step.run("set-status-no-credits", async () => {
        return await db.photoToVideoGeneration.update({
          where: { id: photoToVideo.id },
          data: {
            status: "no credits",
          },
        });
      });
    }
  },
);

export const translateVideo = inngest.createFunction(
  {
    id: "translate-video",
    concurrency: {
      limit: 5,
      key: "event.data.userId",
    },
    onFailure: async ({ event, error }) => {
      await db.videoTranslationGeneration.update({
        where: {
          id: (event?.data?.event.data as { videoTranslationId: string })
            .videoTranslationId,
        },
        data: {
          status: "failed",
        },
      });
    },
  },
  { event: "translate-video-event" },
  async ({ event, step }) => {
    const { videoTranslationId } = event.data as {
      videoTranslationId: string;
      userId: string;
    };

    const videoTranslation = await step.run(
      "get-video-translation",
      async () => {
        return await db.videoTranslationGeneration.findUniqueOrThrow({
          where: {
            id: videoTranslationId,
          },
          include: {
            user: {
              select: {
                id: true,
                credits: true,
              },
            },
          },
        });
      },
    );

    if (videoTranslation.user.credits > 0) {
      await step.run("set-status-processing", async () => {
        return await db.videoTranslationGeneration.update({
          where: { id: videoTranslation.id },
          data: {
            status: "processing",
          },
        });
      });

      const videoUrl = await step.run("create-presigned-urls", async () => {
        const videoUrl = await getPresignedUrl(
          videoTranslation.sourceVideoS3Key!,
        );
        return videoUrl;
      });

      if (!videoUrl) {
        throw new Error("Missing video URL: " + videoUrl);
      }

      const { request_id } = await step.run("submit-fal-dubbing", async () => {
        return await fal.queue.submit("fal-ai/dubbing", {
          input: {
            video_url: videoUrl,
            target_language: videoTranslation.targetLanguage as
              | "hindi"
              | "turkish"
              | "english",
            do_lipsync: true,
          },
          webhookUrl: `https://${process.env.VERCEL_URL}/api/fal`,
        });
      });

      await step.run("update-db-with-fal-request-id", async () => {
        return await db.videoTranslationGeneration.update({
          where: { id: videoTranslation.id },
          data: {
            falJobId: request_id,
          },
        });
      });

      await step.run("deduct-credits", async () => {
        return await db.user.update({
          where: {
            id: videoTranslation.user.id,
          },
          data: { credits: { decrement: 1 } },
        });
      });
    } else {
      await step.run("set-status-no-credits", async () => {
        return await db.videoTranslationGeneration.update({
          where: { id: videoTranslation.id },
          data: {
            status: "no credits",
          },
        });
      });
    }
  },
);

export const changeVideoAudio = inngest.createFunction(
  {
    id: "change-video-audio",
    concurrency: {
      limit: 5,
      key: "event.data.userId",
    },
    onFailure: async ({ event, error }) => {
      await db.changeVideoAudioGeneration.update({
        where: {
          id: (event?.data?.event.data as { changeVideoAudioId: string })
            .changeVideoAudioId,
        },
        data: {
          status: "failed",
        },
      });
    },
  },
  { event: "change-video-audio-event" },
  async ({ event, step }) => {
    const { changeVideoAudioId } = event.data as {
      changeVideoAudioId: string;
      userId: string;
    };

    const generation = await step.run("get-generation-record", async () => {
      return await db.changeVideoAudioGeneration.findUniqueOrThrow({
        where: {
          id: changeVideoAudioId,
        },
        include: {
          user: {
            select: {
              id: true,
              credits: true,
            },
          },
        },
      });
    });

    if (generation.user.credits > 0) {
      await step.run("set-status-processing", async () => {
        return await db.changeVideoAudioGeneration.update({
          where: { id: generation.id },
          data: {
            status: "processing",
          },
        });
      });

      const [videoUrl, audioUrl] = await step.run(
        "create-presigned-urls",
        async () => {
          const videoUrl = await getPresignedUrl(generation.sourceVideoS3Key!);
          const audioUrl = await getPresignedUrl(generation.newAudioS3Key!);
          return [videoUrl, audioUrl];
        },
      );

      if (!videoUrl || !audioUrl)
        throw new Error(
          "Missing video or audio URL: " + videoUrl + ", " + audioUrl,
        );

      const { request_id } = await step.run("submit-fal-lipsync", async () => {
        return await fal.queue.submit("fal-ai/sync-lipsync", {
          input: {
            video_url: videoUrl,
            audio_url: audioUrl,
            model: "lipsync-1.9.0-beta",
            sync_mode: "cut_off",
          },
          webhookUrl: `https://${process.env.VERCEL_URL}/api/fal`,
        });
      });

      await step.run("update-db-with-fal-request-id", async () => {
        return await db.changeVideoAudioGeneration.update({
          where: { id: generation.id },
          data: {
            falJobId: request_id,
          },
        });
      });

      await step.run("deduct-credits", async () => {
        return await db.user.update({
          where: {
            id: generation.user.id,
          },
          data: { credits: { decrement: 1 } },
        });
      });
    } else {
      await step.run("set-status-no-credits", async () => {
        return await db.changeVideoAudioGeneration.update({
          where: { id: generation.id },
          data: {
            status: "no credits",
          },
        });
      });
    }
  },
);
