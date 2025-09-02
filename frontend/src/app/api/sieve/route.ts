import { err } from "inngest/types";
import { NextRequest, NextResponse } from "next/server";
import { env } from "~/env";
import { db } from "~/server/db";

interface SieveOutputDataItem {
  Key: string;
  Value: string;
}

interface SieveOutput {
  data: SieveOutputDataItem[];
}

interface SieveWebhookBody {
  job_id: string;
  status: string;
  outputs?: SieveOutput[];
}

interface SieveWebhookPayload {
  body: SieveWebhookBody;
}

export async function POST(req: Request) {
  try {
    const payload = (await req.json()) as SieveWebhookPayload;
    console.log("HEJ: ", payload);

    const { job_id, status, outputs } = payload.body;

    if (!job_id) {
      return NextResponse.json({ error: "Missing job ID" }, { status: 400 });
    }

    const ptvGeneration = await db.photoToVideoGeneration.findFirst({
      where: {
        sieveJobId: job_id,
      },
    });

    if (ptvGeneration) {
      if (status === "finished") {
        const outputData = outputs?.[0]?.data;
        const urlData = outputData?.find((item) => item.Key === "url");
        const videoUrl = urlData?.Value;

        if (!videoUrl) {
          await db.photoToVideoGeneration.update({
            where: {
              id: ptvGeneration.id,
            },
            data: {
              status: "failed",
            },
          });

          throw new Error("Webhook payload missing output video URL");
        }

        const { s3Key } = await importSieveVideoToS3(videoUrl);

        await db.photoToVideoGeneration.update({
          where: { id: ptvGeneration.id },
          data: {
            videoS3Key: s3Key,
            status: "completed",
          },
        });
      } else if (status === "error" || status === "failed") {
        await db.photoToVideoGeneration.update({
          where: {
            id: ptvGeneration.id,
          },
          data: {
            status: "failed",
          },
        });
      }
      return NextResponse.json({
        message: "Webhook processed successfully for PTV",
      });
    }

    const vtGeneration = await db.videoTranslationGeneration.findFirst({
      where: {
        sieveJobId: job_id,
      },
    });

    if (vtGeneration) {
      if (status === "finished") {
        const outputData = outputs?.[0]?.data;
        const urlData = outputData?.find((item) => item.Key === "url");
        const videoUrl = urlData?.Value;

        if (!videoUrl) {
          await db.videoTranslationGeneration.update({
            where: {
              id: vtGeneration.id,
            },
            data: {
              status: "failed",
            },
          });

          throw new Error("Webhook payload missing output video URL");
        }

        const { s3Key } = await importSieveVideoToS3(videoUrl);

        await db.videoTranslationGeneration.update({
          where: { id: vtGeneration.id },
          data: {
            videoS3Key: s3Key,
            status: "completed",
          },
        });
      } else if (status === "error" || status === "failed") {
        await db.videoTranslationGeneration.update({
          where: {
            id: vtGeneration.id,
          },
          data: {
            status: "failed",
          },
        });
      }
      return NextResponse.json({
        message: "Webhook processed successfully for VT",
      });
    }

    const cvaGeneration = await db.changeVideoAudioGeneration.findFirst({
      where: {
        sieveJobId: job_id,
      },
    });

    if (cvaGeneration) {
      if (status === "finished") {
        const outputData = outputs?.[0]?.data;
        const urlData = outputData?.find((item) => item.Key === "url");
        const videoUrl = urlData?.Value;

        if (!videoUrl) {
          await db.changeVideoAudioGeneration.update({
            where: {
              id: cvaGeneration.id,
            },
            data: {
              status: "failed",
            },
          });

          throw new Error("Webhook payload missing output video URL");
        }

        const { s3Key } = await importSieveVideoToS3(videoUrl);

        await db.changeVideoAudioGeneration.update({
          where: { id: cvaGeneration.id },
          data: {
            videoS3Key: s3Key,
            status: "completed",
          },
        });
      } else if (status === "error" || status === "failed") {
        await db.changeVideoAudioGeneration.update({
          where: {
            id: cvaGeneration.id,
          },
          data: {
            status: "failed",
          },
        });
      }
      return NextResponse.json({
        message: "Webhook processed successfully for CVA",
      });
    }

    return NextResponse.json(
      {
        error: `Generation record now found for sieveJobId: ${job_id}`,
      },
      { status: 400 },
    );
  } catch (error) {
    const errorMessage =
      error instanceof Error ? error.message : "Unknown error";

    return NextResponse.json(
      {
        error: `Internal server error`,
        details: errorMessage,
      },
      { status: 500 },
    );
  }
}

async function importSieveVideoToS3(videoUrl: string) {
  const res = await fetch(env.SIEVE_FILE_TO_S3_ENDPOINT, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Modal-Key": env.MODAL_KEY,
      "Modal-Secret": env.MODAL_SECRET,
    },
    body: JSON.stringify({
      video_url: videoUrl,
    }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Sieve import failed ${text}`);
  }

  const data = (await res.json()) as { s3_key: string };

  return { s3Key: data.s3_key };
}
