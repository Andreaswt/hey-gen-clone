import { NextResponse } from "next/server";
import { env } from "~/env";
import { db } from "~/server/db";

interface FalWebhookPayload {
  request_id: string;
  status: string;
  payload?: {
    video?: {
      url: string;
      file_size: number;
      file_name: string;
      content_type: string;
    };
    seed?: number;
    detail?: string;
  };
  error?: string | null;
}

export async function POST(req: Request) {
  let payload: FalWebhookPayload;

  try {
    payload = await req.json();
    console.log("FAL Webhook payload:", JSON.stringify(payload, null, 2));

    const { request_id, status } = payload;

    if (!request_id) {
      return NextResponse.json(
        { error: "Missing request ID" },
        { status: 400 },
      );
    }

    // Try to find the generation record in all three tables
    const ptvGeneration = await db.photoToVideoGeneration.findFirst({
      where: { falJobId: request_id },
    });

    const cvaGeneration = await db.changeVideoAudioGeneration.findFirst({
      where: { falJobId: request_id },
    });

    const vtGeneration = await db.videoTranslationGeneration.findFirst({
      where: { falJobId: request_id },
    });

    const generation = ptvGeneration || cvaGeneration || vtGeneration;
    const generationType = ptvGeneration ? "ptv" : cvaGeneration ? "cva" : "vt";

    if (!generation) {
      console.log(
        `No generation record found for requestId: ${request_id} - likely old webhook retry`,
      );
      return NextResponse.json({
        message: `No generation record found for requestId: ${request_id}`,
      });
    }

    // Check if already processed
    if (generation.status === "completed" || generation.status === "failed") {
      console.log(
        `Generation ${request_id} already processed with status: ${generation.status}`,
      );
      return NextResponse.json({
        message: `Generation already processed with status: ${generation.status}`,
      });
    }

    // Handle successful completion
    if (status === "OK") {
      const videoUrl = payload.payload?.video?.url;

      if (!videoUrl) {
        console.error("No video URL found in payload:", payload);

        // Update the correct table based on generation type
        await updateGenerationStatus(generationType, generation.id, "failed");

        return NextResponse.json(
          { error: "Webhook payload missing output video URL" },
          { status: 400 },
        );
      }

      console.log(
        `Processing successful ${generationType} generation for ${request_id}, video URL: ${videoUrl}`,
      );

      try {
        // Import the video from FAL to your S3 bucket
        const { s3Key } = await importFalVideoToS3(videoUrl);

        // Update the correct table based on generation type
        await updateGenerationStatus(
          generationType,
          generation.id,
          "completed",
          s3Key,
        );

        console.log(
          `Successfully updated ${generationType} generation ${request_id} with s3Key: ${s3Key}`,
        );

        return NextResponse.json({
          message: `Webhook processed successfully for FAL ${generationType} generation`,
        });
      } catch (importError) {
        console.error("Failed to import video to S3:", importError);

        // Update the correct table based on generation type
        await updateGenerationStatus(generationType, generation.id, "failed");

        return NextResponse.json(
          { error: "Failed to import video to S3" },
          { status: 500 },
        );
      }
    } else if (
      status === "failed" ||
      status === "error" ||
      status === "ERROR"
    ) {
      // Update the correct table based on generation type
      await updateGenerationStatus(generationType, generation.id, "failed");

      const errorMessage =
        payload.error || payload.payload?.detail || "Unknown error";

      return NextResponse.json({
        message: "Webhook processed - generation failed",
        error: errorMessage,
      });
    }

    // For other statuses (in_progress, queued, etc.)
    return NextResponse.json({
      message: `Webhook received - status: ${status}`,
    });
  } catch (parseError) {
    console.error("FAL webhook parse error:", parseError);

    return NextResponse.json(
      {
        error: "Failed to parse webhook payload",
        details:
          parseError instanceof Error
            ? parseError.message
            : "Unknown parse error",
      },
      { status: 400 },
    );
  }
}

// Helper function to update the correct table based on generation type
async function updateGenerationStatus(
  generationType: string,
  generationId: string,
  status: string,
  videoS3Key?: string,
) {
  const updateData: { status: string; videoS3Key?: string } = { status };
  if (videoS3Key) {
    updateData.videoS3Key = videoS3Key;
  }

  switch (generationType) {
    case "ptv":
      await db.photoToVideoGeneration.update({
        where: { id: generationId },
        data: updateData,
      });
      break;
    case "cva":
      await db.changeVideoAudioGeneration.update({
        where: { id: generationId },
        data: updateData,
      });
      break;
    case "vt":
      await db.videoTranslationGeneration.update({
        where: { id: generationId },
        data: updateData,
      });
      break;
  }
}

async function importFalVideoToS3(videoUrl: string) {
  const res = await fetch(env.FILE_TO_S3_ENDPOINT, {
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
    throw new Error(`FAL video import failed: ${text}`);
  }

  const data = (await res.json()) as { s3_key: string };

  return { s3Key: data.s3_key };
}
