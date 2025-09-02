"use server";

import { randomUUID } from "crypto";
import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { env } from "~/env";
import { auth } from "~/lib/auth";
import { headers } from "next/headers";
import { redirect } from "next/navigation";
import { db } from "~/server/db";
import { revalidatePath } from "next/cache";
import { inngest } from "~/inngest/client";

export async function getPresignedUrl(
  fileName: string,
  fileType: string,
  purpose:
    | "ptvPhoto"
    | "ptvAudio"
    | "ttsVoiceClone"
    | "videoTranslationSource"
    | "cvaVideo"
    | "cvaAudio",
) {
  let folder: string | null = null;

  if (purpose === "ptvPhoto" || purpose === "ptvAudio") folder = "ptv";
  if (purpose === "ttsVoiceClone") folder = "tts";
  if (purpose === "videoTranslationSource") folder = "vt";
  if (purpose === "cvaVideo" || purpose === "cvaAudio") folder = "cva";

  if (folder === null) {
    throw new Error("Invalid purpose for presigned upload URL");
  }

  const ext = fileName.split(".").pop();
  const uuid = randomUUID();
  const key = `${folder}/${uuid}.${ext}`;

  const s3Client = new S3Client({
    region: env.AWS_REGION,
    credentials: {
      accessKeyId: env.AWS_ACCESS_KEY_ID,
      secretAccessKey: env.AWS_SECRET_ACCESS_KEY_ID,
    },
  });

  const command = new PutObjectCommand({
    Bucket: env.S3_BUCKET_NAME,
    Key: key,
    ContentType: fileType,
  });

  const url = await getSignedUrl(s3Client, command, { expiresIn: 600 });

  return { url, key };
}

export interface GenerateRequest {
  photoS3Key: string;
  script?: string;
  audioS3Key?: string | null;
  voiceS3Key?: string | null;
  expressiveness?: number;
  enhancement: boolean;
  experimentalModel: boolean;
  resolution: string;
}

export async function photoToVideo(generateRequest: GenerateRequest) {
  const session = await auth.api.getSession({
    headers: await headers(),
  });

  if (!session) redirect("/auth/sign-in");

  const photoToVideo = await db.photoToVideoGeneration.create({
    data: {
      name: generateRequest.script
        ? generateRequest.script.slice(0, 32)
        : new Date().toISOString(),
      photoS3Key: generateRequest.photoS3Key,
      script: generateRequest.script,
      drivingAudioS3Key: generateRequest.audioS3Key,
      voiceS3Key: generateRequest.voiceS3Key,
      expressiveness: generateRequest.expressiveness,
      enhancement: generateRequest.enhancement,
      experimentalModel: generateRequest.experimentalModel,
      resolution: generateRequest.resolution,
      userId: session.user.id,
      status: "queued",
    },
  });

  // Queue the job
  await inngest.send({
    name: "photo-to-video-event",
    data: {
      photoToVideoId: photoToVideo.id,
      userId: photoToVideo.userId,
    },
  });

  revalidatePath("/");
}

export interface TranslateRequest {
  sourceVideoS3Key: string;
  targetLanguage: string;
}

export async function translateVideo(translateRequest: TranslateRequest) {
  const session = await auth.api.getSession({
    headers: await headers(),
  });

  if (!session) redirect("/auth/sign-in");

  const videoTranslation = await db.videoTranslationGeneration.create({
    data: {
      name: `${new Date().toISOString()} (${translateRequest.targetLanguage})`,
      sourceVideoS3Key: translateRequest.sourceVideoS3Key,
      targetLanguage: translateRequest.targetLanguage,
      userId: session.user.id,
      status: "queued",
    },
  });

  // Queue the job
  await inngest.send({
    name: "translate-video-event",
    data: {
      videoTranslationId: videoTranslation.id,
      userId: videoTranslation.userId,
    },
  });

  revalidatePath("/");
}

export interface ChangeVideoAudioRequest {
  sourceVideoS3Key: string;
  newAudioS3Key: string;
  videoName: string;
}

export async function changeVideoAudio(request: ChangeVideoAudioRequest) {
  const session = await auth.api.getSession({
    headers: await headers(),
  });

  if (!session) redirect("/auth/sign-in");

  const changeVideoAudio = await db.changeVideoAudioGeneration.create({
    data: {
      name: request.videoName,
      sourceVideoS3Key: request.sourceVideoS3Key,
      newAudioS3Key: request.newAudioS3Key,
      userId: session.user.id,
      status: "queued",
    },
  });

  // Queue the job
  await inngest.send({
    name: "change-video-audio-event",
    data: {
      changeVideoAudioId: changeVideoAudio.id,
      userId: changeVideoAudio.userId,
    },
  });

  revalidatePath("/");
}
