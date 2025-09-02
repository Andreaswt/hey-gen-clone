"use server";

import { revalidatePath } from "next/cache";
import { headers } from "next/headers";
import { redirect } from "next/navigation";
import { auth } from "~/lib/auth";
import { db } from "~/server/db";

export async function updateCreationName(
  id: string,
  name: string,
  type: string,
) {
  const session = await auth.api.getSession({
    headers: await headers(),
  });

  if (!session) redirect("/auth/sign-in");

  const updateData = { where: { id, userId: session.user.id }, data: { name } };

  if (type === "Photo to Video") {
    await db.photoToVideoGeneration.update(updateData);
  } else if (type === "Translate Video") {
    await db.videoTranslationGeneration.update(updateData);
  } else if (type === "Change Video Audio") {
    await db.changeVideoAudioGeneration.update(updateData);
  } else {
    throw new Error("Invalid creation type");
  }

  revalidatePath("/");
}
