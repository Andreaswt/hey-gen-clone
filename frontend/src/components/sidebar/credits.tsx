"use server";

import { headers } from "next/headers";
import { auth } from "~/lib/auth";
import { db } from "~/server/db";

export async function Credits() {
  const session = await auth.api.getSession({
    headers: await headers(),
  });
  if (!session) return null;

  const user = await db.user.findUniqueOrThrow({
    where: { id: session.user.id },
    select: {
      credits: true,
    },
  });
  return (
    <>
      <span className="font-semibold">{user.credits}</span>
      <span className="text-muted-foreground">Credits</span>
    </>
  );
}
