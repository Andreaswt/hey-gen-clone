"use client";

import { useRouter } from "next/navigation";
import { Button } from "../ui/button";
import { ArrowLeftIcon } from "lucide-react";

export function BackButton() {
  const router = useRouter();

  return (
    <Button
      variant="outline"
      className="mb-6 cursor-pointer"
      onClick={() => router.push("/")}
    >
      <ArrowLeftIcon />
      Back
    </Button>
  );
}
