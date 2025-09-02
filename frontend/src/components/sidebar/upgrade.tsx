"use client";

import { env } from "~/env";
import { authClient } from "~/lib/auth-client";
import { Button } from "../ui/button";

export default function Upgrade() {
  const upgrade = async () => {
    await authClient.checkout({
      products: [
        env.NEXT_PUBLIC_SMALL_CREDIT_PACK_ID,
        env.NEXT_PUBLIC_MEDIUM_CREDIT_PACK_ID,
        env.NEXT_PUBLIC_LARGE_CREDIT_PACK_ID,
      ],
    });
  };

  return (
    <Button
      variant="outline"
      size="sm"
      className="ml-2 text-purple-600"
      onClick={upgrade}
    >
      Upgrade
    </Button>
  );
}
