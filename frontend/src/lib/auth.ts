import { betterAuth } from "better-auth";
import { prismaAdapter } from "better-auth/adapters/prisma";
import { db } from "~/server/db";
import { Polar } from "@polar-sh/sdk";
import { env } from "~/env";
import {
  polar,
  checkout,
  portal,
  usage,
  webhooks,
} from "@polar-sh/better-auth";

const polarClient = new Polar({
  accessToken: env.POLAR_ACCESS_TOKEN,
  server: "sandbox",
});

export const auth = betterAuth({
  database: prismaAdapter(db, {
    provider: "postgresql",
  }),
  emailAndPassword: {
    enabled: true,
  },
  plugins: [
    polar({
      client: polarClient,
      createCustomerOnSignUp: true,
      use: [
        checkout({
          products: [
            { productId: env.NEXT_PUBLIC_SMALL_CREDIT_PACK_ID, slug: "small" },
            {
              productId: env.NEXT_PUBLIC_MEDIUM_CREDIT_PACK_ID,
              slug: "medium",
            },
            { productId: env.NEXT_PUBLIC_LARGE_CREDIT_PACK_ID, slug: "large" },
          ],
          successUrl: "/?purchase_success=true",
          authenticatedUsersOnly: true,
        }),
        portal(),
        webhooks({
          secret: env.POLAR_WEBHOOK_SECRET,
          onOrderPaid: async (order) => {
            const externalCustomerId = order.data.customer.externalId;
            if (!externalCustomerId) {
              throw new Error("No external customer id found.");
            }

            const productId = order.data.productId;
            let creditsToAdd = 0;
            switch (productId) {
              case env.NEXT_PUBLIC_SMALL_CREDIT_PACK_ID:
                creditsToAdd = 10;
                break;
              case env.NEXT_PUBLIC_MEDIUM_CREDIT_PACK_ID:
                creditsToAdd = 25;
                break;
              case env.NEXT_PUBLIC_LARGE_CREDIT_PACK_ID:
                creditsToAdd = 50;
                break;
            }

            await db.user.update({
              where: { id: externalCustomerId },
              data: {
                credits: {
                  increment: creditsToAdd,
                },
              },
            });
          },
        }),
      ],
    }),
  ],
});
