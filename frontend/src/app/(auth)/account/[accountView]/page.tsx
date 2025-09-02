import { AccountView } from "@daveyplate/better-auth-ui";
import { accountViewPaths } from "@daveyplate/better-auth-ui/server";
import { BackButton } from "~/components/account/back-button";
import { Button } from "~/components/ui/button";

export const dynamicParams = false;

export function generateStaticParams() {
  return Object.values(accountViewPaths).map((accountView) => ({
    accountView,
  }));
}

export default async function AccountPage({
  params,
}: {
  params: Promise<{ accountView: string }>;
}) {
  const { accountView } = await params;

  return (
    <main className="container p-4 md:p-6">
      <BackButton />
      <AccountView pathname={accountView} />
    </main>
  );
}
