import "~/styles/globals.css";

import { type Metadata } from "next";
import { Geist } from "next/font/google";
import { Toaster } from "~/components/ui/sonner";
import { Providers } from "~/components/providers";

export const metadata: Metadata = {
  title: "HeyGen",
  description: "HeyGen",
  icons: [{ rel: "icon", url: "/favicon.ico" }],
};

const geist = Geist({
  subsets: ["latin"],
  variable: "--font-geist-sans",
});

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en" className={`${geist.variable} `}>
      <body className="flex min-h-svh flex-col items-center justify-center">
        <Providers>
          {children}
          <Toaster />
        </Providers>
      </body>
    </html>
  );
}
