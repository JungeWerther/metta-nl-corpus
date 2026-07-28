import type { Metadata } from "next";
import Link from "next/link";
import { Suspense } from "react";
import { Geist, Geist_Mono } from "next/font/google";
import { StackProvider, StackTheme } from "@hexclave/next";
import { stackServerApp } from "@/stack/server";
import { UserMenu } from "./user-menu";
import "./globals.css";

const geistSans = Geist({ variable: "--font-geist-sans", subsets: ["latin"] });
const geistMono = Geist_Mono({ variable: "--font-geist-mono", subsets: ["latin"] });

export const metadata: Metadata = {
  title: "MeTTa Review Tool",
  description: "Human review of NL to MeTTa translation pairs",
};

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en" className={`${geistSans.variable} ${geistMono.variable} h-full antialiased`}>
      <body className="min-h-full flex flex-col">
        <StackProvider app={stackServerApp}>
          <StackTheme>
            <header className="flex items-center gap-4 border-b border-neutral-200 px-4 py-2">
              <Link href="/review" className="text-sm font-semibold">
                MeTTa Review
              </Link>
              <nav className="flex gap-3 text-sm text-neutral-600">
                <Link href="/review" className="hover:text-black">
                  Review
                </Link>
                <Link href="/failed" className="hover:text-black">
                  Failed
                </Link>
                <Link href="/dashboard" className="hover:text-black">
                  Dashboard
                </Link>
                <Link href="/export" className="hover:text-black">
                  Export
                </Link>
              </nav>
              <div className="ml-auto">
                <Suspense fallback={null}>
                  <UserMenu />
                </Suspense>
              </div>
            </header>
            {children}
          </StackTheme>
        </StackProvider>
      </body>
    </html>
  );
}
