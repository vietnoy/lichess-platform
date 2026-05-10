import Link from "next/link";
import Header from "@/components/Header";

export default function NotFound() {
  return (
    <>
      <Header />
      <main className="max-w-md mx-auto px-6 py-24 text-center space-y-4">
        <h1 className="text-2xl font-medium tracking-tight">Page not found</h1>
        <p className="text-muted text-sm">
          The page you're looking for doesn't exist or has been moved.
        </p>
        <Link
          href="/"
          className="inline-block bg-accent text-bg font-medium px-4 py-2 rounded-md hover:opacity-90"
        >
          Back to home
        </Link>
      </main>
    </>
  );
}
