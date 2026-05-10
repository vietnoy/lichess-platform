import Header from "@/components/Header";

export default function Loading() {
  return (
    <>
      <Header />
      <main className="max-w-3xl mx-auto px-6 py-12 space-y-4">
        <div className="h-7 w-44 rounded-md bg-surface animate-pulse" />
        <div className="space-y-2">
          <div className="h-4 w-full rounded bg-surface animate-pulse" />
          <div className="h-4 w-5/6 rounded bg-surface animate-pulse" />
          <div className="h-4 w-4/6 rounded bg-surface animate-pulse" />
        </div>
      </main>
    </>
  );
}
