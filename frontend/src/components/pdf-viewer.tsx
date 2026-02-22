"use client";

import dynamic from "next/dynamic";

interface PdfViewerProps {
  highlightPage: number | null;
  highlightBbox: number[] | null;
  citationSection?: string;
}

// react-pdf uses browser APIs (DOMMatrix, canvas) — must be client-only
const PdfViewerInner = dynamic(() => import("./pdf-viewer-inner"), {
  ssr: false,
  loading: () => (
    <div className="rounded-lg border bg-muted/30 p-8 text-sm text-muted-foreground">
      Loading PDF viewer…
    </div>
  ),
});

export function PdfViewer(props: PdfViewerProps) {
  return <PdfViewerInner {...props} />;
}
