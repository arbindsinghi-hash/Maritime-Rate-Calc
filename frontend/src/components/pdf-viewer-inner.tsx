"use client";

import { useState, useCallback } from "react";
import { Document, Page, pdfjs } from "react-pdf";
import { tariffPdfUrl } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { ChevronLeft, ChevronRight } from "lucide-react";
import "react-pdf/dist/Page/AnnotationLayer.css";
import "react-pdf/dist/Page/TextLayer.css";

pdfjs.GlobalWorkerOptions.workerSrc = "/pdf.worker.min.mjs";

interface Props {
  highlightPage: number | null;
  highlightBbox: number[] | null;
  citationSection?: string;
}

export default function PdfViewerInner({
  highlightPage,
  highlightBbox,
  citationSection,
}: Props) {
  const [numPages, setNumPages] = useState<number | null>(null);
  const [userPage, setUserPage] = useState(1);
  const [scale] = useState(1.2);
  const [prevHighlight, setPrevHighlight] = useState<number | null>(null);

  if (highlightPage !== prevHighlight) {
    setPrevHighlight(highlightPage);
    if (highlightPage != null && highlightPage >= 1) {
      setUserPage(highlightPage);
    }
  }

  const pageNumber = userPage;

  const onDocumentLoadSuccess = useCallback(
    ({ numPages: n }: { numPages: number }) => {
      setNumPages(n);
    },
    [],
  );

  return (
    <div className="rounded-lg border bg-muted/30 overflow-hidden">
      {/* Toolbar */}
      <div className="flex items-center gap-3 px-3 py-2 bg-muted border-b">
        <Button
          variant="outline"
          size="sm"
          disabled={pageNumber <= 1}
          onClick={() => setUserPage((p) => Math.max(1, p - 1))}
        >
          <ChevronLeft className="h-4 w-4" />
        </Button>
        <span className="text-sm text-muted-foreground">
          Page {pageNumber} of {numPages ?? "\u2014"}
        </span>
        <Button
          variant="outline"
          size="sm"
          disabled={numPages != null && pageNumber >= numPages}
          onClick={() => setUserPage((p) => Math.min(numPages ?? p, p + 1))}
        >
          <ChevronRight className="h-4 w-4" />
        </Button>
        {citationSection && (
          <Badge variant="outline" className="ml-auto text-xs">
            {citationSection}
          </Badge>
        )}
      </div>

      {/* PDF Document */}
      <div className="p-4 flex justify-center">
        <Document
          file={tariffPdfUrl()}
          onLoadSuccess={onDocumentLoadSuccess}
          loading={
            <div className="py-8 text-muted-foreground text-sm">
              Loading PDF...
            </div>
          }
          error={
            <div className="py-8 text-destructive text-sm">
              Failed to load tariff PDF. Ensure backend is running and the file
              exists in storage/pdfs.
            </div>
          }
        >
          <div className="relative inline-block">
            <Page
              pageNumber={pageNumber}
              scale={scale}
              renderTextLayer
              renderAnnotationLayer
            />
            {highlightBbox && highlightBbox.length >= 4 && (
              <div
                className="absolute pointer-events-none"
                style={bboxToStyle(highlightBbox, scale)}
                aria-hidden
              />
            )}
          </div>
        </Document>
      </div>
    </div>
  );
}

function bboxToStyle(
  bbox: number[],
  scale: number,
): React.CSSProperties {
  const [x0, y0, x1, y1] = bbox;
  return {
    left: x0 * scale,
    top: y0 * scale,
    width: (x1 - x0) * scale,
    height: (y1 - y0) * scale,
    border: "2px solid rgba(255, 200, 0, 0.9)",
    backgroundColor: "rgba(255, 200, 0, 0.15)",
  };
}
