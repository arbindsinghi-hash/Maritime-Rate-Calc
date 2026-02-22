"use client";

import { useState, useEffect } from "react";
import Image from "next/image";
import { calculate, chat, getPromptsConfig } from "@/lib/api";
import type { CalculationResponse, ChargeBreakdown } from "@/lib/types";
import { StructuredForm } from "@/components/structured-form";
import { ChatMode } from "@/components/chat-mode";
import { BreakdownTable } from "@/components/breakdown-table";
import { PdfViewer } from "@/components/pdf-viewer";
import { AuditPanel } from "@/components/audit-panel";
import { PromptsPanel } from "@/components/prompts-panel";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Separator } from "@/components/ui/separator";
import { Badge } from "@/components/ui/badge";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Loader2, Info } from "lucide-react";

export default function Home() {
  const [response, setResponse] = useState<CalculationResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [auditId, setAuditId] = useState<number | null>(null);
  const [selectedCharge, setSelectedCharge] =
    useState<ChargeBreakdown | null>(null);
  const [extractedFields, setExtractedFields] = useState<Record<
    string,
    unknown
  > | null>(null);
  const [promptPanelEnabled, setPromptPanelEnabled] = useState(false);

  // Check if the developer prompt panel is enabled on the backend
  useEffect(() => {
    getPromptsConfig()
      .then((cfg) => setPromptPanelEnabled(cfg.enabled))
      .catch(() => setPromptPanelEnabled(false));
  }, []);

  const handleCalculate = async (
    body: Parameters<typeof calculate>[0],
  ) => {
    setLoading(true);
    setError(null);
    setExtractedFields(null);
    try {
      const res = await calculate(body);
      setResponse(res);
      setAuditId(res.audit_id ?? null);
      setSelectedCharge(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setResponse(null);
      setAuditId(null);
    } finally {
      setLoading(false);
    }
  };

  const handleChat = async (message: string, apiKey?: string) => {
    setLoading(true);
    setError(null);
    setExtractedFields(null);
    try {
      const res = await chat(message, apiKey);
      setResponse(res);
      setAuditId(res.audit_id ?? null);
      setSelectedCharge(null);
      setExtractedFields(res.extracted_fields ?? null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setResponse(null);
      setAuditId(null);
    } finally {
      setLoading(false);
    }
  };

  const totalZar = response?.total_with_vat ?? response?.total_zar ?? 0;
  const breakdown = response?.breakdown ?? [];

  return (
    <div className="mx-auto max-w-6xl px-4 py-6 sm:px-6">
      {/* Header */}
      <header className="flex items-center gap-3 mb-6 pb-4 border-b">
        <div className="flex items-center gap-2 shrink-0">
          <Image
            src="/marc-logo-icon.png"
            alt="Marc logo"
            width={40}
            height={40}
            className="h-10 w-auto"
          />
          <span className="text-xl font-bold tracking-tight text-blue-900">
            Marc
          </span>
        </div>
        <div className="flex-1">
          <h1 className="text-2xl font-semibold tracking-tight">
            Port Tariff Calculator
          </h1>
          <p className="text-sm text-muted-foreground">
            Compute vessel dues with auditable citations
          </p>
        </div>
      </header>

      {/* Input Section */}
      <section className="mb-8">
        <Tabs defaultValue="form">
          <TabsList>
            <TabsTrigger value="form">Structured Form</TabsTrigger>
            <TabsTrigger value="chat">Document Q&amp;A</TabsTrigger>
            {promptPanelEnabled && (
              <TabsTrigger value="prompts">
                Prompts
                <Badge variant="outline" className="ml-1.5 text-[10px] px-1 py-0">
                  dev
                </Badge>
              </TabsTrigger>
            )}
          </TabsList>
          <TabsContent value="form" className="mt-4">
            <StructuredForm onSubmit={handleCalculate} disabled={loading} />
          </TabsContent>
          <TabsContent value="chat" className="mt-4">
            <ChatMode onSend={handleChat} disabled={loading} />
          </TabsContent>
          {promptPanelEnabled && (
            <TabsContent value="prompts" className="mt-4">
              <PromptsPanel />
            </TabsContent>
          )}
        </Tabs>
      </section>

      {/* Loading / Error */}
      {loading && (
        <div className="flex items-center gap-2 text-primary text-sm mb-4">
          <Loader2 className="h-4 w-4 animate-spin" />
          Calculating...
        </div>
      )}
      {error && (
        <Alert className="mb-4 border-blue-200 bg-blue-50 text-blue-900">
          <Info className="h-4 w-4 text-blue-600" />
          <AlertDescription className="text-sm">{error}</AlertDescription>
        </Alert>
      )}

      {/* Results */}
      {response && (
        <section className="mb-8 space-y-4">
          {extractedFields && (
            <details className="rounded-lg border border-blue-200 bg-blue-50 text-sm">
              <summary className="cursor-pointer px-4 py-2 font-semibold text-blue-900">
                Extracted from your message
              </summary>
              <pre className="px-4 py-2 border-t border-blue-200 text-xs overflow-x-auto whitespace-pre-wrap break-all font-mono">
                {JSON.stringify(extractedFields, null, 2)}
              </pre>
            </details>
          )}

          <h2 className="text-lg font-semibold">Breakdown</h2>
          <BreakdownTable
            breakdown={breakdown}
            totalZar={totalZar}
            currency={response.currency}
            onRowClick={setSelectedCharge}
          />
          {response.tariff_version && (
            <Badge variant="outline" className="text-xs">
              Tariff: {response.tariff_version}
            </Badge>
          )}
        </section>
      )}

      <Separator className="my-6" />

      {/* PDF Viewer */}
      <section className="mb-8">
        <h2 className="text-lg font-semibold mb-3">Tariff PDF</h2>
        <PdfViewer
          highlightPage={selectedCharge?.citation?.page ?? null}
          highlightBbox={selectedCharge?.citation?.bounding_box ?? null}
          citationSection={selectedCharge?.citation?.section}
        />
      </section>

      {/* Audit Panel */}
      <AuditPanel auditId={auditId} />
    </div>
  );
}
