/**
 * API client for MRCA Tariff backend.
 * Uses Next.js rewrites — all /api calls are proxied to FastAPI.
 */

import type {
  CalculationRequest,
  CalculationResponse,
  ChatResponse,
  ChatStatus,
  AuditResponse,
  AuditSummary,
  FormConfig,
  PromptsConfig,
  PromptLog,
} from "./types";

const BASE = "/api/v1";

async function checkResponse(res: Response): Promise<void> {
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    const msg =
      typeof body.detail === "string"
        ? body.detail
        : Array.isArray(body.detail)
          ? JSON.stringify(body.detail)
          : res.statusText;
    throw new Error(msg);
  }
}

export async function calculate(
  request: CalculationRequest,
): Promise<CalculationResponse> {
  const res = await fetch(`${BASE}/calculate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(request),
  });
  await checkResponse(res);
  return res.json();
}

export async function chat(
  message: string,
  apiKey?: string,
): Promise<ChatResponse> {
  const body: Record<string, string> = { message };
  if (apiKey) body.api_key = apiKey;

  const res = await fetch(`${BASE}/chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  await checkResponse(res);
  return res.json();
}

export async function getChatStatus(): Promise<ChatStatus> {
  const res = await fetch(`${BASE}/chat/status`);
  await checkResponse(res);
  return res.json();
}

export async function getAudit(auditId: number): Promise<AuditResponse> {
  const res = await fetch(`${BASE}/audit/${auditId}`);
  await checkResponse(res);
  return res.json();
}

/** Fetch recent audit summaries for the dropdown selector. */
export async function getAuditList(
  limit = 50,
): Promise<AuditSummary[]> {
  const res = await fetch(`${BASE}/audit?limit=${limit}`);
  await checkResponse(res);
  return res.json();
}

/** Fetch valid ports, vessel types, and purposes from the backend YAML config. */
export async function getFormConfig(): Promise<FormConfig> {
  const res = await fetch(`${BASE}/config`);
  await checkResponse(res);
  return res.json();
}

/** URL for full tariff PDF (proxy to backend). */
export function tariffPdfUrl(filename = "Port Tariff.pdf"): string {
  return `${BASE}/tariff-pdf?filename=${encodeURIComponent(filename)}`;
}

/** URL for a single citation page PDF. */
export function citationPageUrl(
  chargeName: string,
  pdfFilename = "Port Tariff.pdf",
): string {
  return `${BASE}/citation/${encodeURIComponent(chargeName)}/page?pdf_filename=${encodeURIComponent(pdfFilename)}`;
}

/** Check whether the developer prompt panel is enabled on the backend. */
export async function getPromptsConfig(): Promise<PromptsConfig> {
  const res = await fetch(`${BASE}/prompts/config`);
  await checkResponse(res);
  return res.json();
}

/** Fetch recent chat interaction logs for the developer prompt panel. */
export async function getPromptLogs(limit = 50): Promise<PromptLog[]> {
  const res = await fetch(`${BASE}/prompts?limit=${limit}`);
  await checkResponse(res);
  return res.json();
}
