"use client";

import { useState, useEffect, useRef, useCallback } from "react";
import { getAudit, getAuditList } from "@/lib/api";
import type { AuditResponse, AuditSummary } from "@/lib/types";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { ChevronRight, ChevronDown, RefreshCw } from "lucide-react";

interface Props {
  /** Auto-set by the parent when a new calculation completes. */
  auditId: number | null;
}

export function AuditPanel({ auditId }: Props) {
  const [open, setOpen] = useState(false);
  const [audit, setAudit] = useState<AuditResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  /** The ID currently displayed — may come from the prop or the dropdown. */
  const [selectedId, setSelectedId] = useState<number | null>(null);

  /** List of recent audit summaries for the dropdown. */
  const [auditList, setAuditList] = useState<AuditSummary[]>([]);
  const [listLoading, setListLoading] = useState(false);

  const prevPropIdRef = useRef<number | null>(null);

  // ── Fetch the audit list ───────────────────────────────────────────────
  const fetchList = useCallback(() => {
    setListLoading(true);
    getAuditList()
      .then(setAuditList)
      .catch((e) => console.warn("Failed to load audit list:", e))
      .finally(() => setListLoading(false));
  }, []);

  // Refresh list whenever the panel opens
  useEffect(() => {
    if (open) fetchList();
  }, [open, fetchList]);

  // ── Sync selectedId when a new calculation comes in via prop ───────────
  useEffect(() => {
    if (auditId != null && auditId !== prevPropIdRef.current) {
      prevPropIdRef.current = auditId;
      setSelectedId(auditId);
      setOpen(true);
      // Refresh the list so the new entry appears in the dropdown
      fetchList();
    } else if (auditId == null) {
      prevPropIdRef.current = null;
    }
  }, [auditId, fetchList]);

  // ── Fetch full audit record when selectedId changes ────────────────────
  useEffect(() => {
    if (selectedId == null) {
      setAudit(null);
      setError(null);
      setLoading(false);
      return;
    }
    if (audit?.id === selectedId) return;

    let cancelled = false;
    setAudit(null);
    setError(null);
    setLoading(true);

    getAudit(selectedId)
      .then((data) => {
        if (!cancelled) setAudit(data);
      })
      .catch((e) => {
        if (!cancelled)
          setError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [selectedId, audit?.id]);

  /** Format a summary for the dropdown label. Prefer Document Q&A question when present. */
  const formatLabel = (s: AuditSummary) => {
    const ts = s.timestamp
      ? new Date(s.timestamp).toLocaleString(undefined, {
          month: "short",
          day: "numeric",
          hour: "2-digit",
          minute: "2-digit",
        })
      : "";
    const primary =
      s.user_message && s.user_message.trim()
        ? s.user_message.trim().length > 60
          ? s.user_message.trim().slice(0, 57) + "…"
          : s.user_message.trim()
        : s.vessel_name;
    return `#${s.id} — ${primary}${ts ? ` (${ts})` : ""}`;
  };

  return (
    <Collapsible
      open={open}
      onOpenChange={setOpen}
      className="rounded-lg border"
    >
      <CollapsibleTrigger asChild>
        <Button
          variant="ghost"
          className="w-full justify-start rounded-none px-4 py-3 text-sm font-medium hover:bg-muted"
        >
          {open ? (
            <ChevronDown className="mr-2 h-4 w-4" />
          ) : (
            <ChevronRight className="mr-2 h-4 w-4" />
          )}
          Audit trail
          {selectedId != null ? ` (#${selectedId})` : ""}
        </Button>
      </CollapsibleTrigger>

      <CollapsibleContent className="border-t bg-muted/30 p-4 space-y-4">
        {/* ── Dropdown selector ──────────────────────────────────────── */}
        <div className="flex items-center gap-2">
          <Select
            value={selectedId != null ? String(selectedId) : ""}
            onValueChange={(v) => setSelectedId(Number(v))}
          >
            <SelectTrigger className="w-full">
              <SelectValue
                placeholder={
                  listLoading
                    ? "Loading audits…"
                    : auditList.length === 0
                      ? "No audits yet"
                      : "Select an audit…"
                }
              />
            </SelectTrigger>
            <SelectContent>
              {auditList.map((s) => (
                <SelectItem key={s.id} value={String(s.id)}>
                  {formatLabel(s)}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Button
            type="button"
            variant="outline"
            size="icon"
            onClick={fetchList}
            disabled={listLoading}
            title="Refresh audit list"
          >
            <RefreshCw
              className={`h-4 w-4 ${listLoading ? "animate-spin" : ""}`}
            />
          </Button>
        </div>

        {/* ── Content ────────────────────────────────────────────────── */}
        {selectedId == null && (
          <p className="text-sm text-muted-foreground">
            Run a calculation or select an audit from the dropdown above.
          </p>
        )}
        {selectedId != null && loading && (
          <p className="text-sm text-muted-foreground">Loading…</p>
        )}
        {selectedId != null && error && (
          <p className="text-sm text-destructive">{error}</p>
        )}
        {selectedId != null && audit && (
          <pre className="text-xs overflow-x-auto whitespace-pre-wrap break-all font-mono">
            {JSON.stringify(
              {
                id: audit.id,
                vessel_name: audit.vessel_name,
                imo_number: audit.imo_number,
                timestamp: audit.timestamp,
                tariff_version: audit.tariff_version,
                input_data: audit.input_data,
                output_data: audit.output_data,
              },
              null,
              2,
            )}
          </pre>
        )}
      </CollapsibleContent>
    </Collapsible>
  );
}
