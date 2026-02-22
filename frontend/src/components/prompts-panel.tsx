"use client";

import { useState, useEffect, useCallback } from "react";
import { getPromptLogs } from "@/lib/api";
import type { PromptLog } from "@/lib/types";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { RefreshCw, Clock, AlertTriangle, CheckCircle2, Info, ChevronDown, ChevronRight } from "lucide-react";

export function PromptsPanel() {
  const [logs, setLogs] = useState<PromptLog[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expandedId, setExpandedId] = useState<string | null>(null);

  const fetchLogs = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await getPromptLogs(100);
      setLogs(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchLogs();
  }, [fetchLogs]);

  const toggleExpand = (id: string) => {
    setExpandedId((prev) => (prev === id ? null : id));
  };

  const formatTime = (ts: string) => {
    try {
      const d = new Date(ts);
      return d.toLocaleString();
    } catch {
      return ts;
    }
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold">LLM Prompt Interactions</h2>
          <p className="text-sm text-muted-foreground">
            Developer view of Document Q&amp;A queries, system prompts, and LLM responses.
          </p>
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={fetchLogs}
          disabled={loading}
        >
          <RefreshCw className={`mr-2 h-4 w-4 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </Button>
      </div>

      {error && (
        <p className="text-sm text-destructive">{error}</p>
      )}

      {logs.length === 0 && !loading && !error && (
        <p className="text-sm text-muted-foreground py-8 text-center">
          No interactions yet. Use the Document Q&amp;A tab to send a query.
        </p>
      )}

      <div className="space-y-3">
        {logs.map((log) => {
          const isExpanded = expandedId === log.id;
          const isOffTopic = log.error?.startsWith("off_topic:");
          const hasError = !!log.error && !isOffTopic;

          return (
            <Card
              key={log.id}
              className={`transition-colors ${
                hasError
                  ? "border-red-300"
                  : isOffTopic
                    ? "border-slate-200"
                    : "border-green-200"
              }`}
            >
              {/* Clickable summary row */}
              <div
                className="flex items-start gap-3 px-4 py-3 cursor-pointer hover:bg-muted/50"
                onClick={() => toggleExpand(log.id)}
              >
                <div className="mt-0.5 shrink-0">
                  {isExpanded ? (
                    <ChevronDown className="h-4 w-4 text-muted-foreground" />
                  ) : (
                    <ChevronRight className="h-4 w-4 text-muted-foreground" />
                  )}
                </div>

                <div className="flex-1 min-w-0">
                  <p className="text-sm font-medium truncate">
                    &ldquo;{log.user_message}&rdquo;
                  </p>
                  <div className="flex items-center gap-3 mt-1 text-xs text-muted-foreground">
                    <span className="flex items-center gap-1">
                      <Clock className="h-3 w-3" />
                      {formatTime(log.timestamp)}
                    </span>
                    <span>{log.duration_ms}ms</span>
                    <Badge
                      variant={hasError ? "destructive" : "secondary"}
                      className="text-[10px] px-1.5 py-0"
                    >
                      {isOffTopic ? "off-topic" : hasError ? "error" : "ok"}
                    </Badge>
                  </div>
                </div>

                <div className="shrink-0 mt-0.5">
                  {hasError ? (
                    <AlertTriangle className="h-4 w-4 text-amber-500" />
                  ) : isOffTopic ? (
                    <Info className="h-4 w-4 text-slate-500" />
                  ) : (
                    <CheckCircle2 className="h-4 w-4 text-green-500" />
                  )}
                </div>
              </div>

              {/* Expanded detail */}
              {isExpanded && (
                <CardContent className="pt-0 space-y-4 border-t">
                  {/* Outcome (or Error for real errors) */}
                  {log.error && (
                    <div>
                      <CardHeader className="p-0 pt-3 pb-1">
                        <CardTitle className={`text-xs font-semibold uppercase ${hasError ? "text-destructive" : "text-muted-foreground"}`}>
                          {hasError ? "Error / Outcome" : "Outcome"}
                        </CardTitle>
                      </CardHeader>
                      <pre className={`text-xs rounded p-2 whitespace-pre-wrap break-all font-mono ${hasError ? "bg-red-50 text-red-800" : "bg-muted text-foreground"}`}>
                        {log.error}
                      </pre>
                    </div>
                  )}

                  {/* Parsed Data */}
                  {log.parsed_data && (
                    <div>
                      <CardHeader className="p-0 pb-1">
                        <CardTitle className="text-xs font-semibold uppercase text-muted-foreground">
                          Parsed Extraction
                        </CardTitle>
                      </CardHeader>
                      <pre className="text-xs bg-green-50 text-green-900 rounded p-2 whitespace-pre-wrap break-all font-mono">
                        {JSON.stringify(log.parsed_data, null, 2)}
                      </pre>
                    </div>
                  )}

                  {/* Raw LLM Response */}
                  {log.raw_llm_response && (
                    <div>
                      <CardHeader className="p-0 pb-1">
                        <CardTitle className="text-xs font-semibold uppercase text-muted-foreground">
                          Raw LLM Response
                        </CardTitle>
                      </CardHeader>
                      <pre className="text-xs bg-muted rounded p-2 whitespace-pre-wrap break-all font-mono max-h-48 overflow-y-auto">
                        {log.raw_llm_response}
                      </pre>
                    </div>
                  )}

                  {/* System Prompt */}
                  <div>
                    <CardHeader className="p-0 pb-1">
                      <CardTitle className="text-xs font-semibold uppercase text-muted-foreground">
                        System Prompt Sent
                      </CardTitle>
                      <CardDescription className="text-[10px]">
                        The full prompt appended before the user&apos;s message
                      </CardDescription>
                    </CardHeader>
                    <pre className="text-xs bg-muted rounded p-2 whitespace-pre-wrap break-all font-mono max-h-48 overflow-y-auto">
                      {log.system_prompt}
                    </pre>
                  </div>
                </CardContent>
              )}
            </Card>
          );
        })}
      </div>
    </div>
  );
}
