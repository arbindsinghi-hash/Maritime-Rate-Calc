"use client";

import { useState, useEffect, useRef, useCallback } from "react";
import { getChatStatus } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { KeyRound, CheckCircle2, AlertTriangle, Send, Mic, MicOff } from "lucide-react";

/* ---------- Web Speech API type shims (not in all TS libs) ---------- */
interface SpeechRecognitionResult {
  readonly isFinal: boolean;
  readonly length: number;
  [index: number]: { transcript: string; confidence: number };
}
interface SpeechRecognitionResultList {
  readonly length: number;
  [index: number]: SpeechRecognitionResult;
}
interface SpeechRecognitionEvent extends Event {
  readonly resultIndex: number;
  readonly results: SpeechRecognitionResultList;
}
interface SpeechRecognitionInstance extends EventTarget {
  continuous: boolean;
  interimResults: boolean;
  lang: string;
  start(): void;
  stop(): void;
  abort(): void;
  onresult: ((event: SpeechRecognitionEvent) => void) | null;
  onerror: ((event: Event) => void) | null;
  onend: (() => void) | null;
}
type SpeechRecognitionCtor = new () => SpeechRecognitionInstance;

declare global {
  interface Window {
    SpeechRecognition?: SpeechRecognitionCtor;
    webkitSpeechRecognition?: SpeechRecognitionCtor;
  }
}

const API_KEY_STORAGE_KEY = "marc_gemini_api_key";

interface Props {
  onSend: (message: string, apiKey?: string) => void;
  disabled?: boolean;
}

export function ChatMode({ onSend, disabled }: Props) {
  const [message, setMessage] = useState("");
  const [apiKey, setApiKey] = useState("");
  const [serverHasKey, setServerHasKey] = useState<boolean | null>(null);
  const [showKeyInput, setShowKeyInput] = useState(false);
  const [listening, setListening] = useState(false);
  const [speechSupported, setSpeechSupported] = useState(false);
  const recognitionRef = useRef<SpeechRecognitionInstance | null>(null);

  // Check for Web Speech API support
  useEffect(() => {
    const SpeechRecognitionCtor =
      typeof window !== "undefined"
        ? window.SpeechRecognition || window.webkitSpeechRecognition
        : undefined;
    if (SpeechRecognitionCtor) {
      setSpeechSupported(true);
      const recognition = new SpeechRecognitionCtor();
      recognition.continuous = true;
      recognition.interimResults = false;
      recognition.lang = "en-US";
      recognition.onresult = (event: SpeechRecognitionEvent) => {
        let transcript = "";
        for (let i = event.resultIndex; i < event.results.length; i++) {
          if (event.results[i].isFinal) {
            transcript += event.results[i][0].transcript;
          }
        }
        if (transcript) {
          setMessage((prev) => (prev ? prev + " " + transcript.trim() : transcript.trim()));
        }
      };
      recognition.onerror = () => setListening(false);
      recognition.onend = () => setListening(false);
      recognitionRef.current = recognition;
    }
    return () => {
      recognitionRef.current?.abort();
    };
  }, []);

  const toggleListening = useCallback(() => {
    const rec = recognitionRef.current;
    if (!rec) return;
    if (listening) {
      rec.stop();
    } else {
      rec.start();
      setListening(true);
    }
  }, [listening]);

  // Load saved key from localStorage (client-side only)
  useEffect(() => {
    const saved = localStorage.getItem(API_KEY_STORAGE_KEY) || "";
    setApiKey(saved);
  }, []);

  // Check if server already has a Gemini key configured
  useEffect(() => {
    getChatStatus()
      .then((s) => {
        setServerHasKey(s.gemini_configured);
        if (!s.gemini_configured && !localStorage.getItem(API_KEY_STORAGE_KEY)) {
          setShowKeyInput(true);
        }
      })
      .catch(() => setServerHasKey(false));
  }, []);

  const handleApiKeyChange = (val: string) => {
    setApiKey(val);
    if (val) {
      localStorage.setItem(API_KEY_STORAGE_KEY, val);
    } else {
      localStorage.removeItem(API_KEY_STORAGE_KEY);
    }
  };

  const effectiveKey = serverHasKey ? undefined : apiKey || undefined;
  const canSend = message.trim() && (serverHasKey || apiKey);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const m = message.trim();
    if (m && canSend) {
      onSend(m, effectiveKey);
      setMessage("");
    }
  };

  return (
    <form onSubmit={handleSubmit} className="space-y-4 max-w-2xl">
      <p className="text-sm text-muted-foreground">
        Ask about port tariff charges in plain English — powered by{" "}
        <Badge variant="secondary" className="text-xs">
          Gemini 2.5 Flash
        </Badge>
      </p>

      {/* API Key section — show if server has no key */}
      {serverHasKey === false && (
        <Card className="border-amber-200 bg-amber-50">
          <CardContent className="pt-4 space-y-3">
            {!showKeyInput ? (
              <Button
                type="button"
                variant="ghost"
                size="sm"
                onClick={() => setShowKeyInput(true)}
                className="text-amber-700 hover:text-amber-900"
              >
                <KeyRound className="mr-2 h-4 w-4" />
                Provide your Gemini API key
              </Button>
            ) : (
              <div className="flex items-center gap-2">
                <Input
                  type="password"
                  value={apiKey}
                  onChange={(e) => handleApiKeyChange(e.target.value)}
                  placeholder="Paste your Gemini API key here"
                  className="font-mono text-sm"
                />
                {apiKey && (
                  <span className="flex items-center gap-1 text-green-600 text-sm whitespace-nowrap">
                    <CheckCircle2 className="h-4 w-4" />
                    Saved
                  </span>
                )}
              </div>
            )}
            {!apiKey && showKeyInput && (
              <p className="text-xs text-muted-foreground">
                Get a free key at{" "}
                <a
                  href="https://aistudio.google.com/apikey"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-primary underline underline-offset-2"
                >
                  aistudio.google.com/apikey
                </a>
                . Your key is stored locally and sent only to this server.
              </p>
            )}
          </CardContent>
        </Card>
      )}

      {serverHasKey && (
        <p className="flex items-center gap-1.5 text-sm text-green-600">
          <CheckCircle2 className="h-4 w-4" />
          Server has Gemini API key configured
        </p>
      )}

      <div className="flex gap-2">
        <Input
          type="text"
          value={message}
          onChange={(e) => setMessage(e.target.value)}
          placeholder="Calculate dues for a 51300 GT bulk carrier at Durban for 3 days..."
          disabled={disabled}
          className="flex-1"
        />
        {speechSupported && (
          <Button
            type="button"
            variant={listening ? "destructive" : "outline"}
            size="icon"
            onClick={toggleListening}
            disabled={disabled}
            title={listening ? "Stop listening" : "Speak your query"}
            className={listening ? "animate-pulse" : ""}
          >
            {listening ? (
              <MicOff className="h-4 w-4" />
            ) : (
              <Mic className="h-4 w-4" />
            )}
          </Button>
        )}
        <Button type="submit" disabled={disabled || !canSend}>
          <Send className="mr-2 h-4 w-4" />
          Send
        </Button>
      </div>

      {!serverHasKey && !apiKey && (
        <Alert variant="destructive" className="border-amber-300 bg-amber-50 text-amber-800">
          <AlertTriangle className="h-4 w-4" />
          <AlertDescription>
            A Gemini API key is required for Document Q&amp;A. Use the{" "}
            <strong>Structured Form</strong> tab if you don&apos;t have one.
          </AlertDescription>
        </Alert>
      )}
    </form>
  );
}
