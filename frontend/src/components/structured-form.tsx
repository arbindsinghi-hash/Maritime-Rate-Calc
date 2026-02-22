"use client";

import { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import type { CalculationRequest, FormConfig } from "@/lib/types";
import { getFormConfig } from "@/lib/api";

/* ── Fallback defaults (used only until the backend config loads) ──── */

const FALLBACK_PORTS: Record<string, string> = {
  durban: "Durban",
  other: "Other",
};

const FALLBACK_VESSEL_TYPES = [
  { id: "bulk_carrier", label: "Bulk Carrier" },
  { id: "other", label: "Other" },
];

const FALLBACK_PURPOSES = [
  { id: "cargo_loading", label: "Cargo Loading" },
  { id: "other", label: "Other" },
];

function defaultRequest(): CalculationRequest {
  const now = new Date();
  const dep = new Date(now);
  dep.setDate(dep.getDate() + 3);
  return {
    vessel_metadata: { name: "", flag: "" },
    technical_specs: {
      type: "Bulk Carrier",
      vessel_type: "bulk_carrier",
      gross_tonnage: 51300,
      loa_meters: 229.2,
    },
    operational_data: {
      port_id: "durban",
      days_alongside: 3.39,
      arrival_time: now.toISOString().slice(0, 19),
      departure_time: dep.toISOString().slice(0, 19),
      activity: "Exporting Iron Ore",
      purpose: "cargo_loading",
      num_operations: 2,
      num_holds: 7,
      num_tug_operations: 2,
      is_coaster: false,
      is_cargo_working: true,
      certifications: [],
    },
  };
}

interface Props {
  onSubmit: (req: CalculationRequest) => void;
  disabled?: boolean;
}

export function StructuredForm({ onSubmit, disabled }: Props) {
  const [req, setReq] = useState<CalculationRequest>(defaultRequest);
  const [cfg, setCfg] = useState<FormConfig>({
    ports: FALLBACK_PORTS,
    vessel_types: FALLBACK_VESSEL_TYPES,
    purposes: FALLBACK_PURPOSES,
  });
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [daysManuallyEdited, setDaysManuallyEdited] = useState(false);

  useEffect(() => {
    getFormConfig()
      .then(setCfg)
      .catch((err) => console.warn("Failed to load form config:", err));
  }, []);

  // Derived lookups
  const vesselTypeMap: Record<string, string> = {};
  for (const vt of cfg.vessel_types) {
    vesselTypeMap[vt.label] = vt.id;
  }

  const update = (
    path: keyof CalculationRequest,
    field: string,
    value: unknown,
  ) => {
    setReq((prev) => {
      const next = { ...prev };
      const obj = { ...(next[path] as Record<string, unknown>) };
      obj[field] = value;
      (next as Record<string, unknown>)[path] = obj;
      return next;
    });
    // Clear the error for this field when user edits it
    const errKey = `${path}.${field}`;
    setErrors((prev) => {
      if (!prev[errKey]) return prev;
      const next = { ...prev };
      delete next[errKey];
      return next;
    });
  };

  /** Compute days_alongside from arrival & departure datetimes */
  const computeDaysFromDates = (arrival: string, departure: string): number | null => {
    const a = new Date(arrival);
    const d = new Date(departure);
    if (isNaN(a.getTime()) || isNaN(d.getTime())) return null;
    const diffMs = d.getTime() - a.getTime();
    if (diffMs <= 0) return null;
    return Math.round((diffMs / (1000 * 60 * 60 * 24)) * 100) / 100; // 2 decimal places
  };

  /** Update arrival/departure and auto-sync days_alongside */
  const updateDateTime = (field: "arrival_time" | "departure_time", value: string) => {
    const isoValue = value + ":00";
    setReq((prev) => {
      const next = { ...prev };
      const op = { ...next.operational_data };
      (op as Record<string, unknown>)[field] = isoValue;

      // Auto-compute days if user hasn't manually overridden
      if (!daysManuallyEdited) {
        const arrival = field === "arrival_time" ? isoValue : op.arrival_time;
        const departure = field === "departure_time" ? isoValue : op.departure_time;
        const days = computeDaysFromDates(arrival, departure);
        if (days !== null) {
          op.days_alongside = days;
        }
      }

      next.operational_data = op;
      return next;
    });
    // Clear errors for the field and days
    setErrors((prev) => {
      const next = { ...prev };
      delete next[`operational_data.${field}`];
      if (!daysManuallyEdited) delete next["operational_data.days_alongside"];
      return next;
    });
  };

  const validate = (): boolean => {
    const errs: Record<string, string> = {};

    // Vessel Name
    if (!req.vessel_metadata.name.trim()) {
      errs["vessel_metadata.name"] = "Vessel name is required";
    }

    // Gross Tonnage
    if (
      req.technical_specs.gross_tonnage == null ||
      req.technical_specs.gross_tonnage <= 0
    ) {
      errs["technical_specs.gross_tonnage"] = "Must be greater than 0";
    }

    // LOA
    if (
      req.technical_specs.loa_meters == null ||
      req.technical_specs.loa_meters <= 0
    ) {
      errs["technical_specs.loa_meters"] = "Must be greater than 0";
    }

    // Port
    if (!req.operational_data.port_id) {
      errs["operational_data.port_id"] = "Please select a port";
    }

    // Days Alongside
    if (
      req.operational_data.days_alongside == null ||
      req.operational_data.days_alongside <= 0
    ) {
      errs["operational_data.days_alongside"] = "Must be greater than 0";
    }

    // Gross Tonnage max sanity check
    if (req.technical_specs.gross_tonnage > 500000) {
      errs["technical_specs.gross_tonnage"] =
        "Value seems too high — please verify (max 500,000 GT)";
    }

    // LOA max sanity check
    if (req.technical_specs.loa_meters > 500) {
      errs["technical_specs.loa_meters"] =
        "Value seems too high — please verify (max 500m)";
    }

    // Days sanity check
    if (req.operational_data.days_alongside > 365) {
      errs["operational_data.days_alongside"] =
        "Value seems too high — max 365 days";
    }

    // Num operations sanity
    if (
      req.operational_data.num_operations != null &&
      req.operational_data.num_operations > 50
    ) {
      errs["operational_data.num_operations"] = "Value seems too high (max 50)";
    }

    // Departure must be after Arrival
    {
      const a = new Date(req.operational_data.arrival_time);
      const d = new Date(req.operational_data.departure_time);
      if (!isNaN(a.getTime()) && !isNaN(d.getTime()) && d <= a) {
        errs["operational_data.departure_time"] =
          "Departure must be after arrival";
      }
    }

    setErrors(errs);
    return Object.keys(errs).length === 0;
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (validate()) {
      onSubmit(req);
    }
  };

  /** Helper: renders a label with required asterisk */
  const RequiredLabel = ({
    htmlFor,
    children,
  }: {
    htmlFor: string;
    children: React.ReactNode;
  }) => (
    <Label htmlFor={htmlFor}>
      {children} <span className="text-destructive">*</span>
    </Label>
  );

  /** Helper: renders field error message */
  const FieldError = ({ field }: { field: string }) =>
    errors[field] ? (
      <p className="text-xs text-destructive mt-1">{errors[field]}</p>
    ) : null;

  return (
    <form onSubmit={handleSubmit} className="space-y-6 max-w-2xl">
      {/* Vessel Details */}
      <Card>
        <CardHeader className="pb-4">
          <CardTitle className="text-base">Vessel Details</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-4 sm:grid-cols-2">
          <div className="space-y-2">
            <RequiredLabel htmlFor="vessel-name">Name</RequiredLabel>
            <Input
              id="vessel-name"
              value={req.vessel_metadata.name}
              onChange={(e) =>
                update("vessel_metadata", "name", e.target.value)
              }
              required
              placeholder="MV Example"
            />
            <FieldError field="vessel_metadata.name" />
          </div>
          <div className="space-y-2">
            <Label htmlFor="vessel-flag">Flag</Label>
            <Input
              id="vessel-flag"
              value={req.vessel_metadata.flag || ""}
              onChange={(e) =>
                update("vessel_metadata", "flag", e.target.value)
              }
              placeholder="e.g. Panama"
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="imo">IMO Number</Label>
            <Input
              id="imo"
              value={req.technical_specs.imo_number || ""}
              onChange={(e) =>
                update(
                  "technical_specs",
                  "imo_number",
                  e.target.value || null,
                )
              }
              placeholder="Optional"
            />
          </div>
          <div className="space-y-2">
            <RequiredLabel htmlFor="vessel-type">Type</RequiredLabel>
            <Select
              value={req.technical_specs.type}
              onValueChange={(v) => {
                update("technical_specs", "type", v);
                update(
                  "technical_specs",
                  "vessel_type",
                  vesselTypeMap[v] ?? "other",
                );
              }}
            >
              <SelectTrigger id="vessel-type">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {cfg.vessel_types.map((vt) => (
                  <SelectItem key={vt.id} value={vt.label}>
                    {vt.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <RequiredLabel htmlFor="gt">Gross Tonnage</RequiredLabel>
            <Input
              id="gt"
              type="number"
              min={1}
              value={req.technical_specs.gross_tonnage}
              onChange={(e) =>
                update("technical_specs", "gross_tonnage", Number(e.target.value))
              }
              required
            />
            <FieldError field="technical_specs.gross_tonnage" />
          </div>
          <div className="space-y-2">
            <RequiredLabel htmlFor="loa">LOA (m)</RequiredLabel>
            <Input
              id="loa"
              type="number"
              min={0}
              step={0.1}
              value={req.technical_specs.loa_meters}
              onChange={(e) =>
                update("technical_specs", "loa_meters", Number(e.target.value))
              }
              required
            />
            <FieldError field="technical_specs.loa_meters" />
          </div>
          <div className="space-y-2">
            <Label htmlFor="dwt">DWT</Label>
            <Input
              id="dwt"
              type="number"
              min={0}
              value={req.technical_specs.dwt ?? ""}
              onChange={(e) =>
                update(
                  "technical_specs",
                  "dwt",
                  e.target.value ? Number(e.target.value) : null,
                )
              }
              placeholder="Optional"
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="nt">Net Tonnage</Label>
            <Input
              id="nt"
              type="number"
              min={0}
              value={req.technical_specs.net_tonnage ?? ""}
              onChange={(e) =>
                update(
                  "technical_specs",
                  "net_tonnage",
                  e.target.value ? Number(e.target.value) : null,
                )
              }
              placeholder="Optional"
            />
          </div>
        </CardContent>
      </Card>

      {/* Visit Details */}
      <Card>
        <CardHeader className="pb-4">
          <CardTitle className="text-base">Visit Details</CardTitle>
        </CardHeader>
        <CardContent className="grid gap-4 sm:grid-cols-2">
          <div className="space-y-2">
            <RequiredLabel htmlFor="port">Port</RequiredLabel>
            <Select
              value={req.operational_data.port_id}
              onValueChange={(v) => update("operational_data", "port_id", v)}
            >
              <SelectTrigger id="port">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {Object.entries(cfg.ports).map(([id, label]) => (
                  <SelectItem key={id} value={id}>
                    {label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <FieldError field="operational_data.port_id" />
          </div>
          <div className="space-y-2">
            <Label htmlFor="purpose">Purpose</Label>
            <Select
              value={req.operational_data.purpose ?? "other"}
              onValueChange={(v) => update("operational_data", "purpose", v)}
            >
              <SelectTrigger id="purpose">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {cfg.purposes.map((p) => (
                  <SelectItem key={p.id} value={p.id}>
                    {p.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <RequiredLabel htmlFor="days">Days Alongside</RequiredLabel>
            <Input
              id="days"
              type="number"
              min={0}
              step={0.01}
              value={req.operational_data.days_alongside}
              onChange={(e) => {
                setDaysManuallyEdited(true);
                update(
                  "operational_data",
                  "days_alongside",
                  Number(e.target.value),
                );
              }}
            />
            {!daysManuallyEdited && (
              <p className="text-xs text-muted-foreground">
                Auto-calculated from arrival / departure
              </p>
            )}
            {daysManuallyEdited && (
              <button
                type="button"
                className="text-xs text-primary underline"
                onClick={() => {
                  setDaysManuallyEdited(false);
                  const days = computeDaysFromDates(
                    req.operational_data.arrival_time,
                    req.operational_data.departure_time,
                  );
                  if (days !== null) {
                    update("operational_data", "days_alongside", days);
                  }
                }}
              >
                Re-sync from dates
              </button>
            )}
            <FieldError field="operational_data.days_alongside" />
          </div>
          <div className="space-y-2">
            <Label htmlFor="arrival">Arrival</Label>
            <Input
              id="arrival"
              type="datetime-local"
              value={req.operational_data.arrival_time.slice(0, 16)}
              onChange={(e) => updateDateTime("arrival_time", e.target.value)}
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="departure">Departure</Label>
            <Input
              id="departure"
              type="datetime-local"
              value={req.operational_data.departure_time.slice(0, 16)}
              onChange={(e) => updateDateTime("departure_time", e.target.value)}
            />
            <FieldError field="operational_data.departure_time" />
          </div>
          <div className="space-y-2">
            <Label htmlFor="cargo-qty">Cargo Quantity (in MT)</Label>
            <Input
              id="cargo-qty"
              type="number"
              min={0}
              step={0.01}
              value={req.operational_data.cargo_quantity_mt ?? ""}
              onChange={(e) =>
                update(
                  "operational_data",
                  "cargo_quantity_mt",
                  e.target.value ? Number(e.target.value) : null,
                )
              }
              placeholder="Optional"
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="activity">Activity</Label>
            <Input
              id="activity"
              value={req.operational_data.activity}
              onChange={(e) =>
                update("operational_data", "activity", e.target.value)
              }
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="num-ops">Num Operations</Label>
            <Input
              id="num-ops"
              type="number"
              min={0}
              value={req.operational_data.num_operations ?? 0}
              onChange={(e) =>
                update(
                  "operational_data",
                  "num_operations",
                  Number(e.target.value),
                )
              }
            />
            <FieldError field="operational_data.num_operations" />
          </div>
          <div className="space-y-2">
            <Label htmlFor="num-holds">Num Holds</Label>
            <Input
              id="num-holds"
              type="number"
              min={0}
              value={req.operational_data.num_holds ?? 0}
              onChange={(e) =>
                update(
                  "operational_data",
                  "num_holds",
                  Number(e.target.value),
                )
              }
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="tug-ops">Tug Operations</Label>
            <Input
              id="tug-ops"
              type="number"
              min={0}
              value={req.operational_data.num_tug_operations ?? 0}
              onChange={(e) =>
                update(
                  "operational_data",
                  "num_tug_operations",
                  Number(e.target.value),
                )
              }
            />
          </div>
          <div className="flex items-center gap-4 sm:col-span-2">
            <label className="flex items-center gap-2 cursor-pointer text-sm">
              <input
                type="checkbox"
                checked={req.operational_data.is_coaster ?? false}
                onChange={(e) =>
                  update("operational_data", "is_coaster", e.target.checked)
                }
                className="rounded border-input"
              />
              Coaster vessel
            </label>
            <label className="flex items-center gap-2 cursor-pointer text-sm">
              <input
                type="checkbox"
                checked={req.operational_data.is_cargo_working ?? true}
                onChange={(e) =>
                  update(
                    "operational_data",
                    "is_cargo_working",
                    e.target.checked,
                  )
                }
                className="rounded border-input"
              />
              Cargo working
            </label>
          </div>
        </CardContent>
      </Card>

      <Button type="submit" disabled={disabled} className="w-full sm:w-auto">
        Calculate
      </Button>
    </form>
  );
}
