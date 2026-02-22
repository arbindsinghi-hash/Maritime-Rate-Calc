"use client";

import type { ChargeBreakdown } from "@/lib/types";
import {
  Table,
  TableBody,
  TableCell,
  TableFooter,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";

interface Props {
  breakdown: ChargeBreakdown[];
  totalZar: number;
  currency?: string;
  onRowClick?: (charge: ChargeBreakdown) => void;
}

function formatNum(n: number): string {
  return n.toLocaleString("en-ZA", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

export function BreakdownTable({
  breakdown,
  totalZar,
  currency = "ZAR",
  onRowClick,
}: Props) {
  if (!breakdown.length) {
    return (
      <p className="text-muted-foreground italic text-sm">
        No charges. Submit a calculation via the form or Document Q&amp;A.
      </p>
    );
  }

  return (
    <div className="rounded-md border overflow-x-auto">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Charge</TableHead>
            <TableHead className="text-right">Basis</TableHead>
            <TableHead className="text-right">Rate</TableHead>
            <TableHead>Formula</TableHead>
            <TableHead className="text-right">
              Result ({currency})
            </TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {breakdown.map((row) => (
            <TableRow
              key={row.charge}
              onClick={() => onRowClick?.(row)}
              className={
                onRowClick
                  ? "cursor-pointer hover:bg-blue-50 transition-colors"
                  : undefined
              }
              title={
                onRowClick
                  ? `Page ${row.citation?.page}: ${row.citation?.section}`
                  : undefined
              }
            >
              <TableCell className="font-medium">{row.charge}</TableCell>
              <TableCell className="text-right tabular-nums">
                {formatNum(row.basis)}
              </TableCell>
              <TableCell className="text-right tabular-nums">
                {formatNum(row.rate)}
              </TableCell>
              <TableCell className="font-mono text-xs text-muted-foreground max-w-[240px] truncate">
                {row.formula}
              </TableCell>
              <TableCell className="text-right tabular-nums font-medium">
                {formatNum(row.result)}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
        <TableFooter>
          <TableRow>
            <TableCell colSpan={4} className="text-right font-semibold">
              Total
            </TableCell>
            <TableCell className="text-right tabular-nums font-bold">
              {formatNum(totalZar)}
            </TableCell>
          </TableRow>
        </TableFooter>
      </Table>
    </div>
  );
}
