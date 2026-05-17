import { useEffect, useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { api, ApiError } from "@/lib/api";
import { formatNumber, formatTime } from "@/lib/utils";

type Active = {
  zone: string;
  forecast_for: string;
  consumption_forecast: number;
  ratio_to_peak?: number;
  alert_level: "CRITICAL" | "WARNING" | "INFO" | "NORMAL";
  model_name?: string;
};

type AuditEntry = {
  triggered_at: string;
  zone: string;
  alert_level: string;
  forecast_kwh: number | null;
  ratio_to_peak: number | null;
  model_name: string | null;
  triggered_by: string | null;
};

export function AlertsPage() {
  const [active, setActive] = useState<Active[] | null>(null);
  const [audit, setAudit] = useState<AuditEntry[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const [a, h] = await Promise.all([
          api.get<{ items: Active[] }>("/api/alerts/active"),
          api.get<{ items: AuditEntry[] }>("/api/alerts/history?limit=100"),
        ]);
        if (!cancelled) { setActive(a.items); setAudit(h.items); setError(null); }
      } catch (err) {
        if (!cancelled) setError(err instanceof ApiError ? err.message : "failed to load alerts");
      }
    };
    load();
    const id = setInterval(load, 30_000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold tracking-tight">Predictive alerts</h2>
        <p className="text-sm text-muted-foreground">Active alerts + immutable audit log (Section H)</p>
      </div>

      {error && (
        <div className="rounded-md border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm text-destructive">
          {error}
        </div>
      )}

      <Card>
        <CardHeader>
          <CardTitle>Active alerts</CardTitle>
          <CardDescription>Forecasts above the saturation threshold for the next windows</CardDescription>
        </CardHeader>
        <CardContent>
          {!active ? (
            <p className="text-sm text-muted-foreground">Loading…</p>
          ) : active.length === 0 ? (
            <p className="text-sm text-muted-foreground">No active alerts — all zones within normal load.</p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Zone</TableHead>
                  <TableHead>Level</TableHead>
                  <TableHead>Forecast for</TableHead>
                  <TableHead>Forecast kWh</TableHead>
                  <TableHead>Ratio to peak</TableHead>
                  <TableHead>Model</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {active.map((a, i) => (
                  <TableRow key={i}>
                    <TableCell className="font-medium">{a.zone}</TableCell>
                    <TableCell>
                      <Badge variant={a.alert_level === "CRITICAL" ? "destructive" : "warning"}>{a.alert_level}</Badge>
                    </TableCell>
                    <TableCell className="whitespace-nowrap text-xs">{formatTime(a.forecast_for)}</TableCell>
                    <TableCell>{formatNumber(a.consumption_forecast, 4)}</TableCell>
                    <TableCell>{a.ratio_to_peak != null ? `${formatNumber(a.ratio_to_peak * 100, 0)}%` : "—"}</TableCell>
                    <TableCell className="text-xs">{a.model_name || "—"}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Audit log</CardTitle>
          <CardDescription>Every triggered alert is persisted to <code>dashboard_alerts</code> for review</CardDescription>
        </CardHeader>
        <CardContent>
          {!audit ? (
            <p className="text-sm text-muted-foreground">Loading…</p>
          ) : audit.length === 0 ? (
            <p className="text-sm text-muted-foreground">Audit log is empty.</p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Triggered at</TableHead>
                  <TableHead>Zone</TableHead>
                  <TableHead>Level</TableHead>
                  <TableHead>Forecast kWh</TableHead>
                  <TableHead>Model</TableHead>
                  <TableHead>By</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {audit.map((row, i) => (
                  <TableRow key={i}>
                    <TableCell className="whitespace-nowrap text-xs">{formatTime(row.triggered_at)}</TableCell>
                    <TableCell>{row.zone}</TableCell>
                    <TableCell><Badge variant={row.alert_level === "CRITICAL" ? "destructive" : "warning"}>{row.alert_level}</Badge></TableCell>
                    <TableCell>{row.forecast_kwh != null ? formatNumber(row.forecast_kwh, 4) : "—"}</TableCell>
                    <TableCell className="text-xs">{row.model_name || "—"}</TableCell>
                    <TableCell className="text-xs">{row.triggered_by || "—"}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
