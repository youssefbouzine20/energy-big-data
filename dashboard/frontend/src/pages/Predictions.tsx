import { useEffect, useMemo, useState } from "react";
import { LineChart, Line, XAxis, YAxis, Tooltip, CartesianGrid, Legend, ResponsiveContainer } from "recharts";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { api, ApiError } from "@/lib/api";

type ActualPoint = { zone: string; window_start: string; avg_consumption: number };
type PredictedPoint = { zone: string; forecast_for: string; consumption_forecast: number; model_name?: string };
type PredictionsResp = { actual: ActualPoint[]; predicted: PredictedPoint[]; zone: string | null; hours: number };

const ZONES = ["A", "B", "C", "D"];

export function PredictionsPage() {
  const [zone, setZone] = useState("A");
  const [data, setData] = useState<PredictionsResp | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const res = await api.get<PredictionsResp>(`/api/predictions?zone=${zone}&hours=6`);
        if (!cancelled) { setData(res); setError(null); }
      } catch (err) {
        if (!cancelled) setError(err instanceof ApiError ? err.message : "failed to load predictions");
      }
    };
    load();
    const id = setInterval(load, 30_000);
    return () => { cancelled = true; clearInterval(id); };
  }, [zone]);

  const series = useMemo(() => {
    if (!data) return [];
    const byTime = new Map<string, { t: string; actual?: number; predicted?: number }>();
    for (const a of data.actual) {
      const t = new Date(a.window_start).toISOString();
      byTime.set(t, { t, actual: a.avg_consumption });
    }
    for (const p of data.predicted) {
      const t = new Date(p.forecast_for).toISOString();
      const existing = byTime.get(t) || { t };
      existing.predicted = p.consumption_forecast;
      byTime.set(t, existing);
    }
    return [...byTime.values()]
      .sort((a, b) => a.t.localeCompare(b.t))
      .map(p => ({ ...p, label: new Date(p.t).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }) }));
  }, [data]);

  return (
    <div className="space-y-6">
      <div className="flex items-end justify-between">
        <div>
          <h2 className="text-2xl font-bold tracking-tight">Predictions vs actuals</h2>
          <p className="text-sm text-muted-foreground">Last 6 hours, 15-min granularity</p>
        </div>
        <div className="flex gap-1">
          {ZONES.map(z => (
            <Button key={z} variant={zone === z ? "default" : "outline"} size="sm" onClick={() => setZone(z)}>
              Zone {z}
            </Button>
          ))}
        </div>
      </div>

      {error && (
        <div className="rounded-md border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm text-destructive">
          {error}
        </div>
      )}

      <Card>
        <CardHeader>
          <CardTitle>Zone {zone}</CardTitle>
          <CardDescription>Solid: actual avg consumption · Dashed: ML forecast</CardDescription>
        </CardHeader>
        <CardContent className="h-[420px]">
          {series.length === 0 ? (
            <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
              No data yet. Waiting for Spark aggregates and ML predictions…
            </div>
          ) : (
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={series}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="label" />
                <YAxis tickFormatter={(v) => v.toFixed(3)} />
                <Tooltip />
                <Legend />
                <Line type="monotone" dataKey="actual" stroke="#2563eb" dot={false} strokeWidth={2} name="Actual" />
                <Line type="monotone" dataKey="predicted" stroke="#f97316" strokeDasharray="6 4" dot={false} strokeWidth={2} name="Predicted" />
              </LineChart>
            </ResponsiveContainer>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
