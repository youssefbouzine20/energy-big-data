import { useEffect, useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { api, ApiError } from "@/lib/api";
import { formatNumber, formatTime } from "@/lib/utils";

type Zone = {
  zone: string;
  avg_kwh: number;
  total_kwh: number;
  anomalies: number;
  lat: number;
  lon: number;
};

export function HeatmapPage() {
  const [items, setItems] = useState<Zone[] | null>(null);
  const [asOf, setAsOf] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const data = await api.get<{ items: Zone[]; as_of: string }>("/api/heatmap");
        if (cancelled) return;
        setItems(data.items);
        setAsOf(data.as_of);
        setError(null);
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof ApiError ? err.message : "failed to load heatmap");
      }
    };
    load();
    const id = setInterval(load, 30_000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);

  const max = items?.reduce((m, z) => Math.max(m, z.avg_kwh), 0) || 1;

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold tracking-tight">Real-time consumption heatmap</h2>
        <p className="text-sm text-muted-foreground">
          Zones A-D · last 30 minutes
          {asOf && ` · ${formatTime(asOf)}`}
        </p>
      </div>

      {error && (
        <div className="rounded-md border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm text-destructive">
          {error}
        </div>
      )}

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        {(items || []).map((z) => {
          const intensity = Math.min(z.avg_kwh / max, 1);
          const hue = 60 - intensity * 60;
          return (
            <Card key={z.zone}>
              <CardHeader className="pb-2">
                <CardTitle>Zone {z.zone}</CardTitle>
                <CardDescription>{z.lat?.toFixed(3)}, {z.lon?.toFixed(3)}</CardDescription>
              </CardHeader>
              <CardContent className="space-y-3">
                <div
                  className="h-24 w-full rounded-md"
                  style={{ background: `hsl(${hue}, 90%, ${85 - intensity * 35}%)` }}
                />
                <div className="text-sm space-y-1">
                  <div>Avg: <span className="font-medium">{formatNumber(z.avg_kwh, 4)} kWh</span></div>
                  <div>Total: <span className="font-medium">{formatNumber(z.total_kwh, 2)} kWh</span></div>
                  <div>Anomalies: <span className="font-medium">{z.anomalies}</span></div>
                </div>
              </CardContent>
            </Card>
          );
        })}
        {items && items.length === 0 && (
          <Card className="md:col-span-2 lg:col-span-4">
            <CardContent className="py-6 text-sm text-muted-foreground">
              Waiting for first aggregated 15-min window from Spark…
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  );
}
