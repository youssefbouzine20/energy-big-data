import { useEffect, useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { api, ApiError } from "@/lib/api";
import { formatNumber, formatTime } from "@/lib/utils";

type Metric = {
  topic: string;
  window_start: string;
  completeness_pct: number;
  noise_rate_pct: number;
  anomaly_rate_pct: number;
  temporal_coverage_pct: number;
  alert: string | null;
};
type LatestItem = { topic: string; latest: Metric | null };

export function QualityPage() {
  const [items, setItems] = useState<LatestItem[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const res = await api.get<{ items: LatestItem[] }>("/api/quality/latest");
        if (!cancelled) { setItems(res.items); setError(null); }
      } catch (err) {
        if (!cancelled) setError(err instanceof ApiError ? err.message : "failed to load quality metrics");
      }
    };
    load();
    const id = setInterval(load, 60_000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold tracking-tight">Data quality</h2>
        <p className="text-sm text-muted-foreground">Per-topic completeness, noise, and coverage (Section E)</p>
      </div>

      {error && (
        <div className="rounded-md border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm text-destructive">
          {error}
        </div>
      )}

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        {(items || []).map(({ topic, latest }) => (
          <Card key={topic}>
            <CardHeader>
              <div className="flex items-center justify-between">
                <CardTitle className="text-base">{topic}</CardTitle>
                {latest?.alert ? (
                  <Badge variant={latest.alert === "CRIT" ? "destructive" : "warning"}>{latest.alert}</Badge>
                ) : (
                  <Badge variant="success">OK</Badge>
                )}
              </div>
              <CardDescription>
                {latest ? `Window ${formatTime(latest.window_start)}` : "No data yet"}
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-2 text-sm">
              {latest ? (
                <>
                  <Stat label="Completeness" value={`${formatNumber(latest.completeness_pct, 1)}%`} />
                  <Stat label="Noise rate" value={`${formatNumber(latest.noise_rate_pct, 2)}%`} />
                  <Stat label="Anomaly rate" value={`${formatNumber(latest.anomaly_rate_pct, 1)}%`} />
                  <Stat label="Temporal coverage" value={`${formatNumber(latest.temporal_coverage_pct, 0)}%`} />
                </>
              ) : (
                <p className="text-muted-foreground">Waiting for Spark to write data_quality_metrics…</p>
              )}
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between">
      <span className="text-muted-foreground">{label}</span>
      <span className="font-medium">{value}</span>
    </div>
  );
}
