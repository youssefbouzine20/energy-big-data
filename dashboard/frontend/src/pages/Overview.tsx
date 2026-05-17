import { useEffect, useState } from "react";
import { Activity, AlertTriangle, BellRing, Zap } from "lucide-react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { api, ApiError } from "@/lib/api";
import { formatNumber, formatTime } from "@/lib/utils";

type Kpis = {
  total_kwh_24h: number;
  anomalies_1h: number;
  active_incidents: number;
  avg_voltage: number;
  as_of: string;
};

type ActiveAlert = {
  zone: string;
  forecast_for: string;
  consumption_forecast: number;
  ratio_to_peak?: number;
  alert_level: "CRITICAL" | "WARNING" | "INFO" | "NORMAL";
  model_name?: string;
};

const REFRESH_MS = 30_000;

export function OverviewPage() {
  const [kpis, setKpis] = useState<Kpis | null>(null);
  const [alerts, setAlerts] = useState<ActiveAlert[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const [k, a] = await Promise.all([
          api.get<Kpis>("/api/overview/kpis"),
          api.get<{ items: ActiveAlert[] }>("/api/alerts/active"),
        ]);
        if (cancelled) return;
        setKpis(k);
        setAlerts(a.items);
        setError(null);
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof ApiError ? err.message : "failed to load overview");
      }
    };
    load();
    const id = setInterval(load, REFRESH_MS);
    return () => { cancelled = true; clearInterval(id); };
  }, []);

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold tracking-tight">Overview</h2>
        <p className="text-sm text-muted-foreground">
          Live grid status · auto-refresh every 30 s
          {kpis?.as_of && ` · last update ${formatTime(kpis.as_of)}`}
        </p>
      </div>

      {error && (
        <div className="rounded-md border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm text-destructive">
          {error}
        </div>
      )}

      <section>
        <h3 className="mb-3 text-sm font-medium text-muted-foreground">Active predictive alerts</h3>
        {!alerts ? (
          <SkeletonCard label="Loading alerts…" />
        ) : alerts.length === 0 ? (
          <Card>
            <CardContent className="py-6 text-sm text-muted-foreground">
              ✅ All zones operating within normal load.
            </CardContent>
          </Card>
        ) : (
          <div className="grid gap-3 md:grid-cols-2">
            {alerts.map((a, i) => (
              <Card key={i} className={a.alert_level === "CRITICAL" ? "border-destructive" : "border-amber-500"}>
                <CardHeader className="pb-2">
                  <div className="flex items-center justify-between">
                    <CardTitle className="text-base">Zone {a.zone}</CardTitle>
                    <Badge variant={a.alert_level === "CRITICAL" ? "destructive" : "warning"}>
                      {a.alert_level}
                    </Badge>
                  </div>
                  <CardDescription>Forecast for {formatTime(a.forecast_for)}</CardDescription>
                </CardHeader>
                <CardContent className="space-y-1 text-sm">
                  <div>Forecast: <span className="font-medium">{formatNumber(a.consumption_forecast, 3)} kWh</span></div>
                  {a.ratio_to_peak != null && (
                    <div>Ratio to peak: <span className="font-medium">{formatNumber(a.ratio_to_peak * 100, 0)}%</span></div>
                  )}
                  {a.model_name && <div className="text-xs text-muted-foreground">Model: {a.model_name}</div>}
                </CardContent>
              </Card>
            ))}
          </div>
        )}
      </section>

      <section className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <Kpi icon={<Zap className="h-4 w-4" />} label="Total kWh (24h)" value={kpis ? formatNumber(kpis.total_kwh_24h, 1) : "—"} />
        <Kpi icon={<AlertTriangle className="h-4 w-4" />} label="Anomalies (1h)" value={kpis ? String(kpis.anomalies_1h) : "—"} />
        <Kpi icon={<BellRing className="h-4 w-4" />} label="Active incidents" value={kpis ? String(kpis.active_incidents) : "—"} />
        <Kpi icon={<Activity className="h-4 w-4" />} label="Avg voltage (1h)" value={kpis ? `${formatNumber(kpis.avg_voltage, 1)} V` : "—"} />
      </section>
    </div>
  );
}

function Kpi({ icon, label, value }: { icon: React.ReactNode; label: string; value: string }) {
  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle className="text-sm font-medium text-muted-foreground">{label}</CardTitle>
        <div className="text-muted-foreground">{icon}</div>
      </CardHeader>
      <CardContent>
        <div className="text-2xl font-bold">{value}</div>
      </CardContent>
    </Card>
  );
}

function SkeletonCard({ label }: { label: string }) {
  return (
    <Card>
      <CardContent className="py-6 text-sm text-muted-foreground">{label}</CardContent>
    </Card>
  );
}
