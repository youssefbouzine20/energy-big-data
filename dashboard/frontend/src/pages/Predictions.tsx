import { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { Header } from '../components/Header';
import { api } from '../lib/api';
import { DemoBanner } from '../components/DemoBanner';

const ZONES = ['A', 'B', 'C', 'D'];
const HOURS = [3, 6, 12, 24];

const MODEL_LABEL: Record<string, string> = {
  linear_regression: 'Linear Regression',
  random_forest:     'Random Forest',
  gradient_boosting: 'Gradient Boosting',
};

type ModelMetrics = {
  name: string;
  r2: number;
  mae: number;
  rmse: number;
  infer_time_us_per_sample?: number;
};

export function Predictions() {
  const [zone, setZone]         = useState('A');
  const [hours, setHours]       = useState(6);
  const [data, setData]         = useState<any[]>([]);
  const [loading, setLoading]   = useState(true);
  const [metrics, setMetrics]   = useState<ModelMetrics | null>(null);
  const [usingDemo, setUsingDemo] = useState(false);

  // ML metrics from ml/metrics.json (best model by R²). Refreshed on mount.
  useEffect(() => {
    api.get<any>('/api/ml/metrics')
      .then(m => { if (m?.best) setMetrics(m.best as ModelMetrics); })
      .catch(() => setMetrics(null));
  }, []);

  // Prediction chart data. actual=past windows, predicted=future forecasts.
  useEffect(() => {
    const load = async () => {
      setLoading(true);
      try {
        const d = await api.get<any>(`/api/predictions?zone=${zone}&hours=${hours}`);
        const actualArr: any[]    = d?.actual    ?? [];
        const predictedArr: any[] = d?.predicted ?? [];

        if (actualArr.length || predictedArr.length) {
          // Bucket both arrays by 15-min timestamp so actual and predicted line
          // up on the X axis. Where a bucket has only actual OR only predicted,
          // the other field stays undefined and Recharts renders a gap (with
          // connectNulls={false} below).
          const BUCKET_MS = 15 * 60 * 1000;
          const bucket = (dt: Date) => Math.floor(dt.getTime() / BUCKET_MS) * BUCKET_MS;
          const fmt = (ms: number) =>
            new Date(ms).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

          const rows = new Map<number, { ts: number; t: string; actual?: number; predicted?: number }>();
          for (const pt of actualArr) {
            const ts  = bucket(new Date(pt.window_start ?? pt.ts ?? pt.timestamp));
            const row = rows.get(ts) ?? { ts, t: fmt(ts) };
            row.actual = pt.avg_consumption ?? pt.value ?? pt.consumption;
            rows.set(ts, row);
          }
          for (const pt of predictedArr) {
            const ts  = bucket(new Date(pt.forecast_for ?? pt.ts ?? pt.timestamp));
            const row = rows.get(ts) ?? { ts, t: fmt(ts) };
            row.predicted = pt.consumption_forecast ?? pt.value ?? pt.forecast;
            rows.set(ts, row);
          }
          const merged = [...rows.values()].sort((a, b) => a.ts - b.ts);
          setData(merged);
          setUsingDemo(false);
        } else {
          // demo fallback
          const base = ({ A: 3200, B: 4100, C: 2100, D: 2800 } as any)[zone] ?? 3000;
          setData(Array.from({ length: hours * 4 }, (_, i) => {
            const actual = base + Math.sin(i / 6) * 600 + Math.random() * 150;
            return {
              t: `+${Math.floor(i / 4)}h${(i % 4) * 15}m`,
              actual: Math.round(actual),
              predicted: Math.round(actual * (0.96 + Math.random() * 0.08)),
            };
          }));
          setUsingDemo(true);
        }
      } catch {
        const base = ({ A: 3200, B: 4100, C: 2100, D: 2800 } as any)[zone] ?? 3000;
        setData(Array.from({ length: hours * 4 }, (_, i) => {
          const actual = base + Math.sin(i / 6) * 600 + Math.random() * 150;
          return {
            t: `+${Math.floor(i / 4)}h${(i % 4) * 15}m`,
            actual: Math.round(actual),
            predicted: Math.round(actual * (0.96 + Math.random() * 0.08)),
          };
        }));
        setUsingDemo(true);
      } finally {
        setLoading(false);
      }
    };
    load();
    const t = setInterval(load, 30_000);
    return () => clearInterval(t);
  }, [zone, hours]);

  // KPIs from training metrics — truthful, not derived from chart math.
  const r2Pct      = metrics?.r2  != null ? `${(metrics.r2 * 100).toFixed(1)}%` : '—';
  const maeDisplay = metrics?.mae != null ? `${metrics.mae.toFixed(4)} kWh`     : '—';
  const modelLabel = metrics?.name ? (MODEL_LABEL[metrics.name] ?? metrics.name) : '—';

  return (
    <div style={{ animation: 'fade-in 0.4s ease-out both' }}>
      <Header title="Predictions" subtitle="Actual vs predicted consumption curves" />
      <div style={{ paddingTop: 24 }}>

        {usingDemo && <DemoBanner sources={['/api/predictions']} message="Predictions chart shows synthetic demonstration data" />}

        {/* Controls */}
        <div style={{ display: 'flex', gap: 12, marginBottom: 20 }}>
          <div className="card-atonist" style={{ padding: '8px 12px', display: 'flex', alignItems: 'center', gap: 8 }}>
            <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>Zone:</span>
            {ZONES.map(z => (
              <button key={z} onClick={() => setZone(z)} style={{
                padding: '5px 14px', borderRadius: 6, border: 'none', cursor: 'pointer',
                background: zone === z ? 'var(--accent-purple)' : 'var(--bg-input)',
                color: zone === z ? '#fff' : 'var(--text-secondary)',
                fontSize: 13, fontWeight: zone === z ? 600 : 400,
                transition: 'all 0.18s',
              }}>Zone {z}</button>
            ))}
          </div>
          <div className="card-atonist" style={{ padding: '8px 12px', display: 'flex', alignItems: 'center', gap: 8 }}>
            <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>Window:</span>
            {HOURS.map(h => (
              <button key={h} onClick={() => setHours(h)} style={{
                padding: '5px 14px', borderRadius: 6, border: 'none', cursor: 'pointer',
                background: hours === h ? 'var(--accent-purple)' : 'var(--bg-input)',
                color: hours === h ? '#fff' : 'var(--text-secondary)',
                fontSize: 13, fontWeight: hours === h ? 600 : 400,
                transition: 'all 0.18s',
              }}>{h}h</button>
            ))}
          </div>
        </div>

        {/* KPI cards — values come from ml/metrics.json via /api/ml/metrics */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: 16, marginBottom: 20 }}>
          {[
            { label: 'Best Model R²', value: r2Pct,                 sub: modelLabel,          color: 'var(--success)' },
            { label: 'Test MAE',      value: maeDisplay,            sub: 'per meter / 15 min', color: 'var(--accent-gold)' },
            { label: 'Data Points',   value: String(data.length),   sub: 'in chart',           color: 'var(--accent-purple-light)' },
          ].map(({ label, value, sub, color }) => (
            <div key={label} className="card-atonist" style={{ padding: '16px 20px' }}>
              <div style={{ fontSize: 12, color: 'var(--text-muted)', marginBottom: 6 }}>{label}</div>
              <div style={{ fontFamily: 'Space Grotesk', fontSize: 28, fontWeight: 700, color }}>{value}</div>
              <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 4 }}>{sub}</div>
            </div>
          ))}
        </div>

        {/* Main chart */}
        <div className="card-atonist" style={{ padding: '20px' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 20 }}>
            <h3 style={{ fontFamily: 'Space Grotesk', fontSize: 15, fontWeight: 600, color: 'var(--text-primary)', margin: 0 }}>
              Zone {zone} — Actual vs Predicted ({hours}h window)
            </h3>
            {loading && <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>Loading…</span>}
          </div>
          <ResponsiveContainer width="100%" height={340}>
            <LineChart data={data} margin={{ top: 5, right: 20, left: -10, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border-default)" />
              <XAxis dataKey="t" tick={{ fontSize: 10, fill: 'var(--text-muted)' }} tickLine={false} axisLine={false} interval={Math.max(0, Math.floor(data.length / 8))} />
              <YAxis tick={{ fontSize: 10, fill: 'var(--text-muted)' }} tickLine={false} axisLine={false} />
              <Tooltip
                contentStyle={{ background: 'var(--bg-card)', border: '1px solid var(--border-subtle)', borderRadius: 8, fontSize: 12 }}
                labelStyle={{ color: 'var(--text-primary)', fontWeight: 600 }}
              />
              <Legend iconType="circle" iconSize={8} wrapperStyle={{ fontSize: 12, color: 'var(--text-secondary)' }} />
              <Line type="monotone" dataKey="actual"    name="Actual"    stroke="#7c3aed" strokeWidth={2.5} dot={false} activeDot={{ r: 4, fill: '#7c3aed' }} connectNulls={false} />
              <Line type="monotone" dataKey="predicted" name="Predicted" stroke="#f59e0b" strokeWidth={2}   strokeDasharray="6 3" dot={false} activeDot={{ r: 4, fill: '#f59e0b' }} connectNulls={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
}
