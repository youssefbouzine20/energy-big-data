import { useState, useEffect } from 'react';
import { Header } from '../components/Header';
import { api } from '../lib/api';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

const COLLECTIONS = [
  { key: 'meters_aggregated_15min', label: 'Meters (15min agg.)', icon: '⚡' },
  { key: 'incidents_enriched',       label: 'Incidents Enriched',  icon: '🔧' },
  { key: 'ml_predictions',           label: 'ML Predictions',      icon: '🤖' },
  { key: 'feedback_nlp',             label: 'Feedback NLP',        icon: '💬' },
  { key: 'data_quality_metrics',     label: 'Quality Metrics',     icon: '📊' },
  { key: 'dashboard_alerts',         label: 'Dashboard Alerts',    icon: '🔔' },
];

function getGrade(pct: number) {
  if (pct >= 95) return { label: 'Excellent', color: '#22c55e' };
  if (pct >= 80) return { label: 'Good',      color: '#f59e0b' };
  if (pct >= 60) return { label: 'Fair',      color: '#f97316' };
  return { label: 'Poor', color: '#ef4444' };
}

export function Quality() {
  const [metrics, setMetrics] = useState<any[]>([]);

  useEffect(() => {
    const load = async () => {
      try {
        const d = await api.get('/api/quality/latest');
        if (d?.metrics?.length) { setMetrics(d.metrics); return; }
      } catch {}
      // fallback demo
      setMetrics(COLLECTIONS.map(c => ({
        collection: c.key,
        completeness: [97, 84, 92, 78, 100, 95][COLLECTIONS.indexOf(c)],
        freshness_minutes: [5, 12, 8, 45, 2, 1][COLLECTIONS.indexOf(c)],
        record_count: [142300, 8920, 34560, 2100, 500, 89][COLLECTIONS.indexOf(c)],
      })));
    };
    load();
    const t = setInterval(load, 30_000);
    return () => clearInterval(t);
  }, []);

  const chartData = metrics.map((m, i) => ({
    name: COLLECTIONS[i]?.label?.split(' ')[0] ?? m.collection,
    completeness: m.completeness,
  }));

  return (
    <div style={{ animation: 'fade-in 0.4s ease-out both' }}>
      <Header title="Data Quality" subtitle="Collection completeness and freshness metrics" />
      <div style={{ paddingTop: 24 }}>

        {/* Cards */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: 16, marginBottom: 24 }}>
          {COLLECTIONS.map((col, i) => {
            const m = metrics[i] ?? {};
            const pct = m.completeness ?? 0;
            const { label, color } = getGrade(pct);
            return (
              <div key={col.key} className="card-atonist" style={{ padding: '18px 20px' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 12 }}>
                  <div style={{ fontSize: 20 }}>{col.icon}</div>
                  <span style={{ fontSize: 10, fontWeight: 600, color, background: `${color}18`, padding: '3px 8px', borderRadius: 4 }}>{label}</span>
                </div>
                <div style={{ fontSize: 12, fontFamily: 'monospace', color: 'var(--text-muted)', marginBottom: 6 }}>{col.key}</div>
                <div style={{ fontFamily: 'Space Grotesk', fontSize: 28, fontWeight: 700, color, marginBottom: 10 }}>{pct}%</div>
                <div style={{ height: 6, borderRadius: 3, background: 'var(--bg-input)', overflow: 'hidden', marginBottom: 10 }}>
                  <div style={{ height: '100%', width: `${pct}%`, background: color, borderRadius: 3, transition: 'width 1.5s ease' }} />
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, color: 'var(--text-muted)' }}>
                  <span>{(m.record_count ?? 0).toLocaleString()} records</span>
                  <span>Fresh {m.freshness_minutes ?? 0}m ago</span>
                </div>
              </div>
            );
          })}
        </div>

        {/* Bar chart */}
        <div className="card-atonist" style={{ padding: '20px' }}>
          <h3 style={{ fontFamily: 'Space Grotesk', fontSize: 15, fontWeight: 600, color: 'var(--text-primary)', margin: '0 0 20px' }}>
            Completeness Overview
          </h3>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={chartData} margin={{ top: 5, right: 20, left: -10, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border-default)" />
              <XAxis dataKey="name" tick={{ fontSize: 11, fill: 'var(--text-muted)' }} tickLine={false} axisLine={false} />
              <YAxis domain={[0, 100]} tick={{ fontSize: 10, fill: 'var(--text-muted)' }} tickLine={false} axisLine={false} />
              <Tooltip
                contentStyle={{ background: 'var(--bg-card)', border: '1px solid var(--border-subtle)', borderRadius: 8, fontSize: 12 }}
                formatter={(v: any) => [`${v}%`, 'Completeness']}
              />
              <Bar dataKey="completeness" fill="#7c3aed" radius={[4,4,0,0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
}
