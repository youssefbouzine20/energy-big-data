import { useState, useEffect } from 'react';
import { Header } from '../components/Header';
import { api, ApiError } from '../lib/api';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

type Metric = {
  topic: string;
  window_start: string;
  temporal_coverage_pct: number;
  completeness_pct: number;
  noise_rate_pct: number;
  anomaly_rate_pct: number;
  alert: string | null;
};
type LatestItem = { topic: string; latest: Metric | null };

const TOPIC_META: Record<string, { label: string; icon: string }> = {
  'smart-meters':     { label: 'Smart Meters',  icon: '⚡' },
  'weather':          { label: 'Weather',       icon: '🌤️' },
  'incident-reports': { label: 'Incidents',     icon: '🚨' },
  'rss-feeds':        { label: 'RSS Feeds',     icon: '📰' },
  'market-prices':    { label: 'Market Prices', icon: '💶' },
  'user-feedback':    { label: 'User Feedback', icon: '💬' },
};

const TOPIC_ORDER = [
  'smart-meters', 'weather', 'incident-reports',
  'rss-feeds', 'market-prices', 'user-feedback',
];

function getGrade(pct: number) {
  if (pct >= 95) return { label: 'Excellent', color: '#22c55e' };
  if (pct >= 80) return { label: 'Good',      color: '#f59e0b' };
  if (pct >= 60) return { label: 'Fair',      color: '#f97316' };
  return { label: 'Poor', color: '#ef4444' };
}

export function Quality() {
  const [items, setItems] = useState<LatestItem[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const d = await api.get<{ items: LatestItem[] }>('/api/quality/latest');
        if (!cancelled) { setItems(d.items ?? []); setError(null); }
      } catch (err) {
        if (!cancelled) setError(err instanceof ApiError ? err.message : 'Failed to reach API');
      }
    };
    load();
    const t = setInterval(load, 30_000);
    return () => { cancelled = true; clearInterval(t); };
  }, []);

  const ordered = TOPIC_ORDER.map(topic =>
    items.find(it => it.topic === topic) ?? { topic, latest: null }
  );

  const chartData = ordered.map(it => ({
    name: TOPIC_META[it.topic]?.label?.split(' ')[0] ?? it.topic,
    completeness: it.latest?.completeness_pct ?? 0,
  }));

  return (
    <div style={{ animation: 'fade-in 0.4s ease-out both' }}>
      <Header title="Data Quality" subtitle="Per-topic completeness, noise and coverage (Section E)" />
      <div style={{ paddingTop: 24 }}>

        {error && (
          <div style={{
            background: '#fee2e2', border: '1px solid #fca5a5', color: '#b91c1c',
            padding: '10px 14px', borderRadius: 8, marginBottom: 16, fontSize: 13,
          }}>
            ⚠ Quality API unreachable — {error}
          </div>
        )}

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: 16, marginBottom: 24 }}>
          {ordered.map(it => {
            const meta = TOPIC_META[it.topic] ?? { label: it.topic, icon: '📦' };
            const m = it.latest;
            const pct = m?.completeness_pct ?? 0;
            const { label, color } = getGrade(pct);
            return (
              <div key={it.topic} className="card-atonist" style={{ padding: '18px 20px' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 12 }}>
                  <div style={{ fontSize: 20 }}>{meta.icon}</div>
                  <span style={{ fontSize: 10, fontWeight: 600, color, background: `${color}18`, padding: '3px 8px', borderRadius: 4 }}>
                    {m ? label : 'Waiting…'}
                  </span>
                </div>
                <div style={{ fontSize: 12, fontFamily: 'monospace', color: 'var(--text-muted)', marginBottom: 6 }}>{it.topic}</div>
                <div style={{ fontFamily: 'Space Grotesk', fontSize: 28, fontWeight: 700, color, marginBottom: 10 }}>
                  {m ? `${pct.toFixed(1)}%` : '—'}
                </div>
                <div style={{ height: 6, borderRadius: 3, background: 'var(--bg-input)', overflow: 'hidden', marginBottom: 10 }}>
                  <div style={{ height: '100%', width: `${pct}%`, background: color, borderRadius: 3, transition: 'width 1.5s ease' }} />
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, color: 'var(--text-muted)' }}>
                  <span>noise {m ? `${m.noise_rate_pct.toFixed(1)}%` : '—'}</span>
                  <span>coverage {m ? `${m.temporal_coverage_pct.toFixed(0)}%` : '—'}</span>
                </div>
                {m?.alert && (
                  <div style={{ marginTop: 8, fontSize: 11, color: m.alert.startsWith('CRIT') ? '#dc2626' : '#d97706' }}>
                    {m.alert}
                  </div>
                )}
              </div>
            );
          })}
        </div>

        <div className="card-atonist" style={{ padding: '20px' }}>
          <h3 style={{ fontFamily: 'Space Grotesk', fontSize: 15, fontWeight: 600, color: 'var(--text-primary)', margin: '0 0 20px' }}>
            Completeness by Topic
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
              <Bar dataKey="completeness" fill="#7c3aed" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
}
