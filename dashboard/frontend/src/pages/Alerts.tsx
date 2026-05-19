import { useState, useEffect } from 'react';
import { Header } from '../components/Header';
import { api } from '../lib/api';
import { DemoBanner } from '../components/DemoBanner';
import { AlertTriangle, CheckCircle, Info, Zap } from 'lucide-react';

const LEVEL_CONFIG: Record<string, any> = {
  CRITICAL: { icon: AlertTriangle, color: '#ef4444', bg: 'rgba(239,68,68,0.1)',  border: 'rgba(239,68,68,0.25)',  label: 'Critical' },
  WARNING:  { icon: Zap,           color: '#f97316', bg: 'rgba(249,115,22,0.1)', border: 'rgba(249,115,22,0.25)', label: 'Warning'  },
  INFO:     { icon: Info,          color: '#3b82f6', bg: 'rgba(59,130,246,0.1)', border: 'rgba(59,130,246,0.25)', label: 'Info'     },
  OK:       { icon: CheckCircle,   color: '#22c55e', bg: 'rgba(34,197,94,0.1)',  border: 'rgba(34,197,94,0.25)',  label: 'OK'       },
};

const DEMO_ALERTS = [
  { _id: '1', level: 'CRITICAL', message: 'High consumption predicted in Zone B', details: 'Risk of load shedding in next 2 hours', zone: 'B', triggered_at: new Date(Date.now() - 5*60000).toISOString() },
  { _id: '2', level: 'WARNING',  message: 'Transformer overload detected', details: 'Station T-04 is overloaded', zone: 'A', triggered_at: new Date(Date.now() - 15*60000).toISOString() },
  { _id: '3', level: 'INFO',     message: 'Voltage fluctuation in Zone C', details: 'Unstable voltage levels detected', zone: 'C', triggered_at: new Date(Date.now() - 32*60000).toISOString() },
  { _id: '4', level: 'OK',       message: 'System normal', details: 'All systems operating within normal range', zone: 'ALL', triggered_at: new Date(Date.now() - 60*60000).toISOString() },
  { _id: '5', level: 'WARNING',  message: 'Zone D approaching threshold', details: '68% load — monitor closely', zone: 'D', triggered_at: new Date(Date.now() - 2*3600000).toISOString() },
];

function normalizeAlert(x: any) {
  const zone = x.zone ?? '?';
  const level = (x.alert_level ?? x.level ?? 'INFO').toUpperCase();
  const kwh = x.forecast_kwh ?? x.consumption_forecast;
  return {
    _id: x._id ?? x.incident_id ?? `${zone}-${x.triggered_at ?? x.forecast_for ?? Math.random()}`,
    level,
    message: x.message ?? `${level} forecast Zone ${zone}${kwh ? ` — ${Math.round(kwh)} kWh` : ''}`,
    details: x.details ?? (x.model_name ? `Model: ${x.model_name}` : ''),
    zone,
    triggered_at: x.triggered_at ?? x.forecast_for ?? new Date().toISOString(),
  };
}

export function Alerts() {
  const [active, setActive]   = useState<any[]>([]);
  const [history, setHistory] = useState<any[]>([]);
  const [filter, setFilter]   = useState<string>('ALL');
  const [usingDemo, setUsingDemo] = useState({ active: false, history: false });

  useEffect(() => {
    const load = async () => {
      try {
        const [a, h] = await Promise.all([
          api.get<any>('/api/alerts/active').catch(() => null),
          api.get<any>('/api/alerts/history').catch(() => null),
        ]);
        const aItems = a?.items ?? a?.alerts ?? [];
        const hItems = h?.items ?? h?.alerts ?? [];
        if (aItems.length) {
          setActive(aItems.map(normalizeAlert));
          setUsingDemo(d => ({ ...d, active: false }));
        } else {
          setActive(DEMO_ALERTS.slice(0, 3));
          setUsingDemo(d => ({ ...d, active: true }));
        }
        if (hItems.length) {
          setHistory(hItems.map(normalizeAlert));
          setUsingDemo(d => ({ ...d, history: false }));
        } else {
          setHistory(DEMO_ALERTS);
          setUsingDemo(d => ({ ...d, history: true }));
        }
      } catch {
        setActive(DEMO_ALERTS.slice(0, 3));
        setHistory(DEMO_ALERTS);
        setUsingDemo({ active: true, history: true });
      }
    };
    load();
    const t = setInterval(load, 30_000);
    return () => clearInterval(t);
  }, []);

  const allHistory = history.length ? history : DEMO_ALERTS;
  const filtered   = filter === 'ALL' ? allHistory : allHistory.filter(a => a.level === filter);
  const counts     = Object.fromEntries(Object.keys(LEVEL_CONFIG).map(k => [k, allHistory.filter(a => a.level === k).length]));

  return (
    <div style={{ animation: 'fade-in 0.4s ease-out both' }}>
      <Header title="Alerts" subtitle="Predictive alerts and audit log" alertCount={active.length} />
      <div style={{ paddingTop: 24 }}>

        {(usingDemo.active || usingDemo.history) && (
          <DemoBanner
            sources={[usingDemo.active ? '/api/alerts/active' : null, usingDemo.history ? '/api/alerts/history' : null].filter(Boolean) as string[]}
            message="Alerts panel shows demonstration content"
          />
        )}

        {/* Summary cards */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4,1fr)', gap: 16, marginBottom: 24 }}>
          {Object.entries(LEVEL_CONFIG).map(([level, cfg]) => {
            const Icon = cfg.icon;
            return (
              <div key={level} className="card-atonist" style={{ padding: '16px 20px', borderColor: cfg.border }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 8 }}>
                  <div style={{ width: 32, height: 32, borderRadius: 8, background: cfg.bg, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                    <Icon size={16} color={cfg.color} />
                  </div>
                  <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>{cfg.label}</span>
                </div>
                <div style={{ fontFamily: 'Space Grotesk', fontSize: 32, fontWeight: 700, color: cfg.color }}>
                  {counts[level] ?? 0}
                </div>
              </div>
            );
          })}
        </div>

        {/* Active alerts banner */}
        {active.length > 0 && (
          <div className="card-atonist" style={{
            padding: '14px 20px', marginBottom: 20,
            background: 'rgba(239,68,68,0.06)', borderColor: 'rgba(239,68,68,0.2)',
            display: 'flex', alignItems: 'center', gap: 12,
          }}>
            <div style={{ width: 8, height: 8, borderRadius: '50%', background: '#ef4444', animation: 'pulse-green 1.5s infinite' }} />
            <span style={{ fontSize: 14, color: 'var(--text-primary)', fontWeight: 500 }}>
              {active.length} active alert(s) require attention
            </span>
          </div>
        )}

        {/* History table */}
        <div className="card-atonist" style={{ padding: '20px' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
            <h3 style={{ fontFamily: 'Space Grotesk', fontSize: 15, fontWeight: 600, color: 'var(--text-primary)', margin: 0 }}>
              Alert Log
            </h3>
            <div style={{ display: 'flex', gap: 6 }}>
              {['ALL', ...Object.keys(LEVEL_CONFIG)].map(f => (
                <button key={f} onClick={() => setFilter(f)} style={{
                  padding: '4px 12px', borderRadius: 6, border: '1px solid var(--border-default)',
                  cursor: 'pointer', fontSize: 11, fontWeight: 500,
                  background: filter === f ? 'var(--accent-purple)' : 'transparent',
                  color: filter === f ? '#fff' : 'var(--text-muted)',
                  transition: 'all 0.18s',
                }}>{f}</button>
              ))}
            </div>
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {filtered.map((a: any, i: number) => {
              const cfg = LEVEL_CONFIG[a.level] ?? LEVEL_CONFIG.INFO;
              const Icon = cfg.icon;
              const ago = a.triggered_at
                ? (() => {
                    const diff = Math.round((Date.now() - new Date(a.triggered_at).getTime()) / 60000);
                    return diff < 60 ? `${diff}m ago` : `${Math.floor(diff/60)}h ago`;
                  })()
                : 'just now';
              return (
                <div key={a._id ?? i} className="alert-item">
                  <div style={{ width: 36, height: 36, borderRadius: 9, background: cfg.bg, border: `1px solid ${cfg.border}`, display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0 }}>
                    <Icon size={16} color={cfg.color} />
                  </div>
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 2 }}>
                      <span style={{ fontSize: 13, fontWeight: 500, color: 'var(--text-primary)' }}>{a.message}</span>
                      <span style={{ fontSize: 10, fontWeight: 600, color: cfg.color, background: cfg.bg, padding: '2px 6px', borderRadius: 4 }}>{a.level}</span>
                    </div>
                    <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>{a.details} {a.zone && `· Zone ${a.zone}`}</div>
                  </div>
                  <div style={{ fontSize: 11, color: 'var(--text-muted)', whiteSpace: 'nowrap', flexShrink: 0 }}>{ago}</div>
                </div>
              );
            })}
            {filtered.length === 0 && (
              <div style={{ textAlign: 'center', padding: 40, color: 'var(--text-muted)', fontSize: 14 }}>
                No alerts for this filter.
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}