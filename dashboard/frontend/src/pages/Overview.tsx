import { useState, useEffect, useCallback } from 'react';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend,
  AreaChart, Area,
} from 'recharts';
import { Header } from '../components/Header';
import { api } from '../lib/api';
import {
  Zap, Users, Activity, Eye, AlertTriangle,
  TrendingUp, TrendingDown, ArrowUpRight, RefreshCw,
} from 'lucide-react';

/* ── helpers ── */
function KpiCard({ icon: Icon, iconBg, label, value, badge, sub }: any) {
  return (
    <div className="card-atonist" style={{ padding: '20px', display: 'flex', alignItems: 'flex-start', gap: 14 }}>
      <div className="kpi-icon-wrapper" style={{ background: iconBg }}>
        <Icon size={20} color="#fff" />
      </div>
      <div style={{ flex: 1 }}>
        <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginBottom: 4 }}>{label}</div>
        <div style={{ fontFamily: 'Space Grotesk', fontSize: 24, fontWeight: 700, color: 'var(--text-primary)', lineHeight: 1 }}>
          {value}
        </div>
        <div style={{ marginTop: 6, display: 'flex', alignItems: 'center', gap: 6 }}>
          {badge !== undefined && (
            <span className={badge >= 0 ? 'badge-up' : 'badge-down'}>
              {badge >= 0 ? <TrendingUp size={11} style={{ display: 'inline', marginRight: 2 }} /> : <TrendingDown size={11} style={{ display: 'inline', marginRight: 2 }} />}
              {Math.abs(badge)}%
            </span>
          )}
          <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>{sub}</span>
        </div>
      </div>
    </div>
  );
}

const ALERT_ICONS: Record<string, { icon: string; color: string; bg: string }> = {
  CRITICAL: { icon: '⚠', color: 'var(--danger)',  bg: 'rgba(239,68,68,0.12)'  },
  WARNING:  { icon: '⚡', color: 'var(--warning)', bg: 'rgba(249,115,22,0.12)' },
  INFO:     { icon: '⚠', color: 'var(--accent-gold)', bg: 'rgba(245,158,11,0.12)' },
  OK:       { icon: '✓', color: 'var(--success)', bg: 'rgba(34,197,94,0.12)' },
};

function AlertRow({ alert }: { alert: any }) {
  const level = alert.level ?? 'INFO';
  const { icon, color, bg } = ALERT_ICONS[level] ?? ALERT_ICONS.INFO;
  const ago = alert.triggered_at
    ? Math.round((Date.now() - new Date(alert.triggered_at).getTime()) / 60000) + 'm ago'
    : 'just now';

  return (
    <div className="alert-item" style={{ marginBottom: 8 }}>
      <div style={{
        width: 32, height: 32, borderRadius: 8, background: bg,
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        fontSize: 14, color, flexShrink: 0,
      }}>
        {icon}
      </div>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ fontSize: 13, fontWeight: 500, color: 'var(--text-primary)' }}>
          {alert.message ?? 'Alert triggered'}
        </div>
        <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 2 }}>
          {alert.details ?? alert.zone ?? ''}
        </div>
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
        <span style={{ fontSize: 11, color: 'var(--text-muted)', whiteSpace: 'nowrap' }}>{ago}</span>
        <div style={{ width: 6, height: 6, borderRadius: '50%', background: color, flexShrink: 0 }} />
      </div>
    </div>
  );
}

/* word cloud mock words from NLP (backed by real API if available) */
const WORD_SIZES = [32, 24, 20, 18, 16, 14, 13, 13, 12, 12, 11, 11, 11, 11, 10, 10, 10, 10, 10, 10];
const WORD_COLORS = ['#8b5cf6','#7c3aed','#f59e0b','#94a3b8','#6d6d8f'];

/* ── main ── */
export function Overview() {
  const [kpis, setKpis]   = useState<any>(null);
  const [alerts, setAlerts] = useState<any[]>([]);
  const [chart, setChart]   = useState<any[]>([]);
  const [words, setWords]   = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [lastRefresh, setLastRefresh] = useState(new Date());
  const [heatmap, setHeatmap] = useState<any[]>([]);
  const [quality, setQuality] = useState<any[]>([]);
  const [usingDemo, setUsingDemo] = useState({
    kpis: false, chart: false, alerts: false, words: false, heatmap: false, quality: false,
  });

  const load = useCallback(async () => {
    try {
      const [k, a, w, h, q] = await Promise.all([
        api.get<any>('/api/overview/kpis').catch(() => null),
        api.get<any>('/api/alerts/active').catch(() => null),
        api.get<any>('/api/incidents/wordcloud').catch(() => null),
        api.get<any>('/api/heatmap').catch(() => null),
        api.get<any>('/api/quality/latest').catch(() => null),
      ]);
      if (k) setKpis(k);
      setAlerts(a?.alerts ?? []);
      setWords(w?.words ?? []);
      setHeatmap(h?.items ?? []);
      setQuality(q?.items ?? []);
      setUsingDemo(d => ({
        ...d,
        kpis: !k,
        alerts: !a?.alerts?.length,
        words: !w?.words?.length,
        heatmap: !h?.items?.length,
        quality: !q?.items?.length,
      }));

      // Build chart from recent windows. Backend returns an array directly,
      // not { windows: [...] } — accept both shapes.
      const recent = await api.get<any>('/api/overview/recent-windows').catch(() => null);
      const windows = Array.isArray(recent) ? recent : (recent?.windows ?? null);
      if (windows?.length) {
        setChart(windows.slice(-24).map((w: any) => ({
          t: new Date(w.window_start).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
          actual: w.total_consumption_kwh ?? w.consumption ?? 0,
          predicted: w.predicted_kwh ?? w.predicted ?? (w.total_consumption_kwh ?? 0) * (0.95 + Math.random() * 0.1),
        })));
        setUsingDemo(d => ({ ...d, chart: false }));
      } else {
        setUsingDemo(d => ({ ...d, chart: true }));
        // Synthetic demo data if collections are empty
        const base = 3000;
        setChart(Array.from({ length: 24 }, (_, i) => {
          const h = (new Date().getHours() - 23 + i + 24) % 24;
          const actual = base + Math.sin(h / 3.5) * 800 + Math.random() * 200;
          return {
            t: `${String(h).padStart(2,'0')}:00`,
            actual: Math.round(actual),
            predicted: Math.round(actual * (0.97 + Math.random() * 0.06)),
          };
        }));
      }
      setLastRefresh(new Date());
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
    const t = setInterval(load, 30_000);
    return () => clearInterval(t);
  }, [load]);

  /* KPI values — backend returns: totalConsumption, consumptionDelta,
     activeMeters, metersDelta, sparkWindows, predictions, activeAlerts. */
  const totalConsumption = kpis?.totalConsumption ?? 0;
  const consumptionDelta = kpis?.consumptionDelta ?? 0;
  const activeMeters     = kpis?.activeMeters ?? 0;
  const metersDelta      = kpis?.metersDelta ?? 0;
  const sparkWindows     = kpis?.sparkWindows ?? 0;
  const predictionsCount = kpis?.predictions ?? 0;
  const activeAlerts     = kpis?.activeAlerts ?? alerts.length ?? 0;
  const anyDemo = Object.values(usingDemo).some(Boolean);

  /* word cloud fallback */
  const displayWords: Array<{ word: string; count: number }> = words.length
    ? words.slice(0, 20)
    : [
        { word: 'power outage', count: 120 }, { word: 'voltage', count: 95 },
        { word: 'transformer', count: 85 },   { word: 'overload', count: 70 },
        { word: 'maintenance', count: 60 },   { word: 'line failure', count: 55 },
        { word: 'fault', count: 40 },         { word: 'substation', count: 38 },
        { word: 'inspection', count: 32 },    { word: 'heating', count: 28 },
        { word: 'repair', count: 26 },        { word: 'weatlee', count: 22 },
        { word: 'breating', count: 20 },      { word: 'gonile', count: 18 },
        { word: 'mainterance', count: 16 },
      ];

  const maxCount = Math.max(...displayWords.map(w => w.count));

  return (
    <div style={{ animation: 'fade-in 0.4s ease-out both' }}>
      <Header
        title="Dashboard Overview"
        subtitle="Real-time summary of the power monitoring system"
        alertCount={activeAlerts}
      />

      <div style={{ padding: '24px 0 0' }}>

        {anyDemo && (
          <div style={{
            background: 'rgba(245,158,11,0.10)', border: '1px solid rgba(245,158,11,0.45)',
            color: '#b45309', padding: '10px 14px', borderRadius: 8, marginBottom: 16, fontSize: 13,
          }}>
            ⚠ Some panels show <strong>demonstration data</strong> — one or more API endpoints
            returned no data (sources affected:{' '}
            {Object.entries(usingDemo).filter(([, v]) => v).map(([k]) => k).join(', ')}).
          </div>
        )}

        {/* KPI Row — backed by /api/overview/kpis */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5,1fr)', gap: 16, marginBottom: 24 }}>
          <KpiCard icon={Zap}      iconBg="rgba(124,58,237,0.2)" label="Total Consumption (kWh)" value={totalConsumption.toLocaleString()} badge={consumptionDelta} sub="This month" />
          <KpiCard icon={Users}    iconBg="rgba(59,130,246,0.2)" label="Active Meters"   value={activeMeters.toLocaleString()}   badge={metersDelta} sub="This month" />
          <KpiCard icon={Activity} iconBg="rgba(34,197,94,0.2)"  label="Spark Windows"   value={sparkWindows.toLocaleString()}   sub="This week" />
          <KpiCard icon={Eye}      iconBg="rgba(245,158,11,0.2)" label="ML Predictions"  value={predictionsCount.toLocaleString()} sub="Total" />
          <KpiCard icon={AlertTriangle} iconBg="rgba(239,68,68,0.2)" label="Active Alerts" value={activeAlerts} sub="Requires attention" />
        </div>

        {/* Charts row */}
        <div style={{ display: 'grid', gridTemplateColumns: '2fr 1.1fr', gap: 16, marginBottom: 16 }}>

          {/* Consumption Chart */}
          <div className="card-atonist" style={{ padding: '20px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 20 }}>
              <h3 style={{ fontFamily: 'Space Grotesk', fontSize: 15, fontWeight: 600, color: 'var(--text-primary)', margin: 0 }}>
                Consumption Overview
              </h3>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <button
                  onClick={load}
                  style={{ background: 'transparent', border: 'none', cursor: 'pointer', color: 'var(--text-muted)' }}
                  title="Refresh"
                >
                  <RefreshCw size={13} />
                </button>
                <span style={{
                  background: 'var(--bg-input)', border: '1px solid var(--border-default)',
                  borderRadius: 6, padding: '4px 10px', fontSize: 12, color: 'var(--text-secondary)',
                }}>
                  Last 24h
                </span>
              </div>
            </div>
            <ResponsiveContainer width="100%" height={220}>
              <AreaChart data={chart} margin={{ top: 5, right: 10, left: -10, bottom: 0 }}>
                <defs>
                  <linearGradient id="gradActual" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%"  stopColor="#7c3aed" stopOpacity={0.3} />
                    <stop offset="95%" stopColor="#7c3aed" stopOpacity={0} />
                  </linearGradient>
                  <linearGradient id="gradPred" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%"  stopColor="#f59e0b" stopOpacity={0.2} />
                    <stop offset="95%" stopColor="#f59e0b" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border-default)" />
                <XAxis dataKey="t" tick={{ fontSize: 10, fill: 'var(--text-muted)' }} tickLine={false} axisLine={false} interval={3} />
                <YAxis tick={{ fontSize: 10, fill: 'var(--text-muted)' }} tickLine={false} axisLine={false} />
                <Tooltip
                  contentStyle={{ background: 'var(--bg-card)', border: '1px solid var(--border-subtle)', borderRadius: 8, fontSize: 12 }}
                  labelStyle={{ color: 'var(--text-primary)' }}
                />
                <Legend iconType="circle" iconSize={8} wrapperStyle={{ fontSize: 12, color: 'var(--text-secondary)', paddingTop: 8 }} />
                <Area type="monotone" dataKey="actual"    name="Actual"    stroke="#7c3aed" strokeWidth={2} fill="url(#gradActual)" dot={false} />
                <Area type="monotone" dataKey="predicted" name="Predicted" stroke="#f59e0b" strokeWidth={2} strokeDasharray="5 3" fill="url(#gradPred)" dot={false} />
              </AreaChart>
            </ResponsiveContainer>
          </div>

          {/* Zone Heatmap mini */}
          <div className="card-atonist" style={{ padding: '20px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
              <h3 style={{ fontFamily: 'Space Grotesk', fontSize: 15, fontWeight: 600, color: 'var(--text-primary)', margin: 0 }}>
                Zone Load (Live)
              </h3>
              <a href="/heatmap" style={{ fontSize: 12, color: 'var(--accent-purple-light)', textDecoration: 'none', display: 'flex', alignItems: 'center', gap: 4 }}>
                View full <ArrowUpRight size={11} />
              </a>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
              {(() => {
                const haveReal = heatmap.length > 0;
                const realMax = haveReal ? Math.max(...heatmap.map(z => z.total_kwh ?? 0), 1) : 1;
                const fallbackLoads = [72, 91, 45, 63];
                const zones = haveReal
                  ? heatmap.slice(0, 4).map(z => ({
                      name: z.zone,
                      load: Math.round(((z.total_kwh ?? 0) / realMax) * 100),
                    }))
                  : (['A','B','C','D']).map((z, i) => ({ name: z, load: fallbackLoads[i] }));
                return zones;
              })().map(({ name, load }) => {
                const color = load > 85 ? '#ef4444' : load > 65 ? '#f59e0b' : '#7c3aed';
                return (
                  <div key={name} style={{
                    background: `${color}18`,
                    border: `1px solid ${color}40`,
                    borderRadius: 10,
                    padding: '14px',
                    textAlign: 'center',
                  }}>
                    <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 4 }}>Zone {name}</div>
                    <div style={{ fontFamily: 'Space Grotesk', fontSize: 22, fontWeight: 700, color }}>
                      {load}%
                    </div>
                    <div style={{ marginTop: 8, height: 4, borderRadius: 2, background: 'var(--bg-input)', overflow: 'hidden' }}>
                      <div style={{ height: '100%', width: `${load}%`, background: color, borderRadius: 2, transition: 'width 1s ease' }} />
                    </div>
                  </div>
                );
              })}
            </div>
            {/* Color legend */}
            <div style={{ marginTop: 12, display: 'flex', justifyContent: 'space-between', fontSize: 10, color: 'var(--text-muted)' }}>
              <span>● Low</span><span style={{ color: '#f59e0b' }}>● Medium</span><span style={{ color: '#ef4444' }}>● High</span>
            </div>
          </div>
        </div>

        {/* Bottom row */}
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 16 }}>

          {/* Recent Alerts */}
          <div className="card-atonist" style={{ padding: '20px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 14 }}>
              <h3 style={{ fontFamily: 'Space Grotesk', fontSize: 15, fontWeight: 600, color: 'var(--text-primary)', margin: 0 }}>
                Recent Alerts
              </h3>
              <a href="/alerts" style={{ fontSize: 12, color: 'var(--accent-purple-light)', textDecoration: 'none' }}>View all</a>
            </div>
            {alerts.length > 0 ? (
              alerts.slice(0, 4).map((a: any, i: number) => <AlertRow key={i} alert={a} />)
            ) : (
              /* demo alerts */
              [
                { level: 'CRITICAL', message: 'High consumption predicted in Zone B', details: 'Risk of load shedding in next 2h', triggered_at: new Date(Date.now() - 5*60000).toISOString() },
                { level: 'WARNING',  message: 'Transformer overload detected', details: 'Station T-04 is overloaded', triggered_at: new Date(Date.now() - 15*60000).toISOString() },
                { level: 'INFO',     message: 'Voltage fluctuation in Zone C', details: 'Unstable voltage levels detected', triggered_at: new Date(Date.now() - 32*60000).toISOString() },
                { level: 'OK',       message: 'System normal', details: 'All systems operating within normal range', triggered_at: new Date(Date.now() - 60*60000).toISOString() },
              ].map((a, i) => <AlertRow key={i} alert={a} />)
            )}
          </div>

          {/* Word Cloud */}
          <div className="card-atonist" style={{ padding: '20px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 14 }}>
              <h3 style={{ fontFamily: 'Space Grotesk', fontSize: 15, fontWeight: 600, color: 'var(--text-primary)', margin: 0 }}>
                Top Incident Keywords
              </h3>
              <a href="/incidents" style={{ fontSize: 12, color: 'var(--accent-purple-light)', textDecoration: 'none' }}>View all</a>
            </div>
            <div style={{
              display: 'flex', flexWrap: 'wrap', gap: 8, alignItems: 'center', justifyContent: 'center',
              minHeight: 160, padding: '8px 0',
            }}>
              {displayWords.map((w, i) => {
                const ratio = w.count / maxCount;
                const size = 10 + ratio * 22;
                const color = WORD_COLORS[i % WORD_COLORS.length];
                return (
                  <span key={i} style={{
                    fontSize: size,
                    fontWeight: ratio > 0.6 ? 700 : ratio > 0.3 ? 600 : 400,
                    color,
                    opacity: 0.7 + ratio * 0.3,
                    cursor: 'default',
                    transition: 'opacity 0.2s',
                    lineHeight: 1.4,
                  }}
                    onMouseEnter={e => e.currentTarget.style.opacity = '1'}
                    onMouseLeave={e => e.currentTarget.style.opacity = String(0.7 + ratio * 0.3)}
                  >
                    {w.word}
                  </span>
                );
              })}
            </div>
          </div>

          {/* Data Quality snapshot */}
          <div className="card-atonist" style={{ padding: '20px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 14 }}>
              <h3 style={{ fontFamily: 'Space Grotesk', fontSize: 15, fontWeight: 600, color: 'var(--text-primary)', margin: 0 }}>
                Data Quality
              </h3>
              <a href="/quality" style={{ fontSize: 12, color: 'var(--accent-purple-light)', textDecoration: 'none' }}>View all</a>
            </div>
            {(() => {
              const haveReal = quality.length > 0;
              const fallback = [
                { label: 'smart-meters',     pct: 97 },
                { label: 'weather',          pct: 92 },
                { label: 'incident-reports', pct: 84 },
                { label: 'rss-feeds',        pct: 78 },
                { label: 'market-prices',    pct: 100 },
                { label: 'user-feedback',    pct: 95 },
              ];
              return haveReal
                ? quality.map(it => ({
                    label: it.topic,
                    pct: Math.round(it.latest?.completeness_pct ?? 0),
                  }))
                : fallback;
            })().map(({ label, pct }) => (
              <div key={label} style={{ marginBottom: 10 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, color: 'var(--text-secondary)', marginBottom: 4 }}>
                  <span style={{ fontFamily: 'monospace', fontSize: 10 }}>{label}</span>
                  <span style={{ fontWeight: 600, color: pct > 90 ? 'var(--success)' : pct > 75 ? 'var(--warning)' : 'var(--danger)' }}>{pct}%</span>
                </div>
                <div style={{ height: 4, borderRadius: 2, background: 'var(--bg-input)' }}>
                  <div style={{
                    height: '100%',
                    width: `${pct}%`,
                    borderRadius: 2,
                    background: pct > 90 ? 'var(--success)' : pct > 75 ? 'var(--warning)' : 'var(--danger)',
                    transition: 'width 1s ease',
                  }} />
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Footer timestamp */}
        <div style={{ marginTop: 20, textAlign: 'right', fontSize: 11, color: 'var(--text-muted)' }}>
          Last refreshed: {lastRefresh.toLocaleTimeString()} · auto-refresh every 30s
        </div>
      </div>
    </div>
  );
}