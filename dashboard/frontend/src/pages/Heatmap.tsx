import { useState, useEffect } from 'react';
import { Header } from '../components/Header';
import { api } from '../lib/api';
import { DemoBanner } from '../components/DemoBanner';

const ZONES = ['A', 'B', 'C', 'D'];

function getColor(pct: number) {
  if (pct > 90) return { bg: 'rgba(239,68,68,0.15)', border: '#ef4444', text: '#ef4444', label: 'Critical' };
  if (pct > 70) return { bg: 'rgba(249,115,22,0.12)', border: '#f97316', text: '#f97316', label: 'High' };
  if (pct > 45) return { bg: 'rgba(245,158,11,0.10)', border: '#f59e0b', text: '#f59e0b', label: 'Medium' };
  return { bg: 'rgba(124,58,237,0.10)', border: '#7c3aed', text: '#8b5cf6', label: 'Low' };
}

export function Heatmap() {
  const [data, setData] = useState<any[]>([]);
  const [, setLoading] = useState(true);
  const [selectedZone, setSelectedZone] = useState<string | null>(null);
  const [usingDemo, setUsingDemo] = useState(false);

  useEffect(() => {
    const load = async () => {
      try {
        const d = await api.get<any>('/api/heatmap');
        const items: any[] = d?.items ?? d?.zones ?? [];
        if (items.length) {
          const max = Math.max(...items.map(it => it.total_kwh ?? 0), 1);
          setData(items.map(it => ({
            zone: it.zone,
            load_pct: Math.round(((it.total_kwh ?? 0) / max) * 100),
            consumption_kwh: Math.round(it.total_kwh ?? 0),
            meters: it.meters ?? '—',
            incidents: it.anomalies ?? '—',
          })));
          setUsingDemo(false);
        } else {
          throw new Error('empty');
        }
      } catch {
        setData(ZONES.map(z => ({
          zone: z,
          load_pct: [72, 94, 45, 63][ZONES.indexOf(z)],
          consumption_kwh: [18400, 24300, 11200, 16800][ZONES.indexOf(z)],
          meters: [120, 145, 98, 112][ZONES.indexOf(z)],
          incidents: [3, 8, 1, 4][ZONES.indexOf(z)],
        })));
        setUsingDemo(true);
      } finally {
        setLoading(false);
      }
    };
    load();
    const t = setInterval(load, 30_000);
    return () => clearInterval(t);
  }, []);

  return (
    <div style={{ animation: 'fade-in 0.4s ease-out both' }}>
      <Header title="Consumption Heatmap" subtitle="Real-time zone load analysis" />
      <div style={{ paddingTop: 24 }}>

        {usingDemo && <DemoBanner sources={['/api/heatmap']} message="Heatmap shows demonstration data" />}

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2,1fr)', gap: 20, marginBottom: 24 }}>
          {(data.length ? data : ZONES.map(z => ({ zone: z, load_pct: 0 }))).map((zone: any) => {
            const pct  = zone.load_pct ?? zone.loadPct ?? 0;
            const { bg, border, text, label } = getColor(pct);
            const selected = selectedZone === zone.zone;
            return (
              <div
                key={zone.zone}
                className="card-atonist"
                onClick={() => setSelectedZone(selected ? null : zone.zone)}
                style={{
                  padding: 24, cursor: 'pointer',
                  borderColor: selected ? border : undefined,
                  boxShadow: selected ? `0 0 0 1px ${border}` : undefined,
                  background: bg,
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 20 }}>
                  <div>
                    <div style={{ fontSize: 13, color: 'var(--text-muted)', marginBottom: 4 }}>Zone {zone.zone}</div>
                    <div style={{ fontFamily: 'Space Grotesk', fontSize: 48, fontWeight: 800, color: text, lineHeight: 1 }}>
                      {pct}%
                    </div>
                    <div style={{ fontSize: 12, color: text, marginTop: 4, fontWeight: 500 }}>{label}</div>
                  </div>
                  <div style={{
                    background: `${border}20`, border: `1px solid ${border}40`,
                    borderRadius: 8, padding: '6px 12px',
                    fontSize: 12, color: text, fontWeight: 600,
                  }}>
                    {zone.consumption_kwh?.toLocaleString() ?? '—'} kWh
                  </div>
                </div>

                {/* Load bar */}
                <div style={{ height: 8, borderRadius: 4, background: 'var(--bg-input)', overflow: 'hidden', marginBottom: 16 }}>
                  <div style={{
                    height: '100%', width: `${pct}%`,
                    background: `linear-gradient(90deg, ${border}80, ${border})`,
                    borderRadius: 4, transition: 'width 1.5s ease',
                  }} />
                </div>

                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
                  {[
                    { label: 'Active Meters', value: zone.meters ?? '—' },
                    { label: 'Incidents',     value: zone.incidents ?? '—' },
                  ].map(({ label, value }) => (
                    <div key={label} style={{
                      background: 'rgba(0,0,0,0.2)', borderRadius: 8, padding: '10px 12px',
                    }}>
                      <div style={{ fontSize: 10, color: 'var(--text-muted)', marginBottom: 2 }}>{label}</div>
                      <div style={{ fontFamily: 'Space Grotesk', fontSize: 18, fontWeight: 700, color: 'var(--text-primary)' }}>
                        {value}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            );
          })}
        </div>

        {/* Legend */}
        <div className="card-atonist" style={{ padding: '16px 20px', display: 'flex', alignItems: 'center', gap: 24 }}>
          <span style={{ fontSize: 12, color: 'var(--text-muted)', fontWeight: 500 }}>Load Level:</span>
          {[
            { color: '#7c3aed', label: 'Low (< 45%)' },
            { color: '#f59e0b', label: 'Medium (45–70%)' },
            { color: '#f97316', label: 'High (70–90%)' },
            { color: '#ef4444', label: 'Critical (> 90%)' },
          ].map(({ color, label }) => (
            <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <div style={{ width: 12, height: 12, borderRadius: 3, background: color }} />
              <span style={{ fontSize: 12, color: 'var(--text-secondary)' }}>{label}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}