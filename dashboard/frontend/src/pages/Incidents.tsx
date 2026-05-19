import { useState, useEffect } from 'react';
import { Header } from '../components/Header';
import { api } from '../lib/api';
import { DemoBanner } from '../components/DemoBanner';

const COLORS = ['#8b5cf6','#7c3aed','#f59e0b','#94a3b8','#6d6d8f','#a78bfa','#fbbf24'];

const DEMO_WORDS = [
  { word: 'power outage', count: 120 }, { word: 'voltage', count: 95 },
  { word: 'transformer', count: 85 },   { word: 'overload', count: 70 },
  { word: 'maintenance', count: 60 },   { word: 'line failure', count: 55 },
  { word: 'fault', count: 40 },         { word: 'substation', count: 38 },
  { word: 'inspection', count: 32 },    { word: 'heating', count: 28 },
  { word: 'repair', count: 26 },        { word: 'weatlee', count: 22 },
  { word: 'breating', count: 20 },      { word: 'gonile', count: 18 },
  { word: 'mainterance', count: 16 },   { word: 'fire', count: 14 },
  { word: 'cable', count: 13 },         { word: 'grid', count: 12 },
  { word: 'outage', count: 11 },        { word: 'zone', count: 10 },
];

const DEMO_INCIDENTS = [
  { _id:'1', title:'Transformer overload Zone B', severity:'high',   status:'open',     created_at: new Date(Date.now()-1*3600000).toISOString(), zone:'B', description:'Transformer T-04 is operating above 90% capacity.' },
  { _id:'2', title:'Line fault on substation S-12', severity:'medium',status:'resolved', created_at: new Date(Date.now()-3*3600000).toISOString(), zone:'A', description:'Line interruption detected and resolved.' },
  { _id:'3', title:'Voltage fluctuation Zone C',  severity:'low',    status:'monitoring',created_at: new Date(Date.now()-5*3600000).toISOString(), zone:'C', description:'Minor voltage deviation within acceptable range.' },
  { _id:'4', title:'Cable overheating detected',  severity:'high',   status:'open',     created_at: new Date(Date.now()-8*3600000).toISOString(), zone:'D', description:'High temperature recorded on cable segment D-07.' },
];

const SEV: Record<string, any> = {
  high:   { color: '#ef4444', bg: 'rgba(239,68,68,0.1)',  label: 'High'   },
  medium: { color: '#f59e0b', bg: 'rgba(245,158,11,0.1)', label: 'Medium' },
  low:    { color: '#22c55e', bg: 'rgba(34,197,94,0.1)',  label: 'Low'    },
};
const STATUS_COLOR: Record<string, string> = {
  open: '#ef4444', resolved: '#22c55e', monitoring: '#f59e0b',
};

export function Incidents() {
  const [words, setWords]         = useState<any[]>(DEMO_WORDS);
  const [incidents, setIncidents] = useState<any[]>(DEMO_INCIDENTS);
  const [usingDemo, setUsingDemo] = useState({ words: true, incidents: true });

  useEffect(() => {
    const load = async () => {
      try {
        const [w, inc] = await Promise.all([
          api.get<any>('/api/incidents/wordcloud').catch(() => null),
          api.get<any>('/api/incidents').catch(() => null),
        ]);
        const wItems = w?.items ?? w?.words ?? [];
        if (wItems.length) {
          setWords(wItems.map((x: any) => ({ word: x.text ?? x.word, count: x.value ?? x.count })));
          setUsingDemo(d => ({ ...d, words: false }));
        } else {
          setUsingDemo(d => ({ ...d, words: true }));
        }
        const iItems = inc?.items ?? inc?.incidents ?? [];
        if (iItems.length) {
          setIncidents(iItems.map((x: any) => ({
            _id: x.incident_id ?? x._id,
            title: `${x.type ?? 'Incident'} — Zone ${x.zone ?? '?'}`,
            severity: String(x.severity ?? 'medium').toLowerCase(),
            status: x.resolved ? 'resolved' : 'open',
            created_at: x.timestamp ?? x.created_at,
            zone: x.zone,
            description: x.description,
          })));
          setUsingDemo(d => ({ ...d, incidents: false }));
        } else {
          setUsingDemo(d => ({ ...d, incidents: true }));
        }
      } catch {}
    };
    load();
    const t = setInterval(load, 30_000);
    return () => clearInterval(t);
  }, []);

  const maxCount = Math.max(...words.map(w => w.count));

  return (
    <div style={{ animation: 'fade-in 0.4s ease-out both' }}>
      <Header title="Incidents" subtitle="Technical incident tracking and NLP analysis" />
      <div style={{ paddingTop: 24 }}>

        {(usingDemo.words || usingDemo.incidents) && (
          <DemoBanner
            sources={[usingDemo.words ? '/api/incidents/wordcloud' : null, usingDemo.incidents ? '/api/incidents' : null].filter(Boolean) as string[]}
            message="Incident data shows demonstration content"
          />
        )}

        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1.6fr', gap: 20, marginBottom: 20 }}>

          {/* Word Cloud */}
          <div className="card-atonist" style={{ padding: '20px', minHeight: 300 }}>
            <h3 style={{ fontFamily: 'Space Grotesk', fontSize: 15, fontWeight: 600, color: 'var(--text-primary)', margin: '0 0 16px' }}>
              Top Incident Keywords
            </h3>
            <div style={{
              display: 'flex', flexWrap: 'wrap', gap: 10,
              alignItems: 'center', justifyContent: 'center', minHeight: 220,
            }}>
              {words.slice(0, 20).map((w: any, i: number) => {
                const ratio = w.count / maxCount;
                const size  = 10 + ratio * 24;
                const color = COLORS[i % COLORS.length];
                return (
                  <span key={i} style={{
                    fontSize: size, fontWeight: ratio > 0.5 ? 700 : 500,
                    color, opacity: 0.7 + ratio * 0.3, lineHeight: 1.5, cursor: 'default',
                    transition: 'opacity 0.2s, transform 0.2s',
                    display: 'inline-block',
                  }}
                    onMouseEnter={e => { e.currentTarget.style.opacity='1'; e.currentTarget.style.transform='scale(1.1)'; }}
                    onMouseLeave={e => { e.currentTarget.style.opacity=String(0.7+ratio*0.3); e.currentTarget.style.transform='scale(1)'; }}
                    title={`${w.word}: ${w.count} occurrences`}
                  >
                    {w.word}
                  </span>
                );
              })}
            </div>
          </div>

          {/* Stats */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 14 }}>
              {[
                { label: 'Total Incidents', value: incidents.length, color: 'var(--accent-purple-light)' },
                { label: 'Open',     value: incidents.filter(i => i.status==='open').length,     color: '#ef4444' },
                { label: 'Resolved', value: incidents.filter(i => i.status==='resolved').length, color: '#22c55e' },
              ].map(({ label, value, color }) => (
                <div key={label} className="card-atonist" style={{ padding: '14px 16px' }}>
                  <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 6 }}>{label}</div>
                  <div style={{ fontFamily: 'Space Grotesk', fontSize: 30, fontWeight: 700, color }}>{value}</div>
                </div>
              ))}
            </div>

            {/* Severity distribution */}
            <div className="card-atonist" style={{ padding: '16px 20px', flex: 1 }}>
              <div style={{ fontSize: 13, fontWeight: 500, color: 'var(--text-secondary)', marginBottom: 12 }}>Severity Distribution</div>
              {['high','medium','low'].map(sev => {
                const cnt = incidents.filter(i => i.severity===sev).length;
                const pct = incidents.length ? Math.round(cnt/incidents.length*100) : 0;
                const { color, label } = SEV[sev];
                return (
                  <div key={sev} style={{ marginBottom: 10 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, color: 'var(--text-secondary)', marginBottom: 4 }}>
                      <span>{label}</span><span style={{ color }}>{cnt} ({pct}%)</span>
                    </div>
                    <div style={{ height: 6, borderRadius: 3, background: 'var(--bg-input)' }}>
                      <div style={{ height: '100%', width: `${pct}%`, borderRadius: 3, background: color, transition: 'width 1s ease' }} />
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>

        {/* Incidents table */}
        <div className="card-atonist" style={{ padding: '20px' }}>
          <h3 style={{ fontFamily: 'Space Grotesk', fontSize: 15, fontWeight: 600, color: 'var(--text-primary)', margin: '0 0 14px' }}>
            Recent Incidents
          </h3>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {incidents.map((inc: any) => {
              const sev = SEV[inc.severity] ?? SEV.low;
              const statusColor = STATUS_COLOR[inc.status] ?? '#94a3b8';
              const ago = inc.created_at
                ? (() => { const d = Math.round((Date.now()-new Date(inc.created_at).getTime())/60000); return d<60?`${d}m ago`:`${Math.floor(d/60)}h ago`; })()
                : '';
              return (
                <div key={inc._id} className="alert-item">
                  <div style={{ width: 10, height: 10, borderRadius: '50%', background: sev.color, flexShrink: 0, marginTop: 4 }} />
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ display: 'flex', gap: 8, alignItems: 'center', marginBottom: 2 }}>
                      <span style={{ fontSize: 13, fontWeight: 500, color: 'var(--text-primary)' }}>{inc.title}</span>
                      <span style={{ fontSize: 10, background: sev.bg, color: sev.color, padding: '2px 6px', borderRadius: 4, fontWeight: 600 }}>{sev.label}</span>
                      <span style={{ fontSize: 10, color: statusColor, fontWeight: 500 }}>{inc.status}</span>
                    </div>
                    <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>{inc.description} {inc.zone && `· Zone ${inc.zone}`}</div>
                  </div>
                  <div style={{ fontSize: 11, color: 'var(--text-muted)', whiteSpace: 'nowrap', flexShrink: 0 }}>{ago}</div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
}