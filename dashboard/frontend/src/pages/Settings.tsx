import { useState, useEffect } from 'react';
import { Header } from '../components/Header';
import { api } from '../lib/api';
import { useAuth } from '../lib/auth';

export function Settings() {
  const { user } = useAuth();
  const [retention, setRetention] = useState<any[]>([]);
  const [system, setSystem]       = useState<any>(null);

  useEffect(() => {
    const load = async () => {
      try {
        const [r, s] = await Promise.all([
          api.get('/api/settings/retention').catch(() => null),
          api.get('/api/settings/system').catch(() => null),
        ]);
        setRetention(r?.policies ?? []);
        setSystem(s);
      } catch {}
    };
    load();
  }, []);

  const DEMO_RETENTION = [
    { collection: 'meters_aggregated_15min', ttl_days: 90,  record_count: 142300 },
    { collection: 'incidents_enriched',       ttl_days: 365, record_count: 8920   },
    { collection: 'ml_predictions',           ttl_days: 30,  record_count: 34560  },
    { collection: 'feedback_nlp',             ttl_days: 180, record_count: 2100   },
    { collection: 'data_quality_metrics',     ttl_days: 60,  record_count: 500    },
    { collection: 'dashboard_alerts',         ttl_days: 365, record_count: 89     },
  ];

  const displayRetention = retention.length ? retention : DEMO_RETENTION;

  return (
    <div style={{ animation: 'fade-in 0.4s ease-out both' }}>
      <Header title="Settings" subtitle="System configuration and retention policies" />
      <div style={{ paddingTop: 24 }}>

        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20 }}>

          {/* Retention policies */}
          <div className="card-atonist" style={{ padding: '20px' }}>
            <h3 style={{ fontFamily: 'Space Grotesk', fontSize: 15, fontWeight: 600, color: 'var(--text-primary)', margin: '0 0 16px' }}>
              TTL Retention Policies
            </h3>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              {displayRetention.map((p: any) => (
                <div key={p.collection} style={{
                  display: 'flex', alignItems: 'center', gap: 12,
                  padding: '10px 14px', background: 'var(--bg-input)',
                  borderRadius: 8, border: '1px solid var(--border-default)',
                }}>
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ fontSize: 12, fontFamily: 'monospace', color: 'var(--text-secondary)', marginBottom: 2 }}>
                      {p.collection}
                    </div>
                    <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>
                      {(p.record_count ?? 0).toLocaleString()} records
                    </div>
                  </div>
                  <div style={{
                    background: 'rgba(124,58,237,0.12)', border: '1px solid rgba(124,58,237,0.25)',
                    borderRadius: 6, padding: '4px 10px',
                    fontSize: 12, fontWeight: 600, color: 'var(--accent-purple-light)',
                    whiteSpace: 'nowrap',
                  }}>
                    {p.ttl_days}d TTL
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* System info + account */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
            {/* System */}
            <div className="card-atonist" style={{ padding: '20px' }}>
              <h3 style={{ fontFamily: 'Space Grotesk', fontSize: 15, fontWeight: 600, color: 'var(--text-primary)', margin: '0 0 14px' }}>
                System Info
              </h3>
              {[
                { label: 'Backend Port',  value: system?.port ?? 4000 },
                { label: 'Frontend Port', value: 5173 },
                { label: 'Refresh Rate',  value: '30s' },
                { label: 'Auth Method',   value: 'JWT (httpOnly cookie)' },
                { label: 'DB Driver',     value: 'MongoDB official driver' },
                { label: 'Stack',         value: 'React + Vite + Express' },
              ].map(({ label, value }) => (
                <div key={label} style={{
                  display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                  padding: '8px 0', borderBottom: '1px solid var(--border-default)',
                  fontSize: 13,
                }}>
                  <span style={{ color: 'var(--text-muted)' }}>{label}</span>
                  <span style={{ color: 'var(--text-primary)', fontFamily: typeof value === 'number' ? 'monospace' : undefined }}>
                    {String(value)}
                  </span>
                </div>
              ))}
            </div>

            {/* Account */}
            <div className="card-atonist" style={{ padding: '20px' }}>
              <h3 style={{ fontFamily: 'Space Grotesk', fontSize: 15, fontWeight: 600, color: 'var(--text-primary)', margin: '0 0 14px' }}>
                Account
              </h3>
              <div style={{ display: 'flex', alignItems: 'center', gap: 14 }}>
                <div style={{
                  width: 48, height: 48, borderRadius: '50%',
                  background: 'linear-gradient(135deg, var(--accent-purple), var(--accent-gold))',
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                  fontSize: 18, fontWeight: 700, color: '#fff',
                }}>
                  {user?.username?.[0]?.toUpperCase() ?? 'U'}
                </div>
                <div>
                  <div style={{ fontSize: 16, fontWeight: 600, color: 'var(--text-primary)' }}>{user?.username ?? 'user'}</div>
                  <div style={{
                    fontSize: 11, fontWeight: 600, marginTop: 2, textTransform: 'capitalize',
                    color: user?.role === 'admin' ? 'var(--accent-purple-light)' : 'var(--accent-gold)',
                    background: user?.role === 'admin' ? 'rgba(124,58,237,0.12)' : 'rgba(245,158,11,0.12)',
                    padding: '2px 8px', borderRadius: 4, display: 'inline-block',
                  }}>
                    {user?.role ?? 'operator'}
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}