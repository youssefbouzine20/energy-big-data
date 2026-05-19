import { useState, useEffect } from 'react';
import { Search, Bell, Moon, Maximize2 } from 'lucide-react';

interface HeaderProps {
  title: string;
  subtitle?: string;
  alertCount?: number;
}

export function Header({ title, subtitle, alertCount = 0 }: HeaderProps) {
  const [now, setNow] = useState(new Date());

  useEffect(() => {
    const t = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(t);
  }, []);

  const fmtDate = now.toLocaleDateString('en-GB', { month: 'long', year: 'numeric' });

  return (
    <header style={{
      height: 60,
      background: 'var(--bg-secondary)',
      borderBottom: '1px solid var(--border-default)',
      display: 'flex',
      alignItems: 'center',
      padding: '0 24px',
      gap: 16,
      position: 'sticky',
      top: 0,
      zIndex: 40,
    }}>
      {/* Page title */}
      <div style={{ flex: 1 }}>
        <h1 style={{ fontFamily: 'Space Grotesk', fontSize: 18, fontWeight: 700, color: 'var(--text-primary)', margin: 0 }}>
          {title}
        </h1>
        {subtitle && (
          <p style={{ fontSize: 12, color: 'var(--text-muted)', margin: 0, marginTop: 1 }}>{subtitle}</p>
        )}
      </div>

      {/* Search */}
      <div style={{ position: 'relative', display: 'flex', alignItems: 'center' }}>
        <Search size={14} style={{ position: 'absolute', left: 12, color: 'var(--text-muted)' }} />
        <input
          className="search-bar"
          placeholder="Search..."
          style={{ paddingLeft: 34 }}
        />
      </div>

      {/* Date picker mock */}
      <div style={{
        display: 'flex', alignItems: 'center', gap: 8,
        background: 'var(--bg-card)', border: '1px solid var(--border-default)',
        borderRadius: 8, padding: '6px 12px', fontSize: 13, color: 'var(--text-secondary)',
        cursor: 'default',
      }}>
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <rect x="3" y="4" width="18" height="18" rx="2" ry="2"/>
          <line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/>
          <line x1="3" y1="10" x2="21" y2="10"/>
        </svg>
        {fmtDate}
        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <polyline points="6 9 12 15 18 9"/>
        </svg>
      </div>

      {/* Auto-refresh indicator */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 12, color: 'var(--text-secondary)' }}>
        <span className="pulse-dot" />
        Auto refresh: 30s
      </div>

      {/* Notifications */}
      <button className="header-icon-btn">
        <Bell size={16} />
        {alertCount > 0 && (
          <span className="notification-badge">{alertCount > 9 ? '9+' : alertCount}</span>
        )}
      </button>

      {/* Dark mode toggle (decorative) */}
      <button className="header-icon-btn">
        <Moon size={16} />
      </button>

      {/* Fullscreen */}
      <button
        className="header-icon-btn"
        onClick={() => {
          if (!document.fullscreenElement) document.documentElement.requestFullscreen?.();
          else document.exitFullscreen?.();
        }}
      >
        <Maximize2 size={16} />
      </button>
    </header>
  );
}