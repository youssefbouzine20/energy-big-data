type Props = { sources?: string[]; message?: string };

export function DemoBanner({ sources = [], message }: Props) {
  return (
    <div style={{
      background: 'rgba(245,158,11,0.10)',
      border: '1px solid rgba(245,158,11,0.45)',
      color: '#b45309',
      padding: '10px 14px',
      borderRadius: 8,
      marginBottom: 16,
      fontSize: 13,
    }}>
      ⚠ {message ?? 'This panel shows demonstration data'} — API returned no records
      {sources.length > 0 && (
        <> (sources affected: <strong>{sources.join(', ')}</strong>)</>
      )}.
    </div>
  );
}
