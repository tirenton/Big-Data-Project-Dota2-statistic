'use client';

interface KpiCardsProps {
  overview: {
    total_matches: number;
    unique_heroes: number;
    avg_duration: number;
    radiant_win_rate: number;
    unique_players: number;
    avg_gpm: number;
  };
}

export function KpiCards({ overview }: KpiCardsProps) {
  const formatDuration = (seconds: number) => {
    const mins = Math.floor(seconds / 60);
    const secs = seconds % 60;
    return `${mins}m ${secs}s`;
  };

  const cards = [
    {
      icon: '⚔️',
      value: overview.total_matches?.toLocaleString() || '0',
      label: 'Total Matches',
    },
    {
      icon: '🛡️',
      value: overview.unique_heroes?.toString() || '0',
      label: 'Unique Heroes',
    },
    {
      icon: '⏱️',
      value: formatDuration(overview.avg_duration || 0),
      label: 'Avg Duration',
    },
    {
      icon: '🌿',
      value: `${overview.radiant_win_rate || 0}%`,
      label: 'Radiant Win Rate',
    },
  ];

  return (
    <div className="kpi-grid">
      {cards.map((card, i) => (
        <div
          key={card.label}
          className={`kpi-card fade-in fade-in-delay-${i + 1}`}
        >
          <div className="kpi-icon">{card.icon}</div>
          <div className="kpi-value">{card.value}</div>
          <div className="kpi-label">{card.label}</div>
        </div>
      ))}
    </div>
  );
}
