'use client';

interface PlayerLeaderboardProps {
  players: Array<{
    player_name: string;
    games_played: number;
    wins: number;
    win_rate: number;
    avg_kills: number;
    avg_deaths: number;
    avg_assists: number;
    avg_kda: number;
    avg_gpm: number;
    avg_xpm: number;
    favorite_hero: string;
  }>;
}

export function PlayerLeaderboard({ players }: PlayerLeaderboardProps) {
  const winRateClass = (rate: number) => {
    if (rate >= 55) return 'high';
    if (rate >= 45) return 'mid';
    return 'low';
  };

  return (
    <div className="data-table-wrapper">
      <table className="data-table" id="player-leaderboard">
        <thead>
          <tr>
            <th style={{ width: 44 }}>#</th>
            <th>Player</th>
            <th>Games</th>
            <th>Win Rate</th>
            <th>Avg KDA</th>
            <th>K / D / A</th>
            <th>GPM</th>
            <th>XPM</th>
            <th>Favorite Hero</th>
          </tr>
        </thead>
        <tbody>
          {players.slice(0, 30).map((p, i) => (
            <tr key={`${p.player_name}-${i}`}>
              <td>
                <span className={`rank-cell ${i < 3 ? 'top-3' : ''}`}>
                  {i + 1}
                </span>
              </td>
              <td>{p.player_name || 'Anonymous'}</td>
              <td>{p.games_played}</td>
              <td>
                <div className="win-rate-bar">
                  <div className="win-rate-track">
                    <div
                      className="win-rate-fill"
                      style={{ width: `${Math.min(p.win_rate, 100)}%` }}
                    />
                  </div>
                  <span className={`win-rate-label ${winRateClass(p.win_rate)}`}>
                    {p.win_rate}%
                  </span>
                </div>
              </td>
              <td style={{ color: 'var(--gold)', fontWeight: 600 }}>
                {p.avg_kda}
              </td>
              <td>
                <span style={{ color: 'var(--radiant)' }}>{p.avg_kills}</span>
                {' / '}
                <span style={{ color: 'var(--dire)' }}>{p.avg_deaths}</span>
                {' / '}
                <span style={{ color: 'var(--info)' }}>{p.avg_assists}</span>
              </td>
              <td>{p.avg_gpm}</td>
              <td>{p.avg_xpm}</td>
              <td style={{ color: 'var(--purple)' }}>{p.favorite_hero}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
