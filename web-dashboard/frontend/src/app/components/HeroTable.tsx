'use client';

import { useState, useMemo } from 'react';

interface HeroTableProps {
  heroes: Array<{
    hero_name: string;
    total_picks: number;
    total_wins: number;
    win_rate: number;
    avg_kills: number;
    avg_deaths: number;
    avg_assists: number;
    avg_kda: number;
    avg_gpm: number;
    avg_xpm: number;
  }>;
}

type SortKey = 'hero_name' | 'total_picks' | 'win_rate' | 'avg_kda' | 'avg_gpm' | 'avg_xpm';

export function HeroTable({ heroes }: HeroTableProps) {
  const [sortKey, setSortKey] = useState<SortKey>('total_picks');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');

  const handleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir(sortDir === 'asc' ? 'desc' : 'asc');
    } else {
      setSortKey(key);
      setSortDir('desc');
    }
  };

  const sorted = useMemo(() => {
    return [...heroes].sort((a, b) => {
      const aVal = a[sortKey];
      const bVal = b[sortKey];
      if (typeof aVal === 'string' && typeof bVal === 'string') {
        return sortDir === 'asc'
          ? aVal.localeCompare(bVal)
          : bVal.localeCompare(aVal);
      }
      return sortDir === 'asc'
        ? (aVal as number) - (bVal as number)
        : (bVal as number) - (aVal as number);
    });
  }, [heroes, sortKey, sortDir]);

  const arrow = (key: SortKey) =>
    sortKey === key ? (sortDir === 'asc' ? ' ▲' : ' ▼') : '';

  const winRateClass = (rate: number) => {
    if (rate >= 55) return 'high';
    if (rate >= 45) return 'mid';
    return 'low';
  };

  return (
    <div className="data-table-wrapper">
      <table className="data-table" id="hero-table">
        <thead>
          <tr>
            <th style={{ width: 44 }}>#</th>
            <th
              className={sortKey === 'hero_name' ? 'sorted' : ''}
              onClick={() => handleSort('hero_name')}
            >
              Hero{arrow('hero_name')}
            </th>
            <th
              className={sortKey === 'total_picks' ? 'sorted' : ''}
              onClick={() => handleSort('total_picks')}
            >
              Picks{arrow('total_picks')}
            </th>
            <th
              className={sortKey === 'win_rate' ? 'sorted' : ''}
              onClick={() => handleSort('win_rate')}
            >
              Win Rate{arrow('win_rate')}
            </th>
            <th
              className={sortKey === 'avg_kda' ? 'sorted' : ''}
              onClick={() => handleSort('avg_kda')}
            >
              Avg KDA{arrow('avg_kda')}
            </th>
            <th>K / D / A</th>
            <th
              className={sortKey === 'avg_gpm' ? 'sorted' : ''}
              onClick={() => handleSort('avg_gpm')}
            >
              GPM{arrow('avg_gpm')}
            </th>
            <th
              className={sortKey === 'avg_xpm' ? 'sorted' : ''}
              onClick={() => handleSort('avg_xpm')}
            >
              XPM{arrow('avg_xpm')}
            </th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((hero, i) => (
            <tr key={hero.hero_name}>
              <td>
                <span className={`rank-cell ${i < 3 ? 'top-3' : ''}`}>
                  {i + 1}
                </span>
              </td>
              <td>{hero.hero_name}</td>
              <td>{hero.total_picks}</td>
              <td>
                <div className="win-rate-bar">
                  <div className="win-rate-track">
                    <div
                      className="win-rate-fill"
                      style={{ width: `${Math.min(hero.win_rate, 100)}%` }}
                    />
                  </div>
                  <span className={`win-rate-label ${winRateClass(hero.win_rate)}`}>
                    {hero.win_rate}%
                  </span>
                </div>
              </td>
              <td style={{ color: 'var(--gold)', fontWeight: 600 }}>
                {hero.avg_kda}
              </td>
              <td>
                <span style={{ color: 'var(--radiant)' }}>{hero.avg_kills}</span>
                {' / '}
                <span style={{ color: 'var(--dire)' }}>{hero.avg_deaths}</span>
                {' / '}
                <span style={{ color: 'var(--info)' }}>{hero.avg_assists}</span>
              </td>
              <td>{hero.avg_gpm}</td>
              <td>{hero.avg_xpm}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
