'use client';

import { useEffect, useState, useCallback } from 'react';
import { KpiCards } from './components/KpiCards';
import { HeroPickChart } from './components/HeroPickChart';
import { HeroWinRateChart } from './components/HeroWinRateChart';
import { TeamDistribution } from './components/TeamDistribution';
import { HeroTable } from './components/HeroTable';
import { PlayerLeaderboard } from './components/PlayerLeaderboard';

interface DashboardData {
  overview: any;
  heroStats: any[];
  playerStats: any[];
  teamDist: any[];
}

export default function Home() {
  const [data, setData] = useState<DashboardData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);

  const fetchData = useCallback(async (isRefresh = false) => {
    try {
      if (isRefresh) setRefreshing(true);
      else setLoading(true);
      setError(null);

      const [overviewRes, heroRes, playerRes, teamRes] = await Promise.all([
        fetch('/api/match-overview'),
        fetch('/api/hero-stats'),
        fetch('/api/player-stats'),
        fetch('/api/team-distribution'),
      ]);

      if (!overviewRes.ok) throw new Error('Backend unavailable');

      const [overview, heroStats, playerStats, teamDist] = await Promise.all([
        overviewRes.json(),
        heroRes.json(),
        playerRes.json(),
        teamRes.json(),
      ]);

      setData({ overview, heroStats, playerStats, teamDist });
      setLastUpdated(new Date());
    } catch (err: any) {
      setError(
        err.message === 'Backend unavailable'
          ? 'Cannot connect to the backend API. Make sure the NestJS server is running.'
          : 'Failed to load dashboard data. Check that all services are running.'
      );
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  // ─── Loading ──────────────────────────────────────
  if (loading) {
    return (
      <>
        <Header connected={false} refreshing={false} onRefresh={() => {}} />
        <main className="dashboard">
          <div className="loading-screen">
            <div className="loading-spinner" />
            <p className="loading-text">Connecting to Dota 2 Analytics…</p>
          </div>
        </main>
      </>
    );
  }

  const hasData = data && data.heroStats.length > 0;

  return (
    <>
      <Header
        connected={!error}
        refreshing={refreshing}
        onRefresh={() => fetchData(true)}
        lastUpdated={lastUpdated}
      />

      <main className="dashboard">
        {/* Error banner */}
        {error && (
          <div className="error-banner fade-in">
            <span>⚠️</span>
            <span>{error}</span>
          </div>
        )}

        {/* Empty state */}
        {!error && !hasData && (
          <div className="empty-state fade-in">
            <div className="empty-icon">🎮</div>
            <h2 className="empty-title">No match data yet</h2>
            <p className="empty-desc">
              Run the data collector pipeline first to populate Elasticsearch
              with Dota 2 match data, then refresh this page.
            </p>
          </div>
        )}

        {/* Dashboard content */}
        {hasData && data && (
          <>
            {/* KPI Cards */}
            <section className="fade-in">
              <KpiCards overview={data.overview} />
            </section>

            {/* Charts Row 1: Hero Picks + Win Rate */}
            <div className="section-label fade-in fade-in-delay-1">
              Hero Analytics
            </div>
            <div className="charts-row fade-in fade-in-delay-1">
              <div className="panel">
                <div className="panel-header">
                  <h2 className="panel-title">Hero Pick Rate</h2>
                  <span className="panel-badge">Top 15</span>
                </div>
                <div className="panel-body">
                  <div className="chart-container">
                    <HeroPickChart heroes={data.heroStats.slice(0, 15)} />
                  </div>
                </div>
              </div>

              <div className="panel">
                <div className="panel-header">
                  <h2 className="panel-title">Hero Win Rate</h2>
                  <span className="panel-badge">Top 15</span>
                </div>
                <div className="panel-body">
                  <div className="chart-container">
                    <HeroWinRateChart heroes={data.heroStats.slice(0, 15)} />
                  </div>
                </div>
              </div>
            </div>

            {/* Charts Row 2: Win Rate bar + Team Distribution */}
            <div className="section-label fade-in fade-in-delay-2">
              Match Insights
            </div>
            <div className="charts-row-3 fade-in fade-in-delay-2">
              {/* Hero Performance Table */}
              <div className="panel">
                <div className="panel-header">
                  <h2 className="panel-title">Hero Performance</h2>
                  <span className="panel-badge">{data.heroStats.length} Heroes</span>
                </div>
                <div className="panel-body">
                  <HeroTable heroes={data.heroStats} />
                </div>
              </div>

              {/* Team Distribution Doughnut */}
              <div className="panel">
                <div className="panel-header">
                  <h2 className="panel-title">Radiant vs Dire</h2>
                </div>
                <div className="panel-body">
                  <div className="chart-container-sm">
                    <TeamDistribution
                      teams={data.teamDist}
                      overview={data.overview}
                    />
                  </div>
                </div>
              </div>
            </div>

            {/* Player Leaderboard */}
            <div className="section-label fade-in fade-in-delay-3">
              Player Leaderboard
            </div>
            <div className="panel fade-in fade-in-delay-3">
              <div className="panel-header">
                <h2 className="panel-title">Top Players by KDA</h2>
                <span className="panel-badge">{data.playerStats.length} Players</span>
              </div>
              <div className="panel-body">
                <PlayerLeaderboard players={data.playerStats} />
              </div>
            </div>
          </>
        )}
      </main>
    </>
  );
}

/* ─── Header component (inline) ──────────────────────── */
function Header({
  connected,
  refreshing,
  onRefresh,
  lastUpdated,
}: {
  connected: boolean;
  refreshing: boolean;
  onRefresh: () => void;
  lastUpdated?: Date | null;
}) {
  return (
    <header className="header">
      <div className="header-brand">
        <span className="header-logo">🎮</span>
        <div>
          <div className="header-title">Dota 2 Analytics</div>
          <div className="header-subtitle">Big Data Pipeline Dashboard</div>
        </div>
      </div>
      <div className="header-controls">
        {lastUpdated && (
          <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>
            Updated {lastUpdated.toLocaleTimeString()}
          </span>
        )}
        <div className={`status-badge ${connected ? '' : 'error'}`}>
          <span className="status-dot" />
          {connected ? 'Connected' : 'Disconnected'}
        </div>
        <button
          className={`refresh-btn ${refreshing ? 'spinning' : ''}`}
          onClick={onRefresh}
          disabled={refreshing}
        >
          <svg
            width="14"
            height="14"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2.5"
            strokeLinecap="round"
            strokeLinejoin="round"
          >
            <path d="M21.5 2v6h-6" />
            <path d="M2.5 22v-6h6" />
            <path d="M2 11.5a10 10 0 0 1 18.8-4.3L21.5 8" />
            <path d="M22 12.5a10 10 0 0 1-18.8 4.2L2.5 16" />
          </svg>
          Refresh
        </button>
      </div>
    </header>
  );
}
