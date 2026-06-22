'use client';

import { Doughnut } from 'react-chartjs-2';
import { Chart as ChartJS, ArcElement, Tooltip, Legend } from 'chart.js';

ChartJS.register(ArcElement, Tooltip, Legend);

interface TeamDistributionProps {
  teams: Array<{
    team: string;
    total: number;
    breakdown: Array<{ result: string; count: number }>;
  }>;
  overview: {
    radiant_wins: number;
    dire_wins: number;
  };
}

export function TeamDistribution({ teams, overview }: TeamDistributionProps) {
  const radiantWins = overview.radiant_wins || 0;
  const direWins = overview.dire_wins || 0;

  const data = {
    labels: ['Radiant Wins', 'Dire Wins'],
    datasets: [
      {
        data: [radiantWins, direWins],
        backgroundColor: [
          'rgba(16, 185, 129, 0.75)',
          'rgba(239, 68, 68, 0.75)',
        ],
        borderColor: ['#10b981', '#ef4444'],
        borderWidth: 2,
        hoverBackgroundColor: [
          'rgba(16, 185, 129, 0.9)',
          'rgba(239, 68, 68, 0.9)',
        ],
        spacing: 4,
        borderRadius: 4,
      },
    ],
  };

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    cutout: '62%',
    plugins: {
      legend: {
        position: 'bottom' as const,
        labels: {
          color: '#94a3b8',
          font: { family: 'Inter', size: 12, weight: 'normal' as const },
          padding: 20,
          usePointStyle: true,
          pointStyleWidth: 12,
        },
      },
      tooltip: {
        backgroundColor: 'rgba(15, 23, 42, 0.95)',
        titleColor: '#f1f5f9',
        bodyColor: '#94a3b8',
        borderColor: 'rgba(232, 185, 49, 0.2)',
        borderWidth: 1,
        cornerRadius: 8,
        padding: 12,
        titleFont: { family: 'Inter', weight: 'bold' as const },
        bodyFont: { family: 'Inter' },
      },
    },
  };

  const total = radiantWins + direWins;

  return (
    <div style={{ position: 'relative', width: '100%', height: '100%' }}>
      <Doughnut data={data} options={options} />
      {/* Center text */}
      <div
        style={{
          position: 'absolute',
          top: '42%',
          left: '50%',
          transform: 'translate(-50%, -50%)',
          textAlign: 'center',
          pointerEvents: 'none',
        }}
      >
        <div
          style={{
            fontSize: 28,
            fontWeight: 800,
            color: '#f1f5f9',
            letterSpacing: -1,
          }}
        >
          {total}
        </div>
        <div
          style={{
            fontSize: 11,
            color: '#64748b',
            textTransform: 'uppercase',
            letterSpacing: 1,
            fontWeight: 600,
          }}
        >
          Total
        </div>
      </div>
    </div>
  );
}
