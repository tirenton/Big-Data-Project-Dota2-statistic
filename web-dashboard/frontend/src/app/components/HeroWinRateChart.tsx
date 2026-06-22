'use client';

import { Bar } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Tooltip,
  Legend,
} from 'chart.js';

ChartJS.register(CategoryScale, LinearScale, BarElement, Tooltip, Legend);

interface HeroWinRateChartProps {
  heroes: Array<{
    hero_name: string;
    win_rate: number;
    total_picks: number;
  }>;
}

export function HeroWinRateChart({ heroes }: HeroWinRateChartProps) {
  const sorted = [...heroes].sort((a, b) => b.win_rate - a.win_rate);

  const getBarColor = (rate: number) => {
    if (rate >= 55) return { bg: 'rgba(16, 185, 129, 0.7)', border: '#10b981' };
    if (rate >= 45) return { bg: 'rgba(232, 185, 49, 0.7)', border: '#e8b931' };
    return { bg: 'rgba(239, 68, 68, 0.6)', border: '#ef4444' };
  };

  const data = {
    labels: sorted.map((h) => h.hero_name),
    datasets: [
      {
        label: 'Win Rate %',
        data: sorted.map((h) => h.win_rate),
        backgroundColor: sorted.map((h) => getBarColor(h.win_rate).bg),
        borderColor: sorted.map((h) => getBarColor(h.win_rate).border),
        borderWidth: 1,
        borderRadius: 4,
        borderSkipped: false,
      },
    ],
  };

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { display: false },
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
        callbacks: {
          label: (ctx: any) => {
            const hero = sorted[ctx.dataIndex];
            return [
              `Win Rate: ${hero.win_rate}%`,
              `Total Picks: ${hero.total_picks}`,
            ];
          },
        },
      },
    },
    scales: {
      x: {
        grid: { display: false },
        ticks: {
          color: '#94a3b8',
          font: { family: 'Inter', size: 10 },
          maxRotation: 45,
          minRotation: 45,
        },
      },
      y: {
        min: 0,
        max: 100,
        grid: { color: 'rgba(148, 163, 184, 0.06)' },
        ticks: {
          color: '#64748b',
          font: { family: 'Inter', size: 11 },
          callback: (val: any) => `${val}%`,
        },
      },
    },
  };

  return <Bar data={data} options={options} />;
}
