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

interface HeroPickChartProps {
  heroes: Array<{
    hero_name: string;
    total_picks: number;
  }>;
}

export function HeroPickChart({ heroes }: HeroPickChartProps) {
  const data = {
    labels: heroes.map((h) => h.hero_name),
    datasets: [
      {
        label: 'Picks',
        data: heroes.map((h) => h.total_picks),
        backgroundColor: heroes.map(
          (_, i) =>
            `hsla(${38 + i * 3}, 85%, ${55 - i * 1.5}%, 0.8)`
        ),
        borderColor: heroes.map(
          (_, i) =>
            `hsla(${38 + i * 3}, 90%, ${60 - i * 1.5}%, 1)`
        ),
        borderWidth: 1,
        borderRadius: 4,
        borderSkipped: false,
      },
    ],
  };

  const options = {
    indexAxis: 'y' as const,
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
      },
    },
    scales: {
      x: {
        grid: { color: 'rgba(148, 163, 184, 0.06)' },
        ticks: { color: '#64748b', font: { family: 'Inter', size: 11 } },
      },
      y: {
        grid: { display: false },
        ticks: {
          color: '#94a3b8',
          font: { family: 'Inter', size: 11, weight: 'normal' as const },
        },
      },
    },
  };

  return <Bar data={data} options={options} />;
}
