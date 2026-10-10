import { AreaChart, Area, ResponsiveContainer, XAxis } from 'recharts';

interface MiniSparklineProps {
  data: Array<{ timestamp: number; value: number | null }>;
  color?: string;
  height?: number;
}

/**
 * Minimal sparkline chart for metric cards
 */
export function MiniSparkline({ data, color = '#3b82f6', height = 40 }: MiniSparklineProps) {
  if (!data || data.length === 0) return null;

  return (
    <ResponsiveContainer width="100%" height={height}>
      <AreaChart data={data} margin={{ top: 0, right: 0, left: 0, bottom: 0 }}>
        <XAxis dataKey="timestamp" type="number" domain={['dataMin', 'dataMax']} hide />
        <Area
          connectNulls={false}
          isAnimationActive={false}
          type="monotone"
          dataKey="value"
          stroke={color}
          fill={color}
          fillOpacity={0.3}
          strokeWidth={2}
        />
      </AreaChart>
    </ResponsiveContainer>
  );
}
