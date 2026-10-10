import { MetricStatCard } from './MetricStatCard';

const formatBytes = (bytes: number) => {
  if (bytes >= 1073741824) return `${(bytes / 1073741824).toFixed(1)} GB`;
  if (bytes >= 1048576) return `${(bytes / 1048576).toFixed(1)} MB`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${bytes} B`;
};

export function MetricTableSizeCard({ tableName, selector = 'total_size_bytes', ...props }: {
  title: string; tableName: string; url: string; resolution?: string; sparklineColor?: string;
  selector?: 'total_size_bytes' | 'table_size_bytes' | 'index_size_bytes';
}) {
  return <MetricStatCard {...props} metricKey={`table:${tableName}:${selector}`}
    formatValue={formatBytes} subtitle="Latest observed size" showSparkline />;
}
