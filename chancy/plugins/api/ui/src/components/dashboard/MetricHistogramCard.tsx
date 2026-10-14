import { MetricStatCard, type MetricStatCardProps } from './MetricStatCard';

export function MetricHistogramCard(props: MetricStatCardProps) {
  return <MetricStatCard {...props} showSparkline />;
}
