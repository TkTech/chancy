import { Link } from 'react-router';
import { formatExecutionTime } from '../../utils';
import { MetricStatCard } from './MetricStatCard';
import { MetricHistogramCard } from './MetricHistogramCard';
import { MetricSuccessRateCard } from './MetricSuccessRateCard';

export function QueueMetrics({ queueName, url, resolution = '5min' }: { queueName: string; url: string; resolution?: string }) {
  const prefix = `queue:${queueName}`;
  const props = { url, resolution };
  return <section className="mb-4">
    <h6><Link to={`/queues/${encodeURIComponent(queueName)}`}>{queueName}</Link></h6>
    <div className="row g-3">
      <div className="col-6 col-md-3"><MetricSuccessRateCard {...props} title="Success rate" succeededKey={`${prefix}:succeeded`} failedKey={`${prefix}:failed`} /></div>
      <div className="col-6 col-md-3"><MetricStatCard {...props} title="Succeeded updates" metricKey={`${prefix}:succeeded`} showSparkline sparklineColor="#10b981" /></div>
      <div className="col-6 col-md-3"><MetricStatCard {...props} title="Failed updates" metricKey={`${prefix}:failed`} showSparkline sparklineColor="#ef4444" /></div>
      <div className="col-6 col-md-3"><MetricHistogramCard {...props} title="Avg final attempt time" metricKey={`${prefix}:execution_time`} formatValue={formatExecutionTime} /></div>
    </div>
  </section>;
}
