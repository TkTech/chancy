import { useServerConfiguration } from '../hooks/useServerConfiguration';
import { useWorkers } from '../hooks/useWorkers';
import { useQueues } from '../hooks/useQueues';
import { PageHeader } from '../components/common/PageHeader';
import { MetricCard } from '../components/dashboard/MetricCard';
import { MetricStatCard } from '../components/dashboard/MetricStatCard';
import { MetricSuccessRateCard } from '../components/dashboard/MetricSuccessRateCard';
import { MetricTableSizeCard } from '../components/dashboard/MetricTableSizeCard';
import { QueueMetrics } from '../components/dashboard/QueueMetrics';

const formatNumber = (num: number) => {
  if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
  if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
  return num.toString();
};

export function Dashboard() {
  const { url } = useServerConfiguration();
  const resolution = '5min';

  // Fetch worker and queue counts
  const { data: workers } = useWorkers(url);
  const { data: queues } = useQueues(url);

  return (
    <div className="container-fluid">
      <PageHeader
        title="Dashboard"
        description="Overview of job queue system metrics"
      />

      {/* Metric cards */}
      <div className="row g-3 mb-4">
        <div className="col-12 col-md-6 col-xl-3">
          <MetricCard
            title="Workers Active"
            value={workers?.length || 0}
            loading={!workers}
          />
        </div>
        <div className="col-12 col-md-6 col-xl-3">
          <MetricCard
            title="Queues"
            value={queues?.length || 0}
            loading={!queues}
          />
        </div>
        <div className="col-12 col-md-6 col-xl-3">
          <MetricSuccessRateCard
            title="Success Rate"
            succeededKey="global:status:succeeded"
            failedKey="global:status:failed"
            url={url!}
            resolution={resolution}
          />
        </div>
        <div className="col-12 col-md-6 col-xl-3">
          <MetricStatCard
            title="Tasks Succeeded"
            metricKey="global:status:succeeded"
            url={url!}
            resolution={resolution}
            subtitle="last 24 hours"
            formatValue={formatNumber}
            showSparkline={true}
            sparklineColor="#10b981"
          />
        </div>
      </div>

      {/* Secondary row */}
      <div className="row g-3 mb-4">
        <div className="col-12 col-md-6 col-xl-3">
          <MetricStatCard
            title="Tasks Failed"
            metricKey="global:status:failed"
            url={url!}
            resolution={resolution}
            subtitle="last 24 hours"
            formatValue={formatNumber}
            showSparkline={true}
            sparklineColor="#ef4444"
          />
        </div>
        <div className="col-12 col-md-6 col-xl-3">
          <MetricTableSizeCard
            title="Jobs Table Size"
            tableName="jobs"
            url={url!}
            resolution={resolution}
            sparklineColor="#8b5cf6"
          />
        </div>
        <div className="col-12 col-md-6 col-xl-3">
          <MetricTableSizeCard
            title="Metrics Table Size"
            tableName="metrics"
            url={url!}
            resolution={resolution}
            sparklineColor="#f97316"
          />
        </div>
        <div className="col-xl-3 d-none d-xl-flex align-items-center justify-content-center">
          <img src="/logo_small.png" alt="Chancy" width={"128px"} height={"128px"}/>
        </div>
      </div>

      {/* Per-queue metrics */}
      {queues && queues.length > 0 && (
        <div className="mb-4">
          <h5 className="mb-3">Queue Metrics</h5>
          {queues.map((queue) => (
            <QueueMetrics key={queue.name} queueName={queue.name} url={url!} resolution={resolution} />
          ))}
        </div>
      )}
    </div>
  );
}
