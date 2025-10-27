import { useMetricDetail } from '../../hooks/useMetrics';
import { MiniSparkline } from './MiniSparkline';
import { Link } from 'react-router-dom';

interface QueueMetricsProps {
  queueName: string;
  url: string;
  resolution?: string;
}

const formatNumber = (num: number) => {
  if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
  if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
  return num.toString();
};

const formatTime = (ms: number) => {
  if (ms >= 1000) return `${(ms / 1000).toFixed(1)}s`;
  return `${Math.round(ms)}ms`;
};

/**
 * Per-queue metric cards showing key metrics for a specific queue
 */
export function QueueMetrics({ queueName, url, resolution = '5min' }: QueueMetricsProps) {
  const limit = {
    '1min': 60,
    '5min': 60,
    '1hour': 24,
    '1day': 30
  }[resolution] || 60;

  // Fetch queue-specific metrics
  const { data: succeededData } = useMetricDetail({
    url,
    key: `queue:${queueName}:succeeded`,
    resolution,
    limit,
  });

  const { data: failedData } = useMetricDetail({
    url,
    key: `queue:${queueName}:failed`,
    resolution,
    limit,
  });

  const { data: executionTimeData } = useMetricDetail({
    url,
    key: `queue:${queueName}:execution_time`,
    resolution,
    limit,
  });

  // Calculate metrics
  const calculateTotal = (data: any) => {
    if (!data) return 0;
    const key = Object.keys(data)[0];
    if (!key || !data[key]?.data) return 0;
    return data[key].data.reduce((sum: number, point: any) => sum + (point.value || 0), 0);
  };

  const calculateAverage = (data: any) => {
    if (!data) return 0;
    const key = Object.keys(data)[0];
    if (!key || !data[key]?.data) return 0;

    // For histogram metrics, the value is an object with avg, min, max
    const isHistogram = data[key].type === 'histogram';

    if (isHistogram) {
      // Get the latest avg value from histogram
      const latestPoint = data[key].data[data[key].data.length - 1];
      return latestPoint?.value?.avg || 0;
    }

    // For regular metrics, calculate average
    const values = data[key].data.filter((point: any) => point.value > 0);
    if (values.length === 0) return 0;
    const sum = values.reduce((acc: number, point: any) => acc + point.value, 0);
    return sum / values.length;
  };

  const getSparklineData = (data: any, useAvg = false) => {
    if (!data) {
      // Return array of zeros to show flat line
      return Array(10).fill({ value: 0 });
    }

    const key = Object.keys(data)[0];
    if (!key || !data[key]?.data || data[key].data.length === 0) {
      // Return array of zeros to show flat line
      return Array(10).fill({ value: 0 });
    }

    const isHistogram = data[key].type === 'histogram';

    // Extract values from the data points
    return data[key].data.map((point: any) => {
      let value = 0;
      if (useAvg && isHistogram) {
        value = point.value?.avg || 0;
      } else if (typeof point.value === 'number') {
        value = point.value;
      } else if (point.value) {
        value = point.value.avg || 0;
      }
      return { value };
    });
  };

  const succeededTotal = calculateTotal(succeededData);
  const failedTotal = calculateTotal(failedData);
  const avgExecutionTime = calculateAverage(executionTimeData);

  const successRate = succeededTotal + failedTotal > 0
    ? ((succeededTotal / (succeededTotal + failedTotal)) * 100).toFixed(1)
    : '0.0';

  return (
    <div className="card mb-3">
      <div className="card-header d-flex justify-content-between align-items-center">
        <h6 className="mb-0">
          <Link to={`/queues/${queueName}`} className="text-decoration-none">
            {queueName}
          </Link>
        </h6>
      </div>
      <div className="card-body">
        <div className="row g-3">
          <div className="col-6 col-md-3">
            <div className="text-uppercase text-secondary mb-1" style={{ fontSize: '0.65rem', letterSpacing: '0.05em' }}>
              Success Rate
            </div>
            <div className="h5 mb-0">{successRate}%</div>
          </div>
          <div className="col-6 col-md-3">
            <div className="text-uppercase text-secondary mb-1" style={{ fontSize: '0.65rem', letterSpacing: '0.05em' }}>
              Succeeded
            </div>
            <div className="h5 mb-0 text-success">{formatNumber(succeededTotal)}</div>
            <div style={{ height: '30px', marginTop: '4px' }}>
              <MiniSparkline data={getSparklineData(succeededData)} color="#10b981" height={30} />
            </div>
          </div>
          <div className="col-6 col-md-3">
            <div className="text-uppercase text-secondary mb-1" style={{ fontSize: '0.65rem', letterSpacing: '0.05em' }}>
              Failed
            </div>
            <div className="h5 mb-0 text-danger">{formatNumber(failedTotal)}</div>
            <div style={{ height: '30px', marginTop: '4px' }}>
              <MiniSparkline data={getSparklineData(failedData)} color="#ef4444" height={30} />
            </div>
          </div>
          <div className="col-6 col-md-3">
            <div className="text-uppercase text-secondary mb-1" style={{ fontSize: '0.65rem', letterSpacing: '0.05em' }}>
              Avg Time
            </div>
            <div className="h5 mb-0">{formatTime(avgExecutionTime)}</div>
            <div style={{ height: '30px', marginTop: '4px' }}>
              <MiniSparkline data={getSparklineData(executionTimeData, true)} color="#3b82f6" height={30} />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
