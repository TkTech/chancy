import React from 'react';
import { useServerConfiguration } from '../hooks/useServerConfiguration';
import { useMetricDetail, MetricPoint } from '../hooks/useMetrics';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

const STATE_COLORS = {
  running: '#3b82f6',    // blue
  succeeded: '#10b981',  // green
  failed: '#ef4444',     // red
  retrying: '#8b5cf6',   // purple
};

const STATE_ORDER = ['running', 'retrying', 'failed', 'succeeded'];

const formatTimestamp = (timestamp: string) => {
  const date = new Date(timestamp);
  return `${date.getUTCHours().toString().padStart(2, '0')}:${date.getUTCMinutes().toString().padStart(2, '0')}`;
};

const tooltipStyles = {
  wrapperStyle: {
    backgroundColor: "var(--bs-card-bg)",
    border: "1px solid var(--bs-border-color)",
    borderRadius: "0.375rem",
    boxShadow: "0 2px 8px rgba(0, 0, 0, 0.15)",
  },
  contentStyle: {
    backgroundColor: 'transparent',
    border: "none",
    color: "var(--bs-body-color)",
  },
};

export function JobStateBarGraph() {
  const { url } = useServerConfiguration();
  const resolution = '1min';
  const limit = 60;

  // Fetch metrics for each state (excluding pending as they haven't been processed yet)
  const { data: runningData } = useMetricDetail({
    url,
    key: 'global:status:running',
    resolution,
    limit,
  });

  const { data: succeededData } = useMetricDetail({
    url,
    key: 'global:status:succeeded',
    resolution,
    limit,
  });

  const { data: failedData } = useMetricDetail({
    url,
    key: 'global:status:failed',
    resolution,
    limit,
  });

  const { data: retryingData } = useMetricDetail({
    url,
    key: 'global:status:retrying',
    resolution,
    limit,
  });

  // Combine all data points by timestamp
  const combinedData = React.useMemo(() => {
    // Generate time points for the last hour from NOW, aligned to UTC minute boundaries
    const now = new Date();
    now.setUTCSeconds(0, 0); // Round to the UTC minute
    const timePoints: Date[] = [];
    const interval = 60 * 1000; // 1 minute in milliseconds

    for (let i = 0; i < limit; i++) {
      const timePoint = new Date(now.getTime() - (interval * i));
      timePoints.unshift(timePoint);
    }

    // Create a map of data points by timestamp
    const dataMap = new Map<number, any>();

    const addDataPoints = (data: any, state: string) => {
      if (!data) return;
      const key = `global:status:${state}`;
      const metricData = data[key];
      if (!metricData?.data) return;

      metricData.data.forEach((point: MetricPoint) => {
        const timestamp = new Date(point.timestamp).getTime();
        if (!dataMap.has(timestamp)) {
          dataMap.set(timestamp, {});
        }
        const entry = dataMap.get(timestamp);
        entry[state] = typeof point.value === 'number' ? point.value : 0;
      });
    };

    addDataPoints(runningData, 'running');
    addDataPoints(succeededData, 'succeeded');
    addDataPoints(failedData, 'failed');
    addDataPoints(retryingData, 'retrying');

    // Map time points to data, filling with 0 where no data exists
    return timePoints.map(timePoint => {
      const timestamp = timePoint.getTime();
      const data = dataMap.get(timestamp) || {};

      return {
        time: formatTimestamp(timePoint.toISOString()),
        running: data.running || 0,
        retrying: data.retrying || 0,
        failed: data.failed || 0,
        succeeded: data.succeeded || 0,
      };
    });
  }, [runningData, succeededData, failedData, retryingData, limit]);

  if (combinedData.length === 0) {
    return null;
  }

  return (
    <div className="job-state-histogram mb-3">
      <ResponsiveContainer width="100%" height={160}>
        <AreaChart
          data={combinedData}
          margin={{ top: 5, right: 5, left: 5, bottom: 5 }}
        >
          <CartesianGrid strokeDasharray="3 3" stroke="var(--bs-border-color)" />
          <XAxis
            dataKey="time"
            interval={4}
            tick={{ fill: 'var(--bs-secondary-color)', fontSize: 12 }}
          />
          <YAxis
            tick={{ fill: 'var(--bs-secondary-color)', fontSize: 12 }}
          />
          <Tooltip {...tooltipStyles} cursor={{ fill: 'var(--bs-secondary-bg)' }} />
          <Legend
            wrapperStyle={{ fontSize: '12px' }}
            iconType="square"
          />
          {STATE_ORDER.map(state => (
            <Area
              key={state}
              type="monotone"
              dataKey={state}
              stackId="1"
              stroke={STATE_COLORS[state as keyof typeof STATE_COLORS]}
              fill={STATE_COLORS[state as keyof typeof STATE_COLORS]}
              fillOpacity={0.8}
            />
          ))}
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
