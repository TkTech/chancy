import { useServerConfiguration } from '../hooks/useServerConfiguration';
import { useMetricDetail } from '../hooks/useMetrics';
import { metricTimeline, formatMetricTimestamp } from '../services/metrics';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

const STATES = { retrying: '#8b5cf6', failed: '#ef4444', succeeded: '#10b981' };

export function JobStateBarGraph() {
  const { url } = useServerConfiguration();
  const { data, isError, isLoading } = useMetricDetail({ url, key: 'global:status', resolution: '1min', range: 3600 });
  if (isError) return <p role="alert">Could not load job update metrics.</p>;
  if (!data) return <p>{isLoading ? 'Loading metrics…' : 'No observations.'}</p>;
  const timelines = Object.fromEntries(Object.keys(STATES).map(state => [state,
    metricTimeline(data, data.series[`global:status:${state}`])]));
  const points = timelines.succeeded.map((point, index) => ({ timestamp: point.timestamp,
    ...Object.fromEntries(Object.keys(STATES).map(state => [state, timelines[state][index].value])) }));
  return <div className="mb-3">
    <p className="small text-secondary">Job update events · Last hour · Gaps indicate no observations</p>
    <ResponsiveContainer width="100%" height={160}>
      <LineChart data={points}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="timestamp" type="number" domain={['dataMin', 'dataMax']}
          tickFormatter={value => formatMetricTimestamp(value, '1min')} />
        <YAxis /><Tooltip labelFormatter={value => `${new Date(Number(value)).toISOString()} (UTC)`} /><Legend />
        {Object.entries(STATES).map(([state, color]) => <Line key={state} dataKey={state}
          stroke={color} connectNulls={false} dot={{ r: 2 }} isAnimationActive={false} />)}
      </LineChart>
    </ResponsiveContainer>
  </div>;
}
