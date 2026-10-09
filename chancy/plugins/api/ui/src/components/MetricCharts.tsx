import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import type { MetricData, MetricDetail } from '../services/schemas';
import { defaultMetricRange, formatMetricTimestamp, metricStatus, metricTimeline, RETENTION_SECONDS } from '../services/metrics';
import { useMetricDetail } from '../hooks/useMetrics';
import { Loading } from './Loading';
import { Link } from 'react-router';
import { MiniSparkline } from './dashboard/MiniSparkline';

export const ResolutionSelector = ({ resolution, setResolution, range }: {
  resolution: string; setResolution: (res: string) => void; range?: number;
}) => <div className="btn-group mb-3" role="group" aria-label="Metric resolution">
  {['1min', '5min', '1hour', '1day'].map(res => <button key={res}
    disabled={range !== undefined && (range > RETENTION_SECONDS[res] || (res === '1day' && range < 86400))}
    className={`btn btn-sm ${resolution === res ? 'btn-primary' : 'btn-outline-primary'}`}
    onClick={() => setResolution(res)}>{res}</button>)}
</div>;

export function SparklineChart({ window, metric, height = 30, width = 80 }: {
  window: MetricDetail; metric: MetricData | undefined; height?: number; width?: number;
}) {
  if (!metric?.data.length) return <span>—</span>;
  return <div style={{ width }}><MiniSparkline data={metricTimeline(window, metric)} height={height} /></div>;
}

export function MetricChart({ window, metric, height = 400 }: {
  window: MetricDetail; metric: MetricData; height?: number;
}) {
  if (!metric.data.length) return <div className="alert alert-info">No observations in this range.</div>;
  const stats = metric.type === 'histogram' ? ['avg', 'min', 'max'] : ['value'];
  const colors = ['#8884d8', '#82ca9d', '#ffc658'];
  return <>
    <p className="small text-secondary">{metric.unit} · {metric.aggregation} · {metricStatus(window, metric, false)}</p>
    <ResponsiveContainer width="100%" height={height}>
      <LineChart data={metricTimeline(window, metric)} margin={{ top: 10, right: 30, left: 5, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="timestamp" type="number" domain={['dataMin', 'dataMax']}
          tickFormatter={value => formatMetricTimestamp(value, window.resolution)} />
        <YAxis tickFormatter={value => new Intl.NumberFormat(undefined, { notation: 'compact' }).format(value)} />
        <Tooltip labelFormatter={value => `${new Date(Number(value)).toISOString()} (UTC)`} />
        <Legend />
        {stats.map((stat, index) => <Line key={stat} dataKey={stat} name={`${stat} (${metric.unit})`}
          stroke={colors[index]} connectNulls={false} dot={{ r: 2 }} isAnimationActive={false} />)}
      </LineChart>
    </ResponsiveContainer>
  </>;
}

export function QueueMetrics({ apiUrl, queueName, resolution, workerId }: {
  apiUrl: string | null; queueName: string; resolution: string; workerId?: string;
}) {
  const prefix = `queue:${queueName}`;
  const { data, isLoading, isError } = useMetricDetail({ url: apiUrl, key: prefix, resolution,
    range: defaultMetricRange(resolution), worker_id: workerId });
  return <div className="mb-4">
    <div className="d-flex mb-3 align-items-center">
      <h4 className="flex-grow-1">{queueName}</h4>
      <Link to={`/queues/${encodeURIComponent(queueName)}`} className="btn btn-sm btn-outline-primary">View Queue Details</Link>
    </div>
    {isLoading ? <Loading /> : isError ? <p role="alert">Could not load metrics.</p> : <div className="row g-4">
      {([['throughput', 'Job update events'], ['execution_time', 'Final attempt time']] as const).map(([key, title]) => {
        const metric = data?.series[`${prefix}:${key}`];
        return <div className="col-12 col-lg-6" key={key}><div className="card">
          <div className="card-header">{title}</div><div className="card-body">
            {data && metric ? <MetricChart window={data} metric={metric} height={200} /> : 'No observations in this range.'}
          </div></div></div>;
      })}
    </div>}
  </div>;
}
