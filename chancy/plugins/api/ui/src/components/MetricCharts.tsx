import {
  LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer, AreaChart, Area
} from 'recharts';
import { MetricPoint, useMetricDetail, MetricType } from '../hooks/useMetrics';
import { Loading } from './Loading';
import { Link } from 'react-router-dom';
import { useSlidePanels } from './SlidePanelContext';
import { Queue } from '../pages/Queues';

const formatTimestamp = (timestamp: string) => {
  const date = new Date(timestamp);
  return `${date.getUTCHours().toString().padStart(2, '0')}:${date.getUTCMinutes().toString().padStart(2, '0')}`;
};

const formatValue = (value: number | Record<string, number>): number => {
  if (typeof value === 'number') {
    return value;
  }
  
  if (typeof value === 'object') {
    if ('avg' in value) {
      return value.avg;
    }
    
    if ('count' in value) {
      return value.count;
    }
  }
  
  return 0;
};

/**
 * Format a number with appropriate units for display on the Y-axis
 * This helps with large values (like table sizes in bytes) to be displayed more compactly
 */
const formatYAxisTick = (value: number): string => {
  if (value >= 1_000_000_000) {
    return `${(value / 1_000_000_000).toFixed(1)}G`;
  } else if (value >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(1)}M`;
  } else if (value >= 1_000) {
    return `${(value / 1_000).toFixed(1)}K`;
  } else if (Math.floor(value) === value) {
    return value.toString();
  } else {
    return value.toFixed(1);
  }
};

export const ResolutionSelector = ({ resolution, setResolution }: {
  resolution: string;
  setResolution: (res: string) => void;
}) => (
  <div className="d-flex mb-4">
    <div className="btn-group" role="group">
      {['1min', '5min', '1hour', '1day'].map(res => (
        <button 
          key={res}
          className={`btn btn-sm ${resolution === res ? 'btn-primary' : 'btn-outline-primary'}`}
          onClick={() => setResolution(res)}
        >
          {res === '1min' ? '1 Min' : 
           res === '5min' ? '5 Min' : 
           res === '1hour' ? '1 Hour' : '1 Day'}
        </button>
      ))}
    </div>
  </div>
);


const tooltipStyles = {
  wrapperStyle: {
    backgroundColor: "#1a1a1a",
    border: "1px solid #2d2d2d",
  },
  contentStyle: {
    backgroundColor: 'transparent',
    border: "none",
  },
};

const generateTimePoints = (resolution: string, count: number) => {
  const now = new Date();
  const timePoints: Date[] = [];
  
  now.setMilliseconds(0);
  now.setSeconds(0);
  
  let interval: number;
  switch (resolution) {
    case '1min':
      interval = 60 * 1000;
      now.setUTCMinutes(now.getUTCMinutes(), 0, 0);
      break;
    case '5min':
      interval = 5 * 60 * 1000;
      now.setUTCMinutes(Math.floor(now.getUTCMinutes() / 5) * 5, 0, 0);
      break;
    case '1hour':
      interval = 60 * 60 * 1000;
      now.setUTCMinutes(0, 0, 0);
      break;
    case '1day':
      interval = 24 * 60 * 60 * 1000;
      now.setUTCHours(0, 0, 0, 0); // Round to the current day at midnight
      // For 1day resolution, we want to ensure we're at midnight
      now.setUTCHours(0, 0, 0, 0);
      break;
    default:
      interval = 5 * 60 * 1000;
      now.setUTCMinutes(Math.floor(now.getUTCMinutes() / 5) * 5, 0, 0);
  }
  
  for (let i = 0; i < count; i++) {
    const timePoint = new Date(now.getTime() - (interval * i));
    timePoints.unshift(timePoint);
  }
  
  return timePoints;
};

// Sparkline-style chart for compact display in tables
export const SparklineChart = ({ 
  points, 
  height = 30,
  width = 80,
  resolution = '5min'
}: { 
  points: MetricPoint[];
  height?: number;
  width?: number;
  resolution?: string;
}) => {
  if (!points || points.length === 0) {
    return <div style={{ height, width }} className="text-center">-</div>;
  }
  
  const timePoints = generateTimePoints(resolution, 20);
  
  const dataPointsMap = new Map();
  points.forEach(point => {
    dataPointsMap.set(new Date(point.timestamp).getTime(), point);
  });
  
  const completeData = timePoints.map(timePoint => {
    const timestamp = timePoint.getTime();
    const existingPoint = dataPointsMap.get(timestamp);
    
    return {
      value: existingPoint ? formatValue(existingPoint.value) : 0
    };
  });

  return (
    <ResponsiveContainer width={width} height={height}>
      <LineChart data={completeData} margin={{ top: 0, right: 0, bottom: 0, left: 0 }}>
        <Line 
          type="monotone" 
          dataKey="value" 
          stroke="#8884d8" 
          strokeWidth={1.5}
          dot={false}
          isAnimationActive={false}
        />
      </LineChart>
    </ResponsiveContainer>
  );
};

const createCompleteDataset = (
  timePoints: Date[], 
  dataPoints: MetricPoint[], 
  isHistogram: boolean, 
  histogramStats: string[] = []
) => {
  const dataPointsMap = new Map();
  dataPoints.forEach(point => {
    dataPointsMap.set(new Date(point.timestamp).getTime(), point);
  });
  
  return timePoints.map(timePoint => {
    const timestamp = timePoint.getTime();
    const existingPoint = dataPointsMap.get(timestamp);
    
    if (existingPoint) {
      if (isHistogram) {
        const obj: Record<string, string | number> = {
          time: formatTimestamp(existingPoint.timestamp),
          rawTimestamp: existingPoint.timestamp
        };
        
        for (const [key, val] of Object.entries(existingPoint.value as Record<string, number>)) {
          obj[key] = val;
        }
        
        return obj;
      } else {
        return {
          time: formatTimestamp(existingPoint.timestamp),
          value: formatValue(existingPoint.value),
          rawTimestamp: existingPoint.timestamp
        };
      }
    } else {
      const iso = timePoint.toISOString();
      
      if (isHistogram) {
        const obj: Record<string, string | number> = {
          time: formatTimestamp(iso),
          rawTimestamp: iso
        };
        
        histogramStats.forEach(stat => {
          obj[stat] = 0;
        });
        
        return obj;
      } else {
        return {
          time: formatTimestamp(iso),
          value: 0,
          rawTimestamp: iso
        };
      }
    }
  });
};

export const MetricChart = ({
  points, 
  metricType,
  height = 400,
  resolution = '5min'
}: { 
  points: MetricPoint[];
  metricType: MetricType;
  height?: number;
  minTickGap?: number;
  resolution?: string;
}) => {
  if (!points || points.length === 0) {
    return (
      <div className="alert alert-info">
        No data available for this time range.
      </div>
    );
  }

  const isMetricHistogram = metricType === 'histogram';
  
  const metricHistogramStats = isMetricHistogram ? ['avg', 'min', 'max'] : [];
  const colors = ['#8884d8', '#82ca9d', '#ffc658', '#ff8042', '#00C49F'];
  
  const timePointCount = {
    '1min': 60,
    '5min': 60,
    '1hour': 24,
    '1day': 30
  }[resolution] || 60;
  
  const timePoints = generateTimePoints(resolution, timePointCount);
  
  if (isMetricHistogram) {
    const completeData = createCompleteDataset(timePoints, points, true, metricHistogramStats);
    
    return (
      <ResponsiveContainer width="100%" height={height}>
        <LineChart
          data={completeData}
          margin={{ top: 10, right: 30, left: 5, bottom: 0 }}
        >
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis 
            dataKey="time" 
            interval={resolution === '1day' ? 'preserveEnd' : 'preserveStartEnd'}
          />
          <YAxis tickFormatter={formatYAxisTick} />
          <Tooltip {...tooltipStyles} />
          <Legend />
          {metricHistogramStats.map((stat, idx) => (
            <Line 
              key={stat}
              type="monotone"
              dataKey={stat}
              name={stat}
              stroke={colors[idx % colors.length]}
              dot={false}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    );
  }
  
  const completeData = createCompleteDataset(timePoints, points, false);
  
  return (
    <ResponsiveContainer width="100%" height={height}>
      <AreaChart
        data={completeData}
        margin={{ top: 10, right: 30, left: 5, bottom: 0 }}
      >
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis 
          dataKey="time" 
          interval={resolution === '1day' ? 'preserveEnd' : 'preserveStartEnd'}
        />
        <YAxis tickFormatter={formatYAxisTick} />
        <Tooltip {...tooltipStyles} />
        <Area 
          type="monotone" 
          dataKey="value" 
          stroke="#8884d8" 
          fill="#8884d8"
          fillOpacity={0.3}
        />
      </AreaChart>
    </ResponsiveContainer>
  );
};

export function QueueMetrics({
  apiUrl, 
  queueName, 
  resolution, 
  workerId 
}: {
  apiUrl: string | null;
  queueName: string;
  resolution: string;
  workerId?: string;
}) {
  const throughputKey = `queue:${queueName}:throughput`;
  const { data: throughputData, isLoading: throughputLoading } = useMetricDetail({
    url: apiUrl,
    key: throughputKey,
    resolution,
    enabled: !!apiUrl,
    worker_id: workerId,
  });

  const executionTimeKey = `queue:${queueName}:execution_time`;
  const { data: executionTimeData, isLoading: executionTimeLoading } = useMetricDetail({
    url: apiUrl,
    key: executionTimeKey,
    resolution,
    enabled: !!apiUrl,
    worker_id: workerId,
  });
  
  const hasThroughputData = throughputData && throughputData[throughputKey]?.data;
  const hasExecutionTimeData = executionTimeData && executionTimeData[executionTimeKey]?.data;
  const { openPanel } = useSlidePanels();
  
  const handleQueueClick = (queueName: string, e: React.MouseEvent) => {
    e.preventDefault();
    openPanel({
      title: "Queue Details",
      content: <Queue queueName={queueName} inPanel={true} />
    });
  };
  
  return (
    <div className={"mb-4"}>
      <div className={"d-flex mb-3 flex-wrap align-items-center"}>
        <h4 className="flex-grow-1 mb-0">
          <Link to={`/queues/${queueName}`} onClick={(e) => handleQueueClick(queueName, e)}>{queueName}</Link>
        </h4>
      </div>
      
      <div className="row row-cols-4 row-cols-lg-1 g-4">
        <div className="col">
          <div className="card h-100">
            <div className="card-header">
              <h5 className="mb-0">Throughput</h5>
            </div>
            <div className="card-body">
              {throughputLoading ? (
                <Loading />
              ) : !hasThroughputData ? (
                <div className="alert alert-secondary">No throughput data available</div>
              ) : (
                <MetricChart 
                  points={throughputData[throughputKey].data}
                  metricType={throughputData[throughputKey].type}
                  height={200}
                  resolution={resolution}
                />
              )}
            </div>
          </div>
        </div>
        <div className="col">
          <div className="card h-100">
            <div className="card-header">
              <h5 className="mb-0">Execution Time</h5>
            </div>
            <div className="card-body">
              {executionTimeLoading ? (
                <Loading />
              ) : !hasExecutionTimeData ? (
                <div className="alert alert-secondary">No execution time data available</div>
              ) : (
                <MetricChart 
                  points={executionTimeData[executionTimeKey].data}
                  metricType={executionTimeData[executionTimeKey].type}
                  height={200}
                  resolution={resolution}
                />
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}