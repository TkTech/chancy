import { 
  LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer, AreaChart, Area
} from 'recharts';
import { MetricPoint } from '../hooks/useMetrics';

// Function to format timestamps
export const formatTimestamp = (timestamp: string) => {
  const date = new Date(timestamp);
  return `${date.getHours().toString().padStart(2, '0')}:${date.getMinutes().toString().padStart(2, '0')}`;
};

// Function to format a value that might be complex
export const formatValue = (value: number | Record<string, number>): number => {
  if (typeof value === 'number') {
    return value;
  }
  
  if (typeof value === 'object') {
    // For counters, just return the value
    if ('count' in value) {
      return value.count;
    }
    
    // For histograms, return the average
    if ('avg' in value) {
      return value.avg;
    }
  }
  
  return 0;
};

// Reusable component for resolution selector
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

// Custom tooltip style settings
export const tooltipStyles = {
  wrapperStyle: {
    backgroundColor: "#1a1a1a",
    border: "1px solid #2d2d2d",
  },
  contentStyle: {
    backgroundColor: 'transparent',
    border: "none",
  },
  labelFormatter: (time: string, items: Array<{payload?: {rawTimestamp?: string}}>) => {
    const item = items?.[0];
    if (item?.payload?.rawTimestamp) {
      return new Date(item.payload.rawTimestamp).toLocaleString();
    }
    return time;
  }
};

// Common chart components
export const MetricChart = ({ 
  points, 
  isHistogram = false, 
  height = 400,
  minTickGap = 30
}: { 
  points: MetricPoint[];
  isHistogram?: boolean;
  height?: number;
  minTickGap?: number;
}) => {
  if (!points || points.length === 0) {
    return (
      <div className="alert alert-info">
        No data available for this time range.
      </div>
    );
  }

  // Check if this specific metric is a histogram
  const isMetricHistogram = isHistogram || (
    points.length > 0 && 
    typeof points[0].value === 'object' && 
    'avg' in points[0].value
  );
  
  // Set stats to display for histogram
  const metricHistogramStats = isMetricHistogram ? ['avg', 'count', 'min', 'max'] : [];
  const colors = ['#8884d8', '#82ca9d', '#ffc658', '#ff8042', '#00C49F'];

  if (isMetricHistogram) {
    return (
      <ResponsiveContainer width="100%" height={height}>
        <LineChart
          data={points.map(point => {
            const obj: Record<string, string | number> = {
              time: formatTimestamp(point.timestamp),
              rawTimestamp: point.timestamp
            };
            
            // Add all values from the histogram
            for (const [key, val] of Object.entries(point.value as Record<string, number>)) {
              obj[key] = val;
            }
            
            return obj;
          }).reverse()}
          margin={{ top: 10, right: 30, left: 0, bottom: 0 }}
        >
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis 
            dataKey="time" 
            interval="preserveStartEnd"
            minTickGap={minTickGap}
          />
          <YAxis />
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
  
  return (
    <ResponsiveContainer width="100%" height={height}>
      <AreaChart
        data={points.map(point => ({
          time: formatTimestamp(point.timestamp),
          value: formatValue(point.value),
          rawTimestamp: point.timestamp
        })).reverse()}
        margin={{ top: 10, right: 30, left: 0, bottom: 0 }}
      >
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis 
          dataKey="time" 
          interval="preserveStartEnd"
          minTickGap={minTickGap}
        />
        <YAxis />
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