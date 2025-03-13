import { useState } from 'react';
import { useServerConfiguration } from '../hooks/useServerConfiguration';
import { Loading } from '../components/Loading';
import { useMetricsOverview, useMetricDetail } from '../hooks/useMetrics';
import { Link, useParams } from 'react-router-dom';
import { MetricChart, ResolutionSelector } from '../components/MetricCharts';

// Common loading and error handling wrapper
const MetricsWrapper = ({ 
  isLoading, 
  data,
  errorMessage,
  children
}: { 
  isLoading: boolean;
  data: unknown;
  errorMessage: string;
  children: React.ReactNode;
}) => {
  if (isLoading) {
    return <Loading />;
  }

  if (!data) {
    return <div className="alert alert-info">{errorMessage}</div>;
  }

  return <>{children}</>;
};

export function MetricsList() {
  const { url } = useServerConfiguration();
  const { data: overview, isLoading } = useMetricsOverview({ url });

  return (
    <MetricsWrapper
      isLoading={isLoading}
      data={overview}
      errorMessage="No metrics data available. Make sure the Metrics plugin is enabled."
    >
      <div className="container-fluid">
        <h2 className="mb-4">Available Metrics</h2>
        
        {overview && overview.types && Object.entries(overview.types).length > 0 ? (
          Object.entries(overview.types)
            .sort(([typeA], [typeB]) => typeA.localeCompare(typeB))
            .map(([type, metrics]) => (
              <div className="card mb-4" key={type}>
                <div className="card-header">
                  <h5 className="mb-0">{type.charAt(0).toUpperCase() + type.slice(1)} Metrics</h5>
                </div>
                <div className="card-body p-0">
                  <div className="list-group list-group-flush">
                    {metrics
                      .sort((a, b) => a.localeCompare(b))
                      .map(metric => {
                        const metricKey = `${type}:${metric}`;
                        return (
                          <Link 
                            key={metricKey} 
                            to={`/metrics/${encodeURIComponent(metricKey)}`}
                            className="list-group-item list-group-item-action d-flex justify-content-between align-items-center"
                          >
                            <span>
                              <span className="text-muted">{type}:</span>
                              <strong className="ms-1">{metric}</strong>
                            </span>
                            <i className="bi bi-graph-up text-muted"></i>
                          </Link>
                        );
                      })
                    }
                  </div>
                </div>
              </div>
            ))
        ) : (
          <div className="alert alert-info mt-4">
            No metrics available. Make sure the Metrics plugin is enabled and jobs have been processed.
          </div>
        )}
      </div>
    </MetricsWrapper>
  );
}

export function MetricDetail() {
  const { url } = useServerConfiguration();
  const { metricKey } = useParams<{ metricKey: string }>();
  const [resolution, setResolution] = useState<string>('5min');
  
  // Split the metric key into parts to use with useMetricDetail hook
  const getParts = () => {
    if (!metricKey) return { type: '', name: '' };
    const parts = metricKey.split(':');
    if (parts.length < 2) return { type: parts[0] || '', name: '' };
    return { type: parts[0], name: parts[1] };
  };
  
  const { type, name } = getParts();
  
  // Get aggregated metrics across all workers
  const { data: metrics, isLoading } = useMetricDetail({ 
    url, 
    type,
    name,
    resolution
  });

  return (
    <MetricsWrapper
      isLoading={isLoading}
      data={metrics}
      errorMessage={`No metrics data available for ${metricKey}`}
    >
      <div className="container-fluid">
        <h2 className="mb-4">
          {metricKey}
        </h2>
        
        <ResolutionSelector resolution={resolution} setResolution={setResolution} />

        <div className="row">
          {metrics && Object.entries(metrics).map(([subtype, points]) => {
            const isMetricHistogram = points.length > 0 && 
              typeof points[0].value === 'object' && 
              'avg' in points[0].value;
            
            // Format subtitle label
            const subtitleLabel = subtype === 'default'
              ? `${metricKey}` 
              : `${metricKey}:${subtype}`;
            
            return (
              <div key={subtype} className="col-12 mb-4">
                <div className="card">
                  <div className="card-header">
                    <h5 className="mb-0">{subtitleLabel}</h5>
                  </div>
                  <div className="card-body">
                    <MetricChart 
                      points={points} 
                      isHistogram={isMetricHistogram} 
                    />
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </MetricsWrapper>
  );
}

export function Metrics() {
  return <MetricsList />;
}