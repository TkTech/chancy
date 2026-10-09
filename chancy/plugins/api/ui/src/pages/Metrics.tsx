import React, { useState } from 'react';
import { RETENTION_SECONDS } from '../services/metrics';
import { useServerConfiguration } from '../hooks/useServerConfiguration';
import { Loading } from '../components/Loading';
import { useMetricsOverview, useMetricDetail } from '../hooks/useMetrics';
import { Link, useParams } from 'react-router';
import { MetricChart, ResolutionSelector } from '../components/MetricCharts';

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
        <h2 className="">Available Metrics</h2>
        <p>Persisted observations, usually flushed once a minute. Missing samples are unknown, not zero.</p>
        
        {overview && overview.categories && Object.entries(overview.categories).length > 0 ? (
          Object.entries(overview.categories)
            .sort(([categoryA], [categoryB]) => categoryA.localeCompare(categoryB))
            .map(([category, metrics]) => (
              <div className="card mb-4" key={category}>
                <div className="card-header">
                  <h5 className="mb-0">{category.charAt(0).toUpperCase() + category.slice(1)} Metrics</h5>
                </div>
                <div className="card-body p-0">
                  <div className="list-group list-group-flush">
                    {metrics
                      .sort((a, b) => a.localeCompare(b))
                      .map(metric => {
                        const metricKey = metric ? `${category}:${metric}` : category;

                        return (
                          <Link 
                            key={metricKey} 
                            to={`/metrics/${encodeURIComponent(metricKey)}`}
                            className="list-group-item list-group-item-action align-items-center"
                          >
                            <span className="text-muted">{category}{metric ? ':' : ''}</span>
                            <strong>{metric}</strong>
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
  const [range, setRange] = useState(86400);

  const { data: metrics, isLoading, error } = useMetricDetail({
    url,
    key: metricKey as string,
    resolution, range
  });

  return (
    <MetricsWrapper
      isLoading={isLoading}
      data={metrics}
      errorMessage={error?.message ?? `No metrics data available for ${metricKey}`}
    >
      <div className="container-fluid">
        <h2 className="mb-4 text-break">
          {metricKey}
        </h2>
        
        <div className="d-flex flex-wrap gap-3 align-items-start">
          <label>Range <select className="form-select form-select-sm" value={range} onChange={event => {
            const next = Number(event.target.value);
            setRange(next);
            if (RETENTION_SECONDS[resolution] < next || (resolution === '1day' && next < 86400))
              setResolution(next <= 3600 ? '1min' : next <= 86400 ? '5min' : next <= 604800 ? '1hour' : '1day');
          }}>
            <option value={3600}>Last hour</option><option value={86400}>Last 24 hours</option>
            <option value={604800}>Last 7 days</option><option value={2592000}>Last 30 days</option>
          </select></label>
          <div><div>Resolution</div><ResolutionSelector resolution={resolution} setResolution={setResolution} range={range} /></div>
        </div>
        {metrics && <p className="small text-secondary">{new Date(metrics.start).toLocaleString()} – {new Date(metrics.end).toLocaleString()} · Current bucket may be partial · Gaps are unobserved</p>}
        {error && metrics && <p role="alert">Could not refresh metrics: {error.message}</p>}

        <div className="row">
          {metrics && Object.entries(metrics.series).map(([subtype, metricData]) => {
            return (
              <div key={subtype} className="col-12 mb-4">
                <div className="card">
                  <div className="card-header">
                    <h5 className="mb-0">{subtype}</h5>
                  </div>
                  <div className="card-body">
                    <MetricChart 
                      window={metrics} metric={metricData}
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