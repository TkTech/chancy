import React, { useState } from 'react';
import { useServerConfiguration } from '../hooks/useServerConfiguration';
import { Loading } from '../components/Loading';
import { useMetricsOverview, useMetricDetail } from '../hooks/useMetrics';
import { Link, useParams } from 'react-router-dom';
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
        <p>All raw metrics currently known, synchronized each minute across all workers.</p>
        
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
                        const metricKey = `${category}:${metric}`;

                        return (
                          <Link 
                            key={metricKey} 
                            to={`/metrics/${encodeURIComponent(metricKey)}`}
                            className="list-group-item list-group-item-action align-items-center"
                          >
                            <span className="text-muted">{category}:</span>
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

  const { data: metrics, isLoading } = useMetricDetail({
    url,
    key: metricKey as string,
    resolution
  });

  return (
    <MetricsWrapper
      isLoading={isLoading}
      data={metrics}
      errorMessage={`No metrics data available for ${metricKey}`}
    >
      <div className="container-fluid">
        <h2 className="mb-4 text-break">
          {metricKey}
        </h2>
        
        <ResolutionSelector resolution={resolution} setResolution={setResolution} />

        <div className="row">
          {metrics && Object.entries(metrics).map(([subtype, metricData]) => {
            return (
              <div key={subtype} className="col-12 mb-4">
                <div className="card">
                  <div className="card-header">
                    <h5 className="mb-0">{subtype}</h5>
                  </div>
                  <div className="card-body">
                    <MetricChart 
                      points={metricData.data} 
                      metricType={metricData.type}
                      resolution={resolution}
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