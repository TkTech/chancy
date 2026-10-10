import { DetailCard } from '../components/common/DetailCard';
import { useQuery } from '@tanstack/react-query';
import { useServerConfiguration } from '../hooks/useServerConfiguration';
import { ChancyApi } from '../services/chancy';
import { queryKeys } from '../services/queryKeys';
import { Loading } from '../components/Loading';
import { PageHeader } from '../components/common/PageHeader';
import { MetricTableSizeCard } from '../components/dashboard/MetricTableSizeCard';

const CORE_TABLES = ['jobs', 'queue_rate_limits', 'queues', 'workers'];

const SPARKLINE_COLORS = [
  '#8b5cf6', '#f97316', '#06b6d4', '#ec4899', '#10b981', '#f59e0b', '#3b82f6', '#ef4444'
];

export function System() {
  const { url } = useServerConfiguration();
  const resolution = '5min';

  const { data: systemInfo, isLoading: systemLoading } = useQuery({
    queryKey: queryKeys.system(url),
    queryFn: ({ signal }) => ChancyApi(url!).getSystem(signal),
    enabled: !!url,
  });

  const { data: plugins, isLoading: pluginsLoading } = useQuery({
    queryKey: queryKeys.plugins(url),
    queryFn: ({ signal }) => ChancyApi(url!).listPlugins(signal),
    enabled: !!url,
  });

  if (systemLoading || pluginsLoading || !systemInfo || !plugins) {
    return <Loading />;
  }

  return (
    <div className="container-fluid">
      <PageHeader
        title="System Information"
        description="Internal system information."
      />

      <div className="row">
        <div className="col-md-6">
          <DetailCard title="Version Information" flush>
            <table className="table table-hover mb-0">
              <tbody>
                <tr>
                  <th className="text-nowrap">Chancy Version</th>
                  <td className="font-monospace">{systemInfo.chancy_version}</td>
                </tr>
              </tbody>
            </table>
          </DetailCard>
        </div>

        <div className="col-md-6">
          <DetailCard title="Database" flush>
            <table className="table table-hover mb-0">
              <tbody>
                <tr>
                  <th className="text-nowrap">Version</th>
                  <td className="font-monospace">{systemInfo.database.version}</td>
                </tr>
                <tr>
                  <th className="text-nowrap">Prefix</th>
                  <td className="font-monospace">{systemInfo.database.prefix}</td>
                </tr>
              </tbody>
            </table>
          </DetailCard>
        </div>

      </div>

      {/* Core Tables Section */}
      <h4 className="mb-3">Core Tables</h4>
      <div className="row g-3 mb-4">
        {CORE_TABLES.map((table, idx) => (
          <div key={table} className="col-12 col-md-6">
            <div className="row g-3">
              <div className="col-6">
                <MetricTableSizeCard
                  title={`${table} Table Size`}
                  tableName={table}
                  url={url!}
                  resolution={resolution}
                  selector="table_size_bytes"
                  sparklineColor={SPARKLINE_COLORS[idx * 2 % SPARKLINE_COLORS.length]}
                />
              </div>
              <div className="col-6">
                <MetricTableSizeCard
                  title={`${table} Index Size`}
                  tableName={table}
                  url={url!}
                  resolution={resolution}
                  selector="index_size_bytes"
                  sparklineColor={SPARKLINE_COLORS[(idx * 2 + 1) % SPARKLINE_COLORS.length]}
                />
              </div>
            </div>
          </div>
        ))}
      </div>

      {/* Plugins Section */}
      <h4 className="mb-3">Plugins</h4>
      {plugins.map((plugin, pluginIdx) => (
        <div key={pluginIdx} className="mb-4">
          <h5 className="mb-3">{plugin.identifier}</h5>

          <div className="row">
            <div className="col-md-6">
              <DetailCard title="Plugin Information" flush>
                <table className="table table-hover mb-0">
                  <tbody>
                    <tr>
                      <th className="text-nowrap">Scope</th>
                      <td className="font-monospace w-100">{plugin.scope}</td>
                    </tr>
                    {plugin.migrate_key && (
                      <tr>
                        <th className="text-nowrap">Migration Key</th>
                        <td className="font-monospace w-100">{plugin.migrate_key}</td>
                      </tr>
                    )}
                    {plugin.migrate_package && (
                      <tr>
                        <th className="text-nowrap">Migration Package</th>
                        <td className="font-monospace w-100">{plugin.migrate_package}</td>
                      </tr>
                    )}
                    {plugin.api_plugin && (
                      <tr>
                        <th className="text-nowrap">API Plugin</th>
                        <td className="font-monospace w-100">{plugin.api_plugin}</td>
                      </tr>
                    )}
                    {plugin.dependencies.length > 0 && (
                      <tr>
                        <th className="text-nowrap">Dependencies</th>
                        <td className="font-monospace w-100">{plugin.dependencies.join(', ')}</td>
                      </tr>
                    )}
                    {plugin.tables.length > 0 && (
                      <tr>
                        <th className="text-nowrap">Tables</th>
                        <td className="font-monospace w-100">{plugin.tables.join(', ')}</td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </DetailCard>
            </div>

            <div className="col-md-6">
              {plugin.tables.length > 0 && (
                <div className="row g-3">
                  {plugin.tables.map((table, tableIdx) => (
                    <div key={tableIdx} className="col-12">
                      <div className="row g-3">
                        <div className="col-6">
                          <MetricTableSizeCard
                            title={`${table} Table Size`}
                            tableName={table}
                            url={url!}
                            resolution={resolution}
                            selector="table_size_bytes"
                            sparklineColor={SPARKLINE_COLORS[(pluginIdx * 10 + tableIdx * 2) % SPARKLINE_COLORS.length]}
                          />
                        </div>
                        <div className="col-6">
                          <MetricTableSizeCard
                            title={`${table} Index Size`}
                            tableName={table}
                            url={url!}
                            resolution={resolution}
                            selector="index_size_bytes"
                            sparklineColor={SPARKLINE_COLORS[(pluginIdx * 10 + tableIdx * 2 + 1) % SPARKLINE_COLORS.length]}
                          />
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>
      ))}

    </div>
  );
}
