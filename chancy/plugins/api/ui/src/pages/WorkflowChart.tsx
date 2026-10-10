import type {NodeProps} from '@xyflow/react';
import {Edge, Handle, MarkerType, Node, NodeTypes, Position, ReactFlow, Background, Controls, useReactFlow} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import dagre from '@dagrejs/dagre';
import type { Workflow, Step } from '../services/schemas';
import { StatusBadge } from '../components/common/StatusBadge';
import {statusToColor, extractFunctionName} from '../utils.tsx';
import { stepStatus } from '../features/workflows/diagnostics';

interface WorkflowChartProps {
  workflow: Workflow;
  onStepClick: (stepId: string) => void;
  matchingStepIds?: string[];
}

type CustomNode = Node<Pick<Step, 'job'> & {
  state: string;
  label: string;
  inspect: () => void;
}, 'customNode'>;

const CustomNode = ({ data }: NodeProps<CustomNode>) => {
  const functionName = extractFunctionName(data.job?.func || '');

  return (
    <div style={{
      minWidth: "250px",
    }}>
      <Handle type="target" position={Position.Left} style={{
        opacity: 0,
      }} />
      <button type="button" onClick={data.inspect} aria-label={`Inspect step ${data.label}, ${data.state}`} className={`nodrag nopan w-100 text-start text-body p-3 border border-2 border-${statusToColor(data.state)} bg-body`}>
        <div className="d-flex align-items-center justify-content-between gap-2">
          <div className="fw-bold text-truncate" title={data.label}>{data.label}</div>
          <StatusBadge status={data.state} />
        </div>
        {functionName && (
          <div className={"text-muted small text-truncate"} title={data.job?.func}>
            {functionName}
          </div>
        )}
      </button>
      <Handle type="source" position={Position.Right} style={{
        opacity: 0,
      }}/>
    </div>
  );
};

const nodeTypes: NodeTypes = {
  customNode: CustomNode,
};

const getLayoutedElements = (nodes: Node[], edges: Edge[], direction = 'LR') => {
  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));

  dagreGraph.setGraph({
    rankdir: direction,
    nodesep: 80,  // Horizontal spacing between nodes at the same rank
    ranksep: 120, // Vertical spacing between ranks
  });

  nodes.forEach((node) => {
    dagreGraph.setNode(node.id, { width: 250, height: 70 });
  });

  edges.forEach((edge) => {
    dagreGraph.setEdge(edge.source, edge.target);
  });

  dagre.layout(dagreGraph);

  const layoutNodes = nodes.map((node) => {
    const nodeWithPosition = dagreGraph.node(node.id);
    return {
      ...node,
      position: {
        x: nodeWithPosition.x - nodeWithPosition.width / 2,
        y: nodeWithPosition.y - nodeWithPosition.height / 2,
      },
    };
  });

  return { nodes: layoutNodes, edges };
};

const WorkflowChart: React.FC<WorkflowChartProps> = ({ workflow, onStepClick, matchingStepIds }) => {
    const { fitView } = useReactFlow();
    const matches = matchingStepIds ? new Set(matchingStepIds) : undefined;
    const nodes: Node[] = [];
    const edges: Edge[] = [];

    Object.entries(workflow.steps || {}).forEach(([stepId, step]) => {
        nodes.push({
            id: stepId,
            type: 'customNode',
            data: {
              label: stepId,
              state: stepStatus(step),
              job: step.job,
              inspect: () => onStepClick(stepId),
            },
            style: { opacity: matches && !matches.has(stepId) ? 0.35 : 1 },
            position: { x: 0, y: 0 },
        });

        step.dependencies?.forEach((dependencyId) => {
            edges.push({
                id: `e${dependencyId}-${stepId}`,
                source: dependencyId,
                target: stepId,
                animated: workflow.steps?.[stepId].state === null,
                style: {
                  strokeWidth: 2,
                },
              markerEnd: {
                  type: MarkerType.ArrowClosed,
                },
            });
        });
    });

    const { nodes: layoutNodes, edges: layoutEdges } = getLayoutedElements(nodes, edges);

    return (
        <>
          {matchingStepIds && (
            <button className="btn btn-sm btn-outline-secondary mb-2" disabled={matchingStepIds.length === 0} onClick={() => void fitView({ nodes: matchingStepIds.map(id => ({ id })), padding: 0.2, maxZoom: 1 })}>Focus matching steps</button>
          )}
          <div style={{ width: '100%', height: '500px' }}>
            <ReactFlow
                nodes={layoutNodes}
                edges={layoutEdges}
                nodeTypes={nodeTypes}
                nodesDraggable={false}
                nodesConnectable={false}
                nodesFocusable={false}
                proOptions={{
                    hideAttribution: true,
                }}
                fitView
            >
                <Background gap={20} size={1} />
                <Controls showInteractive={false} />
            </ReactFlow>
          </div>
        </>
    );
};

export default WorkflowChart;
