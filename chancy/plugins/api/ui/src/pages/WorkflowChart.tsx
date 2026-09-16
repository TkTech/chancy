import {useRef} from 'react';
import type {NodeProps} from '@xyflow/react';
import {Edge, Handle, MarkerType, Node, NodeTypes, Position, ReactFlow, Background} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import dagre from '@dagrejs/dagre';
import {Workflow} from '../hooks/useWorkflows.tsx';
import {statusToColor, extractFunctionName} from '../utils.tsx';

interface WorkflowChartProps {
  workflow: Workflow;
}

type CustomNode = Node<{
  label: string,
  jobId: string,
  state: string,
  job: {
    func: string,
    queue: string,
    kwargs: unknown,
    priority: number,
    max_attempts: number,
    limits: {
      key: string,
      value: number
    }[]
  }
}, 'custom'>;

const CustomNode = ({ data }: NodeProps<CustomNode>) => {
  const nodeRef = useRef<HTMLDivElement>(null);
  const functionName = extractFunctionName(data.job?.func || '');

  return (
    <div ref={nodeRef} style={{
      minWidth: "250px",
    }}>
      <Handle type="target" position={Position.Left} style={{
        opacity: 0,
      }} />
      <div className={`p-3 border border-2 border-${statusToColor(data.state)} bg-body`}>
        <div className={"fw-bold text-truncate"} title={data.label}>
          {data.label}
        </div>
        {functionName && (
          <div className={"text-muted small text-truncate"} title={data.job?.func}>
            {functionName}
          </div>
        )}
      </div>
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

const WorkflowChart: React.FC<WorkflowChartProps> = ({ workflow }) => {
    const nodes: Node[] = [];
    const edges: Edge[] = [];

    Object.entries(workflow.steps || {}).forEach(([stepId, step]) => {
        nodes.push({
            id: stepId,
            type: 'customNode',
            data: {
              label: stepId,
              jobId: step.job_id,
              state: step.state,
              job: step.job,
            },
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
        <div style={{ width: '100%', height: '500px' }}>
            <ReactFlow
                nodes={layoutNodes}
                edges={layoutEdges}
                nodeTypes={nodeTypes}
                nodesDraggable={false}
                nodesConnectable={false}
                proOptions={{
                    hideAttribution: true,
                }}
                fitView
            >
                <Background gap={20} size={1} />
            </ReactFlow>
        </div>
    );
};

export default WorkflowChart;