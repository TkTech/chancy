import { useMemo, useRef } from 'react';
import {
  ReactFlow,
  Node,
  Edge,
  useNodesState,
  useEdgesState,
  Position,
  Handle,
  NodeTypes,
} from '@xyflow/react';
import type { NodeProps } from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import dagre from '@dagrejs/dagre';
import {Workflow} from '../hooks/useWorkflows.tsx';

interface WorkflowChartProps {
  workflow: Workflow;
}

type CustomNode = Node<{
  label: string,
  jobId: string,
  color: string
}, 'custom'>;

const CustomNode = ({ data }: NodeProps<CustomNode>) => {
  const nodeRef = useRef<HTMLDivElement>(null);

  return (
    <div ref={nodeRef} style={{
      minWidth: "350px",
    }}>
      <Handle type="target" position={Position.Left} />
      <div className={`p-2 border text-center shadow ${data.color}`}>
        <div className={"fw-bolder"}>{data.label}</div>
        <div className="text-xs">{data.jobId || "-"}</div>
      </div>
      <Handle type="source" position={Position.Right} />
    </div>
  );
};

const nodeTypes: NodeTypes = {
  customNode: CustomNode,
};

const getNodeColor = (state: string): string => {
  switch (state) {
    case 'succeeded': return 'bg-success text-dark';
    case 'pending': return 'bg-info text-dark';
    case 'running': return 'bg-primary text-dark';
    case 'failed': return 'bg-danger text-dark';
    case 'retrying': return 'bg-warning text-dark';
    default: return 'bg-secondary text-muted';
  }
};

const getLayoutedElements = (nodes: Node[], edges: Edge[], direction = 'LR') => {
  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));

  dagreGraph.setGraph({ rankdir: direction });

  nodes.forEach((node) => {
    dagreGraph.setNode(node.id, { width: 350, height: 80 });
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
  const { initialNodes, initialEdges } = useMemo(() => {
    const nodes: Node[] = [];
    const edges: Edge[] = [];

    Object.entries(workflow.steps || {}).forEach(([stepId, step]) => {
      nodes.push({
        id: stepId,
        type: 'customNode',
        data: {
          label: stepId,
          jobId: step.job_id,
          color: getNodeColor(step.state)
        },
        position: { x: 0, y: 0 },
      });

      step.dependencies?.forEach((dependencyId) => {
        edges.push({
          id: `e${dependencyId}-${stepId}`,
          source: dependencyId,
          target: stepId,
          animated: workflow.steps[stepId].state === 'running',
        });
      });
    });

    return { initialNodes: nodes, initialEdges: edges };
  }, [workflow.steps]);

  const { nodes: layoutNodes, edges: layoutEdges } = getLayoutedElements(initialNodes, initialEdges);

  const [nodes, , onNodesChange] = useNodesState(layoutNodes);
  const [edges, , onEdgesChange] = useEdgesState(layoutEdges);

  return (
    <div style={{ width: '100%', height: '500px' }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeTypes={nodeTypes}
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={false}
        proOptions={{
          hideAttribution: true,
        }}
        fitView
      />
    </div>
  );
};

export default WorkflowChart;