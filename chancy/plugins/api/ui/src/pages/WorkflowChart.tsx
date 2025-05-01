import {useRef} from 'react';
import type {NodeProps} from '@xyflow/react';
import {Edge, Handle, MarkerType, Node, NodeTypes, Position, ReactFlow,} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import dagre from '@dagrejs/dagre';
import {Workflow} from '../hooks/useWorkflows.tsx';
import {Link} from 'react-router-dom';
import {statusToColorCode} from '../utils.tsx';
import {useSlidePanels} from '../components/SlidePanelContext.tsx';
import {Job} from './Jobs.tsx';

interface WorkflowChartProps {
  workflow: Workflow;
}

type CustomNode = Node<{
  label: string,
  jobId: string,
  state: string,
  job: {
    func: string
  }
}, 'custom'>;

const CustomNode = ({ data }: NodeProps<CustomNode>) => {
  const nodeRef = useRef<HTMLDivElement>(null);
  const { openPanel } = useSlidePanels();
  
  const handleJobClick = (jobId: string, e: React.MouseEvent) => {
    e.preventDefault();
    openPanel({
      title: "Job Details",
      content: <Job jobId={jobId} inPanel={true} />
    });
  };

  return (
    <div ref={nodeRef} style={{
      minWidth: "350px",
    }}>
      <Handle type="target" position={Position.Left} style={{
        opacity: 0,
      }} />
      <div className={"p-3 border border-2 shadow-lg"} style={{
        // @ts-expect-error: CSS variable
        "--bs-border-color": statusToColorCode(data.state),
        backgroundColor: "rgba(255, 255, 255, 0.1)",
      }}>
        <div className={"fw-bolder"}>{data.label}</div>
        <div className="text-xs">
          {data.jobId ? (
            <Link to={`/jobs/${data.jobId}`} onClick={(e) => handleJobClick(data.jobId, e)}>
              {data.jobId}
            </Link>
          ) : "-"}
        </div>
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
                animated: workflow.steps[stepId].state === null,
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
            />
        </div>
    );
};

export default WorkflowChart;