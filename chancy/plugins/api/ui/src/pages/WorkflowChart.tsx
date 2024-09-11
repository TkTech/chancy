import React, { useCallback, useEffect, useMemo, useRef, useLayoutEffect, useState } from 'react';
import {
  ReactFlow,
  Node,
  Edge,
  Connection,
  useNodesState,
  useEdgesState,
  addEdge,
  Position,
  Handle,
  NodeTypes,
  useReactFlow, ReactFlowProvider
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import dagre from '@dagrejs/dagre';

interface Step {
  step_id: string;
  state: string;
  job_id: string;
  dependencies: string[];
}

interface Workflow {
  id: string;
  name: string;
  state: string;
  created_at: string;
  updated_at: string;
  steps?: {
    [key: string]: Step;
  };
}

interface WorkflowChartProps {
  workflow: Workflow;
}

interface CustomNodeData {
  label: string;
  jobId: string;
  color: string;
}

const CustomNode: React.FC<{ data: CustomNodeData }> = ({ data }) => {
  const nodeRef = useRef<HTMLDivElement>(null);

  useLayoutEffect(() => {
    if (nodeRef.current) {
      const { width, height } = nodeRef.current.getBoundingClientRect();
      nodeRef.current.style.width = `${width}px`;
      nodeRef.current.style.height = `${height}px`;
    }
  }, []);

  return (
    <div ref={nodeRef}>
      <Handle type="target" position={Position.Top} />
      <div className={`p-2 border text-center shadow ${data.color}`}>
        <div className={"fw-bolder"}>{data.label}</div>
        <div className="text-xs">{data.jobId || "-"}</div>
      </div>
      <Handle type="source" position={Position.Bottom} />
    </div>
  );
};

const nodeTypes: NodeTypes = {
  customNode: CustomNode,
};

const getNodeColor = (state: string): string => {
  switch (state) {
    case 'succeeded': return 'bg-success';
    case 'running': return 'bg-primary';
    case 'failed': return 'bg-danger';
    default: return 'bg-gray-500';
  }
};

const getLayoutedElements = (nodes: Node[], edges: Edge[], direction = 'TB') => {
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

  const layoutedNodes = nodes.map((node) => {
    const nodeWithPosition = dagreGraph.node(node.id);
    return {
      ...node,
      position: {
        x: nodeWithPosition.x - nodeWithPosition.width / 2,
        y: nodeWithPosition.y - nodeWithPosition.height / 2,
      },
    };
  });

  return { nodes: layoutedNodes, edges };
};

const WorkflowChart: React.FC<WorkflowChartProps> = ({ workflow }) => {
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [isLayoutApplied, setIsLayoutApplied] = useState(false);
  const { fitView } = useReactFlow();

  const { initialNodes, initialEdges } = useMemo(() => {
    const nodes: Node<CustomNodeData>[] = [];
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

  useEffect(() => {
    setNodes(initialNodes);
    setEdges(initialEdges);
  }, [initialNodes, initialEdges, setNodes, setEdges]);

  useLayoutEffect(() => {
    if (nodes.length > 0 && !isLayoutApplied) {
      window.requestAnimationFrame(() => {
        const {nodes: layoutedNodes, edges: layoutedEdges} = getLayoutedElements(nodes, edges);
        setNodes([...layoutedNodes]);
        setEdges([...layoutedEdges]);
        setIsLayoutApplied(true);
        fitView();
      });
    }
  }, [nodes, edges, setNodes, setEdges, isLayoutApplied, fitView]);

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
      />
    </div>
  );
};

export default WorkflowChart;