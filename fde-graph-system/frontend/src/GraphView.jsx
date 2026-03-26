import { useEffect, useState } from "react";
import ReactFlow, { Background, Controls, MiniMap } from "reactflow";
import "reactflow/dist/style.css";
import axios from "axios";

function GraphView({ highlight = [] }) {
  const [nodes, setNodes] = useState([]);
  const [edges, setEdges] = useState([]);
  const [graph, setGraph] = useState(null);
  const [loading, setLoading] = useState(true);
  const [selectedNodeId, setSelectedNodeId] = useState(null);
  const [showGranularOverlay, setShowGranularOverlay] = useState(false);
  const [showMenu, setShowMenu] = useState(false);
  const [canvasSize, setCanvasSize] = useState({ width: 1800, height: 1400 });
  const [viewportWidth, setViewportWidth] = useState(
    typeof window !== "undefined" ? window.innerWidth : 1200,
  );
  const isMobile = viewportWidth < 900;

  const visibleTypes = new Set([
    "sales_order",
    "sales_order_item",
    "sales_order_schedule_line",
    "delivery",
    "delivery_item",
    "billing",
    "billing_item",
    "billing_cancellation",
    "journal_entry_item",
    "payment",
    "business_partner",
    "customer_sales_area",
    "customer_company",
    "product",
    "product_description",
    "plant",
  ]);

  const typeOrder = [
    "sales_order",
    "sales_order_item",
    "sales_order_schedule_line",
    "delivery",
    "delivery_item",
    "billing",
    "billing_item",
    "billing_cancellation",
    "journal_entry_item",
    "payment",
    "business_partner",
    "customer_sales_area",
    "customer_company",
    "product",
    "product_description",
    "plant",
  ];

  function isHighlighted(nodeId) {
    return highlight.some((value) => {
      const normalized = String(value);
      return (
        normalized === nodeId ||
        nodeId.endsWith(`:${normalized}`) ||
        nodeId.includes(normalized)
      );
    });
  }

  function isSelected(nodeId) {
    return selectedNodeId === nodeId;
  }

  const selectedConnections =
    selectedNodeId && graph
      ? graph.edges
          .filter(
            (edge) =>
              edge.from === selectedNodeId || edge.to === selectedNodeId,
          )
          .slice(0, 12)
      : [];

  const selectedNeighbors = new Set(
    selectedConnections.map((edge) =>
      edge.from === selectedNodeId ? edge.to : edge.from,
    ),
  );

  function isNeighbor(nodeId) {
    if (!selectedNodeId) {
      return false;
    }

    return selectedNeighbors.has(nodeId);
  }

  function formatValue(value) {
    if (value === null || value === undefined || value === "") {
      return "-";
    }

    if (typeof value === "object") {
      return JSON.stringify(value);
    }

    return String(value);
  }

  const selectedNode = graph?.nodes.find((node) => node.id === selectedNodeId);

  useEffect(() => {
    const handleResize = () => {
      setViewportWidth(window.innerWidth);
    };

    window.addEventListener("resize", handleResize);

    axios.get("/graph").then((res) => {
      setGraph(res.data);
      setLoading(false);
    });

    return () => window.removeEventListener("resize", handleResize);
  }, []);

  useEffect(() => {
    if (!graph) {
      return;
    }

    const filteredNodes = graph.nodes.filter(
      (node) =>
        visibleTypes.has(node.type) &&
        (showGranularOverlay ||
          ![
            "sales_order_schedule_line",
            "billing_item",
            "billing_cancellation",
            "customer_sales_area",
            "customer_company",
            "product_description",
          ].includes(node.type)),
    );
    const visibleNodeIds = new Set(filteredNodes.map((node) => node.id));

    const nodesByType = new Map();

    for (const node of filteredNodes) {
      if (!nodesByType.has(node.type)) {
        nodesByType.set(node.type, []);
      }
      nodesByType.get(node.type).push(node);
    }

    const n = [];

    typeOrder.forEach((type, typeIndex) => {
      const typeNodes = nodesByType.get(type) || [];

      typeNodes.forEach((node, nodeIndex) => {
        n.push({
          id: node.id,
          data: { label: node.label || node.id },
          position: {
            x: typeIndex * 260,
            y: nodeIndex * 110,
          },
          style: {
            background: isSelected(node.id)
              ? "#2563eb"
              : isHighlighted(node.id)
                ? "#ef4444"
                : isNeighbor(node.id)
                  ? "#f59e0b"
                  : "#dbeafe",
            color:
              isSelected(node.id) || isHighlighted(node.id)
                ? "#ffffff"
                : "#0f172a",
            border: isSelected(node.id)
              ? "2px solid #1d4ed8"
              : isHighlighted(node.id)
                ? "2px solid #b91c1c"
                : isNeighbor(node.id)
                  ? "2px solid #d97706"
                  : "1px solid rgba(15, 23, 42, 0.12)",
            boxShadow: isSelected(node.id)
              ? "0 0 0 6px rgba(37, 99, 235, 0.18)"
              : isHighlighted(node.id)
                ? "0 0 0 6px rgba(239, 68, 68, 0.18)"
                : isNeighbor(node.id)
                  ? "0 0 0 4px rgba(245, 158, 11, 0.16)"
                  : "0 8px 30px rgba(15, 23, 42, 0.08)",
          },
        });
      });
    });

    const maxX = n.reduce(
      (highest, node) => Math.max(highest, node.position.x),
      0,
    );
    const maxY = n.reduce(
      (highest, node) => Math.max(highest, node.position.y),
      0,
    );

    setCanvasSize({
      width: Math.max(1800, maxX + 520),
      height: Math.max(1400, maxY + 420),
    });

    const e = graph.edges
      .filter(
        (edge) => visibleNodeIds.has(edge.from) && visibleNodeIds.has(edge.to),
      )
      .map((edge, index) => ({
        id: `${index}`,
        source: edge.from,
        target: edge.to,
        type: "smoothstep",
        style: {
          stroke:
            selectedNodeId &&
            (edge.from === selectedNodeId || edge.to === selectedNodeId)
              ? "#d97706"
              : "#94a3b8",
          strokeWidth:
            selectedNodeId &&
            (edge.from === selectedNodeId || edge.to === selectedNodeId)
              ? 2
              : 1,
          opacity: selectedNodeId ? 0.18 : 0.35,
        },
      }));

    setNodes(n);
    setEdges(e);
  }, [graph, highlight, selectedNodeId, showGranularOverlay]);

  return (
    <div
      style={{
        width: "100%",
        height: "100%",
        background: "#f8fafc",
        position: "relative",
        overflow: "auto",
      }}
    >
      <div
        style={{
          width: canvasSize.width,
          height: canvasSize.height,
          minWidth: "100%",
          minHeight: "100%",
          position: "relative",
        }}
      >
        {loading ? (
          <div
            style={{
              position: "absolute",
              zIndex: 2,
              padding: 16,
              color: "#64748b",
            }}
          >
            Loading graph...
          </div>
        ) : null}
        <div
          style={{
            position: "absolute",
            top: isMobile ? 12 : 18,
            left: isMobile ? 12 : 18,
            zIndex: 4,
            background: "rgba(255,255,255,0.95)",
            border: "1px solid rgba(15, 23, 42, 0.12)",
            borderRadius: 16,
            padding: isMobile ? 10 : 14,
            boxShadow: "0 12px 30px rgba(15, 23, 42, 0.08)",
            maxWidth: isMobile ? "calc(100% - 24px)" : 380,
            width: isMobile ? "calc(100% - 24px)" : 380,
          }}
        >
          <div style={{ fontSize: 12, color: "#64748b", marginBottom: 4 }}>
            Mapping / Order to Cash
          </div>
          <div style={{ fontSize: 15, fontWeight: 700, color: "#0f172a" }}>
            Graph Explorer
          </div>
        </div>

        <div
          style={{
            position: "absolute",
            top: isMobile ? 92 : 106,
            left: isMobile ? 12 : 18,
            zIndex: 4,
            display: "flex",
            gap: 10,
            flexWrap: "wrap",
            maxWidth: isMobile ? "calc(100% - 24px)" : 380,
          }}
        >
          <button
            onClick={() => setShowGranularOverlay((current) => !current)}
            style={{
              border: "1px solid rgba(15, 23, 42, 0.12)",
              background: showGranularOverlay
                ? "#111111"
                : "rgba(255,255,255,0.96)",
              color: showGranularOverlay ? "#ffffff" : "#0f172a",
              borderRadius: 10,
              padding: "8px 12px",
              fontSize: 12,
              fontWeight: 600,
              boxShadow: "0 8px 20px rgba(15, 23, 42, 0.08)",
              cursor: "pointer",
            }}
          >
            {showGranularOverlay
              ? "Hide Granular Overlay"
              : "Show Granular Overlay"}
          </button>
          <button
            onClick={() =>
              setSelectedNodeId((current) => (current ? null : current))
            }
            style={{
              border: "1px solid rgba(15, 23, 42, 0.12)",
              background: "rgba(255,255,255,0.96)",
              color: "#0f172a",
              borderRadius: 10,
              padding: "8px 12px",
              fontSize: 12,
              fontWeight: 600,
              boxShadow: "0 8px 20px rgba(15, 23, 42, 0.08)",
              cursor: "pointer",
            }}
          >
            Minimize
          </button>
        </div>

        <button
          onClick={() => setShowMenu((current) => !current)}
          style={{
            position: "absolute",
            top: isMobile ? 12 : 12,
            right: isMobile ? 12 : 12,
            zIndex: 5,
            width: isMobile ? 44 : 52,
            height: isMobile ? 44 : 52,
            border: "none",
            borderRadius: 14,
            background: "#111111",
            color: "#ffffff",
            fontSize: isMobile ? 26 : 30,
            lineHeight: 1,
            boxShadow: "0 10px 24px rgba(15, 23, 42, 0.24)",
            cursor: "pointer",
          }}
          aria-label="Graph menu"
        >
          ...
        </button>
        {showMenu ? (
          <div
            style={{
              position: "absolute",
              top: isMobile ? 68 : 70,
              right: isMobile ? 12 : 12,
              zIndex: 5,
              width: isMobile ? 120 : 140,
              background: "rgba(17,17,17,0.96)",
              color: "#ffffff",
              borderRadius: 14,
              padding: 10,
              boxShadow: "0 16px 34px rgba(15, 23, 42, 0.22)",
            }}
          >
            <div style={{ fontSize: 13, padding: "8px 10px", opacity: 0.92 }}>
              Menu
            </div>
          </div>
        ) : null}
        <ReactFlow
          nodes={nodes}
          edges={edges}
          fitView
          onNodeClick={(event, node) => {
            setSelectedNodeId(node.id);
          }}
          nodesDraggable={false}
          nodesConnectable={false}
          elementsSelectable={false}
          panOnScroll
          panOnScrollMode="free"
          zoomOnScroll={false}
          zoomOnPinch
        >
          <Background color="#cbd5e1" gap={18} />
          <MiniMap zoomable pannable />
          <Controls />
        </ReactFlow>

        {selectedNode ? (
          <div
            style={{
              position: "absolute",
              top: isMobile ? 148 : 138,
              left: isMobile ? 12 : 18,
              zIndex: 4,
              width: isMobile ? "calc(100% - 24px)" : 330,
              maxHeight: isMobile ? "42vh" : "calc(100% - 160px)",
              overflow: "auto",
              background: "rgba(255,255,255,0.98)",
              border: "1px solid rgba(15, 23, 42, 0.12)",
              borderRadius: 18,
              boxShadow: "0 18px 40px rgba(15, 23, 42, 0.10)",
              padding: isMobile ? 12 : 18,
            }}
          >
            <div style={{ fontSize: 18, fontWeight: 800, color: "#0f172a" }}>
              {selectedNode.label || selectedNode.id}
            </div>
            <div style={{ fontSize: 13, color: "#64748b", marginTop: 4 }}>
              Entity: {selectedNode.type}
            </div>

            <div style={{ marginTop: 12, display: "grid", gap: 6 }}>
              {Object.entries(selectedNode.data || {})
                .slice(0, 9)
                .map(([key, value]) => (
                  <div key={key} style={{ fontSize: 13, color: "#475569" }}>
                    <span style={{ fontWeight: 700, color: "#0f172a" }}>
                      {key}:
                    </span>{" "}
                    {formatValue(value)}
                  </div>
                ))}
            </div>

            <div style={{ marginTop: 10, fontSize: 12, color: "#94a3b8" }}>
              Additional fields hidden for readability
            </div>

            <div
              style={{
                marginTop: 10,
                fontSize: 13,
                fontWeight: 700,
                color: "#0f172a",
              }}
            >
              Connections: {selectedConnections.length}
            </div>
          </div>
        ) : null}
      </div>
    </div>
  );
}

export default GraphView;
