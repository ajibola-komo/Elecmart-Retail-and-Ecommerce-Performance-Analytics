import { useState, useRef, useEffect, useCallback } from "react";

const COLORS = {
  bg: "#0d1117",
  surface: "#161b22",
  border: "#30363d",
  fact: { bg: "#1a2332", border: "#2d6a9f", header: "#1c3a5e", text: "#58a6ff", badge: "#2d6a9f" },
  dim: { bg: "#1a2a1a", border: "#2ea043", header: "#1c3d1c", text: "#3fb950", badge: "#2ea043" },
  text: "#e6edf3",
  textMuted: "#8b949e",
  pk: "#f0a500",
  fk: "#a371f7",
  line: "#444c56",
  lineHL: "#58a6ff",
  gold: "#f0a500",
};

const SCHEMA = {
  gold_dim_date: {
    type: "dim", label: "gold_dim_date",
    cols: [
      { name: "date_id", pk: true }, { name: "date" }, { name: "year" }, { name: "quarter" },
      { name: "month" }, { name: "month_name" }, { name: "day" }, { name: "week_of_year" },
      { name: "day_of_week" }, { name: "day_name" }, { name: "is_weekend" },
      { name: "is_month_start" }, { name: "is_month_end" }, { name: "days_in_month" },
      { name: "month_id" }, { name: "year_quarter" },
    ],
  },
  gold_dim_customer: {
    type: "dim", label: "gold_dim_customer",
    cols: [
      { name: "customer_id", pk: true }, { name: "email_address" }, { name: "first_name" },
      { name: "last_name" }, { name: "gender" }, { name: "customer_persona" },
      { name: "age_group" }, { name: "country" }, { name: "state_province" }, { name: "city" },
      { name: "location_type" }, { name: "signup_date" },
      { name: "signup_date_id", fk: "gold_dim_date" }, { name: "signup_channel" },
      { name: "loyalty_status" }, { name: "income_bracket" },
      { name: "email_opt_in" }, { name: "sms_opt_in" },
    ],
  },
  gold_dim_store: {
    type: "dim", label: "gold_dim_store",
    cols: [
      { name: "store_id", pk: true }, { name: "store_name" }, { name: "store_type" },
      { name: "store_size" }, { name: "opening_date" },
      { name: "opening_date_id", fk: "gold_dim_date" },
      { name: "foot_traffic_index" }, { name: "location_type" },
      { name: "country" }, { name: "state_province" }, { name: "city" },
    ],
  },
  gold_dim_product: {
    type: "dim", label: "gold_dim_product",
    cols: [
      { name: "product_id", pk: true }, { name: "product_name" }, { name: "category_name" },
      { name: "subcategory_name" }, { name: "brand_name" }, { name: "unit_cost" },
      { name: "unit_price" }, { name: "warranty_years" }, { name: "product_segment" },
    ],
  },
  gold_dim_campaign: {
    type: "dim", label: "gold_dim_campaign",
    cols: [
      { name: "campaign_id", pk: true }, { name: "campaign_name" }, { name: "campaign_channel" },
      { name: "promo_id", fk: "gold_dim_promotion" },
      { name: "campaign_start_date" }, { name: "campaign_start_date_id", fk: "gold_dim_date" },
      { name: "campaign_end_date" }, { name: "campaign_end_date_id", fk: "gold_dim_date" },
    ],
  },
  gold_dim_promotion: {
    type: "dim", label: "gold_dim_promotion",
    cols: [
      { name: "promo_id", pk: true }, { name: "promo_name" }, { name: "promo_type" },
      { name: "discount_type" }, { name: "discount_value" },
      { name: "promo_start_date" }, { name: "promo_start_date_id", fk: "gold_dim_date" },
      { name: "promo_end_date" }, { name: "promo_end_date_id", fk: "gold_dim_date" },
      { name: "is_active" },
    ],
  },
  gold_fact_transaction: {
    type: "fact", label: "gold_fact_transaction",
    cols: [
      { name: "transaction_id", pk: true },
      { name: "transaction_timestamp" },
      { name: "transaction_date_id", fk: "gold_dim_date" },
      { name: "customer_id", fk: "gold_dim_customer" },
      { name: "store_id", fk: "gold_dim_store" },
      { name: "sales_channel" },
      { name: "session_id", fk: "gold_fact_clickstream" },
      { name: "promo_id", fk: "gold_dim_promotion" },
      { name: "campaign_id", fk: "gold_dim_campaign" },
      { name: "transaction_subtotal" }, { name: "transaction_discount_applied" },
      { name: "transaction_total" }, { name: "transaction_cost" },
      { name: "items_count" }, { name: "payment_type" }, { name: "transaction_status" },
    ],
  },
  gold_fact_sale: {
    type: "fact", label: "gold_fact_sale",
    cols: [
      { name: "sale_id", pk: true },
      { name: "transaction_id", fk: "gold_fact_transaction" },
      { name: "session_id", fk: "gold_fact_clickstream" },
      { name: "transaction_timestamp" },
      { name: "transaction_date_id", fk: "gold_dim_date" },
      { name: "product_id", fk: "gold_dim_product" },
      { name: "quantity" }, { name: "unit_cost" }, { name: "unit_price" },
      { name: "line_cost" }, { name: "line_total" },
      { name: "allocated_line_discount" }, { name: "net_line_revenue" },
      { name: "transaction_status" },
    ],
  },
  gold_fact_inventory: {
    type: "fact", label: "gold_fact_inventory",
    cols: [
      { name: "inventory_id", pk: true },
      { name: "store_id", fk: "gold_dim_store" },
      { name: "product_id", fk: "gold_dim_product" },
      { name: "snapshot_month" },
      { name: "snapshot_month_id", fk: "gold_dim_date" },
      { name: "starting_stock" }, { name: "received_stock" }, { name: "sold_units" },
      { name: "closing_stock" }, { name: "backorder_flag" }, { name: "shrinkage_loss" },
    ],
  },
  gold_fact_clickstream: {
    type: "fact", label: "gold_fact_clickstream",
    cols: [
      { name: "session_id", pk: true },
      { name: "customer_id", fk: "gold_dim_customer" },
      { name: "session_start_time" },
      { name: "session_start_date_id", fk: "gold_dim_date" },
      { name: "session_end_time" },
      { name: "session_end_date_id", fk: "gold_dim_date" },
      { name: "device_type" }, { name: "number_of_pages_viewed" },
      { name: "product_page_visited_flag" }, { name: "added_to_cart_flag" },
      { name: "purchased_flag" }, { name: "traffic_source" },
      { name: "linked_to_a_campaign_flag" },
      { name: "campaign_id", fk: "gold_dim_campaign" },
    ],
  },
};

// Initial positions for nodes
const INITIAL_POSITIONS = {
  gold_dim_date:        { x: 600, y: 20 },
  gold_dim_customer:    { x: 20,  y: 20 },
  gold_dim_store:       { x: 1180, y: 20 },
  gold_dim_product:     { x: 1180, y: 400 },
  gold_dim_campaign:    { x: 20,  y: 500 },
  gold_dim_promotion:   { x: 20,  y: 860 },
  gold_fact_transaction:{ x: 520, y: 420 },
  gold_fact_sale:       { x: 840, y: 720 },
  gold_fact_inventory:  { x: 840, y: 200 },
  gold_fact_clickstream:{ x: 230, y: 1050 },
};

const COL_H = 22;
const HEADER_H = 36;
const TABLE_W = 230;
const PADDING = 8;

function getTableHeight(table) {
  return HEADER_H + table.cols.length * COL_H + PADDING;
}

function getColY(tableY, colIndex) {
  return tableY + HEADER_H + colIndex * COL_H + COL_H / 2;
}

function getTableCenterX(x) { return x + TABLE_W / 2; }

function buildRelationships() {
  const rels = [];
  Object.entries(SCHEMA).forEach(([tableName, table]) => {
    table.cols.forEach((col, colIdx) => {
      if (col.fk) {
        rels.push({ from: tableName, fromCol: col.name, fromColIdx: colIdx, to: col.fk });
      }
    });
  });
  return rels;
}

function Arrow({ x1, y1, x2, y2, highlighted }) {
  const dx = x2 - x1;
  const dy = y2 - y1;
  const midX = (x1 + x2) / 2;
  const d = `M ${x1} ${y1} C ${midX} ${y1}, ${midX} ${y2}, ${x2} ${y2}`;
  return (
    <path
      d={d}
      fill="none"
      stroke={highlighted ? COLORS.lineHL : COLORS.line}
      strokeWidth={highlighted ? 2 : 1.2}
      strokeDasharray={highlighted ? "none" : "4,3"}
      markerEnd={`url(#arrow${highlighted ? "HL" : ""})`}
      opacity={highlighted ? 1 : 0.55}
      style={{ transition: "all 0.2s" }}
    />
  );
}

function TableNode({ id, table, pos, onMouseDown, isHighlighted, highlightedTable, onHover }) {
  const th = getTableHeight(table);
  const colors = table.type === "fact" ? COLORS.fact : COLORS.dim;
  const isActive = highlightedTable === id;
  const isDimmed = highlightedTable && !isHighlighted && !isActive;

  return (
    <g
      transform={`translate(${pos.x}, ${pos.y})`}
      style={{ cursor: "grab", userSelect: "none", opacity: isDimmed ? 0.3 : 1, transition: "opacity 0.2s" }}
      onMouseDown={(e) => onMouseDown(e, id)}
      onMouseEnter={() => onHover(id)}
      onMouseLeave={() => onHover(null)}
    >
      {/* Shadow */}
      <rect x={3} y={3} width={TABLE_W} height={th} rx={6} fill="rgba(0,0,0,0.5)" />
      {/* Body */}
      <rect width={TABLE_W} height={th} rx={6}
        fill={colors.bg}
        stroke={isActive ? colors.text : colors.border}
        strokeWidth={isActive ? 2 : 1}
      />
      {/* Header */}
      <rect width={TABLE_W} height={HEADER_H} rx={6} fill={colors.header} />
      <rect y={HEADER_H - 6} width={TABLE_W} height={6} fill={colors.header} />

      {/* Badge */}
      <rect x={8} y={9} width={table.type === "fact" ? 26 : 22} height={16} rx={3} fill={colors.badge} opacity={0.8} />
      <text x={21} y={21} textAnchor="middle" fontSize={9} fontWeight={700} fill="#fff" fontFamily="monospace">
        {table.type === "fact" ? "FCT" : "DIM"}
      </text>

      {/* Table name */}
      <text x={TABLE_W / 2 + 8} y={23} textAnchor="middle" fontSize={10.5} fontWeight={700}
        fill={colors.text} fontFamily="'JetBrains Mono', monospace">
        {table.label.replace("gold_", "")}
      </text>

      {/* Divider */}
      <line x1={0} y1={HEADER_H} x2={TABLE_W} y2={HEADER_H} stroke={colors.border} strokeWidth={1} />

      {/* Columns */}
      {table.cols.map((col, i) => {
        const cy = HEADER_H + i * COL_H;
        const isEven = i % 2 === 0;
        return (
          <g key={col.name}>
            {isEven && <rect x={0} y={cy} width={TABLE_W} height={COL_H} fill="rgba(255,255,255,0.02)" />}
            {col.pk && (
              <text x={10} y={cy + 14} fontSize={9} fill={COLORS.pk} fontFamily="monospace" fontWeight={700}>PK</text>
            )}
            {col.fk && !col.pk && (
              <text x={10} y={cy + 14} fontSize={9} fill={COLORS.fk} fontFamily="monospace" fontWeight={700}>FK</text>
            )}
            <text x={col.pk || col.fk ? 30 : 10} y={cy + 14} fontSize={10} fill={col.pk ? COLORS.pk : col.fk ? COLORS.fk : COLORS.text}
              fontFamily="'JetBrains Mono', monospace" fontWeight={col.pk ? 700 : 400}>
              {col.name}
            </text>
            <line x1={0} y1={cy + COL_H} x2={TABLE_W} y2={cy + COL_H} stroke={colors.border} strokeWidth={0.5} opacity={0.4} />
          </g>
        );
      })}
    </g>
  );
}

export default function ERD() {
  const [positions, setPositions] = useState(INITIAL_POSITIONS);
  const [dragging, setDragging] = useState(null);
  const [hoveredTable, setHoveredTable] = useState(null);
  const [scale, setScale] = useState(0.62);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  const [isPanning, setIsPanning] = useState(false);
  const panStart = useRef(null);
  const svgRef = useRef(null);
  const relationships = buildRelationships();

  // Connected tables for highlighting
  const connectedTables = useCallback((tableId) => {
    if (!tableId) return new Set();
    const connected = new Set([tableId]);
    relationships.forEach(r => {
      if (r.from === tableId) connected.add(r.to);
      if (r.to === tableId) connected.add(r.from);
    });
    return connected;
  }, [relationships]);

  const highlighted = connectedTables(hoveredTable);

  const handleMouseDown = useCallback((e, id) => {
    e.stopPropagation();
    const svgRect = svgRef.current.getBoundingClientRect();
    setDragging({
      id,
      startX: e.clientX,
      startY: e.clientY,
      origX: positions[id].x,
      origY: positions[id].y,
    });
  }, [positions]);

  const handleSVGMouseDown = useCallback((e) => {
    if (e.target === svgRef.current || e.target.tagName === "svg") {
      setIsPanning(true);
      panStart.current = { x: e.clientX - pan.x, y: e.clientY - pan.y };
    }
  }, [pan]);

  useEffect(() => {
    const onMove = (e) => {
      if (dragging) {
        const dx = (e.clientX - dragging.startX) / scale;
        const dy = (e.clientY - dragging.startY) / scale;
        setPositions(prev => ({
          ...prev,
          [dragging.id]: { x: dragging.origX + dx, y: dragging.origY + dy }
        }));
      } else if (isPanning && panStart.current) {
        setPan({ x: e.clientX - panStart.current.x, y: e.clientY - panStart.current.y });
      }
    };
    const onUp = () => { setDragging(null); setIsPanning(false); };
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
    return () => { window.removeEventListener("mousemove", onMove); window.removeEventListener("mouseup", onUp); };
  }, [dragging, isPanning, scale]);

  const handleWheel = useCallback((e) => {
    e.preventDefault();
    setScale(s => Math.min(2, Math.max(0.25, s - e.deltaY * 0.001)));
  }, []);

  useEffect(() => {
    const el = svgRef.current;
    if (!el) return;
    el.addEventListener("wheel", handleWheel, { passive: false });
    return () => el.removeEventListener("wheel", handleWheel);
  }, [handleWheel]);

  const getConnectionPoints = (rel) => {
    const fromPos = positions[rel.from];
    const toPos = positions[rel.to];
    if (!fromPos || !toPos) return null;
    const fromY = getColY(fromPos.y, rel.fromColIdx);
    const fromX = fromPos.x + TABLE_W;
    const toX = toPos.x;
    const toY = toPos.y + HEADER_H / 2;
    return { x1: fromX, y1: fromY, x2: toX, y2: toY };
  };

  const isRelHighlighted = (rel) => {
    return hoveredTable && (rel.from === hoveredTable || rel.to === hoveredTable);
  };

  return (
    <div style={{ width: "100vw", height: "100vh", background: COLORS.bg, overflow: "hidden", fontFamily: "system-ui", position: "relative" }}>
      {/* Header */}
      <div style={{
        position: "absolute", top: 0, left: 0, right: 0, zIndex: 10,
        background: "linear-gradient(180deg, rgba(13,17,23,0.98) 0%, rgba(13,17,23,0.85) 100%)",
        borderBottom: `1px solid ${COLORS.border}`,
        padding: "12px 24px", display: "flex", alignItems: "center", gap: "16px"
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
          <div style={{ width: 8, height: 8, borderRadius: "50%", background: COLORS.gold }} />
          <span style={{ color: COLORS.text, fontSize: 14, fontWeight: 700, fontFamily: "'JetBrains Mono', monospace", letterSpacing: "0.05em" }}>
            ELECMART · Gold Layer ERD
          </span>
        </div>
        <div style={{ marginLeft: "auto", display: "flex", gap: "20px", alignItems: "center" }}>
          <div style={{ display: "flex", gap: "12px", alignItems: "center" }}>
            <LegendItem color={COLORS.fact.text} label="Fact Table" />
            <LegendItem color={COLORS.dim.text} label="Dimension Table" />
            <LegendItem color={COLORS.pk} label="PK" />
            <LegendItem color={COLORS.fk} label="FK" />
          </div>
          <div style={{ color: COLORS.textMuted, fontSize: 11, fontFamily: "monospace" }}>
            Scroll to zoom · Drag to pan · Hover to highlight
          </div>
        </div>
      </div>

      {/* Canvas */}
      <svg
        ref={svgRef}
        width="100%" height="100%"
        style={{ cursor: isPanning ? "grabbing" : "default" }}
        onMouseDown={handleSVGMouseDown}
      >
        <defs>
          <marker id="arrow" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
            <path d="M0,0 L0,6 L8,3 z" fill={COLORS.line} />
          </marker>
          <marker id="arrowHL" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
            <path d="M0,0 L0,6 L8,3 z" fill={COLORS.lineHL} />
          </marker>
          <filter id="glow">
            <feGaussianBlur stdDeviation="3" result="coloredBlur" />
            <feMerge><feMergeNode in="coloredBlur" /><feMergeNode in="SourceGraphic" /></feMerge>
          </filter>
        </defs>

        <g transform={`translate(${pan.x + 0}, ${pan.y + 52}) scale(${scale})`}>
          {/* Relationships — draw behind tables */}
          {relationships.map((rel, i) => {
            const pts = getConnectionPoints(rel);
            if (!pts) return null;
            return (
              <Arrow key={i} {...pts} highlighted={isRelHighlighted(rel)} />
            );
          })}

          {/* Tables */}
          {Object.entries(SCHEMA).map(([id, table]) => (
            <TableNode
              key={id}
              id={id}
              table={table}
              pos={positions[id]}
              onMouseDown={handleMouseDown}
              isHighlighted={highlighted.has(id)}
              highlightedTable={hoveredTable}
              onHover={setHoveredTable}
            />
          ))}
        </g>
      </svg>

      {/* Zoom controls */}
      <div style={{
        position: "absolute", bottom: 20, right: 20, display: "flex", flexDirection: "column",
        gap: 6, zIndex: 10
      }}>
        {[
          { label: "+", action: () => setScale(s => Math.min(2, s + 0.1)) },
          { label: "⟳", action: () => { setScale(0.62); setPan({ x: 0, y: 0 }); setPositions(INITIAL_POSITIONS); } },
          { label: "−", action: () => setScale(s => Math.max(0.25, s - 0.1)) },
        ].map(btn => (
          <button key={btn.label} onClick={btn.action} style={{
            width: 32, height: 32, background: COLORS.surface, border: `1px solid ${COLORS.border}`,
            color: COLORS.text, borderRadius: 6, cursor: "pointer", fontSize: 14,
            display: "flex", alignItems: "center", justifyContent: "center",
          }}>{btn.label}</button>
        ))}
      </div>

      {/* Scale indicator */}
      <div style={{
        position: "absolute", bottom: 20, left: 20, zIndex: 10,
        color: COLORS.textMuted, fontSize: 11, fontFamily: "monospace",
        background: COLORS.surface, border: `1px solid ${COLORS.border}`,
        padding: "4px 10px", borderRadius: 6,
      }}>
        {Math.round(scale * 100)}%
      </div>
    </div>
  );
}

function LegendItem({ color, label }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
      <div style={{ width: 10, height: 10, borderRadius: 2, background: color, flexShrink: 0 }} />
      <span style={{ color: COLORS.textMuted, fontSize: 11, fontFamily: "monospace" }}>{label}</span>
    </div>
  );
}
