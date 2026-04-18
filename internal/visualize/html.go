package visualize

import (
	"bytes"
	"encoding/json"
	"html/template"
)

const htmlTmpl = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{{.PipelineName}} — Eulantir</title>
  <script src="https://unpkg.com/vis-network@9.1.9/dist/vis-network.min.js"></script>
  <style>
    *, *::before, *::after { box-sizing: border-box; }
    body { margin: 0; font-family: 'SF Mono', 'Fira Code', 'Cascadia Code', monospace; background: #1e1e2e; color: #cdd6f4; }
    #toolbar {
      display: flex; align-items: center; gap: 16px;
      padding: 10px 20px; border-bottom: 1px solid #313244;
      background: #181825; height: 49px;
    }
    #toolbar h1 { margin: 0; font-size: 14px; font-weight: 600; color: #cba6f7; white-space: nowrap; }
    #live-badge {
      font-size: 11px; padding: 2px 8px; border-radius: 10px;
      background: #313244; color: #6c7086;
    }
    .legend { display: flex; gap: 10px; margin-left: auto; font-size: 11px; flex-wrap: wrap; }
    .chip { display: inline-flex; align-items: center; gap: 5px; color: #a6adc8; }
    .dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
    .sep { width: 1px; height: 20px; background: #313244; }
    #network { height: calc(100vh - 49px); background: #1e1e2e; }
  </style>
</head>
<body>
  <div id="toolbar">
    <h1>{{.PipelineName}}</h1>
    <span id="live-badge">{{if .LiveMode}}connecting…{{else}}static{{end}}</span>
    <div class="legend">
      <span class="chip"><span class="dot" style="background:#45475a"></span>pending</span>
      <span class="chip"><span class="dot" style="background:#89b4fa"></span>running</span>
      <span class="chip"><span class="dot" style="background:#a6e3a1"></span>success</span>
      <span class="chip"><span class="dot" style="background:#f38ba8"></span>failed</span>
      <span class="chip"><span class="dot" style="background:#fab387"></span>skipped</span>
      <span class="sep"></span>
      <span class="chip">○ source</span>
      <span class="chip">□ sink</span>
      <span class="chip">◇ transform</span>
    </div>
  </div>
  <div id="network"></div>

  <script>
    const GRAPH = {{.GraphJSON}};

    const COLOR = {
      pending: { bg: "#313244", border: "#45475a", font: "#6c7086" },
      running: { bg: "#1e3a5f", border: "#89b4fa", font: "#89b4fa" },
      success: { bg: "#1e3a2a", border: "#a6e3a1", font: "#a6e3a1" },
      failed:  { bg: "#3a1e2a", border: "#f38ba8", font: "#f38ba8" },
      skipped: { bg: "#3a2a1e", border: "#fab387", font: "#fab387" },
    };
    const SHAPE = { source: "ellipse", sink: "box", transform: "diamond" };

    function toVisNode(n) {
      const c = COLOR[n.status] || COLOR.pending;
      return {
        id: n.id,
        label: n.label,
        shape: SHAPE[n.kind] || "ellipse",
        color: { background: c.bg, border: c.border, highlight: { background: c.bg, border: c.border } },
        font: { color: c.font, face: "monospace", size: 13, multi: false },
        borderWidth: 2,
        margin: 10,
      };
    }

    const nodeDS = new vis.DataSet(GRAPH.nodes.map(toVisNode));
    const edgeDS = new vis.DataSet((GRAPH.edges || []).map(e => ({
      from: e.from,
      to: e.to,
      arrows: "to",
      color: { color: "#45475a", highlight: "#6c7086" },
      smooth: { type: "cubicBezier", forceDirection: "horizontal", roundness: 0.4 },
    })));

    new vis.Network(
      document.getElementById("network"),
      { nodes: nodeDS, edges: edgeDS },
      {
        layout: {
          hierarchical: {
            enabled: true,
            direction: "LR",
            sortMethod: "directed",
            levelSeparation: 220,
            nodeSpacing: 130,
            treeSpacing: 180,
          },
        },
        physics: false,
        interaction: { hover: true, zoomView: true, dragView: true },
      }
    );

    {{if .LiveMode}}
    const badge = document.getElementById("live-badge");

    const es = new EventSource("/events");
    es.onopen = () => {
      badge.textContent = "live";
      badge.style.color = "#a6e3a1";
      badge.style.background = "#1e3a2a";
    };
    es.onerror = () => {
      badge.textContent = "disconnected";
      badge.style.color = "#f38ba8";
      badge.style.background = "#3a1e2a";
    };
    es.onmessage = e => {
      const r = JSON.parse(e.data);
      if (r.status === "done") {
        es.close();
        badge.textContent = "finished";
        badge.style.color = "#a6e3a1";
        badge.style.background = "#1e3a2a";
        return;
      }
      const c = COLOR[r.status] || COLOR.pending;
      nodeDS.update({
        id: r.nodeID,
        color: { background: c.bg, border: c.border, highlight: { background: c.bg, border: c.border } },
        font: { color: c.font, face: "monospace", size: 13 },
      });
    };
    {{end}}
  </script>
</body>
</html>`

type templateData struct {
	PipelineName string
	GraphJSON    template.JS
	LiveMode     bool
}

var tmpl = template.Must(template.New("pipeline").Parse(htmlTmpl))

// RenderHTML generates a self-contained HTML page for the given graph.
// liveMode=true injects SSE client code that connects to /events.
func RenderHTML(g GraphData, liveMode bool) ([]byte, error) {
	graphJSON, err := json.Marshal(g)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, templateData{
		PipelineName: g.PipelineName,
		GraphJSON:    template.JS(graphJSON), //nolint:gosec — graph data is internal, not user-controlled HTML
		LiveMode:     liveMode,
	}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
