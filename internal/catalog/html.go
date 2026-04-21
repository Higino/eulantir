package catalog

import (
	"bytes"
	"encoding/json"
	"html/template"
)

const dashboardTmpl = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{{.PipelineName}} — Catalog</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: 'SF Mono', 'Fira Code', 'Cascadia Code', monospace;
      background: #1e1e2e; color: #cdd6f4; font-size: 13px; line-height: 1.5;
    }
    #toolbar {
      display: flex; align-items: center; gap: 16px;
      padding: 10px 20px; border-bottom: 1px solid #313244;
      background: #181825; height: 49px; position: sticky; top: 0; z-index: 10;
    }
    #toolbar h1 { font-size: 14px; font-weight: 600; color: #cba6f7; white-space: nowrap; }
    #toolbar small { color: #6c7086; font-size: 11px; }
    #toolbar .sep { width: 1px; height: 20px; background: #313244; }
    #content { padding: 24px; display: flex; flex-direction: column; gap: 24px; max-width: 1200px; margin: 0 auto; }

    .dataset {
      background: #181825; border: 1px solid #313244; border-radius: 8px; overflow: hidden;
    }
    .dataset-header {
      padding: 14px 20px; border-bottom: 1px solid #313244;
      display: flex; align-items: baseline; gap: 12px;
    }
    .dataset-name { font-size: 14px; font-weight: 600; color: #cba6f7; }
    .dataset-meta { color: #6c7086; font-size: 11px; }

    table { width: 100%; border-collapse: collapse; }
    thead th {
      padding: 8px 16px; text-align: left; font-size: 11px; font-weight: 600;
      color: #6c7086; text-transform: uppercase; letter-spacing: 0.05em;
      border-bottom: 1px solid #313244; background: #1e1e2e;
    }
    tbody tr:hover { background: #24243e; }
    td { padding: 9px 16px; border-bottom: 1px solid #1e1e2e; vertical-align: middle; }
    tr:last-child td { border-bottom: none; }

    .field-name { color: #89dceb; font-weight: 600; }

    .type-badge {
      display: inline-block; padding: 1px 7px; border-radius: 10px;
      font-size: 11px; font-weight: 600; white-space: nowrap;
    }
    .type-string  { background: #1e3a5f; color: #89b4fa; }
    .type-number  { background: #1e3a2a; color: #a6e3a1; }
    .type-boolean { background: #3a3a1e; color: #f9e2af; }
    .type-mixed   { background: #3a2a1e; color: #fab387; }
    .type-object  { background: #2e1e3a; color: #cba6f7; }
    .type-array   { background: #1e3a3a; color: #94e2d5; }

    .null-cell { display: flex; align-items: center; gap: 8px; }
    .null-bar-track { width: 60px; height: 4px; background: #313244; border-radius: 2px; flex-shrink: 0; }
    .null-bar-fill  { height: 4px; border-radius: 2px; background: #f38ba8; }
    .null-text { color: #a6adc8; min-width: 34px; }

    .cardinality { color: #a6adc8; }

    .pattern-chip {
      display: inline-block; padding: 1px 7px; border-radius: 10px;
      font-size: 11px; background: #313244; color: #f5c2e7;
    }

    .examples { color: #6c7086; font-size: 11px; max-width: 260px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .range { color: #a6adc8; font-size: 11px; }
  </style>
</head>
<body>
  <div id="toolbar">
    <h1>{{.PipelineName}}</h1>
    <span class="sep"></span>
    <small>data catalog · {{.SourceCount}} source{{if gt .SourceCount 1}}s{{end}} · profiled {{.ProfiledAt}}</small>
  </div>
  <div id="content" id="datasets"></div>

  <script>
    const PROFILES = {{.ProfilesJSON}};

    function typeClass(t) {
      return 'type-' + (t || 'string');
    }

    function nullBar(rate) {
      const pct = (rate * 100).toFixed(1);
      const fill = Math.round(rate * 60);
      return '<div class="null-cell">' +
        '<div class="null-bar-track"><div class="null-bar-fill" style="width:' + fill + 'px"></div></div>' +
        '<span class="null-text">' + pct + '%</span>' +
      '</div>';
    }

    function notesCell(f) {
      const parts = [];
      if (f.pattern)
        parts.push('<span class="pattern-chip">' + f.pattern + '</span>');
      if (f.min !== undefined && f.max !== undefined)
        parts.push('<span class="range">min ' + f.min.toPrecision(4) + ' · max ' + f.max.toPrecision(4) + '</span>');
      if (f.min_len !== undefined)
        parts.push('<span class="range">len ' + f.min_len + '–' + f.max_len + '</span>');
      return parts.join(' ');
    }

    function examplesCell(f) {
      if (!f.examples || !f.examples.length) return '';
      return '<span class="examples" title="' + f.examples.join(', ') + '">' +
        f.examples.slice(0, 3).join(', ') +
      '</span>';
    }

    const content = document.getElementById('content');

    PROFILES.forEach(p => {
      const div = document.createElement('div');
      div.className = 'dataset';

      let rows = '';
      (p.fields || []).forEach(f => {
        rows += '<tr>' +
          '<td class="field-name">' + f.name + '</td>' +
          '<td><span class="type-badge ' + typeClass(f.type) + '">' + f.type + '</span></td>' +
          '<td>' + nullBar(f.null_rate) + '</td>' +
          '<td class="cardinality">' + f.cardinality.toLocaleString() + '</td>' +
          '<td>' + notesCell(f) + '</td>' +
          '<td>' + examplesCell(f) + '</td>' +
        '</tr>';
      });

      div.innerHTML =
        '<div class="dataset-header">' +
          '<span class="dataset-name">' + p.source + '</span>' +
          '<span class="dataset-meta">' + p.total_seen.toLocaleString() + ' records sampled · ' +
            (p.fields || []).length + ' fields</span>' +
        '</div>' +
        '<table>' +
          '<thead><tr>' +
            '<th>Field</th><th>Type</th><th>Null rate</th>' +
            '<th>Cardinality</th><th>Notes</th><th>Examples</th>' +
          '</tr></thead>' +
          '<tbody>' + rows + '</tbody>' +
        '</table>';

      content.appendChild(div);
    });
  </script>
</body>
</html>`

type dashboardData struct {
	PipelineName string
	SourceCount  int
	ProfiledAt   string
	ProfilesJSON template.JS
}

var dashTmpl = template.Must(template.New("catalog").Parse(dashboardTmpl))

// RenderDashboard returns a self-contained HTML page for the given profiles.
func RenderDashboard(pipelineName string, profiles any, profiledAt string) ([]byte, error) {
	j, err := json.Marshal(profiles)
	if err != nil {
		return nil, err
	}

	// count profiles — profiles is []* profiler.DatasetProfile but we receive it as any
	// to avoid a circular import; we just measure the JSON array length via reflection.
	type counter interface{ Len() int }
	count := 0
	if raw, ok := json.RawMessage(j), true; ok {
		var arr []json.RawMessage
		if json.Unmarshal(raw, &arr) == nil {
			count = len(arr)
		}
	}

	var buf bytes.Buffer
	if err := dashTmpl.Execute(&buf, dashboardData{
		PipelineName: pipelineName,
		SourceCount:  count,
		ProfiledAt:   profiledAt,
		ProfilesJSON: template.JS(j), //nolint:gosec — profile data is internal, not user HTML
	}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
