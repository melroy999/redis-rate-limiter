#!/usr/bin/env python3
"""Generate an interactive HTML benchmark report from pytest-benchmark results.

Reads all ``run-*.json`` files (produced by the benchmark conftest) and
generates a self-contained HTML file with interactive Plotly.js charts.
A run selector at the top lets you toggle historical runs on and off for
side-by-side comparison.

Usage::

    python scripts/benchmark_report.py [DATA_DIR]

``DATA_DIR`` defaults to ``benchmark-results`` (host mount) or ``.benchmarks``
(container path), whichever contains ``run-*.json`` files.
"""

import json
import sys
from pathlib import Path


def _find_data_dir() -> Path:
    for candidate in [Path("benchmark-results"), Path(".benchmarks")]:
        if list(candidate.glob("run-*.json")):
            return candidate
    print(
        "No run-*.json files found. Run benchmarks first:\n"
        "  docker compose --profile benchmark up --build "
        "--abort-on-container-exit --exit-code-from benchmark"
    )
    sys.exit(1)


def _load_runs(data_dir: Path) -> list[dict]:
    run_files = sorted(data_dir.glob("run-*.json"))
    runs = []
    for path in run_files:
        try:
            with open(path) as f:
                data = json.load(f)
        except (json.JSONDecodeError, ValueError):
            print(f"Skipping invalid JSON: {path.name}", file=sys.stderr)
            continue
        data["_filename"] = path.name
        runs.append(data)
    return runs


def _generate_html(runs: list[dict]) -> str:
    runs_json = json.dumps(runs, separators=(",", ":")).replace("</", "<\\/")
    return _HTML_TEMPLATE.replace("__RUNS_JSON__", runs_json)


def main() -> None:
    data_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else _find_data_dir()
    runs = _load_runs(data_dir)

    if not runs:
        print(f"No run-*.json files found in {data_dir}")
        sys.exit(1)

    html = _generate_html(runs)
    output_path = data_dir / "report.html"
    output_path.write_text(html, encoding="utf-8")
    print(f"Report written to {output_path} ({len(runs)} run(s))")


_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Benchmark Report</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js" charset="utf-8"></script>
<style>
:root {
  --bg: #f8f9fa; --card: #fff; --border: #e0e0e0;
  --text: #212529; --muted: #6c757d;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  background: var(--bg); color: var(--text);
  line-height: 1.5; padding: 32px;
  max-width: 1600px; margin: 0 auto;
}
h1 { font-size: 1.75rem; font-weight: 600; margin-bottom: 4px; }
.subtitle { color: var(--muted); font-size: 0.875rem; margin-bottom: 16px; }
.section {
  background: var(--card); border: 1px solid var(--border);
  border-radius: 8px; padding: 24px; margin-bottom: 24px;
}
.section h2 {
  font-size: 1.25rem; font-weight: 600; margin-bottom: 4px;
  padding-bottom: 8px; border-bottom: 2px solid var(--border);
}
.desc { color: var(--muted); font-size: 0.85rem; margin-bottom: 16px; }
.g2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
.hidden { display: none; }
.soak-legend {
  width: 100%; border-collapse: collapse; font-size: 0.8rem;
  margin-bottom: 16px; color: var(--text);
}
.soak-legend th, .soak-legend td {
  padding: 4px 10px; border: 1px solid var(--border); text-align: left;
}
.soak-legend th { background: var(--bg); font-weight: 600; }
.soak-legend td:first-child { white-space: nowrap; font-weight: 500; }
#run-selector { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
#run-selector label {
  display: inline-flex; align-items: center; gap: 6px;
  padding: 4px 12px; border-radius: 6px; font-size: 0.85rem;
  border: 1px solid var(--border); cursor: pointer;
  transition: background 0.15s, border-color 0.15s;
}
#run-selector label:hover { background: #e9ecef; }
#run-selector label.checked { border-color: #1565C0; background: #E3F2FD; }
.run-dot {
  display: inline-block; width: 10px; height: 10px;
  border-radius: 50%; flex-shrink: 0;
}
@media (max-width: 1200px) { .g2 { grid-template-columns: 1fr; } body { padding: 16px; } }
</style>
</head>
<body>
<h1>Benchmark Report</h1>
<p class="subtitle">Toggle runs to compare. Latest run is selected by default.</p>

<div class="section" id="s-selector">
  <h2>Runs</h2>
  <div id="run-selector"></div>
</div>

<div class="section" id="s-cont">
  <h2>Contention Scaling</h2>
  <p class="desc">Aggregate throughput vs. drainer count (N). Line style distinguishes runs; color distinguishes variants.</p>
  <div class="g2">
    <div id="cont-burst"></div>
    <div id="cont-steady"></div>
    <div id="cont-saturated"></div>
    <div id="cont-concurrency_capped"></div>
  </div>
</div>

<div class="section" id="s-fair">
  <h2>Per-Drainer Fairness</h2>
  <p class="desc">How evenly throughput is distributed across drainers (latest selected run only).</p>
  <div class="g2">
    <div id="fair-burst"></div>
    <div id="fair-steady"></div>
    <div id="fair-saturated"></div>
    <div id="fair-concurrency_capped"></div>
  </div>
</div>

<div class="section" id="s-lat-e2e">
  <h2>End-to-End Latency</h2>
  <p class="desc">Median latency for full operation paths (Python overhead plus Redis round-trip). Whiskers extend to p99. Diamond markers indicate mean; distance from bar top shows tail skew.</p>
  <div id="lat-e2e-chart"></div>
</div>

<div class="section" id="s-lat-script">
  <h2>Script Latency</h2>
  <p class="desc">Median latency for Lua VM execution and EVALSHA round-trips. Whiskers extend to p99. Diamond markers indicate mean; distance from bar top shows tail skew. Native Redis commands (e.g. publish, set_nx) only appear in evalsha-wallclock groups; they have no lua-vm measurement.</p>
  <div id="lat-script-chart"></div>
</div>

<div class="section" id="s-lat-other">
  <h2>Other Latency</h2>
  <p class="desc">Median latency for remaining operations. Whiskers extend to p99. Diamond markers indicate mean; distance from bar top shows tail skew.</p>
  <div id="lat-other-chart"></div>
</div>

<div class="section" id="s-buf">
  <h2>Buffer Depth Scaling</h2>
  <p class="desc">How consume latency varies with the number of pending tasks pre-filled in the sorted set. No tasks are added during the measurement; the buffer is static.</p>
  <div id="buf-chart"></div>
</div>

<div class="section" id="s-decomp">
  <h2>Cost Decomposition</h2>
  <p class="desc">Per-iteration breakdown into Redis round-trip time and Python overhead (latest selected run only).</p>
  <div id="decomp-chart"></div>
</div>

<div class="section" id="s-tail">
  <h2>Tail Latency Ratios</h2>
  <p class="desc">p99 / median ratio per benchmark, split by measurement group. Dashed lines mark the threshold (latest selected run only).</p>
  <div id="tail-container" class="g2"></div>
</div>

<div class="section" id="s-soak">
  <h2>Soak Test</h2>
  <p class="desc">Time-series metrics from sustained load tests. Each variant (sync/async) is plotted separately. Healthy metrics should be flat; any statistically significant linear trend that exceeds its safety threshold is flagged as a failure in the test output.</p>
  <table class="soak-legend"><tr>
    <th>Chart</th><th>What it measures</th><th>Expected trend</th>
  </tr><tr>
    <td>Process Memory</td><td>VmRSS and VmSize from /proc/self/status</td><td>Flat after warmup; growth indicates a Python object or arena leak</td>
  </tr><tr>
    <td>Threads / FDs</td><td>Active thread count and open file descriptors</td><td>Flat; growth indicates threads not joined or sockets not closed</td>
  </tr><tr>
    <td>Connection Pool</td><td>Redis pool connections (created, available, in use)</td><td>Flat; growth in created connections indicates a pool leak</td>
  </tr><tr>
    <td>Redis Memory</td><td>Server-wide used_memory and used_memory_rss</td><td>Flat once the buffer reaches steady state</td>
  </tr><tr>
    <td>Buffer Depth</td><td>ZCARD of the buffer ZSET</td><td>Oscillates near the watermark (sawtooth from feeder refills)</td>
  </tr><tr>
    <td>Concurrency / DLQ</td><td>ZCARD of the concurrency ZSET and LLEN of the DLQ</td><td>Both near zero with instant no-op tasks</td>
  </tr><tr>
    <td>Key Memory</td><td>Per-key MEMORY USAGE of buffer and concurrency ZSETs</td><td>Proportional to cardinality; growth at stable cardinality indicates a memory leak in Redis key management</td>
  </tr><tr>
    <td>Throughput</td><td>Cumulative successful dispatches</td><td>Linear (constant rate); concavity signals degradation</td>
  </tr><tr>
    <td>HeartbeatScheduler</td><td>Entries (active registrations) and heap (lazy-delete queue)</td><td>Bounded; heap growing faster than entries indicates stale accumulation</td>
  </tr><tr>
    <td>GC Collections</td><td>Cumulative cyclic GC collections per generation (gen0, gen1, gen2)</td><td>Flat when no reference cycles exist (reference counting handles cleanup); accelerating gen2 indicates memory pressure from cyclic references</td>
  </tr></table>
  <div id="soak-container" class="g2"></div>
</div>

<div class="section" id="s-growth">
  <h2>Buffer Growth Under Write Pressure</h2>
  <p class="desc">Drain throughput (tasks/s) vs buffer depth while a feeder continuously writes faster than drains consume. A flat trend line means drain performance is independent of buffer size under dynamic growth.</p>
  <div id="growth-depth"></div>
</div>

<div class="section" id="s-acq-cont">
  <h2>Latency Scaling Under Contention</h2>
  <p class="desc">Per-call latency vs concurrent caller count. acquire_lua and release_lua measure the raw Lua script (EVALSHA); limiter_acquire measures the full Python acquire() cycle (schedule, drain loop, consume.lua, BLPOP signal, release). Each bar shows median latency; whiskers extend to p99.</p>
  <div class="g2" id="acq-cont-container"></div>
</div>

<script>
var RUNS = __RUNS_JSON__;

/* ---- Palette ---- */
var RUN_COLORS = ['#1565C0','#C62828','#2E7D32','#E65100','#6A1B9A','#00838F','#AD1457','#F9A825'];
var DASHES = ['solid','dash','dot','dashdot','longdash','longdashdot'];
var VC = {processes:'#1565C0', coroutines:'#2E7D32', threads:'#E65100'};
var GC = {
  'lua-vm':'#6A1B9A', 'evalsha-wallclock':'#1565C0',
  'async-evalsha-wallclock':'#2E7D32', 'end-to-end':'#C62828',
  'async-end-to-end':'#00695C', 'decomposition-sync':'#E65100',
  'decomposition-async':'#00838F', 'pubsub':'#F9A825', 'default':'#455A64'
};
var DRAINER_PALETTE = ['#1565C0','#2E7D32','#E65100','#6A1B9A','#00838F','#AD1457','#F9A825','#4E342E'];
var CFG = {responsive:true, displayModeBar:true, modeBarButtonsToRemove:['lasso2d','select2d']};
var LB = {
  font:{family:'-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif', size:12},
  plot_bgcolor:'#fafafa', paper_bgcolor:'white',
  margin:{t:48,r:24,b:80,l:72}, hoverlabel:{font:{size:12}}
};

var LAT_SECTIONS = [
  {key:'e2e', groups:['end-to-end','async-end-to-end']},
  {key:'script', groups:['lua-vm','evalsha-wallclock','async-evalsha-wallclock']},
  {key:'other', groups:null}
];

function us(s){return s*1e6;}
function hide(id){var e=document.getElementById(id);if(e)e.classList.add('hidden');}
function show(id){var e=document.getElementById(id);if(e)e.classList.remove('hidden');}
function M(base,extra){var o={};for(var k in base)o[k]=base[k];for(var k in extra)o[k]=extra[k];return o;}

/* ---- Run labels ---- */
function runLabel(run, idx) {
  var ts = run.timestamp || run._filename || ('Run ' + idx);
  if (ts.length > 20) ts = ts.substring(0, 19).replace('T', ' ');
  return ts;
}

/* ---- State ---- */
var selected = [];

function getSelected() {
  return selected.map(function(i){ return RUNS[i]; });
}
function latestSelected() {
  if (selected.length === 0) return null;
  return RUNS[selected[selected.length - 1]];
}

/* ---- Run selector ---- */
function buildSelector() {
  var container = document.getElementById('run-selector');
  container.innerHTML = '';
  RUNS.forEach(function(run, idx) {
    var lbl = document.createElement('label');
    lbl.id = 'run-lbl-' + idx;
    var cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.value = idx;
    cb.style.display = 'none';
    if (idx === RUNS.length - 1) { cb.checked = true; selected.push(idx); lbl.classList.add('checked'); }
    var dot = document.createElement('span');
    dot.className = 'run-dot';
    dot.style.background = RUN_COLORS[idx % RUN_COLORS.length];
    var txt = document.createTextNode(runLabel(run, idx));
    cb.addEventListener('change', function() {
      if (this.checked) { selected.push(idx); lbl.classList.add('checked'); }
      else { selected = selected.filter(function(i){return i!==idx;}); lbl.classList.remove('checked'); }
      selected.sort();
      renderAll();
    });
    lbl.appendChild(cb); lbl.appendChild(dot); lbl.appendChild(txt);
    container.appendChild(lbl);
  });
}

/* ================================================================== */
/* Chart renderers                                                     */
/* ================================================================== */

function renderContention() {
  var runs = getSelected();
  var anyContention = runs.some(function(r){ return r.contention && r.contention.length > 0; });
  if (!anyContention) { hide('s-cont'); hide('s-fair'); return; }
  show('s-cont');

  var scenarios = ['burst','steady','saturated','concurrency_capped'];
  var vorder = ['processes','coroutines','threads'];

  scenarios.forEach(function(sc) {
    var traces = [];
    runs.forEach(function(run, ri) {
      var runIdx = selected[ri];
      var sd = (run.contention||[]).filter(function(r){ return r.scenario === sc; });
      var variants = vorder.filter(function(v){ return sd.some(function(r){ return r.variant === v; }); });
      var lbl = runs.length > 1 ? ' (' + runLabel(run, runIdx) + ')' : '';

      variants.forEach(function(v) {
        var vd = sd.filter(function(r){ return r.variant === v; }).sort(function(a,b){ return a.num_drainers - b.num_drainers; });
        traces.push({
          x: vd.map(function(r){ return r.num_drainers; }),
          y: vd.map(function(r){ return r.duration > 0 ? r.total / r.duration : 0; }),
          name: v + lbl,
          type: 'scatter', mode: 'lines+markers',
          line: { color: VC[v] || '#999', width: 2, dash: DASHES[ri % DASHES.length] },
          marker: { size: 7, symbol: ri === 0 ? 'circle' : 'diamond' },
          hovertemplate: '%{y:.1f} tasks/s<extra>' + v + lbl + '</extra>',
          legendgroup: v + '_' + runIdx
        });
      });
    });

    var title = sc.replace(/_/g, ' ').replace(/\b[a-z]/g, function(c){ return c.toUpperCase(); });
    Plotly.newPlot('cont-' + sc, traces, M(LB, {
      title: { text: title, font: { size: 14 } },
      xaxis: { title: 'Drainer Count (N)', dtick: 1 },
      yaxis: { title: 'Throughput (tasks/s)', rangemode: 'tozero' },
      legend: { orientation: 'h', y: -0.25 },
      height: 400
    }), CFG);
  });

  renderFairness();
}

function renderFairness() {
  var latest = latestSelected();
  if (!latest || !latest.contention || latest.contention.length === 0) { hide('s-fair'); return; }
  show('s-fair');

  var scenarios = ['burst','steady','saturated','concurrency_capped'];
  var vorder = ['processes','coroutines','threads'];

  scenarios.forEach(function(sc) {
    var sd = latest.contention.filter(function(r){ return r.scenario === sc; });
    var variants = vorder.filter(function(v){ return sd.some(function(r){ return r.variant === v; }); });

    var items = [];
    variants.forEach(function(v) {
      var vd = sd.filter(function(r){ return r.variant === v; }).sort(function(a,b){ return a.num_drainers - b.num_drainers; });
      vd.forEach(function(r){ items.push({ label: v + ' N=' + r.num_drainers, per_drainer: r.per_drainer, duration: r.duration }); });
    });
    if (items.length === 0) return;

    var maxD = Math.max.apply(null, items.map(function(i){ return i.per_drainer.length; }));
    var traces = [];
    for (var d = 0; d < maxD; d++) {
      (function(idx) {
        traces.push({
          x: items.map(function(i){ return i.label; }),
          y: items.map(function(i){ return idx < i.per_drainer.length ? (i.duration > 0 ? i.per_drainer[idx] / i.duration : 0) : 0; }),
          name: 'Drainer ' + idx, type: 'bar',
          marker: { color: DRAINER_PALETTE[idx % DRAINER_PALETTE.length] },
          hovertemplate: '%{y:.1f} tasks/s<extra>drainer ' + idx + '</extra>'
        });
      })(d);
    }

    var title = sc.replace(/_/g, ' ').replace(/\b[a-z]/g, function(c){ return c.toUpperCase(); });
    Plotly.newPlot('fair-' + sc, traces, M(LB, {
      title: { text: title + ' (per-drainer)', font: { size: 14 } },
      xaxis: { title: 'Variant / Drainer Count', tickangle: -30 },
      yaxis: { title: 'Rate per Drainer (tasks/s)' },
      barmode: 'stack', legend: { orientation: 'h', y: -0.35 },
      height: 420
    }), CFG);
  });
}

/* ------------------------------------------------------------------ */
/* Operation Latency (split into end-to-end, script, other)            */
/* ------------------------------------------------------------------ */
function renderLatency() {
  var runs = getSelected();
  var allBench = [];
  runs.forEach(function(r){ (r.benchmarks||[]).forEach(function(b){ allBench.push(b); }); });
  var regular = allBench.filter(function(b){ return !b.name.includes('buffer_depth'); });
  var known = [];
  LAT_SECTIONS.forEach(function(s){ if(s.groups) known = known.concat(s.groups); });

  LAT_SECTIONS.forEach(function(sec) {
    var sectionId = 's-lat-' + sec.key;
    var chartId = 'lat-' + sec.key + '-chart';
    var catGroups;
    if (sec.groups) {
      catGroups = sec.groups;
    } else {
      var allGroups = [];
      regular.forEach(function(b){ if(allGroups.indexOf(b.group)===-1) allGroups.push(b.group); });
      catGroups = allGroups.filter(function(g){ return known.indexOf(g)===-1; });
    }

    var hasBench = regular.some(function(b){ return catGroups.indexOf(b.group)!==-1; });
    if (!hasBench) { hide(sectionId); return; }
    show(sectionId);

    var traces = [];
    runs.forEach(function(run, ri) {
      var runIdx = selected[ri];
      var rb = (run.benchmarks||[]).filter(function(b){
        return !b.name.includes('buffer_depth') && catGroups.indexOf(b.group)!==-1;
      });
      var groups = []; rb.forEach(function(b){ if(groups.indexOf(b.group)===-1) groups.push(b.group); }); groups.sort();
      var opNames = []; rb.forEach(function(b){ if(opNames.indexOf(b.name)===-1) opNames.push(b.name); }); opNames.sort();
      var lbl = runs.length > 1 ? ' (' + runLabel(run, runIdx) + ')' : '';

      groups.forEach(function(g) {
        var gb = rb.filter(function(b){ return b.group === g; });
        var nameMap = {}; gb.forEach(function(b){ nameMap[b.name] = b; });
        var ops = opNames.filter(function(n){ return !!nameMap[n]; });
        var lgGroup = g + '_' + runIdx;
        traces.push({
          x: ops, y: ops.map(function(n){ return us(nameMap[n].stats.median); }),
          error_y: {
            type: 'data', symmetric: false, visible: true, thickness: 1.5,
            array: ops.map(function(n){ return us(nameMap[n].stats.p99 - nameMap[n].stats.median); }),
            arrayminus: ops.map(function(){ return 0; })
          },
          name: g + lbl, type: 'bar',
          marker: { color: GC[g] || '#999', opacity: ri === 0 ? 1.0 : 0.5 },
          customdata: ops.map(function(n){ return [us(nameMap[n].stats.p99), us(nameMap[n].stats.mean)]; }),
          hovertemplate: 'median: %{y:.2f}\\u00b5s<br>mean: %{customdata[1]:.2f}\\u00b5s<br>p99: %{customdata[0]:.2f}\\u00b5s<extra>' + g + lbl + '</extra>',
          legendgroup: lgGroup, offsetgroup: lgGroup
        });
        traces.push({
          x: ops, y: ops.map(function(n){ return us(nameMap[n].stats.mean); }),
          type: 'scatter', mode: 'markers',
          marker: { symbol: 'diamond', size: 7, color: '#222', line: { color: '#fff', width: 1 } },
          name: 'mean', legendgroup: lgGroup, offsetgroup: lgGroup,
          showlegend: false,
          hovertemplate: 'mean: %{y:.2f}\\u00b5s<extra>' + g + lbl + '</extra>'
        });
      });
    });

    Plotly.newPlot(chartId, traces, M(LB, {
      xaxis: { title: 'Operation', tickangle: -30 },
      yaxis: { title: 'Latency (\\u00b5s)', type: 'log' },
      barmode: 'group', scattermode: 'group', legend: { orientation: 'h', y: -0.3 },
      height: 500
    }), CFG);
  });
}

/* ------------------------------------------------------------------ */
/* Buffer Depth Scaling                                                */
/* ------------------------------------------------------------------ */
function renderBufferDepth() {
  var runs = getSelected();
  var anyDepth = runs.some(function(r){ return (r.benchmarks||[]).some(function(b){ return b.name.includes('buffer_depth'); }); });
  if (!anyDepth) { hide('s-buf'); return; }
  show('s-buf');

  var traces = [];
  runs.forEach(function(run, ri) {
    var runIdx = selected[ri];
    var db = (run.benchmarks||[]).filter(function(b){ return b.name.includes('buffer_depth'); });
    var groups = []; db.forEach(function(b){ if(groups.indexOf(b.group)===-1)groups.push(b.group); }); groups.sort();
    var lbl = runs.length > 1 ? ' (' + runLabel(run, runIdx) + ')' : '';

    groups.forEach(function(g) {
      var gb = db.filter(function(b){ return b.group === g; }).sort(function(a,b){ return (a.params.buffer_depth||0) - (b.params.buffer_depth||0); });
      traces.push({
        x: gb.map(function(b){ return b.params.buffer_depth; }),
        y: gb.map(function(b){ return us(b.stats.median); }),
        error_y: {
          type: 'data', symmetric: false, visible: true, thickness: 1.5,
          array: gb.map(function(b){ return us(b.stats.p99 - b.stats.median); }),
          arrayminus: gb.map(function(){ return 0; })
        },
        name: g + lbl, type: 'scatter', mode: 'lines+markers',
        line: { color: GC[g] || '#999', width: 2, dash: DASHES[ri % DASHES.length] },
        marker: { size: 6, symbol: ri === 0 ? 'circle' : 'diamond' },
        customdata: gb.map(function(b){ return [us(b.stats.p99), us(b.stats.mean)]; }),
        hovertemplate: 'depth: %{x}<br>median: %{y:.2f}\\u00b5s<br>mean: %{customdata[1]:.2f}\\u00b5s<br>p99: %{customdata[0]:.2f}\\u00b5s<extra>' + g + lbl + '</extra>',
        legendgroup: g + '_' + runIdx
      });
    });
  });

  Plotly.newPlot('buf-chart', traces, M(LB, {
    xaxis: { title: 'Buffer Depth', type: 'log' },
    yaxis: { title: 'Latency (\\u00b5s)', type: 'log' },
    legend: { orientation: 'h', y: -0.2 }, height: 450
  }), CFG);
}

/* ------------------------------------------------------------------ */
/* Cost Decomposition (latest selected only)                           */
/* ------------------------------------------------------------------ */
function renderDecomposition() {
  var latest = latestSelected();
  if (!latest || !latest.decomposition || latest.decomposition.length === 0) { hide('s-decomp'); return; }
  show('s-decomp');

  var variants = ['sync','async'];
  var traces = [];
  var rttC = { sync: '#1565C0', async: '#2E7D32' };
  var pyC = { sync: '#64B5F6', async: '#81C784' };

  variants.forEach(function(v) {
    var vd = latest.decomposition.filter(function(d){ return d.variant === v; });
    if (vd.length === 0) return;
    var names = vd.map(function(d){ return d.test_name + ' (' + v + ')'; });
    traces.push({
      x: names, y: vd.map(function(d){ return us(d.rtt_median); }),
      name: 'Redis RTT (' + v + ')', type: 'bar',
      marker: { color: rttC[v] },
      hovertemplate: '%{y:.2f}\\u00b5s<extra>RTT</extra>'
    });
    traces.push({
      x: names, y: vd.map(function(d){ return us(Math.max(0, d.py_median)); }),
      name: 'Python (' + v + ')', type: 'bar',
      marker: { color: pyC[v] },
      hovertemplate: '%{y:.2f}\\u00b5s<extra>Python</extra>'
    });
  });

  Plotly.newPlot('decomp-chart', traces, M(LB, {
    xaxis: { title: 'Operation', tickangle: -30 },
    yaxis: { title: 'Time (\\u00b5s)' },
    barmode: 'stack', legend: { orientation: 'h', y: -0.35 }, height: 450
  }), CFG);
}

/* ------------------------------------------------------------------ */
/* Tail Latency Ratios (per-operation sub-charts, latest selected only)*/
/* ------------------------------------------------------------------ */
function renderTailLatency() {
  var latest = latestSelected();
  if (!latest || !latest.benchmarks || latest.benchmarks.length === 0) { hide('s-tail'); return; }

  var TH = latest.tail_latency_thresholds || {};
  var DT = latest.tail_latency_default_threshold || 8.0;

  var items = latest.benchmarks
    .filter(function(b){ return b.stats.median > 0 && !b.name.includes('buffer_depth'); })
    .map(function(b){
      var ratio = b.stats.p99 / b.stats.median;
      var threshold = TH[b.group] || DT;
      return { name: b.name, group: b.group, ratio: ratio, threshold: threshold, ok: ratio <= threshold };
    });

  if (items.length === 0) { hide('s-tail'); return; }
  show('s-tail');

  var soloGroups = {};
  items.forEach(function(i){ soloGroups[i.group] = (soloGroups[i.group] || 0) + 1; });
  var mergedGroupNames = Object.keys(soloGroups).filter(function(g){ return soloGroups[g] >= 2 && soloGroups[g] <= 4; });
  var mergedItems = items.filter(function(i){ return mergedGroupNames.indexOf(i.group) !== -1; });
  var normalItems = items.filter(function(i){ return mergedGroupNames.indexOf(i.group) === -1; });

  var opNames = [];
  normalItems.forEach(function(i){ if(opNames.indexOf(i.name)===-1) opNames.push(i.name); });
  opNames.sort();

  var container = document.getElementById('tail-container');
  container.innerHTML = '';

  mergedGroupNames.sort();
  mergedGroupNames.forEach(function(grp) {
    var grpItems = mergedItems.filter(function(i){ return i.group === grp; }).sort(function(a,b){ return a.ratio - b.ratio; });
    var divId = 'tail-grp-' + grp.replace(/[^a-z0-9]/g, '-');
    var div = document.createElement('div');
    div.id = divId;
    container.appendChild(div);

    var trace = {
      y: grpItems.map(function(i){ return i.name; }),
      x: grpItems.map(function(i){ return i.ratio; }),
      type: 'bar', orientation: 'h',
      marker: {
        color: GC[grp] || '#455A64',
        line: {
          color: grpItems.map(function(i){ return i.ok ? 'rgba(0,0,0,0)' : '#C62828'; }),
          width: grpItems.map(function(i){ return i.ok ? 0 : 2.5; })
        }
      },
      hovertemplate: '%{y}: %{x:.2f}x<extra></extra>'
    };

    var thresholds = {};
    grpItems.forEach(function(i){ thresholds[i.threshold] = true; });
    var shapes = Object.keys(thresholds).map(function(t){
      return { type: 'line', x0: +t, x1: +t, y0: -0.5, y1: grpItems.length - 0.5, line: { color: '#C62828', width: 2, dash: 'dash' } };
    });
    var annotations = Object.keys(thresholds).map(function(t){
      return { x: +t, y: 1.02, xref: 'x', yref: 'paper', text: t + 'x limit', showarrow: false, font: { color: '#C62828', size: 11 } };
    });

    Plotly.newPlot(divId, [trace], M(LB, {
      title: { text: grp, font: { size: 14 } },
      xaxis: { title: 'p99 / median ratio' },
      yaxis: { automargin: true },
      shapes: shapes, annotations: annotations,
      height: Math.max(200, grpItems.length * 32 + 100),
      margin: { t: 48, r: 24, b: 60, l: 200 }
    }), CFG);
  });

  opNames.forEach(function(op) {
    var opItems = normalItems.filter(function(i){ return i.name === op; }).sort(function(a,b){ return a.ratio - b.ratio; });

    var divId = 'tail-' + op.replace(/[^a-z0-9]/g, '-');
    var div = document.createElement('div');
    div.id = divId;
    container.appendChild(div);

    var trace = {
      y: opItems.map(function(i){ return i.group; }),
      x: opItems.map(function(i){ return i.ratio; }),
      type: 'bar', orientation: 'h',
      marker: {
        color: opItems.map(function(i){ return GC[i.group] || '#455A64'; }),
        line: {
          color: opItems.map(function(i){ return i.ok ? 'rgba(0,0,0,0)' : '#C62828'; }),
          width: opItems.map(function(i){ return i.ok ? 0 : 2.5; })
        }
      },
      hovertemplate: '%{y}: %{x:.2f}x<extra></extra>'
    };

    var thresholds = {};
    opItems.forEach(function(i){ thresholds[i.threshold] = true; });
    var shapes = Object.keys(thresholds).map(function(t){
      return { type: 'line', x0: +t, x1: +t, y0: -0.5, y1: opItems.length - 0.5, line: { color: '#C62828', width: 2, dash: 'dash' } };
    });
    var annotations = Object.keys(thresholds).map(function(t){
      return { x: +t, y: 1.02, xref: 'x', yref: 'paper', text: t + 'x limit', showarrow: false, font: { color: '#C62828', size: 11 } };
    });

    Plotly.newPlot(divId, [trace], M(LB, {
      title: { text: op, font: { size: 14 } },
      xaxis: { title: 'p99 / median ratio' },
      yaxis: { automargin: true },
      shapes: shapes, annotations: annotations,
      height: Math.max(200, opItems.length * 32 + 100),
      margin: { t: 48, r: 24, b: 60, l: 200 }
    }), CFG);
  });
}

/* ---- Soak ---- */
function renderSoak() {
  var container = document.getElementById('soak-container');
  container.innerHTML = '';
  var run = latestSelected();
  if (!run || !run.soak || run.soak.length === 0) { hide('s-soak'); return; }
  show('s-soak');

  var SOAK_COLORS = {sync:'#1565C0', async:'#2E7D32'};
  var GROUPS = [
    {title:'Process Memory', series:['vm_rss_kb','vm_size_kb'], yaxis:'KB'},
    {title:'Threads and File Descriptors', series:['thread_count','fd_count'], yaxis:'count'},
    {title:'Connection Pool', series:['pool_created','pool_available','pool_in_use'], yaxis:'connections'},
    {title:'Redis Memory', series:['redis_used_memory','redis_used_memory_rss'], yaxis:'bytes'},
    {title:'Buffer Depth', series:['buffer_zcard'], yaxis:'count'},
    {title:'Concurrency and DLQ', series:['concurrency_zcard','dlq_llen'], yaxis:'count'},
    {title:'Key Memory', series:['buffer_memory_bytes','concurrency_memory_bytes'], yaxis:'bytes'},
    {title:'Throughput', series:['cumulative_dispatches'], yaxis:'tasks'},
    {title:'HeartbeatScheduler', series:['heartbeat_entries','heartbeat_heap'], yaxis:'entries'},
    {title:'GC Collections', series:['gc_gen0_collections','gc_gen1_collections','gc_gen2_collections'], yaxis:'collections'}
  ];

  GROUPS.forEach(function(grp) {
    var div = document.createElement('div');
    container.appendChild(div);
    var traces = [];
    run.soak.forEach(function(sv) {
      var variant = sv.variant || 'unknown';
      var ts = sv.time_series;
      if (!ts || !ts.elapsed_s) return;
      var x = ts.elapsed_s;
      var color = SOAK_COLORS[variant] || '#455A64';
      grp.series.forEach(function(key, si) {
        if (!ts[key]) return;
        var dash = ['solid','dash','dot','dashdot'][si % 4];
        traces.push({
          x:x, y:ts[key], mode:'lines', name:variant+' '+key,
          line:{color:color, dash:dash, width:1.5}
        });
      });
    });
    if (traces.length === 0) { container.removeChild(div); return; }
    Plotly.newPlot(div, traces, M(LB, {
      title:{text:grp.title, font:{size:13}},
      xaxis:{title:'Elapsed (s)', titlefont:{size:11}},
      yaxis:{title:grp.yaxis, titlefont:{size:11}},
      showlegend:true, legend:{orientation:'h', y:-0.25, font:{size:10}},
      height:280, margin:{t:36,r:16,b:64,l:56}
    }), CFG);
  });
}

/* ---- Buffer Growth ---- */
function renderBufferGrowth() {
  var run = latestSelected();
  if (!run || !run.buffer_growth || run.buffer_growth.length === 0) { hide('s-growth'); return; }
  show('s-growth');

  var bg = run.buffer_growth[0];
  var bins = bg.bins;

  var dx = bins.map(function(b){ return b.buffer_depth; });
  var dy = bins.map(function(b){ return b.throughput; });
  var n = dx.length;
  var sx=0,sy=0,sxx=0,sxy=0,syy=0;
  for(var i=0;i<n;i++){sx+=dx[i];sy+=dy[i];sxx+=dx[i]*dx[i];sxy+=dx[i]*dy[i];syy+=dy[i]*dy[i];}
  var slope=(n*sxy-sx*sy)/(n*sxx-sx*sx);
  var intercept=(sy-slope*sx)/n;
  var ssTot=syy-sy*sy/n;
  var ssRes=0; for(var i=0;i<n;i++){var d=dy[i]-(slope*dx[i]+intercept);ssRes+=d*d;}
  var r2=ssTot>0?1-ssRes/ssTot:0;

  var trendY = dx.map(function(x){ return slope * x + intercept; });
  var slopeLabel = (slope >= 0 ? '+' + slope.toFixed(4) : slope.toFixed(4)) + ', R\\u00b2=' + r2.toFixed(3);

  Plotly.newPlot('growth-depth', [{
    x: dx, y: dy,
    type: 'scatter', mode: 'markers', name: 'measured',
    marker: { color: '#2E7D32', size: 7 },
    hovertemplate: 'depth: %{x}<br>throughput: %{y:.1f}/s<extra>measured</extra>'
  }, {
    x: dx, y: trendY,
    type: 'scatter', mode: 'lines', name: 'trend (' + slopeLabel + ')',
    line: { color: '#C62828', width: 2, dash: 'dash' },
    hovertemplate: 'depth: %{x}<br>fitted: %{y:.1f}/s<extra>trend</extra>'
  }], M(LB, {
    title: { text: 'Throughput vs Buffer Depth', font: { size: 14 } },
    xaxis: { title: 'Buffer Depth' },
    yaxis: { title: 'Throughput (tasks/s)', rangemode: 'tozero' },
    showlegend: true, legend: { orientation: 'h', y: -0.2 }
  }), CFG);
}

/* ---- Acquire Contention ---- */
function renderAcquireContention() {
  var run = latestSelected();
  if (!run || !run.acquire_contention || run.acquire_contention.length === 0) { hide('s-acq-cont'); return; }
  show('s-acq-cont');

  var container = document.getElementById('acq-cont-container');
  container.innerHTML = '';

  var AC = {'acquire_lua':'#1565C0','limiter_acquire':'#2E7D32','release_lua':'#E65100'};

  var groups = {};
  run.acquire_contention.forEach(function(r) {
    if (r.p50_us !== undefined && r.median_us === undefined) { r.median_us = r.p50_us; }
    if (r.mean_us === undefined) { r.mean_us = r.median_us || 0; }
    if (r.test === 'acquire_slot') r.test = 'limiter_acquire';
    if (r.test === 'release') r.test = 'release_lua';
    var key = r.test + (r.scenario ? '_' + r.scenario : '');
    if (!groups[key]) groups[key] = [];
    groups[key].push(r);
  });

  Object.keys(groups).sort().forEach(function(key) {
    var items = groups[key].sort(function(a,b){ return a.num_callers - b.num_callers; });
    var test = items[0].test;
    var scenario = items[0].scenario || '';
    var title = test.replace(/_/g, ' ') + (scenario ? ' (' + scenario.replace(/_/g, ' ') + ')' : '');
    var color = AC[test] || '#455A64';
    var ns = items.map(function(r){ return 'N=' + r.num_callers; });

    var latDiv = document.createElement('div');
    container.appendChild(latDiv);
    Plotly.newPlot(latDiv, [{
      x: ns, y: items.map(function(r){ return r.median_us; }),
      type: 'bar', name: 'median',
      marker: { color: color },
      error_y: {
        type: 'data', symmetric: false, visible: true,
        array: items.map(function(r){ return r.p99_us - r.median_us; }),
        arrayminus: items.map(function(){ return 0; }),
        thickness: 1.5, width: 6
      },
      customdata: items.map(function(r){ return [r.p99_us, r.mean_us]; }),
      hovertemplate: 'median: %{y:.2f}µs<br>mean: %{customdata[1]:.2f}µs<br>p99: %{customdata[0]:.2f}µs<extra></extra>'
    }, {
      x: ns, y: items.map(function(r){ return r.mean_us; }),
      type: 'scatter', mode: 'markers',
      marker: { symbol: 'diamond', size: 7, color: '#222', line: { color: '#fff', width: 1 } },
      name: 'mean', showlegend: false,
      hovertemplate: 'mean: %{y:.2f}µs<extra></extra>'
    }], M(LB, {
      title: { text: title + ' latency', font: { size: 13 } },
      xaxis: { title: 'Callers' },
      yaxis: { title: 'Latency (µs)', type: 'log' },
      height: 320, margin: { t: 36, r: 16, b: 64, l: 64 },
      showlegend: false
    }), CFG);

    if (items[0].throughput !== undefined) {
      var tDiv = document.createElement('div');
      container.appendChild(tDiv);
      Plotly.newPlot(tDiv, [{
        x: items.map(function(r){ return r.num_callers; }),
        y: items.map(function(r){ return r.throughput; }),
        type: 'scatter', mode: 'lines+markers',
        line: { color: color, width: 2 }, marker: { size: 7 },
        hovertemplate: '%{y:.1f} ops/s<extra></extra>'
      }], M(LB, {
        title: { text: title + ' throughput', font: { size: 13 } },
        xaxis: { title: 'Callers (N)', dtick: 1 },
        yaxis: { title: 'Throughput (ops/s)', rangemode: 'tozero' },
        height: 320, margin: { t: 36, r: 16, b: 64, l: 64 }
      }), CFG);
    }
  });
}

/* ---- Orchestration ---- */
function renderAll() {
  renderContention();
  renderLatency();
  renderBufferDepth();
  renderDecomposition();
  renderTailLatency();
  renderSoak();
  renderBufferGrowth();
  renderAcquireContention();
}

buildSelector();
renderAll();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    main()
