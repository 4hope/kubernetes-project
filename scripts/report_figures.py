"""Vector evidence figures drawn with ReportLab's chart library."""
import json
import math
from pathlib import Path
from reportlab.graphics.shapes import Drawing, Rect, String, Line, Polygon
from reportlab.graphics.charts.lineplots import LinePlot
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics import renderSVG
from reportlab.lib.colors import HexColor, white

ROOT = Path(__file__).resolve().parents[1]
BLUE, TEAL, ORANGE = map(HexColor, ["#355FD2", "#087F73", "#B36625"])
INK, GRID = map(HexColor, ["#101E35", "#D9D9D9"])


def architecture_drawing():
    d = Drawing(510, 185)
    def box(x, y, width, height, lines):
        d.add(Rect(x, y, width, height, fillColor=white, strokeColor=INK, strokeWidth=.8))
        for i, text in enumerate(lines):
            d.add(String(x + width / 2, y + height - 16 - 12 * i, text,
                         textAnchor="middle", fontName="Helvetica", fontSize=9, fillColor=INK))
    def arrow(x1, y1, x2, y2, dashed=False):
        d.add(Line(x1, y1, x2, y2, strokeColor=INK, strokeWidth=.9,
                   strokeDashArray=[3, 3] if dashed else []))
        angle = math.atan2(y2 - y1, x2 - x1)
        a, b = angle + 2.65, angle - 2.65
        d.add(Polygon([x2, y2, x2 + 6 * math.cos(a), y2 + 6 * math.sin(a),
                       x2 + 6 * math.cos(b), y2 + 6 * math.sin(b)], fillColor=INK, strokeColor=INK))
    box(4, 43, 110, 43, ["Request generator", "HTTP client Pod"])
    box(192, 43, 112, 43, ["ClusterIP Service", "demo-web:8080"])
    box(395, 83, 110, 43, ["Application Pod 1", "version / Pod / ready"])
    box(395, 13, 110, 43, ["Application Pod 2", "version / Pod / ready"])
    box(172, 137, 168, 43, ["Deployment + ReplicaSet", "desired replicas: 2"])
    arrow(114, 64, 192, 64)
    arrow(304, 64, 395, 104)
    arrow(304, 64, 395, 34)
    arrow(340, 151, 450, 126, True)
    d.add(Line(340, 143, 355, 143, strokeColor=INK, strokeWidth=.9, strokeDashArray=[3, 3]))
    d.add(Line(355, 143, 355, 34, strokeColor=INK, strokeWidth=.9, strokeDashArray=[3, 3]))
    arrow(355, 34, 395, 34, True)
    d.add(String(136, 74, "HTTP", fontName="Helvetica", fontSize=8, fillColor=INK))
    d.add(String(8, 151, "kubectl / API", fontName="Helvetica", fontSize=8, fillColor=INK))
    arrow(68, 139, 172, 157, True)
    d.add(String(8, 126, "experiment process", fontName="Helvetica", fontSize=8, fillColor=INK))
    return d


def recovery_drawing(data):
    d = Drawing(510, 170)
    plot = LinePlot()
    plot.x, plot.y, plot.width, plot.height = 45, 35, 450, 115
    lines = []
    for r in data["cohorts"]["original"]:
        if r["mode"] != "delete":
            continue
        folder = ROOT / r["run"]
        action = next(m["elapsed_s"] for m in json.loads((folder / "markers.json").read_text())
                      if m["event"] == "action_started")
        points = []
        for line in (folder / "states.jsonl").read_text().splitlines():
            s = json.loads(line)
            x = s["elapsed_s"] - action
            if -5 <= x <= 35:
                y = sum(not p["metadata"].get("deletionTimestamp") and any(
                    c["type"] == "Ready" and c["status"] == "True"
                    for c in p.get("status", {}).get("conditions", [])) for p in s["pods"])
                if points:
                    points.append((x, points[-1][1]))
                points.append((x, y))
        lines.append(points)
    plot.data = lines
    plot.xValueAxis.valueMin, plot.xValueAxis.valueMax, plot.xValueAxis.valueStep = -5, 35, 10
    plot.yValueAxis.valueMin, plot.yValueAxis.valueMax, plot.yValueAxis.valueStep = 0, 3, 1
    plot.xValueAxis.labels.fontSize = plot.yValueAxis.labels.fontSize = 8
    plot.yValueAxis.visibleGrid = True
    plot.yValueAxis.gridStrokeColor = GRID
    for i, color in enumerate([BLUE, ORANGE, TEAL]):
        plot.lines[i].strokeColor = color
        plot.lines[i].strokeWidth = 1.4
        d.add(Line(340 + i * 55, 164, 355 + i * 55, 164, strokeColor=color, strokeWidth=1.5))
        d.add(String(358 + i * 55, 161, str(i + 1), fontSize=8, fillColor=INK))
    d.add(plot)
    d.add(String(47, 162, "Ready Pods (terminating Pods excluded)", fontSize=9, fillColor=INK))
    d.add(String(180, 8, "Seconds after deletion command", fontSize=9, fillColor=INK))
    return d


def pressure_drawing(data):
    d = Drawing(510, 180)
    chart = VerticalBarChart()
    chart.x, chart.y, chart.width, chart.height = 45, 35, 450, 120
    rows = data["cohorts"]["pressure_rollout"]
    chart.data = [[100 * r["failed"] / r["sent"] for r in rows if r["mode"] == mode]
                  for mode in ["rollout-a", "rollout-b"]]
    chart.categoryAxis.categoryNames = ["Repeat 1", "Repeat 2", "Repeat 3"]
    chart.categoryAxis.labels.fontSize = chart.valueAxis.labels.fontSize = 8
    chart.valueAxis.valueMin, chart.valueAxis.valueMax, chart.valueAxis.valueStep = 0, 10, 2
    chart.valueAxis.visibleGrid = True
    chart.valueAxis.gridStrokeColor = GRID
    chart.bars[0].fillColor, chart.bars[1].fillColor = BLUE, TEAL
    chart.barLabelFormat = "%.2f"
    chart.barLabels.fontSize = 8
    chart.barLabels.nudge = 5
    d.add(chart)
    d.add(String(47, 168, "Failed requests (%) over the entire run", fontSize=9, fillColor=INK))
    d.add(Rect(340, 164, 8, 8, fillColor=BLUE, strokeColor=BLUE))
    d.add(String(352, 164, "A", fontSize=8, fillColor=INK))
    d.add(Rect(390, 164, 8, 8, fillColor=TEAL, strokeColor=TEAL))
    d.add(String(402, 164, "B", fontSize=8, fillColor=INK))
    return d


def write_figures(data, out):
    for name, drawing in [("architecture", architecture_drawing()),
                          ("recovery", recovery_drawing(data)),
                          ("pressure", pressure_drawing(data))]:
        renderSVG.drawToFile(drawing, str(Path(out) / (name + ".svg")))
