import numpy as np
import somoclu
import plotly.graph_objects as go
import plotly.express as px
from sklearn.datasets import load_iris
from collections import Counter

# -------------------------
# Parámetros del SOM
# -------------------------
n_rows, n_cols = 20, 15
orientation = "pointy"  # "pointy" o "flat"
s = 1.0  # radio del hexágono (centro a vértice). Mantén s=1 para teselado exacto.

# -------------------------
# Datos y entrenamiento
# -------------------------
iris = load_iris()
X = iris.data
y = iris.target
y_names = iris.target_names[y]

som = somoclu.Somoclu(n_columns=n_cols, n_rows=n_rows, gridtype="hexagonal")
som.train(X)

# -------------------------
# Coordenadas de centros coherentes con s
# -------------------------
def hex_center(r, c, s=1.0, orientation="pointy"):
    if orientation == "pointy":
        # pointy-top (odd-r offset): separación horizontal = sqrt(3)*s; vertical = 1.5*s
        x = np.sqrt(3)*s * (c + 0.5*(r % 2))
        y = 1.5*s * r
    else:
        # flat-top (odd-c offset): separación horizontal = 1.5*s; vertical = sqrt(3)*s
        x = 1.5*s * c
        y = np.sqrt(3)*s * (r + 0.5*(c % 2))
    return x, y

# Polígono del hexágono (7 puntos: 6 lados + cierre)
def hex_polygon(xc, yc, s=1.0, orientation="pointy"):
    if orientation == "pointy":
        # vértice arriba
        angles = np.deg2rad([90, 150, 210, 270, 330, 30, 90])
    else:
        # lado arriba (top plano)
        angles = np.deg2rad([0, 60, 120, 180, 240, 300, 0])
    return xc + s*np.cos(angles), yc + s*np.sin(angles)

# -------------------------
# U-Matrix y BMUs
# -------------------------
U = som.umatrix  # forma (n_rows, n_cols)
bmus = som.bmus  # lista de pares (r, c)

# Etiquetas mayoritarias por neurona
labels_by_neuron = {}
for i, (r, c) in enumerate(bmus):
    labels_by_neuron.setdefault((r, c), []).append(y_names[i])

label_text = {}
for neuron, lbls in labels_by_neuron.items():
    mc, count = Counter(lbls).most_common(1)[0]
    label_text[neuron] = f"{mc} ({count})"

# -------------------------
# Colormap YlOrRd invertido
# -------------------------
colorscale = px.colors.sequential.Pinkyl[::-1]
vmin, vmax = float(np.nanmin(U)), float(np.nanmax(U))

def color_for(val):
    if vmax == vmin:
        t = 0.0
    else:
        t = (val - vmin) / (vmax - vmin)
    idx = int(np.clip(t*(len(colorscale)-1), 0, len(colorscale)-1))
    return colorscale[idx]

# -------------------------
# Construcción de la figura
# -------------------------
fig = go.Figure()

xmin = ymin = +1e9
xmax = ymax = -1e9

for r in range(n_rows):
    for c in range(n_cols):
        xc, yc = hex_center(r, c, s=s, orientation=orientation)
        hx, hy = hex_polygon(xc, yc, s=s, orientation=orientation)

        val = U[r, c]
        fig.add_trace(go.Scatter(
            x=hx, y=hy,
            mode="lines",
            fill="toself",
            line=dict(width=1, color="black"),
            fillcolor=color_for(val),
            hoverinfo="text",
            text=f"Neuron ({r},{c}) · U={val:.3f}",
            showlegend=False
        ))

        # Para calcular el rango de ejes
        xmin, xmax = min(xmin, np.min(hx)), max(xmax, np.max(hx))
        ymin, ymax = min(ymin, np.min(hy)), max(ymax, np.max(hy))

# Etiquetas mayoritarias centradas
fig.add_trace(go.Scatter(
    x=[hex_center(r, c, s=s, orientation=orientation)[0] for (r, c) in label_text.keys()],
    y=[hex_center(r, c, s=s, orientation=orientation)[1] for (r, c) in label_text.keys()],
    text=[label_text[(r, c)] for (r, c) in label_text.keys()],
    mode="text",
    textfont=dict(size=10, color="black"),
    hoverinfo="skip",
    showlegend=False
))

# Rango de ejes ajustado al borde del mosaico
pad = 0.05 * max(xmax - xmin, ymax - ymin)
fig.update_layout(
    title="SOM Hex (teselado perfecto) · YlOrRd invertido",
    xaxis=dict(visible=False, range=[xmin - pad, xmax + pad], scaleanchor="y", scaleratio=1),
    yaxis=dict(visible=False, range=[ymin - pad, ymax + pad]),
    plot_bgcolor="white",
    width=900,
    height=800,
    margin=dict(l=10, r=10, t=60, b=10)
)

fig.show()