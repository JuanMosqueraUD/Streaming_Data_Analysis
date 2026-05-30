import os
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, Input, Output, callback_context, dcc, html
from dash import dash_table
import dash_bootstrap_components as dbc


BASE_DIR = Path(__file__).resolve().parent.parent
GOLD_DIR = BASE_DIR / "datalake_gold"
APP_TITLE = "Streaming & Entertainment Public Sentiment"
TOPIC_ANGLE = "Governance and audience/critic quality signals from Bronze → Silver → Gold"
TEAM_NAME = "Workshop 4 Team"


PALETTE = {
    "bg": "#F6F8FB",
    "card": "#FFFFFF",
    "text": "#1F2937",
    "muted": "#6B7280",
    "primary": "#2457C5",
    "primary_light": "#DCE7FF",
    "accent": "#0EA5A4",
    "danger": "#E45756",
    "warning": "#F59E0B",
    "success": "#16A34A",
    "border": "#E5E7EB",
}


def latest_governance_parquet_path(base_dir: Path = GOLD_DIR) -> Optional[Path]:
    """Return the newest governance parquet file or the parquet inside the newest folder."""
    if not base_dir.exists():
        return None

    direct_files = sorted(base_dir.glob("governance_*.parquet"))
    if direct_files:
        return direct_files[-1]

    folders = sorted(base_dir.glob("governance_*.parquet"))
    if not folders:
        return None

    newest = folders[-1]
    parquet_files = sorted(newest.glob("*.parquet"))
    return parquet_files[-1] if parquet_files else None


def load_latest_governance_data(base_dir: Path = GOLD_DIR) -> Tuple[pd.DataFrame, Optional[str]]:
    """Load the latest governance parquet using pandas.read_parquet()."""
    latest_path = latest_governance_parquet_path(base_dir)
    if latest_path is None:
        return pd.DataFrame(), None

    try:
        df = pd.read_parquet(latest_path)
    except Exception:
        # If a folder path was returned, fall back to the first parquet file inside it.
        if latest_path.is_dir():
            part_files = sorted(latest_path.glob("*.parquet"))
            if not part_files:
                return pd.DataFrame(), str(latest_path)
            df = pd.read_parquet(part_files[-1])
            return df, str(part_files[-1])
        raise

    return df, str(latest_path)


def filter_by_kpi_type(df: pd.DataFrame, kpi_type: str) -> pd.DataFrame:
    if df.empty or "kpi_type" not in df.columns:
        return pd.DataFrame()
    return df[df["kpi_type"] == kpi_type].copy()


def build_kpi_card(title: str, value: str, subtitle: str, color: str = PALETTE["primary"]) -> dbc.Col:
    return dbc.Col(
        dbc.Card(
            dbc.CardBody(
                [
                    html.Div(title, className="text-uppercase small fw-semibold", style={"color": PALETTE["muted"]}),
                    html.Div(value, className="display-6 fw-bold", style={"color": color, "lineHeight": "1.1"}),
                    html.Div(subtitle, className="small", style={"color": PALETTE["muted"]}),
                ]
            ),
            className="shadow-sm border-0 h-100",
            style={"borderRadius": "16px", "backgroundColor": PALETTE["card"]},
        ),
        md=3,
        sm=6,
        xs=12,
    )


def _format_value(value) -> str:
    if pd.isna(value):
        return "N/A"
    if isinstance(value, (int, float)):
        if float(value).is_integer():
            return f"{int(value):,}"
        return f"{float(value):,.2f}"
    return str(value)


def _format_last_updated(df: pd.DataFrame) -> str:
    if df.empty or "computed_at" not in df.columns:
        return "No Gold data loaded"
    ts = pd.to_datetime(df["computed_at"], errors="coerce", utc=True).dropna()
    if ts.empty:
        return "No valid computed_at timestamp"
    latest = ts.max().tz_convert(None) if getattr(ts.max(), "tzinfo", None) else ts.max()
    return latest.strftime("%Y-%m-%d %H:%M:%S")


def _data_quality_summary_table(df: pd.DataFrame) -> dash_table.DataTable:
    table_df = df.copy()
    if table_df.empty:
        table_df = pd.DataFrame(
            [{"message": "No governance data available yet."}]
        )

    columns = [{"name": col.replace("_", " ").title(), "id": col} for col in table_df.columns]
    return dash_table.DataTable(
        data=table_df.to_dict("records"),
        columns=columns,
        page_size=8,
        sort_action="native",
        filter_action="native",
        style_table={"overflowX": "hidden"},
        style_header={
            "backgroundColor": PALETTE["primary"],
            "color": "white",
            "fontWeight": "700",
            "border": f"1px solid {PALETTE['border']}",
        },
        style_cell={
            "fontFamily": "Arial, sans-serif",
            "fontSize": "13px",
            "padding": "10px",
            "whiteSpace": "normal",
            "height": "auto",
            "textAlign": "left",
            "border": f"1px solid {PALETTE['border']}",
            "minWidth": "100px",
            "maxWidth": "240px",
            "color": PALETTE["text"],
        },
        style_data_conditional=[
            {"if": {"row_index": "odd"}, "backgroundColor": "#FAFBFC"},
        ],
    )


def _empty_figure(message: str) -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(
        text=message,
        x=0.5,
        y=0.5,
        xref="paper",
        yref="paper",
        showarrow=False,
        font={"size": 16, "color": PALETTE["muted"]},
    )
    fig.update_layout(
        template="plotly_white",
        height=360,
        margin={"l": 20, "r": 20, "t": 20, "b": 20},
        paper_bgcolor=PALETTE["card"],
        plot_bgcolor=PALETTE["card"],
    )
    return fig


def build_null_rate_figure(df: pd.DataFrame) -> go.Figure:
    null_df = filter_by_kpi_type(df, "null_rate")
    if null_df.empty:
        return _empty_figure("No null rate KPIs available")

    plot_df = null_df.copy()
    plot_df["label"] = plot_df["family"].fillna("unknown") + " · " + plot_df["column_name"].fillna("unknown")
    plot_df = plot_df.sort_values("value", ascending=True)

    fig = px.bar(
        plot_df,
        x="value",
        y="label",
        orientation="h",
        color="family",
        color_discrete_sequence=[PALETTE["primary"], PALETTE["accent"], PALETTE["warning"], PALETTE["danger"]],
        title="Null Rate by Family and Column",
    )
    fig.update_traces(texttemplate="%{x:.2f}%", textposition="outside", cliponaxis=False)
    fig.update_layout(
        template="plotly_white",
        height=420,
        margin={"l": 20, "r": 20, "t": 60, "b": 20},
        xaxis_title="Null rate (%)",
        yaxis_title="",
        legend_title="Family",
        paper_bgcolor=PALETTE["card"],
        plot_bgcolor=PALETTE["card"],
        title_font={"size": 18},
    )
    fig.update_xaxes(range=[0, max(100, float(plot_df["value"].max()) * 1.15)])
    return fig


def build_volume_figure(df: pd.DataFrame) -> go.Figure:
    volume_df = filter_by_kpi_type(df, "volume")
    volume_by_source_df = filter_by_kpi_type(df, "volume_by_source")

    if volume_df.empty and volume_by_source_df.empty:
        return _empty_figure("No volume KPIs available")

    plot_df = pd.concat([volume_df, volume_by_source_df], ignore_index=True, sort=False)
    plot_df = plot_df.copy()
    plot_df["label"] = plot_df["family"].fillna("unknown") + " · " + plot_df["metric_name"].fillna("volume")

    fig = px.bar(
        plot_df,
        x="label",
        y="value",
        color="family",
        color_discrete_sequence=[PALETTE["primary"], PALETTE["accent"], PALETTE["warning"]],
        title="Volume Metrics",
    )
    fig.update_layout(
        template="plotly_white",
        height=380,
        margin={"l": 20, "r": 20, "t": 60, "b": 80},
        xaxis_title="Metric",
        yaxis_title="Value",
        xaxis_tickangle=-20,
        paper_bgcolor=PALETTE["card"],
        plot_bgcolor=PALETTE["card"],
        title_font={"size": 18},
        legend_title="Family",
    )
    return fig


def build_schema_compliance_figure(df: pd.DataFrame) -> go.Figure:
    compliance_df = filter_by_kpi_type(df, "schema_compliance")
    if compliance_df.empty:
        return _empty_figure("No schema compliance KPIs available")

    plot_df = compliance_df.copy()
    plot_df["label"] = plot_df["family"].fillna("unknown")
    fig = px.bar(
        plot_df,
        x="label",
        y="value",
        color="family",
        text="value",
        color_discrete_sequence=[PALETTE["success"], PALETTE["accent"], PALETTE["primary"]],
        title="Schema Compliance Rate by Family",
    )
    fig.update_traces(texttemplate="%{text:.2f}%", textposition="outside", cliponaxis=False)
    fig.update_layout(
        template="plotly_white",
        height=380,
        margin={"l": 20, "r": 20, "t": 60, "b": 20},
        yaxis_range=[0, 110],
        xaxis_title="Family",
        yaxis_title="Compliance (%)",
        paper_bgcolor=PALETTE["card"],
        plot_bgcolor=PALETTE["card"],
        title_font={"size": 18},
        showlegend=False,
    )
    return fig


def build_outlier_rate_figure(df: pd.DataFrame) -> go.Figure:
    outlier_df = filter_by_kpi_type(df, "outlier_rate")
    if outlier_df.empty:
        return _empty_figure("No outlier rate KPIs available")

    plot_df = outlier_df.copy()
    plot_df["column_name"] = plot_df["column_name"].fillna("unknown").astype(str)
    plot_df["value"] = pd.to_numeric(plot_df["value"], errors="coerce")
    plot_df = plot_df.dropna(subset=["value"])

    if plot_df.empty:
        return _empty_figure("No outlier rate KPIs available")

    plot_df = plot_df.sort_values("value", ascending=True)

    fig = px.bar(
        plot_df,
        x="value",
        y="column_name",
        orientation="h",
        color="family",
        color_discrete_sequence=[PALETTE["primary"], PALETTE["accent"], PALETTE["warning"], PALETTE["success"]],
        title="Outlier Rate by Column",
    )
    fig.update_traces(texttemplate="%{x:.2f}%", textposition="outside", cliponaxis=False)
    fig.update_layout(
        template="plotly_white",
        height=380,
        margin={"l": 20, "r": 20, "t": 60, "b": 20},
        xaxis_title="Outlier rate (%)",
        yaxis_title="Column",
        paper_bgcolor=PALETTE["card"],
        plot_bgcolor=PALETTE["card"],
        title_font={"size": 18},
        legend_title="Family",
    )
    fig.update_xaxes(range=[0, max(100, float(plot_df["value"].max()) * 1.15)])
    return fig


def build_quality_summary_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["family", "kpi_type", "metric_name", "column_name", "value", "unit", "computed_at", "run_stamp"])

    quality_df = df[df["kpi_type"].isin([
        "null_rate",
        "duplicates",
        "schema_compliance",
        "ingestion_frequency",
        "text_statistics",
    ])].copy()

    if quality_df.empty:
        return quality_df

    summary = quality_df.sort_values(["family", "kpi_type", "metric_name"], ascending=[True, True, True])
    return summary[["family", "kpi_type", "metric_name", "column_name", "value", "unit", "computed_at", "run_stamp"]]


def build_app_layout(df: pd.DataFrame, source_label: Optional[str]) -> html.Div:
    if df.empty:
        last_updated = "No Gold governance data available"
        last_run_stamp = "N/A"
    else:
        last_updated = _format_last_updated(df)
        last_run_stamp = str(df["run_stamp"].dropna().max()) if "run_stamp" in df.columns and not df["run_stamp"].dropna().empty else "N/A"

    total_rows = len(df)
    total_families = df["family"].nunique(dropna=True) if "family" in df.columns and not df.empty else 0
    total_metrics = df["metric_name"].nunique(dropna=True) if "metric_name" in df.columns and not df.empty else 0
    null_rate_avg = 0.0
    if not df.empty:
        null_df = filter_by_kpi_type(df, "null_rate")
        if not null_df.empty:
            null_rate_avg = float(pd.to_numeric(null_df["value"], errors="coerce").fillna(0).mean())

    kpi_cards = dbc.Row(
        [
            build_kpi_card("Gold rows", f"{total_rows:,}", "Governance KPI records loaded", PALETTE["primary"]),
            build_kpi_card("Families", f"{total_families:,}", "Distinct Silver families represented", PALETTE["accent"]),
            build_kpi_card("Metrics", f"{total_metrics:,}", "Unique governance metrics", PALETTE["warning"]),
            build_kpi_card("Avg null rate", f"{null_rate_avg:.2f}%", "Across null_rate KPIs", PALETTE["danger"]),
        ],
        className="g-3",
    )

    header = dbc.Container(
        [
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.H1(APP_TITLE, className="mb-1", style={"color": PALETTE["text"], "fontWeight": 800}),
                            html.Div(TOPIC_ANGLE, className="mb-1", style={"color": PALETTE["muted"], "fontSize": "1.05rem"}),
                            html.Div(TEAM_NAME, style={"color": PALETTE["muted"]}),
                        ],
                        md=8,
                    ),
                    dbc.Col(
                        [
                            dbc.Button(
                                "Refresh data",
                                id="refresh-button",
                                color="primary",
                                className="me-2",
                                n_clicks=0,
                            ),
                            html.Div(
                                [
                                    html.Div("Source", className="small text-uppercase", style={"color": PALETTE["muted"]}),
                                    html.Div(source_label or "No source found", style={"fontWeight": 600, "color": PALETTE["text"]}),
                                    html.Div(f"Last updated: {last_updated}", className="small", style={"color": PALETTE["muted"]}),
                                    html.Div(f"Run stamp: {last_run_stamp}", className="small", style={"color": PALETTE["muted"]}),
                                ],
                                className="mt-3",
                            ),
                        ],
                        md=4,
                        className="text-md-end",
                    ),
                ],
                className="align-items-start g-3",
            ),
            html.Hr(style={"borderColor": PALETTE["border"], "opacity": 1}),
            kpi_cards,
            html.Div(style={"height": "16px"}),
            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="null-rate-chart", figure=build_null_rate_figure(df), config={"displayModeBar": False}), md=12),
                ],
                className="g-3 mb-3",
            ),
            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="volume-chart", figure=build_volume_figure(df), config={"displayModeBar": False}), md=6),
                    dbc.Col(dcc.Graph(id="schema-compliance-chart", figure=build_schema_compliance_figure(df), config={"displayModeBar": False}), md=6),
                ],
                className="g-3 mb-3",
            ),
            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="outlier-rate-chart", figure=build_outlier_rate_figure(df), config={"displayModeBar": False}), md=12),
                ],
                className="g-3 mb-3",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.H4("Data quality summary", className="mb-3", style={"color": PALETTE["text"]}),
                                    _data_quality_summary_table(build_quality_summary_table(df)),
                                ]
                            ),
                            className="shadow-sm border-0",
                            style={"borderRadius": "16px", "backgroundColor": PALETTE["card"]},
                        ),
                        md=12,
                    )
                ]
            ),
            dcc.Store(id="governance-data-refresh", data=0),
        ],
        fluid=True,
        className="py-4 px-4",
    )

    return html.Div(
        [
            html.Div(
                style={
                    "minHeight": "100vh",
                    "background": f"linear-gradient(180deg, {PALETTE['bg']} 0%, #EEF2FF 100%)",
                    "fontFamily": "Arial, sans-serif",
                },
                children=header,
            )
        ]
    )


initial_df, initial_source = load_latest_governance_data()

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], title="Governance Dashboard")
app.layout = build_app_layout(initial_df, initial_source)


@app.callback(
    Output("null-rate-chart", "figure"),
    Output("volume-chart", "figure"),
    Output("schema-compliance-chart", "figure"),
    Output("outlier-rate-chart", "figure"),
    Output("governance-data-refresh", "data"),
    Input("refresh-button", "n_clicks"),
)
def refresh_dashboard(n_clicks):
    # Always reload the latest Gold data when the button is pressed.
    df, _ = load_latest_governance_data()
    refresh_count = int(n_clicks or 0)
    return (
        build_null_rate_figure(df),
        build_volume_figure(df),
        build_schema_compliance_figure(df),
        build_outlier_rate_figure(df),
        refresh_count,
    )


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8050"))
    app.run(debug=True, host="0.0.0.0", port=port)