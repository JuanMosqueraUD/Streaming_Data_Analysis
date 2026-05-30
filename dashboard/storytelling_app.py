import os
from pathlib import Path
from typing import Dict, Optional, Tuple
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html
from dash import dash_table
import dash_bootstrap_components as dbc


BASE_DIR = Path(__file__).resolve().parent.parent
GOLD_DIR = BASE_DIR / "datalake_gold"
APP_TITLE = "Streaming & Entertainment Public Sentiment"
TOPIC_ANGLE = "Narrative storytelling signals from Gold aggregations"
TEAM_NAME = "Workshop 4 Team"


PALETTE = {
    "bg": "#F6F8FB",
    "card": "#FFFFFF",
    "text": "#1F2937",
    "muted": "#6B7280",
    "primary": "#2457C5",
    "accent": "#0EA5A4",
    "success": "#16A34A",
    "danger": "#DC2626",
    "neutral": "#9CA3AF",
    "warning": "#F59E0B",
    "border": "#E5E7EB",
}

SENTIMENT_COLORS = {
    "POSITIVE": PALETTE["success"],
    "NEGATIVE": PALETTE["danger"],
    "NEUTRAL": PALETTE["neutral"],
    "UNKNOWN": PALETTE["warning"],
}

STOPWORDS = {
    "the", "and", "for", "with", "this", "that", "from",
    "but", "his", "her", "their", "they", "them",
    "you", "your", "our", "its", "like",
    "movie", "film", "story", "character", "characters",
    "review", "reviews", "watch", "watching", "series", "show",
    "was", "are", "were", "have", "been"
}

INVALID_KEYWORDS = {"", "<na>", "nan", "none", "unknown"}
def filter_meaningful_keywords(df: pd.DataFrame, keyword_col: str, value_col: str) -> pd.DataFrame:
    work = df.copy()

    work[keyword_col] = (
        work[keyword_col]
        .fillna("")
        .astype(str)
        .str.lower()
        .str.strip()
        .str.replace(r"[^a-z]", "", regex=True)
    )
    work[value_col] = pd.to_numeric(work[value_col], errors="coerce")

    banned_keywords = {
        "",
        "<na>",
        "nan",
        "none",
        "unknown",
        "the",
        "and",
        "with",
        "for",
        "this",
        "that",
        "but",
        "his",
        "like",
        "movie",
        "film",
        "story",
        "review",
        "reviews",
        "watch",
        "watching",
        "series",
        "show",
        "was",
        "were",
        "are",
        "have",
        "been",
        "from",
        "you",
        "your",
        "they",
        "them",
        "their",
    }

    return work[
        (~work[keyword_col].isin(banned_keywords)) &
        (work[keyword_col].str.len() >= 3) &
        (work[value_col].notna())
    ].copy()

def latest_storytelling_path(base_dir: Path = GOLD_DIR) -> Optional[Path]:
    """Return the newest storytelling parquet output, preferring the Spark output folder."""
    if not base_dir.exists():
        return None

    candidates = sorted(base_dir.glob("storytelling_*.parquet"))
    if not candidates:
        return None

    folders = [path for path in candidates if path.is_dir()]
    if folders:
        return folders[-1]

    return candidates[-1]


def load_latest_storytelling_data(base_dir: Path = GOLD_DIR) -> Tuple[pd.DataFrame, Optional[str]]:
    """Load the latest storytelling parquet using pandas.read_parquet()."""
    latest_path = latest_storytelling_path(base_dir)
    if latest_path is None:
        return pd.DataFrame(), None

    try:
        df = pd.read_parquet(latest_path)
        return df, str(latest_path)
    except Exception:
        if latest_path.is_dir():
            part_files = sorted(latest_path.glob("*.parquet"))
            if not part_files:
                return pd.DataFrame(), str(latest_path)
            df = pd.read_parquet(part_files[-1])
            return df, str(part_files[-1])
        raise


def filter_by_aggregation_type(df: pd.DataFrame, aggregation_type: str) -> pd.DataFrame:
    if df.empty or "aggregation_type" not in df.columns:
        return pd.DataFrame()
    return df[df["aggregation_type"] == aggregation_type].copy()


def safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def format_timestamp(df: pd.DataFrame) -> str:
    if df.empty or "computed_at" not in df.columns:
        return "No Gold data loaded"
    timestamps = pd.to_datetime(df["computed_at"], errors="coerce", utc=True).dropna()
    if timestamps.empty:
        return "No valid computed_at timestamp"
    latest = timestamps.max()
    return latest.tz_convert(None).strftime("%Y-%m-%d %H:%M:%S") if getattr(latest, "tzinfo", None) else latest.strftime("%Y-%m-%d %H:%M:%S")


def build_card(title: str, value: str, subtitle: str, color: str = PALETTE["primary"]) -> dbc.Col:
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


def empty_figure(message: str) -> go.Figure:
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


def build_sentiment_distribution_figure(df: pd.DataFrame) -> go.Figure:
    sentiment_df = filter_by_aggregation_type(df, "sentiment_distribution")
    if sentiment_df.empty:
        return empty_figure("No sentiment distribution data available")

    plot_df = sentiment_df.copy()
    plot_df["sentiment"] = (
        plot_df["dimension_value"]
            .fillna("UNKNOWN")
            .astype(str)
            .str.upper()
            .replace({"<NA>": "UNKNOWN", "NAN": "UNKNOWN", "NONE": "UNKNOWN", "": "UNKNOWN"})
    )

    plot_df = plot_df[plot_df["sentiment"].isin(["POSITIVE", "NEGATIVE", "NEUTRAL"])]
    if plot_df.empty:
        return empty_figure("No labeled sentiment rows found")

    plot_df = plot_df.groupby("sentiment", as_index=False)["value"].sum().sort_values("value", ascending=False)
    plot_df["color"] = plot_df["sentiment"].map(SENTIMENT_COLORS).fillna(PALETTE["neutral"])

    fig = go.Figure(
        data=[
            go.Pie(
                labels=plot_df["sentiment"],
                values=plot_df["value"],
                hole=0.55,
                sort=False,
                marker=dict(colors=plot_df["color"].tolist()),
                textinfo="label+percent",
                hovertemplate="%{label}: %{value}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title="Sentiment distribution",
        template="plotly_white",
        height=380,
        margin={"l": 20, "r": 20, "t": 60, "b": 20},
        paper_bgcolor=PALETTE["card"],
        plot_bgcolor=PALETTE["card"],
        title_font={"size": 18},
        legend_title="Sentiment",
    )
    return fig

def build_sentiment_trend_figure(df: pd.DataFrame) -> go.Figure:
    trend_df = filter_by_aggregation_type(df, "sentiment_trend")
    if trend_df.empty:
        return empty_figure("No sentiment trend data available")

    plot_df = trend_df.copy()

    # Keep only RT rows, because TMDb uses a different semantic meaning
    # (average rating by day instead of sentiment count by label/day).
    plot_df = plot_df[plot_df["family"] == "rt_reviews"].copy()

    if plot_df.empty:
        return empty_figure("No RT sentiment trend data available")

    plot_df["day"] = pd.to_datetime(plot_df["dimension_value"], errors="coerce")
    plot_df["value"] = pd.to_numeric(plot_df["value"], errors="coerce")

    # Extract label safely from metric_name, only for RT rows
    plot_df["sentiment"] = (
        plot_df["metric_name"]
        .astype(str)
        .str.extract(r"(POSITIVE|NEGATIVE|NEUTRAL)", expand=False)
        .fillna("UNKNOWN")
    )

    plot_df = plot_df.dropna(subset=["day", "value"])
    plot_df = plot_df[plot_df["sentiment"].isin(["POSITIVE", "NEGATIVE", "NEUTRAL"])]

    if plot_df.empty:
        return empty_figure("No valid labeled RT sentiment trend data found")

    plot_df = (
        plot_df.groupby(["day", "sentiment"], as_index=False)["value"]
        .sum()
        .sort_values("day")
    )

    fig = px.line(
        plot_df,
        x="day",
        y="value",
        color="sentiment",
        markers=True,
        color_discrete_map=SENTIMENT_COLORS,
        title="Sentiment trend over time (RT reviews)",
    )

    fig.update_layout(
        template="plotly_white",
        height=380,
        margin={"l": 20, "r": 20, "t": 60, "b": 20},
        xaxis_title="Day",
        yaxis_title="Mentions",
        paper_bgcolor=PALETTE["card"],
        plot_bgcolor=PALETTE["card"],
        title_font={"size": 18},
        legend_title="Sentiment",
    )
    fig.update_traces(line=dict(width=3))
    return fig

def build_top_keywords_figure(df: pd.DataFrame) -> go.Figure:
    keyword_df = filter_by_aggregation_type(df, "top_keywords")
    if keyword_df.empty:
        return empty_figure("No keyword data available")

    plot_df = keyword_df.copy()
    plot_df["keyword"] = (
        plot_df["dimension_value"]
        .fillna("")
        .astype(str)
        .str.lower()
        .str.strip()
        .str.replace(r"[^a-z]", "", regex=True)
    )
    plot_df["value"] = pd.to_numeric(plot_df["value"], errors="coerce")

    plot_df = filter_meaningful_keywords(plot_df, "keyword", "value")

    if plot_df.empty:
        return empty_figure("No keyword data available")

    plot_df = (
        plot_df.groupby(["keyword", "family"], as_index=False)["value"]
        .sum()
        .sort_values("value", ascending=True)
        .tail(12)
    )

    fig = px.bar(
        plot_df,
        x="value",
        y="keyword",
        orientation="h",
        color="family",
        color_discrete_sequence=[
            PALETTE["primary"],
            PALETTE["accent"],
            PALETTE["warning"],
            PALETTE["success"],
        ],
        title="Most repeated keywords",
    )
    fig.update_layout(
        template="plotly_white",
        height=400,
        margin={"l": 20, "r": 20, "t": 60, "b": 20},
        xaxis_title="Mentions",
        yaxis_title="",
        paper_bgcolor=PALETTE["card"],
        plot_bgcolor=PALETTE["card"],
        title_font={"size": 18},
        legend_title="Family",
    )
    return fig

def build_source_comparison_figure(df: pd.DataFrame) -> go.Figure:
    source_df = df[df["aggregation_type"].isin(["source_volume", "volume_by_source"])].copy()
    if source_df.empty:
        return empty_figure("No source comparison data available")

    plot_df = source_df.copy()
    plot_df["label"] = plot_df["family"].fillna("unknown") + " · " + plot_df["dimension_value"].fillna("unknown")
    plot_df = plot_df.groupby(["label", "family", "aggregation_type"], as_index=False)["value"].sum()
    plot_df = plot_df.sort_values("value", ascending=True)

    fig = px.bar(
        plot_df,
        x="value",
        y="label",
        orientation="h",
        color="aggregation_type",
        color_discrete_map={"source_volume": PALETTE["primary"], "volume_by_source": PALETTE["accent"]},
        title="Source comparison",
    )
    fig.update_layout(
        template="plotly_white",
        height=360,
        margin={"l": 20, "r": 20, "t": 60, "b": 20},
        xaxis_title="Mentions",
        yaxis_title="",
        paper_bgcolor=PALETTE["card"],
        plot_bgcolor=PALETTE["card"],
        title_font={"size": 18},
        legend_title="Aggregation",
    )
    return fig


def build_volume_activity_figure(df: pd.DataFrame) -> go.Figure:
    volume_df = filter_by_aggregation_type(df, "volume_by_date")
    if volume_df.empty:
        return empty_figure("No volume activity data available")

    plot_df = volume_df.copy()
    plot_df["day"] = pd.to_datetime(plot_df["dimension_value"], errors="coerce")
    plot_df = plot_df.dropna(subset=["day"])
    if plot_df.empty:
        return empty_figure("No valid volume dates found")

    plot_df = plot_df.groupby(["day", "family"], as_index=False)["value"].sum().sort_values("day")

    fig = px.area(
        plot_df,
        x="day",
        y="value",
        color="family",
        color_discrete_sequence=[PALETTE["primary"], PALETTE["accent"], PALETTE["warning"], PALETTE["success"]],
        title="Volume activity over time",
    )
    fig.update_layout(
        template="plotly_white",
        height=380,
        margin={"l": 20, "r": 20, "t": 60, "b": 20},
        xaxis_title="Day",
        yaxis_title="Mentions",
        paper_bgcolor=PALETTE["card"],
        plot_bgcolor=PALETTE["card"],
        title_font={"size": 18},
        legend_title="Family",
    )
    return fig


def build_narrative_summary(df: pd.DataFrame) -> str:
    if df.empty:
        return "No Gold storytelling data is available yet."

    pieces = []

    sentiment_df = filter_by_aggregation_type(df, "sentiment_distribution")
    labeled_sentiment = sentiment_df[~sentiment_df["dimension_value"].isin([None, "<NA>", "nan"])] if not sentiment_df.empty else pd.DataFrame()
    if not labeled_sentiment.empty:
        sentiment_totals = labeled_sentiment.groupby("dimension_value", as_index=False)["value"].sum().sort_values("value", ascending=False)
        if not sentiment_totals.empty:
            dominant = str(sentiment_totals.iloc[0]["dimension_value"]).lower()
            pieces.append(f"The dominant sentiment is {dominant}.")

    source_df = df[df["aggregation_type"].isin(["source_volume", "volume_by_source"])].copy()
    if not source_df.empty:
        source_df = source_df.groupby(["family", "dimension_value"], as_index=False)["value"].sum().sort_values("value", ascending=False)
        if not source_df.empty:
            top_source = source_df.iloc[0]
            pieces.append(f"The most active source is {top_source['dimension_value']} in {top_source['family']}.")

    keyword_df = filter_by_aggregation_type(df, "top_keywords")
    if not keyword_df.empty:
        keyword_df = keyword_df.copy()
        keyword_df["dimension_value"] = (
            keyword_df["dimension_value"]
            .fillna("")
            .astype(str)
            .str.lower()
            .str.strip()
            .str.replace(r"[^a-z]", "", regex=True)
        )
        keyword_df["value"] = pd.to_numeric(keyword_df["value"], errors="coerce")

        keyword_df = filter_meaningful_keywords(keyword_df, "dimension_value", "value")

        keyword_df = keyword_df.groupby("dimension_value", as_index=False)["value"].sum().sort_values("value", ascending=False)
        if not keyword_df.empty:
            keyword = str(keyword_df.iloc[0]["dimension_value"])
            pieces.append(f"The most repeated keyword is '{keyword}'.")

    critic_gap_df = filter_by_aggregation_type(df, "critic_vs_audience")
    if not critic_gap_df.empty:
        critic_gap_df = critic_gap_df.assign(abs_gap=critic_gap_df["value"].abs()).sort_values("abs_gap", ascending=False)
        strongest = critic_gap_df.iloc[0]
        gap = float(strongest["value"])
        direction = "critics scored it lower than audiences" if gap < 0 else "critics scored it higher than audiences"
        pieces.append(
            f"The strongest critic-vs-audience gap appears for {strongest['dimension_value']}, where {direction}."
        )
    else:
        titles_df = filter_by_aggregation_type(df, "top_titles_by_reviews")
        if not titles_df.empty:
            titles_df = titles_df.groupby("dimension_value", as_index=False)["value"].sum().sort_values("value", ascending=False)
            if not titles_df.empty:
                pieces.append(f"The most discussed title is {titles_df.iloc[0]['dimension_value']}.")

    if not pieces:
        return "Gold data loaded, but there is not enough information to build a narrative summary yet."

    return " ".join(pieces)

def build_quality_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[
            "aggregation_type", "metric_name", "family", "dimension",
            "dimension_value", "value", "unit", "computed_at", "run_stamp"
        ])

    focus_types = [
        "sentiment_distribution",
        "sentiment_trend",
        "critic_vs_audience",
        "top_keywords",
        "source_volume",
        "volume_by_source",
        "volume_by_date",
        "top_titles_by_reviews",
    ]

    table_df = df[df["aggregation_type"].isin(focus_types)].copy()
    if table_df.empty:
        return table_df

    if "dimension_value" in table_df.columns:
        table_df["dimension_value"] = table_df["dimension_value"].astype(str).str.strip()
        invalid_values = {"<NA>", "nan", "None", "UNKNOWN", ""}
        table_df = table_df[~table_df["dimension_value"].isin(invalid_values)]

    table_df = table_df.sort_values(["aggregation_type", "family", "metric_name"])

    return table_df[[
        "aggregation_type",
        "metric_name",
        "family",
        "dimension",
        "dimension_value",
        "value",
        "unit",
        "computed_at",
        "run_stamp",
    ]]

def build_layout(df: pd.DataFrame, source_label: Optional[str]) -> html.Div:
    last_updated = format_timestamp(df)
    run_stamp = str(df["run_stamp"].dropna().max()) if not df.empty and "run_stamp" in df.columns else "N/A"

    narrative_text = build_narrative_summary(df)
    narrative_card = dbc.Card(
        dbc.CardBody(
            [
                html.Div("Narrative summary", className="text-uppercase small fw-semibold", style={"color": PALETTE["muted"]}),
                html.H4("What the data is saying", className="mb-3", style={"color": PALETTE["text"]}),
                html.P(narrative_text, id="narrative-summary", className="mb-0", style={"fontSize": "1.02rem", "color": PALETTE["text"], "lineHeight": "1.65"}),
            ]
        ),
        className="shadow-sm border-0 h-100",
        style={"borderRadius": "16px", "backgroundColor": PALETTE["card"]},
    )

    cards = dbc.Row(
        [
            build_card("Gold rows", f"{len(df):,}", "Storytelling KPI records loaded", PALETTE["primary"]),
            build_card("Families", f"{df['family'].nunique(dropna=True) if not df.empty else 0:,}", "Distinct families represented", PALETTE["accent"]),
            build_card("Aggregations", f"{df['aggregation_type'].nunique(dropna=True) if not df.empty else 0:,}", "Storytelling views available", PALETTE["warning"]),
            build_card("Last run", run_stamp, "Latest storytelling run stamp", PALETTE["success"]),
        ],
        className="g-3",
    )

    layout = dbc.Container(
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
                            dbc.Button("Refresh data", id="refresh-button", color="primary", n_clicks=0),
                            html.Div(
                                [
                                    html.Div("Source", className="small text-uppercase", style={"color": PALETTE["muted"]}),
                                    html.Div(source_label or "No source found", style={"fontWeight": 600, "color": PALETTE["text"]}),
                                    html.Div(f"Last updated: {last_updated}", className="small", style={"color": PALETTE["muted"]}),
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
            cards,
            html.Div(style={"height": "16px"}),
            dbc.Row(
                [
                    dbc.Col(narrative_card, md=12),
                ],
                className="g-3 mb-3",
            ),
            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="sentiment-distribution-chart", figure=build_sentiment_distribution_figure(df), config={"displayModeBar": False}), md=4),
                    dbc.Col(dcc.Graph(id="sentiment-trend-chart", figure=build_sentiment_trend_figure(df), config={"displayModeBar": False}), md=8),
                ],
                className="g-3 mb-3",
            ),
            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="top-keywords-chart", figure=build_top_keywords_figure(df), config={"displayModeBar": False}), md=6),
                    dbc.Col(dcc.Graph(id="source-comparison-chart", figure=build_source_comparison_figure(df), config={"displayModeBar": False}), md=6),
                ],
                className="g-3 mb-3",
            ),
            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="volume-activity-chart", figure=build_volume_activity_figure(df), config={"displayModeBar": False}), md=12),
                ],
                className="g-3 mb-3",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.H4("Narrative evidence table", className="mb-3", style={"color": PALETTE["text"]}),
                                    dash_table.DataTable(
                                        data=build_quality_table(df).to_dict("records"),
                                        columns=[{"name": col.replace("_", " ").title(), "id": col} for col in build_quality_table(df).columns],
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
                                        style_data_conditional=[{"if": {"row_index": "odd"}, "backgroundColor": "#FAFBFC"}],
                                    ),
                                ]
                            ),
                            className="shadow-sm border-0",
                            style={"borderRadius": "16px", "backgroundColor": PALETTE["card"]},
                        ),
                        md=12,
                    )
                ]
            ),
        ],
        fluid=True,
        className="py-4 px-4",
    )

    return html.Div(
        html.Div(
            children=layout,
            style={
                "minHeight": "100vh",
                "background": f"linear-gradient(180deg, {PALETTE['bg']} 0%, #EEF2FF 100%)",
                "fontFamily": "Arial, sans-serif",
            },
        )
    )


initial_df, initial_source = load_latest_storytelling_data()

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], title="Storytelling Dashboard")
app.layout = build_layout(initial_df, initial_source)


@app.callback(
    Output("sentiment-distribution-chart", "figure"),
    Output("sentiment-trend-chart", "figure"),
    Output("top-keywords-chart", "figure"),
    Output("source-comparison-chart", "figure"),
    Output("volume-activity-chart", "figure"),
    Output("narrative-summary", "children"),
    Input("refresh-button", "n_clicks"),
)
def refresh_dashboard(n_clicks):
    df, _ = load_latest_storytelling_data()
    return (
        build_sentiment_distribution_figure(df),
        build_sentiment_trend_figure(df),
        build_top_keywords_figure(df),
        build_source_comparison_figure(df),
        build_volume_activity_figure(df),
        build_narrative_summary(df),
    )


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8051"))
    app.run(debug=True, host="0.0.0.0", port=port)