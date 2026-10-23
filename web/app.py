
import os
import html
import json

import pandas as pd
import psycopg2
import streamlit as st
import streamlit.components.v1 as components
import plotly.express as px
import plotly.graph_objects as go


# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="Cinemetrics | Movie Insights",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# DATABASE
# ============================================================

@st.cache_data(ttl=30)
def load_data():
    with psycopg2.connect(os.environ["APP_DB_DSN"]) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    title,
                    release_year,
                    genre,
                    tmdb_rating,
                    imdb_rating,
                    runtime_minutes,
                    sources
                FROM public.movies_gold
                ORDER BY release_year DESC, title
            """)
            movie_rows = cur.fetchall()

            cur.execute("""
                SELECT
                    run_id,
                    status,
                    source_counts,
                    row_count,
                    finished_at
                FROM public.pipeline_runs
                ORDER BY finished_at DESC
                LIMIT 15
            """)
            run_rows = cur.fetchall()

            cur.execute("""
                SELECT COUNT(*)
                FROM public.movies_quarantine
            """)
            quarantine_count = cur.fetchone()[0]
            cur.execute("""
                SELECT COALESCE(quarantine_reason, 'suspicious'), COUNT(*)
                FROM public.movies_quarantine
                GROUP BY COALESCE(quarantine_reason, 'suspicious')
            """)
            quarantine_counts = dict(cur.fetchall())

    movies = pd.DataFrame(
        movie_rows,
        columns=[
            "title",
            "year",
            "genre",
            "tmdb_rating",
            "imdb_rating",
            "runtime_minutes",
            "sources",
        ],
    )

    runs = pd.DataFrame(
        run_rows,
        columns=[
            "run_id",
            "status",
            "source_counts",
            "rows",
            "finished_at",
        ],
    )

    return movies, runs, quarantine_count, quarantine_counts


try:
    df, runs, quarantine_count, quarantine_counts = load_data()
except Exception as exc:
    st.error(f"Cannot connect to PostgreSQL: {exc}")
    st.stop()

if df.empty:
    st.warning("No Gold data found. Trigger the Airflow DAG.")
    st.stop()


# ============================================================
# PREPARE DATA
# ============================================================

for column in ["tmdb_rating", "imdb_rating", "runtime_minutes"]:
    df[column] = pd.to_numeric(df[column], errors="coerce")


def parse_sources(value):
    if isinstance(value, (list, tuple, set)):
        return list(value)

    if isinstance(value, str):
        return [
            item.strip()
            for item in value.strip("{}").split(",")
            if item.strip()
        ]

    return []


df["sources"] = df["sources"].apply(parse_sources)


# ============================================================
# THEME STATE
# ============================================================

if "theme_mode" not in st.session_state:
    st.session_state.theme_mode = "Light"


# ============================================================
# SIDEBAR
# ============================================================

with st.sidebar:

    st.markdown("""
    <div class="brand-wrap">
        <div class="brand">
            <span class="brand-icon">◇</span>
            cinemetrics<span class="brand-dot">.</span>
        </div>
        <div class="brand-sub">MOVIE DATA STUDIO</div>
    </div>
    """, unsafe_allow_html=True)

    page = st.radio(
        "Navigation",
        [
            "Overview",
            "Movie Explorer",
            "Analytics",
            "Pipeline Status",
        ],
        label_visibility="collapsed",
    )

    st.divider()

    st.markdown("**Appearance**")

    st.segmented_control(
        "Display theme",
        ["Light", "Dark"],
        key="theme_mode",
        label_visibility="collapsed",
    )

    st.divider()

    st.markdown("**Filters**")

    all_years = sorted(df["year"].dropna().unique().tolist())

    # Native checkboxes/radios avoid the BaseWeb select popup that
    # remains white in some Streamlit versions and custom themes.
    st.markdown("**Release year**")
    year_columns = st.columns(2)
    selected_years = []
    for i, year in enumerate(all_years):
        with year_columns[i % 2]:
            if st.checkbox(str(year), value=True, key=f"movie_year_{year}"):
                selected_years.append(year)

    st.markdown("**Data source**")
    selected_source = st.radio(
        "Data source",
        ["All", "tmdb", "omdb", "wikipedia"],
        key="movie_source_filter",
        label_visibility="collapsed",
    )

    all_genres = sorted({
        genre.strip()
        for value in df["genre"].dropna()
        for genre in str(value).split(",")
        if genre.strip()
    })

    # Collapsed genre choices keep the sidebar compact.
        # ========================================================
    # GENRE FILTER — DARK MODE FRIENDLY
    # ========================================================

    st.markdown("**Genre**")

    with st.container(height=220, border=False):
        selected_genre = st.radio(
            "Choose genre",
            ["All"] + all_genres,
            key="movie_genre_filter",
            label_visibility="collapsed",
        )

    if selected_genre != "All":
        st.caption(f"Genre: {selected_genre}")

    search_text = st.text_input(
        "Search movie",
        placeholder="Search title...",
    )

    st.divider()
    st.caption("DATA INFRASTRUCTURE")
    st.markdown("🟢 PostgreSQL connected")
    st.caption("Airflow · TMDB · OMDb · Wikipedia")


# ============================================================
# COLORS
# ============================================================

DARK = st.session_state.theme_mode == "Dark"

if DARK:
    C = {
        "bg": "#14151E",
        "text": "#F3EEF4",
        "muted": "#A8A0B0",
        "accent": "#EC658B",
        "accent_dark": "#C63863",
        "glass": "rgba(255,255,255,0.055)",
        "glass_strong": "rgba(255,255,255,0.085)",
        "border": "rgba(255,255,255,0.13)",
        "grid": "rgba(255,255,255,0.10)",
        "shadow": "rgba(0,0,0,0.18)",
        "sidebar": "rgba(24,20,31,0.78)",
        "input": "rgba(255,255,255,0.065)",
        "table_bg": "#24212D",
        "table_head": "#302936",
        "table_border": "#393240",
        "table_text": "#F2EAF0",
        "table_muted": "#AAA1AF",
        "hover": "#37303F",
    }
else:
    C = {
        "bg": "#E9F0F2",
        "text": "#28333C",
        "muted": "#7B8C96",
        "accent": "#BB3655",
        "accent_dark": "#982845",
        "glass": "rgba(255,255,255,0.55)",
        "glass_strong": "rgba(255,255,255,0.72)",
        "border": "rgba(255,255,255,0.85)",
        "grid": "rgba(83,105,115,0.12)",
        "shadow": "rgba(87,115,130,0.12)",
        "sidebar": "rgba(255,255,255,0.46)",
        "input": "rgba(255,255,255,0.58)",
        "table_bg": "rgba(255,255,255,0.50)",
        "table_head": "rgba(255,255,255,0.75)",
        "table_border": "rgba(130,150,160,0.18)",
        "table_text": "#28333C",
        "table_muted": "#7B8C96",
        "hover": "#E9EFF2",
    }


# ============================================================
# CSS
# ============================================================

css = """
<style>
@import url(
'https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700;800&display=swap'
);

html, body, [data-testid="stApp"] {
    font-family: 'DM Sans', sans-serif;
}

[data-testid="stAppViewContainer"] {
    background:
        radial-gradient(
            ellipse at 12% 8%,
            GLOW_ONE,
            transparent 35%
        ),
        radial-gradient(
            ellipse at 90% 85%,
            GLOW_TWO,
            transparent 42%
        ),
        BG_COLOR !important;
    color: TEXT_COLOR;
}

[data-testid="stHeader"] {
    background: transparent;
}

[data-testid="stSidebar"] {
    background: SIDEBAR_COLOR !important;
    border-right: 1px solid BORDER_COLOR;
    backdrop-filter: blur(30px);
    -webkit-backdrop-filter: blur(30px);
}

[data-testid="stSidebar"] * {
    color: TEXT_COLOR;
}

.block-container {
    max-width: 1550px;
    padding-top: 2rem;
    padding-bottom: 3rem;
}

h1, h2, h3, h4, p, label {
    color: TEXT_COLOR;
}

/* BRAND */

.brand-wrap {
    padding: 8px 0 22px;
}

.brand {
    color: TEXT_COLOR;
    font-size: 27px;
    font-weight: 800;
    letter-spacing: -1.2px;
}

.brand-icon,
.brand-dot {
    color: ACCENT_COLOR;
}

.brand-sub {
    color: MUTED_COLOR;
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 2px;
    margin-top: 5px;
}

/* GLASS HERO */

.hero {
    background:
        linear-gradient(
            120deg,
            HERO_START,
            HERO_END
        );
    border: 1px solid BORDER_COLOR;
    border-radius: 32px;
    padding: 32px 36px;
    backdrop-filter: blur(28px);
    -webkit-backdrop-filter: blur(28px);
    box-shadow: 0 15px 45px SHADOW_COLOR;
    margin-bottom: 25px;
}

.eyebrow {
    color: ACCENT_COLOR;
    font-size: 11px;
    font-weight: 800;
    letter-spacing: 2px;
}

.hero-title {
    color: TEXT_COLOR;
    font-size: clamp(29px, 3vw, 45px);
    font-weight: 700;
    letter-spacing: -1.7px;
    margin: 10px 0 8px;
}

.hero-subtitle {
    color: MUTED_COLOR;
    font-size: 14px;
    line-height: 1.7;
}

/* GLASS CARDS */

.glass-card {
    background: GLASS_COLOR;
    border: 1px solid BORDER_COLOR;
    border-radius: 27px;
    padding: 24px;
    backdrop-filter: blur(26px);
    -webkit-backdrop-filter: blur(26px);
    box-shadow:
        0 14px 35px SHADOW_COLOR,
        inset 0 1px 0 rgba(255,255,255,0.08);
    transition: transform .2s ease;
}

.glass-card:hover {
    transform: translateY(-2px);
}

.kpi-icon {
    width: 43px;
    height: 43px;
    display: flex;
    align-items: center;
    justify-content: center;
    background: KPI_ICON_BG;
    color: ACCENT_COLOR;
    border-radius: 15px;
    font-size: 21px;
    margin-bottom: 24px;
}

.kpi-value {
    color: TEXT_COLOR;
    font-size: clamp(28px, 2.6vw, 42px);
    font-weight: 700;
    letter-spacing: -1.4px;
}

.kpi-label {
    color: MUTED_COLOR;
    font-size: 13px;
    margin-top: 9px;
}

.kpi-foot {
    color: MUTED_COLOR;
    font-size: 11px;
    opacity: .8;
    margin-top: 7px;
}

.section-title {
    color: TEXT_COLOR;
    font-size: 21px;
    font-weight: 700;
    letter-spacing: -.5px;
    margin: 26px 0 15px;
}

.glass-pill {
    display: inline-block;
    background: GLASS_COLOR;
    border: 1px solid BORDER_COLOR;
    color: TEXT_COLOR;
    border-radius: 999px;
    padding: 9px 15px;
    font-size: 12px;
    margin-right: 7px;
    backdrop-filter: blur(16px);
}

/* PLOTLY GLASS */

[data-testid="stPlotlyChart"] {
    background: GLASS_COLOR;
    border: 1px solid BORDER_COLOR;
    border-radius: 27px;
    padding: 12px;
    backdrop-filter: blur(26px);
    -webkit-backdrop-filter: blur(26px);
    box-shadow: 0 12px 30px SHADOW_COLOR;
    overflow: hidden;
}

/* INPUTS */

[data-baseweb="select"] > div,
[data-baseweb="input"] > div,
[data-testid="stTextInput"] input {
    background: INPUT_COLOR !important;
    color: TEXT_COLOR !important;
    border: 1px solid BORDER_COLOR !important;
    border-radius: 15px !important;
}

[data-testid="stSidebar"] [data-baseweb="select"] > div,
[data-testid="stSidebar"] [data-baseweb="input"] > div,
[data-testid="stSidebar"] [data-testid="stTextInput"] input {
    background-color: INPUT_COLOR !important;
    box-shadow: none !important;
}

[data-baseweb="select"] input,
[data-baseweb="input"] input {
    color: TEXT_COLOR !important;
    -webkit-text-fill-color: TEXT_COLOR !important;
}

[data-baseweb="select"] span,
[data-baseweb="select"] svg {
    color: TEXT_COLOR !important;
}

input::placeholder {
    color: MUTED_COLOR !important;
    opacity: 1;
}

[data-baseweb="tag"] {
    background: ACCENT_COLOR !important;
    border-radius: 10px !important;
}

[data-baseweb="tag"] span {
    color: white !important;
}

/* DROPDOWNS */

[data-baseweb="popover"] [role="listbox"],
[data-baseweb="popover"] [data-baseweb="menu"] {
    background: POPOVER_BG !important;
    color: TEXT_COLOR !important;
    border: 1px solid BORDER_COLOR !important;
}

[role="option"] {
    color: TEXT_COLOR !important;
}

[role="option"]:hover {
    background: HOVER_BG !important;
}

/* NAVIGATION */

[data-testid="stSidebar"] [role="radiogroup"] label {
    padding: 9px 12px;
    border-radius: 15px;
    transition: background .2s;
}

[data-testid="stSidebar"] [role="radiogroup"] label:hover {
    background: HOVER_BG;
}

[data-testid="stSidebar"] [role="radiogroup"]
label:has(input:checked) {
    background: ACTIVE_BG;
    border: 1px solid ACTIVE_BORDER;
}

/* SEGMENTED CONTROL */

[data-testid="stSegmentedControl"] button {
    color: TEXT_COLOR !important;
    border-color: BORDER_COLOR !important;
}

/* GLASS TABLE */

.movie-table-wrap {
    background: GLASS_COLOR;
    border: 1px solid BORDER_COLOR;
    border-radius: 25px;
    overflow: hidden;
    backdrop-filter: blur(24px);
    -webkit-backdrop-filter: blur(24px);
    box-shadow: 0 14px 35px SHADOW_COLOR;
}

.movie-table-scroll {
    overflow: auto;
    max-height: 540px;
}

.movie-table {
    border-collapse: collapse;
    width: 100%;
    min-width: 900px;
    font-size: 12px;
    color: TABLE_TEXT;
}

.movie-table thead th {
    background: TABLE_HEAD;
    color: TABLE_TEXT;
    text-align: left;
    padding: 15px 16px;
    font-weight: 700;
    position: sticky;
    top: 0;
    z-index: 1;
    border-bottom: 1px solid TABLE_BORDER;
}

.movie-table tbody td {
    background: TABLE_BG;
    color: TABLE_TEXT;
    padding: 13px 16px;
    border-bottom: 1px solid TABLE_BORDER;
    white-space: nowrap;
}

.movie-table tbody tr:hover td {
    background: TABLE_HOVER;
}

.movie-table tbody tr:last-child td {
    border-bottom: 0;
}

.movie-table .movie-name {
    font-weight: 600;
}

.movie-table .muted-cell {
    color: TABLE_MUTED;
}

.source-tag {
    display: inline-block;
    background: TAG_BG;
    color: ACCENT_COLOR;
    border: 1px solid TAG_BORDER;
    padding: 4px 9px;
    border-radius: 999px;
    margin: 2px 3px 2px 0;
    font-size: 10px;
    font-weight: 700;
}

/* BUTTONS */

div.stDownloadButton > button {
    border-radius: 999px;
    background: ACCENT_COLOR;
    color: white;
    border: 0;
    padding: 9px 20px;
}

div.stDownloadButton > button:hover {
    background: ACCENT_DARK;
    color: white;
}

/* ALERTS */

[data-testid="stAlert"] {
    background: GLASS_COLOR !important;
    border: 1px solid BORDER_COLOR !important;
    border-radius: 18px !important;
}

/* SCROLLBAR */

::-webkit-scrollbar {
    width: 8px;
    height: 8px;
}

::-webkit-scrollbar-track {
    background: transparent;
}

::-webkit-scrollbar-thumb {
    background: SCROLLBAR_COLOR;
    border-radius: 20px;
}

</style>
"""

replacements = {
    "BG_COLOR": C["bg"],
    "TEXT_COLOR": C["text"],
    "MUTED_COLOR": C["muted"],
    "ACCENT_COLOR": C["accent"],
    "ACCENT_DARK": C["accent_dark"],
    "GLASS_COLOR": C["glass"],
    "BORDER_COLOR": C["border"],
    "SHADOW_COLOR": C["shadow"],
    "SIDEBAR_COLOR": C["sidebar"],
    "INPUT_COLOR": C["input"],
    "TABLE_BG": C["table_bg"],
    "TABLE_HEAD": C["table_head"],
    "TABLE_BORDER": C["table_border"],
    "TABLE_TEXT": C["table_text"],
    "TABLE_MUTED": C["table_muted"],
    "TABLE_HOVER": (
        "rgba(236,101,139,0.12)"
        if DARK else "rgba(187,54,85,0.07)"
    ),
    "TAG_BG": (
        "rgba(236,101,139,0.12)"
        if DARK else "rgba(187,54,85,0.08)"
    ),
    "TAG_BORDER": (
        "rgba(236,101,139,0.22)"
        if DARK else "rgba(187,54,85,0.15)"
    ),
    "GLOW_ONE": (
        "rgba(146,30,67,0.20)"
        if DARK else "rgba(191,70,102,0.16)"
    ),
    "GLOW_TWO": (
        "rgba(131,38,85,0.15)"
        if DARK else "rgba(150,192,205,0.28)"
    ),
    "HERO_START": (
        "rgba(255,255,255,0.08)"
        if DARK else "rgba(255,255,255,0.72)"
    ),
    "HERO_END": (
        "rgba(192,53,89,0.08)"
        if DARK else "rgba(255,255,255,0.34)"
    ),
    "KPI_ICON_BG": (
        "rgba(236,101,139,0.12)"
        if DARK else "rgba(187,54,85,0.10)"
    ),
    "POPOVER_BG": (
        "#292330" if DARK else "#F8FAFB"
    ),
    "HOVER_BG": (
        "rgba(236,101,139,0.12)"
        if DARK else "rgba(187,54,85,0.07)"
    ),
    "ACTIVE_BG": (
        "rgba(212,60,101,0.15)"
        if DARK else "rgba(187,54,85,0.09)"
    ),
    "ACTIVE_BORDER": (
        "rgba(235,94,135,0.20)"
        if DARK else "rgba(187,54,85,0.16)"
    ),
    "SCROLLBAR_COLOR": (
        "rgba(219,87,126,0.32)"
        if DARK else "rgba(110,135,145,0.28)"
    ),
}

# Replace longer tokens first to avoid accidental partial matches.
for key in sorted(replacements, key=len, reverse=True):
    css = css.replace(key, replacements[key])

st.markdown(css, unsafe_allow_html=True)

# Keep native radio/checkbox controls consistent with the glass sidebar.
if DARK:
    st.markdown("""
    <style>
    [data-testid="stSidebar"] [data-testid="stCheckbox"] label,
    [data-testid="stSidebar"] [role="radiogroup"] label {
        background: rgba(255,255,255,.035);
        border: 1px solid rgba(255,255,255,.07);
        border-radius: 11px;
        padding: 6px 8px;
        margin-bottom: 3px;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] {
        background: rgba(255,255,255,.045);
        border: 1px solid rgba(255,255,255,.10);
        border-radius: 13px;
    }
    [data-testid="stSidebar"] [data-testid="stTextInput"] input {
        background: #292330 !important;
        color: #F5EAF1 !important;
        -webkit-text-fill-color: #F5EAF1 !important;
    }
    </style>
    """, unsafe_allow_html=True)


# ============================================================
# FILTER DATA
# ============================================================

filtered = df[df["year"].isin(selected_years)].copy()

if selected_source != "All":
    filtered = filtered[
        filtered["sources"].apply(
            lambda values: selected_source in values
        )
    ]

if selected_genre != "All":
    filtered = filtered[
        filtered["genre"].fillna("").apply(
            lambda value: selected_genre in [
                part.strip()
                for part in str(value).split(",")
            ]
        )
    ]

if search_text:
    filtered = filtered[
        filtered["title"].str.contains(
            search_text,
            case=False,
            regex=False,
            na=False,
        )
    ]


# ============================================================
# METRICS
# ============================================================

movie_count = len(filtered)

rated_count = int(
    (
        filtered["tmdb_rating"].notna()
        | filtered["imdb_rating"].notna()
    ).sum()
)

avg_rating = (
    round(filtered["tmdb_rating"].mean(), 2)
    if filtered["tmdb_rating"].notna().any()
    else None
)

source_count = len({
    source
    for values in filtered["sources"]
    for source in values
})


# ============================================================
# UI HELPERS
# ============================================================

def hero(title, subtitle, eyebrow="MOVIE INTELLIGENCE"):
    st.markdown(
        f"""
        <div class="hero">
            <div class="eyebrow">
                {html.escape(eyebrow)}
            </div>
            <div class="hero-title">
                {html.escape(title)}
            </div>
            <div class="hero-subtitle">
                {html.escape(subtitle)}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def kpi(icon, value, label, foot):
    st.markdown(
        f"""
        <div class="glass-card">
            <div class="kpi-icon">{html.escape(icon)}</div>
            <div class="kpi-value">{html.escape(str(value))}</div>
            <div class="kpi-label">{html.escape(label)}</div>
            <div class="kpi-foot">{html.escape(foot)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def section(title):
    st.markdown(
        f'<div class="section-title">{html.escape(title)}</div>',
        unsafe_allow_html=True,
    )


def chart_style(fig, height=350, legend=False):
    fig.update_layout(
        height=height,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(
            family="DM Sans, sans-serif",
            color=C["text"],
        ),
        title_font=dict(size=16),
        margin=dict(l=22, r=20, t=55, b=30),
        showlegend=legend,
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            font=dict(color=C["muted"]),
        ),
    )

    fig.update_xaxes(
        showgrid=False,
        linecolor=C["grid"],
        tickfont=dict(color=C["muted"]),
    )

    fig.update_yaxes(
        gridcolor=C["grid"],
        zeroline=False,
        tickfont=dict(color=C["muted"]),
    )

    return fig


def cell(value):
    if value is None or pd.isna(value):
        return '<span class="muted-cell">—</span>'
    return html.escape(str(value))


def show_movie_table(data, height=430):
    rows_html = []

    for _, row in data.iterrows():

        tags = "".join(
            f'<span class="source-tag">{html.escape(str(s))}</span>'
            for s in row["sources"]
        )

        rows_html.append(f"""
        <tr>
            <td class="movie-name">{cell(row["title"])}</td>
            <td>{cell(row["year"])}</td>
            <td>{cell(row["genre"])}</td>
            <td>{cell(row["tmdb_rating"])}</td>
            <td>{cell(row["imdb_rating"])}</td>
            <td>{cell(row["runtime_minutes"])}</td>
            <td>{tags}</td>
        </tr>
        """)

    table_html = f"""
    <div class="movie-table-wrap">
        <div class="movie-table-scroll" style="max-height:{height}px;">
            <table class="movie-table">
                <thead>
                    <tr>
                        <th>Movie Title</th>
                        <th>Release Year</th>
                        <th>Genre</th>
                        <th>TMDB Rating</th>
                        <th>IMDb Rating</th>
                        <th>Runtime (min)</th>
                        <th>Sources</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join(rows_html)}
                </tbody>
            </table>
        </div>
    </div>
    """

    components.html(
        f"""
        <style>
        body {{
            margin: 0;
            background: transparent;
            font-family: 'DM Sans', Arial, sans-serif;
        }}
        .movie-table-wrap {{
            border-radius: 22px;
            overflow: hidden;
            background: {'#24212D' if DARK else '#F7FAFB'};
            border: 1px solid {'#393240' if DARK else '#DCE3E7'};
        }}
        .movie-table-scroll {{
            max-height: {height}px;
            overflow: auto;
        }}
        .movie-table {{
            border-collapse: collapse;
            width: 100%;
            min-width: 900px;
            font-size: 12px;
            color: {'#F2EAF0' if DARK else '#28333C'};
        }}
        .movie-table th {{
            position: sticky;
            top: 0;
            background: {'#302936' if DARK else '#E9EFF2'};
            padding: 14px;
            text-align: left;
            z-index: 2;
        }}
        .movie-table td {{
            padding: 13px 14px;
            border-top: 1px solid {'#393240' if DARK else '#DCE3E7'};
        }}
        .movie-table tr:hover td {{
            background: {'#37303F' if DARK else '#E9EFF2'};
        }}
        .source-tag {{
            display: inline-block;
            padding: 4px 9px;
            margin: 2px;
            border-radius: 999px;
            color: {'#FF9BB7' if DARK else '#A42D50'};
            background: {'#483041' if DARK else '#F7DFE7'};
        }}
        .muted-cell {{
            opacity: .55;
        }}
        </style>
        {table_html}
        """,
        height=height + 20,
        scrolling=False,
    )


def show_pipeline_table(data):
    if data.empty:
        st.info("No pipeline history found.")
        return

    rows_html = []

    for _, row in data.iterrows():

        finished = pd.to_datetime(
            row["finished_at"],
            errors="coerce",
        )

        finished_text = (
            finished.strftime("%Y-%m-%d %H:%M UTC")
            if not pd.isna(finished)
            else "—"
        )

        source_counts = row["source_counts"]

        if isinstance(source_counts, dict):
            source_counts = json.dumps(source_counts)

        rows_html.append(f"""
        <tr>
            <td>{cell(row["run_id"])}</td>
            <td>{cell(row["status"])}</td>
            <td>{cell(source_counts)}</td>
            <td>{cell(row["rows"])}</td>
            <td>{html.escape(finished_text)}</td>
        </tr>
        """)

    components.html(
        f"""
        <style>
        body {{
            margin: 0;
            background: transparent;
            font-family: 'DM Sans', Arial, sans-serif;
        }}
        .movie-table-wrap {{
            border-radius: 22px;
            overflow: hidden;
            background: {C["table_bg"]};
            border: 1px solid {C["table_border"]};
        }}
        .movie-table-scroll {{
            max-height: 470px;
            overflow: auto;
        }}
        .movie-table {{
            border-collapse: collapse;
            width: 100%;
            min-width: 760px;
            font-size: 12px;
            color: {C["table_text"]};
        }}
        .movie-table th {{
            position: sticky;
            top: 0;
            background: {C["table_head"]};
            padding: 14px;
            text-align: left;
            z-index: 2;
        }}
        .movie-table td {{
            padding: 13px 14px;
            border-top: 1px solid {C["table_border"]};
        }}
        .movie-table tr:hover td {{
            background: {C["hover"]};
        }}
        .muted-cell {{
            color: {C["table_muted"]};
        }}
        </style>
        <div class="movie-table-wrap">
            <div class="movie-table-scroll">
                <table class="movie-table">
                    <thead>
                        <tr>
                            <th>Run ID</th>
                            <th>Status</th>
                            <th>Source Counts</th>
                            <th>Rows</th>
                            <th>Finished At</th>
                        </tr>
                    </thead>
                    <tbody>
                        {''.join(rows_html)}
                    </tbody>
                </table>
            </div>
        </div>
        """,
        height=490,
        scrolling=False,
    )


# ============================================================
# OVERVIEW
# ============================================================

if page == "Overview":

    hero(
        "Movie Insights",
        "Explore cinema trends, audience ratings and "
        "quality-controlled movie data from 2020–2025.",
        "ANALYTICS / OVERVIEW",
    )

    st.markdown("""
        <span class="glass-pill">● Gold Dataset</span>
        <span class="glass-pill">2020–2025</span>
        <span class="glass-pill">PostgreSQL Live</span>
    """, unsafe_allow_html=True)

    st.write("")

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        kpi(
            "🎞",
            f"{movie_count:,}",
            "Total Movies",
            "Quality-filtered records",
        )

    with c2:
        kpi(
            "★",
            f"{avg_rating:.2f}"
            if avg_rating is not None else "N/A",
            "Avg. TMDB Rating",
            "Scale 0–10",
        )

    with c3:
        kpi(
            "◉",
            f"{rated_count:,}",
            "Rated Movies",
            "TMDB or IMDb",
        )

    with c4:
        kpi(
            "◇",
            str(source_count),
            "Data Sources",
            "In current selection",
        )

    section("Discover the Trends")

    left, right = st.columns([1.6, 1])

    with left:

        yearly = (
            filtered.groupby("year")
            .size()
            .reset_index(name="movies")
        )

        fig = px.bar(
            yearly,
            x="year",
            y="movies",
            title="Movies by Release Year",
            color_discrete_sequence=[C["accent"]],
        )

        fig.update_traces(
            marker_line_width=0,
            marker_opacity=0.9,
        )

        fig.update_xaxes(type="category")

        st.plotly_chart(
            chart_style(fig, 365),
            use_container_width=True,
        )

    with right:

        source_data = pd.DataFrame([
            {
                "Source": name.upper(),
                "Movies": int(
                    filtered["sources"].apply(
                        lambda items: name in items
                    ).sum()
                ),
            }
            for name in ["tmdb", "omdb", "wikipedia"]
        ])

        source_data = source_data[
            source_data["Movies"] > 0
        ]

        if not source_data.empty:

            fig = px.pie(
                source_data,
                names="Source",
                values="Movies",
                hole=0.68,
                title="Source Coverage",
                color_discrete_sequence=[
                    C["accent_dark"],
                    C["accent"],
                    "#E7A5B4",
                ],
            )

            fig.update_traces(
                textinfo="percent",
                textposition="outside",
            )

            st.plotly_chart(
                chart_style(fig, 365, legend=True),
                use_container_width=True,
            )

            st.caption(
                "Movies may belong to multiple sources."
            )

    section("Explore Movies")

    show_movie_table(
        filtered.head(15),
        height=450,
    )


# ============================================================
# MOVIE EXPLORER
# ============================================================

elif page == "Movie Explorer":

    hero(
        "Movie Explorer",
        "Search, filter and explore the cleaned movie collection.",
        "LIBRARY / GOLD DATA",
    )

    c1, c2, c3 = st.columns(3)

    with c1:
        kpi("🎞", f"{movie_count:,}", "Visible Movies", "Filtered records")

    with c2:
        kpi("★", f"{rated_count:,}", "Rated Movies", "With rating data")

    with c3:
        kpi("◇", str(source_count), "Data Sources", "Available sources")

    section("Movie Library")

    show_movie_table(filtered, height=550)

    st.write("")

    export = filtered.copy()

    export["sources"] = export["sources"].apply(
        lambda values: ", ".join(values)
    )

    st.download_button(
        "↓ Download filtered CSV",
        data=export.to_csv(index=False).encode("utf-8-sig"),
        file_name="cinemetrics_movies_gold.csv",
        mime="text/csv",
    )


# ============================================================
# ANALYTICS
# ============================================================

elif page == "Analytics":

    hero(
        "Cinema Analytics",
        "Understand ratings, trends and movie performance.",
        "INTELLIGENCE / ANALYTICS",
    )

    left, right = st.columns(2)

    with left:

        yearly_ratings = (
            filtered.groupby("year")["tmdb_rating"]
            .mean()
            .reset_index()
        )

        fig = px.line(
            yearly_ratings,
            x="year",
            y="tmdb_rating",
            markers=True,
            title="Average TMDB Rating by Year",
            color_discrete_sequence=[C["accent"]],
        )

        fig.update_traces(
            line=dict(width=3),
            marker=dict(size=9),
        )

        fig.update_xaxes(type="category")

        st.plotly_chart(
            chart_style(fig, 380),
            use_container_width=True,
        )

    with right:

        ratings = filtered["tmdb_rating"].dropna()

        fig = px.histogram(
            x=ratings,
            nbins=18,
            title="TMDB Rating Distribution",
            color_discrete_sequence=[C["accent"]],
        )

        fig.update_layout(
            xaxis_title="Rating",
            yaxis_title="Movies",
        )

        st.plotly_chart(
            chart_style(fig, 380),
            use_container_width=True,
        )

    section("Top Rated Movies")

    top_movies = filtered.sort_values(
        "tmdb_rating",
        ascending=False,
    ).head(20)

    show_movie_table(top_movies, height=540)


# ============================================================
# PIPELINE STATUS
# ============================================================

elif page == "Pipeline Status":

    hero(
        "Pipeline Health",
        "Monitor ingestion, data quality and recent Airflow runs.",
        "OPERATIONS / PIPELINE",
    )

    successful_runs = (
        int(
            runs["status"]
            .astype(str)
            .str.lower()
            .eq("success")
            .sum()
        )
        if not runs.empty else 0
    )

    c1, c2, c3 = st.columns(3)

    with c1:
        kpi(
            "✓",
            f"{len(df):,}",
            "Gold Movies",
            "API-supported records",
        )

    with c2:
        kpi(
            "◇",
            f"{quarantine_count:,}",
            "Quarantine",
            "Pending review",
        )

    with c3:
        kpi(
            "↗",
            str(successful_runs),
            "Successful Runs",
            "Latest 15 executions",
        )

    section("Recent Pipeline Activity")

    show_pipeline_table(runs)

    section("Data Quality")

    quality_counts = pd.DataFrame(
        [
            {"Category": "Invalid", "Records": quarantine_counts.get("invalid", 0)},
            {"Category": "Duplicate", "Records": quarantine_counts.get("duplicate", 0)},
            {"Category": "Suspicious", "Records": quarantine_counts.get("suspicious", 0)},
        ]
    )
    st.dataframe(
        quality_counts,
        hide_index=True,
        use_container_width=True,
    )

    st.info(
        "Invalid records fail structure, year, title, rating, or runtime checks. "
        "Duplicate records repeat the same title/year within one source. "
        "Suspicious records have no TMDB or OMDb support and remain in Quarantine."
    )
