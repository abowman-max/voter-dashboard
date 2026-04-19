import json
from pathlib import Path
import base64

import altair as alt
import duckdb
import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="Candidate Connect", layout="wide")

# R2 public-read setup
R2_BASE = "https://b1017650e855cac9d9605c7f4e9647a1.r2.cloudflarestorage.com"
R2_BUCKET = "candidate-connect-data"

LOCAL_ROOT = Path("/tmp/candidate_connect_r2")
LOCAL_MANIFEST = LOCAL_ROOT / "dataset_manifest.json"

CC_LOGO = Path("candidate_connect_logo.png")
TSS_LOGO = Path("TSS_Logo_Transparent.png")

PARTY_COLOR_MAP = {"R": "#c62828", "D": "#1565c0", "O": "#2e7d32"}
AGE_COLOR_RANGE = ["#7a1523","#9f2032","#b8454f","#c96a6c","#d88f87","#e8b8aa","#f2dbcf","#f7ebe5","#fbf5f2"]
GENDER_COLOR_RANGE = ["#7a1523","#4b4f54","#b98088","#9b9da1","#d8b6bb"]

st.markdown("""
<style>
.block-container {padding-top: 1.35rem; padding-bottom: .75rem; max-width: 1600px;}
.top-shell, .section-card, .chart-card, .table-card, .metric-card {
    border: 1px solid #ded7d7;
    border-radius: 14px;
    background: white;
    box-shadow: 0 1px 3px rgba(0,0,0,.04);
}
.top-shell {padding: 1.2rem 1rem 1rem 1rem; margin-top: .35rem; margin-bottom: .95rem; overflow: visible;}
.section-card, .chart-card, .table-card {padding: .8rem .9rem; margin-bottom: .8rem;}
.metric-card {padding: .6rem .7rem; height: 94px; display:flex; flex-direction:column; justify-content:center;}
.metric-label {font-size: 11px; color: #666; margin-bottom: .12rem;}
.metric-value {font-size: 1.55rem; font-weight: 700; color: #24303f; line-height: 1.1;}
.small-header {font-size: 16px; font-weight: 900; color: #142033; margin-bottom: .45rem;}
.tiny-muted {font-size: 10px; color: #596579;}
.brand-grid {display:grid; grid-template-columns: 200px 1fr 170px; gap:18px; align-items:center;}
.brand-left {display:flex; align-items:center; justify-content:flex-start; min-height:78px;}
.brand-center {display:flex; flex-direction:column; justify-content:center;}
.brand-right {display:flex; flex-direction:column; align-items:center; justify-content:center; min-height:78px;}
.brand-title {font-size: 24px; font-weight: 800; color:#153d73; line-height:1.05; margin-bottom:.12rem;}
.brand-sub {font-size: 11px; color:#334a6a; font-weight:700;}
.brand-status {font-size: 11px; color:#506078; margin-top:.28rem; font-weight:600;}
.powered-by {font-size:10px; color:#777; margin-bottom:.18rem; text-align:center; font-weight:700;}
.logo-cc {max-width:168px; height:auto; display:block;}
.logo-tss {max-width:102px; height:auto; display:block; margin:0 auto;}
.section-divider {height:1px; background:linear-gradient(to right, rgba(0,0,0,0), #d7d1d1 12%, #d7d1d1 88%, rgba(0,0,0,0)); margin:.5rem 0 .8rem 0;}
.sidebar-note {font-size:10px; color:#687487; margin-top:-.25rem; margin-bottom:.4rem;}
.stButton > button {width:100%; border-radius:9px; min-height: 2.1rem; font-weight: 600;}
.cc-mini-table {width:100%; border-collapse:collapse; font-size:11px; margin-top:.35rem;}
.cc-mini-table th {text-align:center; padding:4px 6px; color:#364152; font-weight:800; border-bottom:1px solid #ece7e7;}
.cc-mini-table td {padding:4px 6px; border-bottom:1px solid #f0ebeb;}
.cc-mini-table td.label-cell {text-align:left;}
.cc-mini-table td.num-cell {text-align:center;}
.cc-mini-table tr.total-row td {font-weight:700; border-top:1px solid #dcd6d6;}
.cc-swatch {display:inline-block; width:9px; height:9px; border-radius:2px; vertical-align:middle; margin-right:8px; position:relative; top:-1px; border:1px solid rgba(0,0,0,.08);}
.empty-shell {padding: 1.2rem 1rem; text-align:center; color:#556273;}
@media (max-width: 1100px) {
  .brand-grid {grid-template-columns: 1fr; gap:10px;}
  .brand-left, .brand-right {justify-content:center;}
  .brand-center {text-align:center;}
}
</style>
""", unsafe_allow_html=True)

def img_to_data_uri(path: Path) -> str:
    if not path.exists():
        return ""
    encoded = base64.b64encode(path.read_bytes()).decode("utf-8")
    return f"data:image/png;base64,{encoded}"

def file_modified_text(path: Path) -> str:
    if not path.exists():
        return "R2 public source"
    try:
        ts = pd.Timestamp(path.stat().st_mtime, unit="s")
        return ts.strftime("%m/%d/%Y %I:%M %p")
    except Exception:
        return "R2 public source"

def divider():
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

def quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'

def sql_string_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"

@st.cache_resource(show_spinner=False)
def get_conn():
    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=4")
    return con

def first_existing(columns, candidates):
    lower_map = {str(c).strip().lower(): c for c in columns}
    for col in candidates:
        if col in columns:
            return col
        hit = lower_map.get(str(col).strip().lower())
        if hit is not None:
            return hit
    return None

def ensure_parent(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)

def r2_public_url(key: str) -> str:
    return f"{R2_BASE}/{R2_BUCKET}/{key}"

def download_public_object(key: str, local_path: Path):
    if local_path.exists():
        return
    ensure_parent(local_path)
    url = r2_public_url(key)
    with requests.get(url, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

@st.cache_data(show_spinner=True)
def load_manifest():
    LOCAL_ROOT.mkdir(parents=True, exist_ok=True)
    download_public_object("dataset_manifest.json", LOCAL_MANIFEST)
    return json.loads(LOCAL_MANIFEST.read_text(encoding="utf-8"))

@st.cache_data(show_spinner=True)
def ensure_index_shards():
    manifest = load_manifest()
    local_paths = []
    for shard in manifest["index"]["shards"]:
        key = shard["key"]
        local_path = LOCAL_ROOT / key
        download_public_object(key, local_path)
        local_paths.append(str(local_path))
    return local_paths, manifest

@st.cache_data(show_spinner=False)
def get_schema(local_paths):
    con = get_conn()
    paths_sql = "[" + ", ".join(sql_string_literal(p) for p in local_paths) + "]"
    df = con.execute(f"DESCRIBE SELECT * FROM read_parquet({paths_sql})").df()
    return df["column_name"].tolist()

def build_view_sql(columns, local_paths):
    q = quote_ident
    status_col = first_existing(columns, ["VoterStatus", "voterstatus"])
    gender_col = first_existing(columns, ["Gender", "Sex"])
    age_range_col = first_existing(columns, ["Age_Range", "Age Range", "AGERANGE"])
    reg_col = first_existing(columns, ["RegistrationDate", "registrationdate"])
    party_col = first_existing(columns, ["Party"])
    hh_col = first_existing(columns, ["HH_ID"])
    email_col = first_existing(columns, ["Email"])
    landline_col = first_existing(columns, ["Landline"])
    mobile_col = first_existing(columns, ["Mobile"])
    vote_hist_col = first_existing(columns, ["V4A"])
    age_col = first_existing(columns, ["Age"])
    house_col = first_existing(columns, ["House Number"])
    street_col = first_existing(columns, ["Street Name"])
    apt_col = first_existing(columns, ["Apartment Number"])

    exprs = ["*"]

    if status_col:
        exprs.append(f"upper(trim(coalesce(cast({q(status_col)} as varchar), ''))) as _Status")
    else:
        exprs.append("'A' as _Status")

    if party_col:
        exprs.append(
            f"""case
                when upper(trim(coalesce(cast({q(party_col)} as varchar), ''))) in ('', 'NONE', 'NAN', 'U') then 'O'
                else upper(trim(cast({q(party_col)} as varchar)))
            end as _PartyNorm"""
        )
    else:
        exprs.append("'O' as _PartyNorm")

    if gender_col:
        exprs.append(
            f"""case
                when upper(trim(coalesce(cast({q(gender_col)} as varchar), ''))) in ('', 'NONE', 'NAN') then 'U'
                else upper(trim(cast({q(gender_col)} as varchar)))
            end as _Gender"""
        )
    else:
        exprs.append("'U' as _Gender")

    if age_col:
        exprs.append(f"try_cast({q(age_col)} as double) as _AgeNum")
    else:
        exprs.append("NULL::DOUBLE as _AgeNum")

    if age_range_col:
        exprs.append(f"nullif(trim(coalesce(cast({q(age_range_col)} as varchar), '')), '') as _AgeRange")
    else:
        exprs.append("NULL::VARCHAR as _AgeRange")

    if reg_col:
        exprs.append(f"try_cast({q(reg_col)} as timestamp) as _RegistrationDate")
    else:
        exprs.append("NULL::TIMESTAMP as _RegistrationDate")

    for alias, src in [("_HasEmail", email_col), ("_HasLandline", landline_col), ("_HasMobile", mobile_col)]:
        if src:
            exprs.append(
                f"""case
                    when trim(coalesce(cast({q(src)} as varchar), '')) in ('', 'None', 'NONE', 'nan', 'NAN') then false
                    else true
                end as {alias}"""
            )
        else:
            exprs.append(f"false as {alias}")

    if vote_hist_col:
        exprs.append(f"upper(trim(coalesce(cast({q(vote_hist_col)} as varchar), ''))) as _VoteHistory")
    else:
        exprs.append("'' as _VoteHistory")

    if hh_col:
        exprs.append(f"nullif(trim(coalesce(cast({q(hh_col)} as varchar), '')), '') as _HouseholdKey")
    else:
        parts = []
        if house_col:
            parts.append(f"coalesce(cast({q(house_col)} as varchar), '')")
        if street_col:
            parts.append(f"coalesce(cast({q(street_col)} as varchar), '')")
        if apt_col:
            parts.append(f"coalesce(cast({q(apt_col)} as varchar), '')")
        if parts:
            exprs.append("concat_ws('|', " + ", ".join(parts) + ") as _HouseholdKey")
        else:
            exprs.append("NULL::VARCHAR as _HouseholdKey")

    paths_sql = "[" + ", ".join(sql_string_literal(p) for p in local_paths) + "]"
    return "CREATE OR REPLACE VIEW voters AS SELECT\n    " + ",\n    ".join(exprs) + f"\nFROM read_parquet({paths_sql})"

def prepare_db(local_paths):
    con = get_conn()
    cols = get_schema(local_paths)
    con.execute(build_view_sql(cols, local_paths))
    return cols

def sql_literal_list(values):
    return ", ".join(["?"] * len(values))

def current_filter_clause(active, columns):
    where = ["_Status = 'A'"]
    params = []
    geo_cols = [c for c in ["County", "Municipality", "Precinct", "USC", "STS", "STH", "School District"] if c in columns]
    for col in geo_cols:
        picked = active.get(col, [])
        if picked:
            where.append(f"{quote_ident(col)} IN ({sql_literal_list(picked)})")
            params.extend(picked)
    if active.get("party_pick"):
        picked = active["party_pick"]
        where.append(f"_PartyNorm IN ({sql_literal_list(picked)})")
        params.extend(picked)
    if active.get("hh_party_pick") and "HH-Party" in columns:
        picked = active["hh_party_pick"]
        where.append(f'{quote_ident("HH-Party")} IN ({sql_literal_list(picked)})')
        params.extend(picked)
    if active.get("calc_party_pick") and "CalculatedParty" in columns:
        picked = active["calc_party_pick"]
        where.append(f'{quote_ident("CalculatedParty")} IN ({sql_literal_list(picked)})')
        params.extend(picked)
    if active.get("gender_pick"):
        picked = active["gender_pick"]
        where.append(f"_Gender IN ({sql_literal_list(picked)})")
        params.extend(picked)
    if active.get("age_range_pick"):
        picked = active["age_range_pick"]
        where.append(f"_AgeRange IN ({sql_literal_list(picked)})")
        params.extend(picked)
    if active.get("age_slider") is not None:
        where.append("_AgeNum >= ? AND _AgeNum <= ?")
        params.extend([active["age_slider"][0], active["age_slider"][1]])
    if active.get("vote_history_pick"):
        picked = active["vote_history_pick"]
        where.append(f"_VoteHistory IN ({sql_literal_list(picked)})")
        params.extend(picked)
    if active.get("has_email") == "Has Email":
        where.append("_HasEmail = true")
    elif active.get("has_email") == "No Email":
        where.append("_HasEmail = false")
    if active.get("has_landline") == "Has Landline":
        where.append("_HasLandline = true")
    elif active.get("has_landline") == "No Landline":
        where.append("_HasLandline = false")
    if active.get("has_mobile") == "Has Mobile":
        where.append("_HasMobile = true")
    elif active.get("has_mobile") == "No Mobile":
        where.append("_HasMobile = false")
    return " WHERE " + " AND ".join(where), params

def get_distinct_options(column: str, label_expr: str | None = None):
    con = get_conn()
    expr = label_expr or quote_ident(column)
    df = con.execute(
        f"""
        SELECT {expr} AS value
        FROM voters
        WHERE _Status = 'A' AND nullif(trim(cast({quote_ident(column)} as varchar)), '') IS NOT NULL
        GROUP BY 1
        ORDER BY 1
        """
    ).df()
    return [str(v) for v in df["value"].tolist() if str(v).strip() != ""]

def get_basic_options(columns):
    options = {}
    geo_cols = [c for c in ["County", "Municipality", "Precinct", "USC", "STS", "STH", "School District"] if c in columns]
    for col in geo_cols:
        options[col] = get_distinct_options(col)
    options["party_vals"] = get_distinct_options("_PartyNorm", "_PartyNorm") if "Party" in columns else []
    options["gender_vals"] = get_distinct_options("_Gender", "_Gender")
    options["age_range_vals"] = get_distinct_options("_AgeRange", "_AgeRange")
    options["hh_party_vals"] = get_distinct_options("HH-Party") if "HH-Party" in columns else []
    options["calc_party_vals"] = get_distinct_options("CalculatedParty") if "CalculatedParty" in columns else []
    options["vote_history_vals"] = get_distinct_options("_VoteHistory", "_VoteHistory") if "V4A" in columns else []
    con = get_conn()
    age_min, age_max = con.execute(
        "SELECT min(_AgeNum), max(_AgeNum) FROM voters WHERE _Status = 'A' AND _AgeNum IS NOT NULL"
    ).fetchone()
    options["age_min"] = int(age_min) if age_min is not None else None
    options["age_max"] = int(age_max) if age_max is not None else None
    return options

def query_metrics(active, columns):
    con = get_conn()
    where_sql, params = current_filter_clause(active, columns)
    return con.execute(
        f"""
        SELECT
            count(*) AS voters,
            count(DISTINCT coalesce(nullif(_HouseholdKey, ''), cast(row_number() over() as varchar))) AS households,
            sum(CASE WHEN _HasEmail THEN 1 ELSE 0 END) AS emails,
            sum(CASE WHEN _HasLandline THEN 1 ELSE 0 END) AS landlines,
            sum(CASE WHEN _HasMobile THEN 1 ELSE 0 END) AS mobiles,
            count(DISTINCT {quote_ident("County")}) FILTER (WHERE {quote_ident("County")} IS NOT NULL) AS unique_counties,
            count(DISTINCT {quote_ident("Precinct")}) FILTER (WHERE {quote_ident("Precinct")} IS NOT NULL) AS unique_precincts
        FROM voters
        {where_sql}
        """,
        params,
    ).df().iloc[0].to_dict()

def query_chart(active, columns, group_expr, label, not_blank=True):
    con = get_conn()
    where_sql, params = current_filter_clause(active, columns)
    extra = f" AND {group_expr} IS NOT NULL AND cast({group_expr} as varchar) <> ''" if not_blank else ""
    return con.execute(
        f"""
        SELECT {group_expr} AS "{label}", count(*) AS "Count"
        FROM voters
        {where_sql}
        {extra}
        GROUP BY 1
        ORDER BY 2 DESC, 1
        """,
        params,
    ).df()

def query_area_summary(active, columns, area_col):
    con = get_conn()
    where_sql, params = current_filter_clause(active, columns)
    return con.execute(
        f"""
        SELECT
            coalesce(cast({quote_ident(area_col)} as varchar), '(Blank)') AS "{area_col}",
            count(*) AS Individuals,
            count(DISTINCT coalesce(nullif(_HouseholdKey, ''), cast(row_number() over() as varchar))) AS Households
        FROM voters
        {where_sql}
        GROUP BY 1
        ORDER BY Individuals DESC, 1
        """,
        params,
    ).df()

def fmt_pct(v: float) -> str:
    rounded = round(v, 1)
    return f"{int(rounded)}%" if float(rounded).is_integer() else f"{rounded:.1f}%"

def make_summary_table(df_chart: pd.DataFrame, label_col: str, value_col: str, colors):
    total = pd.to_numeric(df_chart[value_col], errors="coerce").fillna(0).sum()
    headers = "<tr><th></th><th>{}</th><th>{}</th><th>%</th></tr>".format(label_col, value_col)
    rows = []
    for i, (_, row) in enumerate(df_chart.iterrows()):
        val = float(pd.to_numeric(row[value_col], errors="coerce"))
        pct = 0 if total == 0 else (val / total) * 100
        color = colors[i] if i < len(colors) else "#999999"
        rows.append(
            f"<tr><td class='num-cell'><span class='cc-swatch' style='background:{color};'></span></td>"
            f"<td class='label-cell'>{row[label_col]}</td><td class='num-cell'>{val:,.0f}</td><td class='num-cell'>{fmt_pct(pct)}</td></tr>"
        )
    rows.append(f"<tr class='total-row'><td></td><td class='label-cell'>Total</td><td class='num-cell'>{total:,.0f}</td><td class='num-cell'>100%</td></tr>")
    return f"<table class='cc-mini-table'><thead>{headers}</thead><tbody>{''.join(rows)}</tbody></table>"

def pie_chart_with_table(df_chart: pd.DataFrame, label_col: str, value_col: str, title: str, color_mode: str):
    st.markdown(f'<div class="small-header">{title}</div>', unsafe_allow_html=True)
    if df_chart.empty:
        st.caption("No data")
        return
    chart_df = df_chart.copy()
    chart_df[value_col] = pd.to_numeric(chart_df[value_col], errors="coerce").fillna(0)
    chart_df = chart_df.sort_values(value_col, ascending=False).reset_index(drop=True)
    total = chart_df[value_col].sum()
    chart_df["Percent"] = 0 if total == 0 else (chart_df[value_col] / total) * 100
    domain = chart_df[label_col].astype(str).tolist()
    if color_mode == "party":
        colors = [PARTY_COLOR_MAP.get(v, "#757575") for v in domain]
    elif color_mode == "age":
        colors = AGE_COLOR_RANGE[:len(domain)]
    else:
        colors = GENDER_COLOR_RANGE[:len(domain)]
    chart = alt.Chart(chart_df).mark_arc(innerRadius=18, outerRadius=60).encode(
        theta=alt.Theta(field=value_col, type="quantitative"),
        color=alt.Color(field=label_col, type="nominal", scale=alt.Scale(domain=domain, range=colors), legend=None),
        tooltip=[alt.Tooltip(f"{label_col}:N"), alt.Tooltip(f"{value_col}:Q", format=","), alt.Tooltip("Percent:Q", format=".1f")]
    ).properties(height=220)
    st.altair_chart(chart, use_container_width=True)
    st.markdown(make_summary_table(chart_df, label_col, value_col, colors), unsafe_allow_html=True)

cc_logo_uri = img_to_data_uri(CC_LOGO)
tss_logo_uri = img_to_data_uri(TSS_LOGO)

header_html = f"""
<div class="top-shell">
  <div class="brand-grid">
    <div class="brand-left">{f'<img class="logo-cc" src="{cc_logo_uri}"/>' if cc_logo_uri else ''}</div>
    <div class="brand-center">
      <div class="brand-title">Candidate Connect</div>
      <div class="brand-sub">DuckDB + R2 Pass 1: Fast counts and filters on R2 index shards</div>
      <div class="brand-status">Storage: Cloudflare R2 Public Read &nbsp;&nbsp;|&nbsp;&nbsp; Last Local Manifest: {file_modified_text(LOCAL_MANIFEST)}</div>
    </div>
    <div class="brand-right"><div class="powered-by">Powered By</div>{f'<img class="logo-tss" src="{tss_logo_uri}"/>' if tss_logo_uri else ''}</div>
  </div>
</div>
"""
st.markdown(header_html, unsafe_allow_html=True)

if "data_loaded" not in st.session_state:
    st.session_state.data_loaded = False
if "filters_applied" not in st.session_state:
    st.session_state.filters_applied = False
if "active_filters" not in st.session_state:
    st.session_state.active_filters = {}
if "columns" not in st.session_state:
    st.session_state.columns = []
if "options" not in st.session_state:
    st.session_state.options = {}

with st.sidebar:
    st.header("Filters")
    st.markdown('<div class="sidebar-note">This version uses public HTTPS downloads from Cloudflare R2 instead of boto3. Make sure your R2 bucket is Public Read enabled.</div>', unsafe_allow_html=True)

    if not st.session_state.data_loaded:
        if st.button("Load Voter Data", use_container_width=True, type="primary"):
            with st.spinner("Downloading manifest and opening R2 index shards..."):
                local_paths, _manifest = ensure_index_shards()
                st.session_state.columns = prepare_db(local_paths)
                st.session_state.options = get_basic_options(st.session_state.columns)
                st.session_state.data_loaded = True
                st.session_state.filters_applied = False
            st.rerun()
    else:
        st.success("R2 index shards loaded")

        cols = st.session_state.columns
        opts = st.session_state.options

        with st.form("filter_form", clear_on_submit=False):
            with st.expander("Geography", expanded=False):
                geo_cols = [c for c in ["County", "Municipality", "Precinct", "USC", "STS", "STH", "School District"] if c in cols]
                geo_selections = {}
                for col in geo_cols:
                    geo_selections[col] = st.multiselect(col, opts.get(col, []), default=st.session_state.active_filters.get(col, []))

            with st.expander("Voter Details", expanded=False):
                party_pick = st.multiselect("Party", opts.get("party_vals", []), default=st.session_state.active_filters.get("party_pick", []))
                hh_party_pick = st.multiselect("Household Party", opts.get("hh_party_vals", []), default=st.session_state.active_filters.get("hh_party_pick", [])) if "HH-Party" in cols else []
                calc_party_pick = st.multiselect("Calculated Party", opts.get("calc_party_vals", []), default=st.session_state.active_filters.get("calc_party_pick", [])) if "CalculatedParty" in cols else []
                gender_pick = st.multiselect("Gender", opts.get("gender_vals", []), default=st.session_state.active_filters.get("gender_pick", []))
                age_range_pick = st.multiselect("Age Range", opts.get("age_range_vals", []), default=st.session_state.active_filters.get("age_range_pick", []))
                age_slider = None
                if opts.get("age_min") is not None and opts.get("age_max") is not None:
                    age_slider = st.slider("Age", opts["age_min"], opts["age_max"], st.session_state.active_filters.get("age_slider", (opts["age_min"], opts["age_max"])))

            with st.expander("Vote History", expanded=False):
                vote_history_pick = st.multiselect("Vote History", opts.get("vote_history_vals", []), default=st.session_state.active_filters.get("vote_history_pick", []))

            with st.expander("Contact Filters", expanded=False):
                email_opts = ["All", "Has Email", "No Email"]
                landline_opts = ["All", "Has Landline", "No Landline"]
                mobile_opts = ["All", "Has Mobile", "No Mobile"]
                has_email = st.selectbox("Email", email_opts, index=email_opts.index(st.session_state.active_filters.get("has_email", "All")))
                has_landline = st.selectbox("Landline", landline_opts, index=landline_opts.index(st.session_state.active_filters.get("has_landline", "All")))
                has_mobile = st.selectbox("Mobile", mobile_opts, index=mobile_opts.index(st.session_state.active_filters.get("has_mobile", "All")))

            st.caption("Counts stay at zero until you click Apply Filters.")
            cols2 = st.columns(2)
            apply_filters = cols2[0].form_submit_button("Apply Filters", use_container_width=True, type="primary")
            clear_filters = cols2[1].form_submit_button("Clear Filters", use_container_width=True)

        if clear_filters:
            st.session_state.active_filters = {}
            st.session_state.filters_applied = False
            st.rerun()

        if apply_filters:
            st.session_state.active_filters = {
                **geo_selections,
                "party_pick": party_pick,
                "hh_party_pick": hh_party_pick,
                "calc_party_pick": calc_party_pick,
                "gender_pick": gender_pick,
                "age_range_pick": age_range_pick,
                "age_slider": age_slider,
                "vote_history_pick": vote_history_pick,
                "has_email": has_email,
                "has_landline": has_landline,
                "has_mobile": has_mobile,
            }
            st.session_state.filters_applied = True
            st.rerun()

if not st.session_state.data_loaded:
    zeros = [("Voters", "0"), ("Households", "0"), ("Emails", "0"), ("Landlines", "0"), ("Mobiles", "0"), ("Unique Counties", "0"), ("Unique Precincts", "0")]
    metric_cols = st.columns(7, gap="small")
    for col, (label, value) in zip(metric_cols, zeros):
        with col:
            st.markdown(f'<div class="metric-card"><div class="metric-label">{label}</div><div class="metric-value">{value}</div></div>', unsafe_allow_html=True)
    divider()
    st.markdown('<div class="section-card empty-shell"><div class="small-header">Ready to load</div><div class="tiny-muted">Click <strong>Load Voter Data</strong> in the sidebar to open the R2 index shards with DuckDB.</div></div>', unsafe_allow_html=True)
    st.stop()

if not st.session_state.filters_applied:
    zeros = [("Voters", "0"), ("Households", "0"), ("Emails", "0"), ("Landlines", "0"), ("Mobiles", "0"), ("Unique Counties", "0"), ("Unique Precincts", "0")]
    metric_cols = st.columns(7, gap="small")
    for col, (label, value) in zip(metric_cols, zeros):
        with col:
            st.markdown(f'<div class="metric-card"><div class="metric-label">{label}</div><div class="metric-value">{value}</div></div>', unsafe_allow_html=True)
    divider()
    st.markdown('<div class="section-card empty-shell"><div class="small-header">Filters are loaded</div><div class="tiny-muted">Choose your filters in the sidebar and click <strong>Apply Filters</strong> to run counts and charts.</div></div>', unsafe_allow_html=True)
    st.stop()

active = st.session_state.active_filters
columns = st.session_state.columns

with st.spinner("Running DuckDB queries..."):
    metrics = query_metrics(active, columns)
    party_df = query_chart(active, columns, "_PartyNorm", "Party")
    gender_df = query_chart(active, columns, "_Gender", "Gender")
    age_df = query_chart(active, columns, "_AgeRange", "Age Range")
    area_choices = [c for c in ["County", "Municipality", "Precinct", "USC", "STS", "STH", "School District"] if c in columns]

metric_cols = st.columns(7, gap="small")
metric_values = [
    ("Voters", f"{int(metrics.get('voters') or 0):,}"),
    ("Households", f"{int(metrics.get('households') or 0):,}"),
    ("Emails", f"{int(metrics.get('emails') or 0):,}"),
    ("Landlines", f"{int(metrics.get('landlines') or 0):,}"),
    ("Mobiles", f"{int(metrics.get('mobiles') or 0):,}"),
    ("Unique Counties", f"{int(metrics.get('unique_counties') or 0):,}"),
    ("Unique Precincts", f"{int(metrics.get('unique_precincts') or 0):,}"),
]
for col, (label, value) in zip(metric_cols, metric_values):
    with col:
        st.markdown(f'<div class="metric-card"><div class="metric-label">{label}</div><div class="metric-value">{value}</div></div>', unsafe_allow_html=True)

divider()

chart_cols = st.columns(3, gap="medium")
with chart_cols[0]:
    st.markdown('<div class="chart-card">', unsafe_allow_html=True)
    pie_chart_with_table(party_df, "Party", "Count", "Party Breakdown", "party")
    st.markdown('</div>', unsafe_allow_html=True)
with chart_cols[1]:
    st.markdown('<div class="chart-card">', unsafe_allow_html=True)
    pie_chart_with_table(gender_df, "Gender", "Count", "Gender Breakdown", "gender")
    st.markdown('</div>', unsafe_allow_html=True)
with chart_cols[2]:
    st.markdown('<div class="chart-card">', unsafe_allow_html=True)
    pie_chart_with_table(age_df, "Age Range", "Count", "Age Range Breakdown", "age")
    st.markdown('</div>', unsafe_allow_html=True)

divider()

st.markdown('<div class="table-card">', unsafe_allow_html=True)
st.markdown('<div class="small-header">Counts by Area</div>', unsafe_allow_html=True)
if area_choices:
    selected_area = st.selectbox("Area", area_choices, label_visibility="collapsed")
    area_df = query_area_summary(active, columns, selected_area).copy()
    area_df["Individuals"] = pd.to_numeric(area_df["Individuals"], errors="coerce").fillna(0).map(lambda x: f"{x:,.0f}")
    area_df["Households"] = pd.to_numeric(area_df["Households"], errors="coerce").fillna(0).map(lambda x: f"{x:,.0f}")
    rows_html = "".join(
        f"<tr><td class='label-cell'>{row[selected_area]}</td><td class='num-cell'>{row['Individuals']}</td><td class='num-cell'>{row['Households']}</td></tr>"
        for _, row in area_df.iterrows()
    )
    table_html = f"<table class='cc-mini-table' style='font-size:12px;'><thead><tr><th style='text-align:left'>{selected_area}</th><th>Individuals</th><th>Households</th></tr></thead><tbody>{rows_html}</tbody></table>"
    st.markdown(table_html, unsafe_allow_html=True)
else:
    st.caption("No area columns found")
st.markdown('</div>', unsafe_allow_html=True)
