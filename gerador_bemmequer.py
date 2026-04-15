#!/usr/bin/env python3
"""
Laboratório Bem Me Quer — Gerador automático do Dashboard Meta Ads
Lê a planilha do Google Sheets (Stract) e gera o HTML atualizado.
"""

import pandas as pd
import json
import re
import hashlib
import requests
from datetime import date, timedelta
from pathlib import Path

# ── CONFIG ────────────────────────────────────────────
SHEET_ID = "1rNDwriAHBml5Qv7tK99nRzZrKB3N1VF9r8zZGt7eRag"
SHEET_TAB = "meta-ads"
OUTPUT_FILE = "index.html"
TEMPLATE_FILE = "template_bemmequer.html"

SHEET_URL    = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_TAB}"
SHEET_URL_GA = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet=breakdown-gender-age"
SHEET_URL_PT = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet=breakdown-platform"


# ── DOWNLOAD DE IMAGENS ───────────────────────────────
def download_thumb(url, img_dir):
    if not url or str(url) == "nan":
        return ""
    try:
        ext = ".png" if ".png" in url.lower() else ".jpg"
        fname = hashlib.md5(url.encode()).hexdigest()[:16] + ext
        fpath = img_dir / fname
        if not fpath.exists():
            r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code == 200:
                fpath.write_bytes(r.content)
            else:
                return ""
        return "imgs/" + fname
    except Exception:
        return ""


# ── LER PLANILHA ──────────────────────────────────────
def load_sheet():
    print("Lendo planilha...")
    df = pd.read_csv(SHEET_URL)

    col_map = {
        "Date": "date",
        "Campaign Name": "campaign",
        "Adset Name": "adset",
        "Ad Name": "ad",
        "Thumbnail URL": "thumb",
        "Spend (Cost, Amount Spent)": "spend",
        "Impressions": "impressions",
        "Clicks": "clicks",
        "Action Link Clicks": "link_clicks",
        "Action Messaging Conversations Started (Onsite Conversion)": "leads",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for c in ["spend", "leads", "impressions", "clicks", "link_clicks"]:
        if c in df.columns:
            df[c] = pd.to_numeric(
                df[c].astype(str).str.replace(",", ".", regex=False),
                errors="coerce"
            ).fillna(0)

    df["ym"] = df["date"].dt.to_period("M")
    df = df.dropna(subset=["date"])

    last = df["date"].max()
    print(f"OK: {len(df)} linhas | {df['date'].min().date()} -> {last.date()}")
    print(f"Spend total: R${df['spend'].sum():,.2f} | Conversões: {int(df['leads'].sum()):,}")
    return df


# ── DADOS DIÁRIOS ─────────────────────────────────────
def build_daily(df):
    daily = df.groupby("date").agg(
        spend=("spend", "sum"),
        leads=("leads", "sum"),
        impressions=("impressions", "sum"),
        link_clicks=("link_clicks", "sum"),
    ).reset_index().sort_values("date")

    all_days = sorted(daily["date"].unique())[-330:]

    out = {k: [] for k in ["days", "spend", "leads", "cpl", "ctr", "cpm"]}

    for d in all_days:
        r = daily[daily["date"] == d].iloc[0]
        tl = int(r["leads"])
        ts = round(float(r["spend"]), 2)
        imp = float(r["impressions"])
        lc = float(r["link_clicks"])

        out["days"].append(pd.Timestamp(d).strftime("%d/%m"))
        out["spend"].append(ts)
        out["leads"].append(tl)
        out["cpl"].append(round(ts / tl, 2) if tl > 0 else None)
        out["ctr"].append(round(lc / imp * 100, 2) if imp > 0 else None)
        out["cpm"].append(round(ts / imp * 1000, 2) if imp > 0 else None)

    last_day = out["days"][-1] if out["days"] else "—"
    return out, last_day, all_days


# ── KPIs POR PERÍODO ──────────────────────────────────
def build_kpis(df, all_days):
    last = pd.Timestamp(all_days[-1])
    kpis = {}

    def kpi_for(p):
        tS = float(p["spend"].sum())
        tL = int(p["leads"].sum())
        imp = float(p["impressions"].sum())
        lc = float(p["link_clicks"].sum())
        return {
            "spend": round(tS, 2), "leads": tL,
            "cpl": round(tS / tL, 2) if tL else None,
            "ctr": round(lc / imp * 100, 2) if imp else None,
            "cpm": round(tS / imp * 1000, 2) if imp else None,
        }

    for n in [1, 7, 14, 30]:
        start = last - pd.Timedelta(days=n - 1)
        p = df[(df["date"] >= start) & (df["date"] <= last)]
        kpis[str(n)] = kpi_for(p)

    all_months = sorted(df["ym"].unique())
    for ym in all_months:
        p = df[df["ym"] == ym]
        if len(p) == 0:
            continue
        kpis[str(ym)] = kpi_for(p)

    return kpis


# ── CAMPANHAS POR PERÍODO ─────────────────────────────
def build_camps_period(df, p, all_months, cur_ym_str):
    if len(p) == 0:
        return []

    camps = p.groupby("campaign").agg(
        spend=("spend", "sum"), leads=("leads", "sum"),
        impressions=("impressions", "sum"), link_clicks=("link_clicks", "sum"),
    ).reset_index()
    camps["cpl"] = (camps["spend"] / camps["leads"]).where(camps["leads"] > 0).round(2)
    camps["cpm"] = (camps["spend"] / camps["impressions"] * 1000).where(camps["impressions"] > 0).round(2)
    camps["ctr"] = (camps["link_clicks"] / camps["impressions"] * 100).where(camps["impressions"] > 0).round(2)
    camps = camps.sort_values("leads", ascending=False)

    try:
        cur_ym = pd.Period(cur_ym_str, "M")
        cur_idx = list(all_months).index(cur_ym)
    except Exception:
        cur_idx = len(all_months) - 1
    spk_months = all_months[max(0, cur_idx - 5):cur_idx + 1]

    out = []
    for _, r in camps.iterrows():
        adsets = p[p["campaign"] == r["campaign"]].groupby("adset").agg(
            spend=("spend", "sum"), leads=("leads", "sum"),
            impressions=("impressions", "sum"), link_clicks=("link_clicks", "sum"),
        ).reset_index()
        adsets["cpl"] = (adsets["spend"] / adsets["leads"]).where(adsets["leads"] > 0).round(2)
        adsets["cpm"] = (adsets["spend"] / adsets["impressions"] * 1000).where(adsets["impressions"] > 0).round(2)
        adsets["ctr"] = (adsets["link_clicks"] / adsets["impressions"] * 100).where(adsets["impressions"] > 0).round(2)
        adsets = adsets.sort_values("leads", ascending=False)

        spk = []
        for sm in spk_months:
            cm = df[(df["ym"] == sm) & (df["campaign"] == r["campaign"])]
            ts2 = float(cm["spend"].sum())
            tl2 = float(cm["leads"].sum())
            spk.append(round(ts2 / tl2, 2) if tl2 > 0 else None)

        conjs = []
        for _, a in adsets.iterrows():
            ads_sub = p[
                (p["campaign"] == r["campaign"]) & (p["adset"] == a["adset"])
            ].groupby("ad").agg(
                spend=("spend", "sum"), leads=("leads", "sum"),
                impressions=("impressions", "sum"), link_clicks=("link_clicks", "sum"),
            ).reset_index()
            ads_sub["cpl"] = (ads_sub["spend"] / ads_sub["leads"]).where(ads_sub["leads"] > 0).round(2)
            ads_sub["cpm"] = (ads_sub["spend"] / ads_sub["impressions"] * 1000).where(ads_sub["impressions"] > 0).round(2)
            ads_sub["ctr"] = (ads_sub["link_clicks"] / ads_sub["impressions"] * 100).where(ads_sub["impressions"] > 0).round(2)
            ads_sub = ads_sub.sort_values("leads", ascending=False)

            anuncios = []
            for _, ad in ads_sub.iterrows():
                thumb_rows = p[
                    (p["campaign"] == r["campaign"]) &
                    (p["adset"] == a["adset"]) &
                    (p["ad"] == ad["ad"])
                ]["thumb"]
                th = str(thumb_rows.iloc[0]) if len(thumb_rows) > 0 else ""
                if th == "nan":
                    th = ""
                anuncios.append({
                    "n": str(ad["ad"]),
                    "spend": round(float(ad["spend"]), 2),
                    "leads": int(ad["leads"]),
                    "cpl": float(ad["cpl"]) if pd.notna(ad["cpl"]) else None,
                    "cpm": float(ad["cpm"]) if pd.notna(ad["cpm"]) else None,
                    "ctr": float(ad["ctr"]) if pd.notna(ad["ctr"]) else None,
                    "thumb": th,
                })

            conjs.append({
                "n": str(a["adset"]),
                "spend": round(float(a["spend"]), 2),
                "leads": int(a["leads"]),
                "cpl": float(a["cpl"]) if pd.notna(a["cpl"]) else None,
                "cpm": float(a["cpm"]) if pd.notna(a["cpm"]) else None,
                "ctr": float(a["ctr"]) if pd.notna(a["ctr"]) else None,
                "ads": anuncios,
            })

        out.append({
            "n": str(r["campaign"]),
            "spend": round(float(r["spend"]), 2),
            "leads": int(r["leads"]),
            "cpl": float(r["cpl"]) if pd.notna(r["cpl"]) else None,
            "cpm": float(r["cpm"]) if pd.notna(r["cpm"]) else None,
            "ctr": float(r["ctr"]) if pd.notna(r["ctr"]) else None,
            "spk": spk,
            "conjs": conjs,
        })
    return out


def build_camps(df, all_days):
    all_months = sorted(df["ym"].unique())
    last = pd.Timestamp(all_days[-1])
    result = {}

    for n in [1, 7, 14, 30]:
        start = last - pd.Timedelta(days=n - 1)
        p = df[(df["date"] >= start) & (df["date"] <= last)]
        cur_ym = str(last.to_period("M"))
        result[str(n)] = build_camps_period(df, p, all_months, cur_ym)
        print(f"   {n}d: {len(result[str(n)])} campanhas")

    for ym in all_months:
        ym_str = str(ym)
        p = df[df["ym"] == ym]
        result[ym_str] = build_camps_period(df, p, all_months, ym_str)
        print(f"   {ym_str}: {len(result[ym_str])} campanhas")

    return result


# ── DADOS MENSAIS ─────────────────────────────────────
def build_monthly(df):
    months = sorted(df["ym"].unique())
    data = {k: [] for k in ["meses", "lbl", "totalS", "totalL", "cplG"]}

    for m in months:
        p = df[df["ym"] == m]
        ts = round(float(p["spend"].sum()), 2)
        tl = int(p["leads"].sum())
        data["meses"].append(str(m))
        data["lbl"].append(pd.Period(m, "M").strftime("%b/%y").capitalize())
        data["totalS"].append(ts)
        data["totalL"].append(tl)
        data["cplG"].append(round(ts / tl, 2) if tl > 0 else None)

    return data


# ── DIAS POR MÊS ─────────────────────────────────────
def build_mes_days(df):
    result = {}
    for ym in df["ym"].unique():
        days = sorted(df[df["ym"] == ym]["date"].unique())
        result[str(ym)] = [pd.Timestamp(d).strftime("%d/%m") for d in days]
    return result


# ── CRIATIVOS ─────────────────────────────────────────
def build_ads_period(df, p, img_dir):
    df_ads = p[
        p["thumb"].notna() &
        (p["thumb"].astype(str) != "") &
        (p["thumb"].astype(str) != "nan")
    ].copy()
    if df_ads.empty:
        return []

    ads_agg = df_ads.groupby(["ad", "thumb"]).agg(
        leads=("leads", "sum"), spend=("spend", "sum"),
        impressions=("impressions", "sum"), link_clicks=("link_clicks", "sum"),
    ).reset_index().sort_values("leads", ascending=False)
    ads_agg["cpl"] = (ads_agg["spend"] / ads_agg["leads"]).where(ads_agg["leads"] > 0).round(2)
    ads_agg["ctr"] = (ads_agg["link_clicks"] / ads_agg["impressions"] * 100).where(ads_agg["impressions"] > 0).round(2)

    result = []
    for _, r in ads_agg.drop_duplicates("ad").iterrows():
        local = download_thumb(str(r["thumb"]), img_dir)
        tL = int(r["leads"])
        tS = float(r["spend"])
        result.append({
            "n": str(r["ad"]),
            "leads": tL,
            "cpl": round(tS / tL, 2) if tL > 0 else None,
            "ctr": float(r["ctr"]) if pd.notna(r["ctr"]) else None,
            "thumb": local,
        })
    return result


def build_ads(df, img_dir, all_days):
    last = pd.Timestamp(all_days[-1])
    all_months = sorted(df["ym"].unique())
    result = {}

    for n in [1, 7, 14, 30]:
        start = last - pd.Timedelta(days=n - 1)
        p = df[(df["date"] >= start) & (df["date"] <= last)]
        result[str(n)] = build_ads_period(df, p, img_dir)

    for ym in all_months:
        ym_str = str(ym)
        p = df[df["ym"] == ym]
        result[ym_str] = build_ads_period(df, p, img_dir)

    return result


# ── BREAKDOWNS ────────────────────────────────────────
def load_breakdown_ga():
    df = pd.read_csv(SHEET_URL_GA)
    df["date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["spend"] = pd.to_numeric(
        df["Spend (Cost, Amount Spent)"].astype(str).str.replace(",", ".", regex=False),
        errors="coerce"
    ).fillna(0)
    df["leads"] = pd.to_numeric(
        df["Action Messaging Conversations Started (Onsite Conversion)"].astype(str).str.replace(",", ".", regex=False),
        errors="coerce"
    ).fillna(0)
    df["impressions"] = pd.to_numeric(
        df["Impressions"].astype(str).str.replace(",", ".", regex=False),
        errors="coerce"
    ).fillna(0)
    df["age"] = df["Age (Breakdown)"].astype(str)
    df["gender"] = df["Gender (Breakdown)"].astype(str)
    df = df[df["age"].notna() & (df["age"] != "nan") & (df["age"] != "")]
    df["ym"] = df["date"].dt.to_period("M")
    return df.dropna(subset=["date"])


def load_breakdown_pt():
    df = pd.read_csv(SHEET_URL_PT)
    df["date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["spend"] = pd.to_numeric(
        df["Spend (Cost, Amount Spent)"].astype(str).str.replace(",", ".", regex=False),
        errors="coerce"
    ).fillna(0)
    df["leads"] = pd.to_numeric(
        df["Action Messaging Conversations Started (Onsite Conversion)"].astype(str).str.replace(",", ".", regex=False),
        errors="coerce"
    ).fillna(0)
    df["impressions"] = pd.to_numeric(
        df["Impressions"].astype(str).str.replace(",", ".", regex=False),
        errors="coerce"
    ).fillna(0)
    df["platform"] = df["Platform Position (Breakdown)"]
    return df.dropna(subset=["date"])


def build_gender_age(df_ga, start, end):
    p = df_ga[(df_ga["date"] >= pd.Timestamp(start)) & (df_ga["date"] <= pd.Timestamp(end))]
    age_order = ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]

    age_agg = p.groupby("age").agg(
        spend=("spend", "sum"), leads=("leads", "sum"), impressions=("impressions", "sum")
    ).reset_index()
    age_agg = age_agg[age_agg["age"].isin(age_order) & (age_agg["spend"] > 0)]
    age_agg["cpl"] = (age_agg["spend"] / age_agg["leads"]).where(age_agg["leads"] > 0).round(2)
    age_agg["cpm"] = (age_agg["spend"] / age_agg["impressions"] * 1000).where(age_agg["impressions"] > 0).round(2)
    age_agg["_o"] = age_agg["age"].apply(lambda x: age_order.index(x) if x in age_order else 99)
    age_agg = age_agg.sort_values("_o")

    gen_agg = p.groupby("gender").agg(
        spend=("spend", "sum"), leads=("leads", "sum"), impressions=("impressions", "sum")
    ).reset_index()
    gen_agg = gen_agg[gen_agg["gender"].isin(["female", "male"]) & (gen_agg["spend"] > 0)]
    gen_agg["cpl"] = (gen_agg["spend"] / gen_agg["leads"]).where(gen_agg["leads"] > 0).round(2)
    gen_agg["cpm"] = (gen_agg["spend"] / gen_agg["impressions"] * 1000).where(gen_agg["impressions"] > 0).round(2)
    gen_agg = gen_agg.sort_values("leads", ascending=False)

    def to_list(df_, dim):
        return [
            {
                "n": str(r[dim]),
                "spend": round(float(r["spend"]), 2),
                "leads": int(r["leads"]),
                "cpl": float(r["cpl"]) if pd.notna(r["cpl"]) else None,
                "cpm": float(r["cpm"]) if pd.notna(r["cpm"]) else None,
            }
            for _, r in df_.iterrows()
        ]

    return {"age": to_list(age_agg, "age"), "gender": to_list(gen_agg, "gender")}


def build_platform(df_pt, start, end, top=15):
    p = df_pt[(df_pt["date"] >= pd.Timestamp(start)) & (df_pt["date"] <= pd.Timestamp(end))]
    if len(p) == 0:
        return []
    agg = p.groupby("platform").agg(
        spend=("spend", "sum"), leads=("leads", "sum"), impressions=("impressions", "sum")
    ).reset_index()
    agg["cpl"] = (agg["spend"] / agg["leads"]).where(agg["leads"] > 0).round(2)
    agg["cpm"] = (agg["spend"] / agg["impressions"] * 1000).where(agg["impressions"] > 0).round(2)
    agg = agg[agg["spend"] > 0].sort_values("leads", ascending=False).head(top)
    return [
        {
            "n": str(r["platform"]),
            "spend": round(float(r["spend"]), 2),
            "leads": int(r["leads"]),
            "cpl": float(r["cpl"]) if pd.notna(r["cpl"]) else None,
            "cpm": float(r["cpm"]) if pd.notna(r["cpm"]) else None,
        }
        for _, r in agg.iterrows()
    ]


def build_breakdowns(df_ga, df_pt, all_days, all_months):
    last = pd.Timestamp(all_days[-1])
    result = {}

    for n in [1, 7, 14, 30]:
        start = last - pd.Timedelta(days=n - 1)
        gd = build_gender_age(df_ga, start, last)
        pt = build_platform(df_pt, start, last)
        result[str(n)] = {"age": gd["age"], "gender": gd["gender"], "platform": pt}

    for ym in all_months:
        ym_str = str(ym)
        ym_pd = pd.Period(ym_str, "M")
        start = ym_pd.start_time
        end_t = min(ym_pd.end_time, last)
        gd = build_gender_age(df_ga, start, end_t)
        pt = build_platform(df_pt, start, end_t)
        result[ym_str] = {"age": gd["age"], "gender": gd["gender"], "platform": pt}

    return result


# ── INJETAR NO HTML ───────────────────────────────────
def inject_data(template_path, daily, last_day, monthly, camps, mes_days, kpis, ads_data, breakdown_data):
    html = Path(template_path).read_text(encoding="utf-8")

    def replace_js_const(html, const_name, value):
        pattern = rf"(const {const_name}\s*=\s*)(\{{[\s\S]*?\}}|\[[^\]]*\]|\"[^\"]*\");"
        replacement = f"const {const_name} = {json.dumps(value, ensure_ascii=False)};"
        new_html, count = re.subn(pattern, replacement, html, count=1)
        if count == 0:
            print(f"  AVISO: não encontrou: {const_name}")
        return new_html

    html = replace_js_const(html, "DAILY", daily)
    html = replace_js_const(html, "MONTHLY", monthly)
    html = replace_js_const(html, "CAMPS_MES", camps)
    html = replace_js_const(html, "MES_DAYS", mes_days)
    html = replace_js_const(html, "KPIS_PERIODO", kpis)
    html = replace_js_const(html, "ADS_DATA", ads_data)
    html = replace_js_const(html, "BREAKDOWN_DATA", breakdown_data)

    html = re.sub(r"Dados até \d{2}/\d{2}", f"Dados até {last_day}", html)
    today_str = date.today().strftime("%d/%m/%Y")
    html = re.sub(r"\d{2}/\d{2}/\d{4} · via planilha", f"{today_str} · via planilha", html)

    return html


# ── MAIN ──────────────────────────────────────────────
def main():
    print("=" * 55)
    print("Laboratório Bem Me Quer — Dashboard Meta Ads")
    print("=" * 55)

    df = load_sheet()

    print("Dados diários...")
    daily, last_day, all_days = build_daily(df)
    print(f"   {len(daily['days'])} dias | último: {last_day}")

    print("KPIs por período...")
    kpis = build_kpis(df, all_days)

    print("Dados mensais...")
    monthly = build_monthly(df)
    all_months = sorted(df["ym"].unique())
    print(f"   {len(monthly['meses'])} meses")

    print("Campanhas por período...")
    camps = build_camps(df, all_days)

    print("Dias por mês...")
    mes_days = build_mes_days(df)

    print("Imagens dos criativos...")
    img_dir = Path("imgs")
    img_dir.mkdir(exist_ok=True)
    ads_data = build_ads(df, img_dir, all_days)

    print("Carregando breakdowns...")
    df_ga = load_breakdown_ga()
    df_pt = load_breakdown_pt()

    print("Gerando breakdowns por período...")
    breakdown_data = build_breakdowns(df_ga, df_pt, all_days, all_months)

    print("Gerando HTML...")
    if not Path(TEMPLATE_FILE).exists():
        print(f"Template não encontrado: {TEMPLATE_FILE}")
        return

    html = inject_data(
        TEMPLATE_FILE, daily, last_day, monthly, camps,
        mes_days, kpis, ads_data, breakdown_data
    )
    Path(OUTPUT_FILE).write_text(html, encoding="utf-8")
    print(f"Dashboard gerado: {OUTPUT_FILE} ({len(html)//1024}KB)")
    print("=" * 55)


if __name__ == "__main__":
    main()
