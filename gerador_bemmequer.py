#!/usr/bin/env python3
"""
Laboratório Bem Me Quer — Gerador automático do Dashboard
Meta Ads + Google Ads — 4 painéis completos
"""

import pandas as pd
import json
import re
import hashlib
import requests
from datetime import date
from pathlib import Path

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG DO CLIENTE — edite apenas esta seção
# ══════════════════════════════════════════════════════════════════════════════

SHEET_ID      = "1rNDwriAHBml5Qv7tK99nRzZrKB3N1VF9r8zZGt7eRag"
TEMPLATE_FILE = "template_bemmequer.html"
OUTPUT_FILE   = "index.html"

# Metas de CPL — Meta Ads
META_CPL_BOM    = 11    # ≤ este valor → verde
META_CPL_MEDIO  = 15    # ≤ este valor → amarelo  |  acima → vermelho

# Metas de CPL — Google Ads
GOOGLE_CPL_BOM   = 10   # ≤ este valor → verde
GOOGLE_CPL_MEDIO = 30   # ≤ este valor → amarelo  |  acima → vermelho

# ══════════════════════════════════════════════════════════════════════════════
# NÃO PRECISA MEXER ABAIXO DESTA LINHA
# ══════════════════════════════════════════════════════════════════════════════

def sheet_url(tab):
    return f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={tab}"

URL_META      = sheet_url("meta-ads")
URL_META_GA   = sheet_url("breakdown-gender-age")
URL_META_PT   = sheet_url("breakdown-platform")
URL_GOOGLE    = sheet_url("google-ads")
URL_GOOGLE_GE = sheet_url("google-breakdown-gender")
URL_GOOGLE_AG = sheet_url("google-breakdown-age")


# ── UTILS ─────────────────────────────────────────────────────────────────────
def to_num(series):
    return pd.to_numeric(
        series.astype(str).str.replace(",", ".", regex=False),
        errors="coerce"
    ).fillna(0)

def safe(v):
    if pd.isna(v) or v is None:
        return None
    f = float(v)
    return round(f, 2) if f != 0 else None

def r2(v):
    return round(float(v), 2) if v and not pd.isna(v) else None


# ── IMAGENS ───────────────────────────────────────────────────────────────────
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


# ══════════════════════════════════════════════════════════════════════════════
# META ADS
# ══════════════════════════════════════════════════════════════════════════════

def load_meta():
    print("  Lendo meta-ads...")
    df = pd.read_csv(URL_META)
    df = df.rename(columns={
        "Date": "date", "Campaign Name": "campaign",
        "Adset Name": "adset", "Ad Name": "ad",
        "Thumbnail URL": "thumb",
        "Spend (Cost, Amount Spent)": "spend",
        "Impressions": "impressions", "Clicks": "clicks",
        "Action Link Clicks": "link_clicks",
        "Action Messaging Conversations Started (Onsite Conversion)": "leads",
    })
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for c in ["spend", "leads", "impressions", "clicks", "link_clicks"]:
        if c in df.columns:
            df[c] = to_num(df[c])
    df["ym"] = df["date"].dt.to_period("M")
    df = df.dropna(subset=["date"])
    print(f"     {len(df)} linhas | {df['date'].min().date()} → {df['date'].max().date()}")
    return df


def meta_daily(df):
    agg = df.groupby("date").agg(
        spend=("spend","sum"), leads=("leads","sum"),
        impressions=("impressions","sum"), link_clicks=("link_clicks","sum")
    ).reset_index().sort_values("date")
    out = {k: [] for k in ["days","spend","leads","cpl","ctr","cpm"]}
    for _, r in agg.iterrows():
        ts = round(float(r["spend"]), 2)
        tl = int(r["leads"])
        imp = float(r["impressions"])
        lc  = float(r["link_clicks"])
        out["days"].append(r["date"].strftime("%d/%m"))
        out["spend"].append(ts)
        out["leads"].append(tl)
        out["cpl"].append(round(ts/tl, 2) if tl > 0 else None)
        out["ctr"].append(round(lc/imp*100, 2) if imp > 0 else None)
        out["cpm"].append(round(ts/imp*1000, 2) if imp > 0 else None)
    return out, out["days"][-1] if out["days"] else "—", sorted(df["date"].unique())


def meta_kpis(df, all_days):
    last = pd.Timestamp(all_days[-1])
    kpis = {}
    def kpi(p):
        ts=float(p["spend"].sum()); tl=int(p["leads"].sum())
        imp=float(p["impressions"].sum()); lc=float(p["link_clicks"].sum())
        return {"spend":round(ts,2),"leads":tl,
                "cpl":round(ts/tl,2) if tl else None,
                "ctr":round(lc/imp*100,2) if imp else None,
                "cpm":round(ts/imp*1000,2) if imp else None}
    for n in [1,7,14,30]:
        start = last - pd.Timedelta(days=n-1)
        kpis[str(n)] = kpi(df[(df["date"]>=start)&(df["date"]<=last)])
    for ym in sorted(df["ym"].unique()):
        kpis[str(ym)] = kpi(df[df["ym"]==ym])
    return kpis


def meta_monthly(df):
    months = sorted(df["ym"].unique())
    PT_MONTHS = {"Jan":"Jan","Feb":"Fev","Mar":"Mar","Apr":"Abr","May":"Mai",
                 "Jun":"Jun","Jul":"Jul","Aug":"Ago","Sep":"Set","Oct":"Out",
                 "Nov":"Nov","Dec":"Dez"}
    data = {k: [] for k in ["meses","lbl","totalS","totalL","cplG","cpmG","ctrG"]}
    for m in months:
        p = df[df["ym"]==m]
        ts=round(float(p["spend"].sum()),2); tl=int(p["leads"].sum())
        imp=float(p["impressions"].sum()); lc=float(p["link_clicks"].sum())
        raw_lbl = pd.Period(m,"M").strftime("%b/%y")
        pt_lbl  = PT_MONTHS.get(raw_lbl[:3], raw_lbl[:3]) + raw_lbl[3:]
        data["meses"].append(str(m))
        data["lbl"].append(pt_lbl)
        data["totalS"].append(ts)
        data["totalL"].append(tl)
        data["cplG"].append(round(ts/tl,2) if tl>0 else None)
        data["cpmG"].append(round(ts/imp*1000,2) if imp>0 else None)
        data["ctrG"].append(round(lc/imp*100,2) if imp>0 else None)
    return data


def meta_mes_days(df):
    result = {}
    for ym in df["ym"].unique():
        days = sorted(df[df["ym"]==ym]["date"].unique())
        result[str(ym)] = [pd.Timestamp(d).strftime("%d/%m") for d in days]
    return result


def meta_camps_period(df, p, all_months, cur_ym_str):
    if len(p) == 0:
        return []
    camps = p.groupby("campaign").agg(
        spend=("spend","sum"), leads=("leads","sum"),
        impressions=("impressions","sum"), link_clicks=("link_clicks","sum")
    ).reset_index()
    camps["cpl"] = (camps["spend"]/camps["leads"]).where(camps["leads"]>0).round(2)
    camps["cpm"] = (camps["spend"]/camps["impressions"]*1000).where(camps["impressions"]>0).round(2)
    camps["ctr"] = (camps["link_clicks"]/camps["impressions"]*100).where(camps["impressions"]>0).round(2)
    camps = camps.sort_values("leads", ascending=False)
    try:
        cur_ym  = pd.Period(cur_ym_str,"M")
        cur_idx = list(all_months).index(cur_ym)
    except Exception:
        cur_idx = len(all_months)-1
    spk_months = all_months[max(0,cur_idx-5):cur_idx+1]
    out = []
    for _, r in camps.iterrows():
        adsets = p[p["campaign"]==r["campaign"]].groupby("adset").agg(
            spend=("spend","sum"), leads=("leads","sum"),
            impressions=("impressions","sum"), link_clicks=("link_clicks","sum")
        ).reset_index()
        adsets["cpl"] = (adsets["spend"]/adsets["leads"]).where(adsets["leads"]>0).round(2)
        adsets["cpm"] = (adsets["spend"]/adsets["impressions"]*1000).where(adsets["impressions"]>0).round(2)
        adsets["ctr"] = (adsets["link_clicks"]/adsets["impressions"]*100).where(adsets["impressions"]>0).round(2)
        adsets = adsets.sort_values("leads", ascending=False)
        spk = []
        for sm in spk_months:
            cm = df[(df["ym"]==sm)&(df["campaign"]==r["campaign"])]
            ts2=float(cm["spend"].sum()); tl2=float(cm["leads"].sum())
            spk.append(round(ts2/tl2,2) if tl2>0 else None)
        conjs = []
        for _, a in adsets.iterrows():
            ads_sub = p[(p["campaign"]==r["campaign"])&(p["adset"]==a["adset"])].groupby("ad").agg(
                spend=("spend","sum"), leads=("leads","sum"),
                impressions=("impressions","sum"), link_clicks=("link_clicks","sum")
            ).reset_index()
            ads_sub["cpl"] = (ads_sub["spend"]/ads_sub["leads"]).where(ads_sub["leads"]>0).round(2)
            ads_sub["cpm"] = (ads_sub["spend"]/ads_sub["impressions"]*1000).where(ads_sub["impressions"]>0).round(2)
            ads_sub["ctr"] = (ads_sub["link_clicks"]/ads_sub["impressions"]*100).where(ads_sub["impressions"]>0).round(2)
            ads_sub = ads_sub.sort_values("leads", ascending=False)
            anuncios = []
            for _, ad in ads_sub.iterrows():
                th_rows = p[(p["campaign"]==r["campaign"])&(p["adset"]==a["adset"])&(p["ad"]==ad["ad"])]["thumb"]
                th = str(th_rows.iloc[0]) if len(th_rows)>0 else ""
                if th == "nan": th = ""
                anuncios.append({"n":str(ad["ad"]),"spend":round(float(ad["spend"]),2),"leads":int(ad["leads"]),
                    "cpl":safe(ad["cpl"]),"cpm":safe(ad["cpm"]),"ctr":safe(ad["ctr"]),"thumb":th})
            conjs.append({"n":str(a["adset"]),"spend":round(float(a["spend"]),2),"leads":int(a["leads"]),
                "cpl":safe(a["cpl"]),"cpm":safe(a["cpm"]),"ctr":safe(a["ctr"]),"ads":anuncios})
        out.append({"n":str(r["campaign"]),"spend":round(float(r["spend"]),2),"leads":int(r["leads"]),
            "cpl":safe(r["cpl"]),"cpm":safe(r["cpm"]),"ctr":safe(r["ctr"]),"spk":spk,"conjs":conjs})
    return out


def meta_camps(df, all_days):
    all_months = sorted(df["ym"].unique())
    last = pd.Timestamp(all_days[-1])
    result = {}
    for n in [1,7,14,30]:
        start = last - pd.Timedelta(days=n-1)
        p = df[(df["date"]>=start)&(df["date"]<=last)]
        result[str(n)] = meta_camps_period(df, p, all_months, str(last.to_period("M")))
    for ym in all_months:
        result[str(ym)] = meta_camps_period(df, df[df["ym"]==ym], all_months, str(ym))
    return result


def meta_ads_period(p, img_dir):
    df_t = p[p["thumb"].notna()&(p["thumb"].astype(str)!="")&(p["thumb"].astype(str)!="nan")].copy()
    if df_t.empty:
        return []
    agg = df_t.groupby(["ad","thumb"]).agg(
        leads=("leads","sum"), spend=("spend","sum"),
        impressions=("impressions","sum"), link_clicks=("link_clicks","sum")
    ).reset_index().sort_values("leads", ascending=False)
    agg["cpl"] = (agg["spend"]/agg["leads"]).where(agg["leads"]>0).round(2)
    agg["ctr"] = (agg["link_clicks"]/agg["impressions"]*100).where(agg["impressions"]>0).round(2)
    result = []
    for _, r in agg.drop_duplicates("ad").iterrows():
        local = download_thumb(str(r["thumb"]), img_dir)
        result.append({"n":str(r["ad"]),"leads":int(r["leads"]),"cpl":safe(r["cpl"]),"ctr":safe(r["ctr"]),"thumb":local})
    return result


def meta_ads(df, img_dir, all_days):
    last = pd.Timestamp(all_days[-1])
    all_months = sorted(df["ym"].unique())
    result = {}
    for n in [1,7,14,30]:
        start = last - pd.Timedelta(days=n-1)
        result[str(n)] = meta_ads_period(df[(df["date"]>=start)&(df["date"]<=last)], img_dir)
    for ym in all_months:
        result[str(ym)] = meta_ads_period(df[df["ym"]==ym], img_dir)
    return result


def load_meta_ga():
    df = pd.read_csv(URL_META_GA)
    df["date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["spend"] = to_num(df["Spend (Cost, Amount Spent)"])
    df["leads"] = to_num(df["Action Messaging Conversations Started (Onsite Conversion)"])
    df["impressions"] = to_num(df["Impressions"])
    df["age"] = df["Age (Breakdown)"].astype(str)
    df["gender"] = df["Gender (Breakdown)"].astype(str)
    df = df[df["age"].notna()&(df["age"]!="nan")&(df["age"]!="")]
    return df.dropna(subset=["date"])


def load_meta_pt():
    df = pd.read_csv(URL_META_PT)
    df["date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["spend"] = to_num(df["Spend (Cost, Amount Spent)"])
    df["leads"] = to_num(df["Action Messaging Conversations Started (Onsite Conversion)"])
    df["impressions"] = to_num(df["Impressions"])
    df["platform"] = df["Platform Position (Breakdown)"]
    return df.dropna(subset=["date"])


def meta_breakdowns(df_ga, df_pt, all_days, all_months):
    last = pd.Timestamp(all_days[-1])
    AGE_ORDER = ["18-24","25-34","35-44","45-54","55-64","65+"]

    def bd(start, end):
        pa = df_ga[(df_ga["date"]>=pd.Timestamp(start))&(df_ga["date"]<=pd.Timestamp(end))]
        aa = pa[pa["age"].isin(AGE_ORDER)].groupby("age").agg(spend=("spend","sum"),leads=("leads","sum"),impressions=("impressions","sum")).reset_index()
        aa = aa[aa["spend"]>0].copy()
        aa["cpl"] = (aa["spend"]/aa["leads"]).where(aa["leads"]>0).round(2)
        aa["cpm"] = (aa["spend"]/aa["impressions"]*1000).where(aa["impressions"]>0).round(2)
        aa["_o"] = aa["age"].apply(lambda x: AGE_ORDER.index(x) if x in AGE_ORDER else 99)
        aa = aa.sort_values("_o")
        ga2 = pa[pa["gender"].isin(["female","male"])].groupby("gender").agg(spend=("spend","sum"),leads=("leads","sum"),impressions=("impressions","sum")).reset_index()
        ga2 = ga2[ga2["spend"]>0].copy()
        ga2["cpl"] = (ga2["spend"]/ga2["leads"]).where(ga2["leads"]>0).round(2)
        ga2["cpm"] = (ga2["spend"]/ga2["impressions"]*1000).where(ga2["impressions"]>0).round(2)
        ga2 = ga2.sort_values("leads", ascending=False)
        pp = df_pt[(df_pt["date"]>=pd.Timestamp(start))&(df_pt["date"]<=pd.Timestamp(end))]
        pla = pp.groupby("platform").agg(spend=("spend","sum"),leads=("leads","sum"),impressions=("impressions","sum")).reset_index()
        pla = pla[pla["spend"]>0].sort_values("leads",ascending=False).head(15)
        pla["cpl"] = (pla["spend"]/pla["leads"]).where(pla["leads"]>0).round(2)
        pla["cpm"] = (pla["spend"]/pla["impressions"]*1000).where(pla["impressions"]>0).round(2)
        def tl(df2, dim):
            return [{"n":str(r[dim]),"spend":round(float(r["spend"]),2),"leads":int(r["leads"]),"cpl":safe(r["cpl"]),"cpm":safe(r["cpm"])} for _,r in df2.iterrows()]
        return {"age":tl(aa,"age"),"gender":tl(ga2,"gender"),"platform":tl(pla,"platform")}

    result = {}
    for n in [1,7,14,30]:
        start = last - pd.Timedelta(days=n-1)
        result[str(n)] = bd(start, last)
    for ym in all_months:
        ym_pd = pd.Period(str(ym),"M")
        result[str(ym)] = bd(ym_pd.start_time, min(ym_pd.end_time, last))
    return result


# ══════════════════════════════════════════════════════════════════════════════
# GOOGLE ADS
# ══════════════════════════════════════════════════════════════════════════════

def load_google():
    print("  Lendo google-ads...")
    df = pd.read_csv(URL_GOOGLE)
    df["date"] = pd.to_datetime(df["Date (Segment)"], errors="coerce")
    df["spend"]       = to_num(df["Cost (Spend, Amount Spent)"])
    df["conversions"] = to_num(df["All Conversions"])
    df["clicks"]      = to_num(df["Clicks"])
    df["impressions"] = to_num(df["Impressions"])
    df["campaign"]    = df["Campaign Name"]
    df["adgroup"]     = df["Ad Group Name"]
    df["keyword"]     = df["Keyword (Ad Group Criterion)"]
    df["match_type"]  = df["Match Type (Segment)"]
    df["ym"]          = df["date"].dt.to_period("M")
    df = df.dropna(subset=["date"])
    print(f"     {len(df)} linhas | {df['date'].min().date()} → {df['date'].max().date()}")
    return df


def google_daily(df):
    agg = df.groupby("date").agg(
        spend=("spend","sum"), conversions=("conversions","sum"),
        clicks=("clicks","sum"), impressions=("impressions","sum")
    ).reset_index().sort_values("date")
    out = {k: [] for k in ["days","spend","conversions","cpl","ctr","cpc"]}
    for _, r in agg.iterrows():
        ts   = round(float(r["spend"]), 2)
        conv = round(float(r["conversions"]), 2)
        cl   = int(r["clicks"])
        imp  = int(r["impressions"])
        out["days"].append(r["date"].strftime("%d/%m"))
        out["spend"].append(ts)
        out["conversions"].append(conv)
        out["cpl"].append(round(ts/conv, 2) if conv > 0 else None)
        out["ctr"].append(round(cl/imp*100, 2) if imp > 0 else None)
        out["cpc"].append(round(ts/cl, 2) if cl > 0 else None)
    return out, out["days"][-1] if out["days"] else "—", sorted(df["date"].unique())


def google_kpis(df, all_days):
    last = pd.Timestamp(all_days[-1])
    def kpi(p):
        ts=float(p["spend"].sum()); conv=float(p["conversions"].sum())
        cl=int(p["clicks"].sum()); imp=int(p["impressions"].sum())
        return {"spend":round(ts,2),"conversions":round(conv,2),
                "cpl":round(ts/conv,2) if conv>0 else None,
                "ctr":round(cl/imp*100,2) if imp>0 else None,
                "cpc":round(ts/cl,2) if cl>0 else None}
    result = {}
    for n in [1,7,14,30]:
        start = last - pd.Timedelta(days=n-1)
        result[str(n)] = kpi(df[(df["date"]>=start)&(df["date"]<=last)])
    for ym in sorted(df["ym"].unique()):
        result[str(ym)] = kpi(df[df["ym"]==ym])
    return result


def google_monthly(df):
    PT_MONTHS = {"Jan":"Jan","Feb":"Fev","Mar":"Mar","Apr":"Abr","May":"Mai",
                 "Jun":"Jun","Jul":"Jul","Aug":"Ago","Sep":"Set","Oct":"Out",
                 "Nov":"Nov","Dec":"Dez"}
    months = sorted(df["ym"].unique())
    data = {k: [] for k in ["meses","lbl","totalS","totalConv","totalClicks","totalImp","cplG","cpcG","ctrG"]}
    for m in months:
        p = df[df["ym"]==m]
        ts=round(float(p["spend"].sum()),2); conv=round(float(p["conversions"].sum()),2)
        cl=int(p["clicks"].sum()); imp=int(p["impressions"].sum())
        raw_lbl = pd.Period(m,"M").strftime("%b/%y")
        pt_lbl  = PT_MONTHS.get(raw_lbl[:3], raw_lbl[:3]) + raw_lbl[3:]
        data["meses"].append(str(m))
        data["lbl"].append(pt_lbl)
        data["totalS"].append(ts); data["totalConv"].append(conv)
        data["totalClicks"].append(cl); data["totalImp"].append(imp)
        data["cplG"].append(round(ts/conv,2) if conv>0 else None)
        data["cpcG"].append(round(ts/cl,2) if cl>0 else None)
        data["ctrG"].append(round(cl/imp*100,2) if imp>0 else None)
    return data


def google_mes_days(df):
    result = {}
    for ym in df["ym"].unique():
        days = sorted(df[df["ym"]==ym]["date"].unique())
        result[str(ym)] = [pd.Timestamp(d).strftime("%d/%m") for d in days]
    return result


def google_camps_period(df, p, all_months, cur_ym_str):
    if len(p) == 0:
        return []
    camps = p.groupby("campaign").agg(
        spend=("spend","sum"), conversions=("conversions","sum"),
        clicks=("clicks","sum"), impressions=("impressions","sum")
    ).reset_index()
    camps["cpl"] = (camps["spend"]/camps["conversions"]).where(camps["conversions"]>0).round(2)
    camps["cpc"] = (camps["spend"]/camps["clicks"]).where(camps["clicks"]>0).round(2)
    camps["ctr"] = (camps["clicks"]/camps["impressions"]*100).where(camps["impressions"]>0).round(2)
    camps = camps.sort_values("conversions", ascending=False)
    try:
        cur_ym  = pd.Period(cur_ym_str,"M")
        cur_idx = list(all_months).index(cur_ym)
    except Exception:
        cur_idx = len(all_months)-1
    spk_months = all_months[max(0,cur_idx-5):cur_idx+1]
    out = []
    for _, r in camps.iterrows():
        adgroups = p[p["campaign"]==r["campaign"]].groupby("adgroup").agg(
            spend=("spend","sum"), conversions=("conversions","sum"),
            clicks=("clicks","sum"), impressions=("impressions","sum")
        ).reset_index()
        adgroups["cpl"] = (adgroups["spend"]/adgroups["conversions"]).where(adgroups["conversions"]>0).round(2)
        adgroups["cpc"] = (adgroups["spend"]/adgroups["clicks"]).where(adgroups["clicks"]>0).round(2)
        adgroups["ctr"] = (adgroups["clicks"]/adgroups["impressions"]*100).where(adgroups["impressions"]>0).round(2)
        adgroups = adgroups.sort_values("conversions", ascending=False)
        spk = []
        for sm in spk_months:
            cm = df[(df["ym"]==sm)&(df["campaign"]==r["campaign"])]
            ts2=float(cm["spend"].sum()); cv2=float(cm["conversions"].sum())
            spk.append(round(ts2/cv2,2) if cv2>0 else None)
        conjs = []
        for _, ag in adgroups.iterrows():
            kws = p[(p["campaign"]==r["campaign"])&(p["adgroup"]==ag["adgroup"])].groupby("keyword").agg(
                spend=("spend","sum"), conversions=("conversions","sum"),
                clicks=("clicks","sum"), impressions=("impressions","sum")
            ).reset_index()
            kws["cpl"] = (kws["spend"]/kws["conversions"]).where(kws["conversions"]>0).round(2)
            kws["cpc"] = (kws["spend"]/kws["clicks"]).where(kws["clicks"]>0).round(2)
            kws = kws.sort_values("conversions", ascending=False)
            kw_list = []
            for _, k in kws.iterrows():
                mt_rows = p[(p["campaign"]==r["campaign"])&(p["adgroup"]==ag["adgroup"])&(p["keyword"]==k["keyword"])]["match_type"]
                match = str(mt_rows.mode()[0]) if len(mt_rows)>0 else ""
                kw_list.append({"n":str(k["keyword"]),"match":match,
                    "spend":round(float(k["spend"]),2),"conv":round(float(k["conversions"]),2),
                    "cpl":safe(k["cpl"]),"cpc":safe(k["cpc"]),"clicks":int(k["clicks"])})
            conjs.append({"n":str(ag["adgroup"]),"spend":round(float(ag["spend"]),2),
                "conv":round(float(ag["conversions"]),2),"cpl":safe(ag["cpl"]),"cpc":safe(ag["cpc"]),
                "ctr":safe(ag["ctr"]),"clicks":int(ag["clicks"]),"keywords":kw_list})
        out.append({"n":str(r["campaign"]),"spend":round(float(r["spend"]),2),
            "conv":round(float(r["conversions"]),2),"cpl":safe(r["cpl"]),"cpc":safe(r["cpc"]),
            "ctr":safe(r["ctr"]),"clicks":int(r["clicks"]),"spk":spk,"adgroups":conjs})
    return out


def google_camps(df, all_days):
    all_months = sorted(df["ym"].unique())
    last = pd.Timestamp(all_days[-1])
    result = {}
    for n in [1,7,14,30]:
        start = last - pd.Timedelta(days=n-1)
        p = df[(df["date"]>=start)&(df["date"]<=last)]
        result[str(n)] = google_camps_period(df, p, all_months, str(last.to_period("M")))
    for ym in all_months:
        result[str(ym)] = google_camps_period(df, df[df["ym"]==ym], all_months, str(ym))
    return result


def google_keywords_period(df, p):
    kws = p.groupby("keyword").agg(
        spend=("spend","sum"), conversions=("conversions","sum"),
        clicks=("clicks","sum"), impressions=("impressions","sum")
    ).reset_index()
    kws["cpl"] = (kws["spend"]/kws["conversions"]).where(kws["conversions"]>0).round(2)
    kws["cpc"] = (kws["spend"]/kws["clicks"]).where(kws["clicks"]>0).round(2)
    kws = kws[kws["spend"]>0].sort_values("conversions", ascending=False).head(20)
    result = []
    for _, k in kws.iterrows():
        mt_rows = p[p["keyword"]==k["keyword"]]["match_type"]
        match = str(mt_rows.mode()[0]) if len(mt_rows)>0 else ""
        result.append({"n":str(k["keyword"]),"match":match,
            "spend":round(float(k["spend"]),2),"conv":round(float(k["conversions"]),2),
            "cpl":safe(k["cpl"]),"cpc":safe(k["cpc"]),"clicks":int(k["clicks"])})
    return result


def google_keywords(df, all_days):
    last = pd.Timestamp(all_days[-1])
    all_months = sorted(df["ym"].unique())
    result = {}
    for n in [1,7,14,30]:
        start = last - pd.Timedelta(days=n-1)
        p = df[(df["date"]>=start)&(df["date"]<=last)]
        result[str(n)] = google_keywords_period(df, p)
    for ym in all_months:
        result[str(ym)] = google_keywords_period(df, df[df["ym"]==ym])
    return result


def load_google_ga():
    AGE_MAP = {"AGE_RANGE_18_24":"18-24","AGE_RANGE_25_34":"25-34","AGE_RANGE_35_44":"35-44",
               "AGE_RANGE_45_54":"45-54","AGE_RANGE_55_64":"55-64","AGE_RANGE_65_UP":"65+"}
    df_a = pd.read_csv(URL_GOOGLE_AG)
    df_a["date"] = pd.to_datetime(df_a["Date (Segment)"], errors="coerce")
    df_a["spend"] = to_num(df_a["Cost (Spend, Amount Spent)"])
    df_a["conv"]  = to_num(df_a["All Conversions"])
    df_a["clicks"]= to_num(df_a["Clicks"])
    df_a["imp"]   = to_num(df_a["Impressions"])
    df_a["age"]   = df_a["Age (Ad Group Criterion)"].map(AGE_MAP).fillna(df_a["Age (Ad Group Criterion)"].astype(str))
    df_a = df_a.dropna(subset=["date"])

    df_g = pd.read_csv(URL_GOOGLE_GE)
    df_g["date"] = pd.to_datetime(df_g["Date (Segment)"], errors="coerce")
    df_g["spend"] = to_num(df_g["Cost (Spend, Amount Spent)"])
    df_g["conv"]  = to_num(df_g["All Conversions"])
    df_g["clicks"]= to_num(df_g["Clicks"])
    df_g["imp"]   = to_num(df_g["Impressions"])
    df_g["gender"]= df_g["Gender (Ad Group Criterion)"].str.lower()
    df_g = df_g.dropna(subset=["date"])
    return df_a, df_g


def google_breakdowns(df_age, df_gen, all_days):
    last = pd.Timestamp(all_days[-1])
    AGE_ORDER = ["18-24","25-34","35-44","45-54","55-64","65+"]
    all_months = sorted(df_age["date"].dt.to_period("M").unique()) if len(df_age)>0 else []

    def bd(start, end):
        pa = df_age[(df_age["date"]>=pd.Timestamp(start))&(df_age["date"]<=pd.Timestamp(end))]
        aa = pa[pa["age"].isin(AGE_ORDER)].groupby("age").agg(spend=("spend","sum"),conv=("conv","sum"),clicks=("clicks","sum"),imp=("imp","sum")).reset_index()
        aa = aa[aa["spend"]>0].copy()
        aa["cpl"] = (aa["spend"]/aa["conv"]).where(aa["conv"]>0).round(2)
        aa["_o"]  = aa["age"].apply(lambda x: AGE_ORDER.index(x) if x in AGE_ORDER else 99)
        aa = aa.sort_values("_o")
        pg = df_gen[(df_gen["date"]>=pd.Timestamp(start))&(df_gen["date"]<=pd.Timestamp(end))]
        ga = pg[pg["gender"].isin(["female","male"])].groupby("gender").agg(spend=("spend","sum"),conv=("conv","sum"),clicks=("clicks","sum"),imp=("imp","sum")).reset_index()
        ga = ga[ga["spend"]>0].sort_values("conv", ascending=False)
        ga["cpl"] = (ga["spend"]/ga["conv"]).where(ga["conv"]>0).round(2)
        def tl(df2, dim):
            return [{"n":str(r[dim]),"spend":round(float(r["spend"]),2),"conv":round(float(r["conv"]),2),"cpl":safe(r["cpl"])} for _,r in df2.iterrows()]
        return {"age":tl(aa,"age"),"gender":tl(ga,"gender")}

    result = {}
    for n in [1,7,14,30]:
        start = last - pd.Timedelta(days=n-1)
        result[str(n)] = bd(start, last)
    for ym in sorted(df_age["date"].dt.to_period("M").unique()):
        ym_pd = pd.Period(str(ym),"M")
        result[str(ym)] = bd(ym_pd.start_time, min(ym_pd.end_time, last))
    return result


# ══════════════════════════════════════════════════════════════════════════════
# INJETAR NO HTML
# ══════════════════════════════════════════════════════════════════════════════

def replace_js_const(html, const_name, value):
    pattern = rf"const {const_name}\s*=\s*(?:\{{[\s\S]*?\}}|\[[\s\S]*?\]|\"[^\"]*\");"
    replacement = f"const {const_name} = {json.dumps(value, ensure_ascii=False)};"
    new_html, count = re.subn(pattern, replacement, html, count=1)
    if count == 0:
        print(f"  AVISO: não encontrou const {const_name}")
    return new_html


def inject_all(template_path,
               meta_daily_d, meta_last, meta_monthly_d, meta_camps_d,
               meta_mes_days_d, meta_kpis_d, meta_ads_d, meta_bd_d,
               g_daily_d, g_last, g_monthly_d, g_camps_d,
               g_mes_days_d, g_kpis_d, g_kw_d, g_bd_d):

    html = Path(template_path).read_text(encoding="utf-8")

    # Meta
    html = replace_js_const(html, "DAILY",          meta_daily_d)
    html = replace_js_const(html, "MONTHLY",        meta_monthly_d)
    html = replace_js_const(html, "CAMPS_MES",      meta_camps_d)
    html = replace_js_const(html, "MES_DAYS",       meta_mes_days_d)
    html = replace_js_const(html, "KPIS_PERIODO",   meta_kpis_d)
    html = replace_js_const(html, "ADS_DATA",       meta_ads_d)
    html = replace_js_const(html, "BREAKDOWN_DATA", meta_bd_d)

    # Google
    html = replace_js_const(html, "GDAILY",          g_daily_d)
    html = replace_js_const(html, "GMONTHLY",        g_monthly_d)
    html = replace_js_const(html, "GCAMPS_MES",      g_camps_d)
    html = replace_js_const(html, "GMES_DAYS",       g_mes_days_d)
    html = replace_js_const(html, "GKPIS_PERIODO",   g_kpis_d)
    html = replace_js_const(html, "GKEYWORDS_DATA",  g_kw_d)
    html = replace_js_const(html, "GBD",             g_bd_d)

    # Dates
    html = re.sub(r"Meta: \d{2}/\d{2}", f"Meta: {meta_last}", html)
    html = re.sub(r"Google: \d{2}/\d{2}", f"Google: {g_last}", html)
    # CPL thresholds → CONFIG block no JS
    html = re.sub(
        r'(const CONFIG = \{[\s\S]*?meta:\s*\{[\s\S]*?cplBom:\s*)\d+',
        rf'\g<1>{META_CPL_BOM}', html, count=1
    )
    html = re.sub(
        r'(const CONFIG = \{[\s\S]*?meta:\s*\{[\s\S]*?cplMedio:\s*)\d+',
        rf'\g<1>{META_CPL_MEDIO}', html, count=1
    )
    html = re.sub(
        r'(const CONFIG = \{[\s\S]*?google:\s*\{[\s\S]*?cplBom:\s*)\d+',
        rf'\g<1>{GOOGLE_CPL_BOM}', html, count=1
    )
    html = re.sub(
        r'(const CONFIG = \{[\s\S]*?google:\s*\{[\s\S]*?cplMedio:\s*)\d+',
        rf'\g<1>{GOOGLE_CPL_MEDIO}', html, count=1
    )

    html = re.sub(r"Dados até \d{2}/\d{2}", f"Dados até {meta_last}", html)
    today = date.today().strftime("%d/%m/%Y")
    html = re.sub(r"\d{2}/\d{2}/\d{4} · via planilha", f"{today} · via planilha", html)

    return html


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("Laboratório Bem Me Quer — Dashboard Meta + Google Ads")
    print("=" * 60)

    # ── META ──────────────────────────────────────────────────
    print("\n[META ADS]")
    df_meta = load_meta()
    all_months_meta = sorted(df_meta["ym"].unique())

    print("  Diário...")
    m_daily, m_last, m_all_days = meta_daily(df_meta)
    print(f"     {len(m_daily['days'])} dias | último: {m_last}")

    print("  KPIs...")
    m_kpis = meta_kpis(df_meta, m_all_days)

    print("  Mensal...")
    m_monthly = meta_monthly(df_meta)

    print("  Campanhas...")
    m_camps = meta_camps(df_meta, m_all_days)

    m_mes_days = meta_mes_days(df_meta)

    print("  Criativos...")
    img_dir = Path("imgs")
    img_dir.mkdir(exist_ok=True)
    m_ads = meta_ads(df_meta, img_dir, m_all_days)

    print("  Breakdowns...")
    df_meta_ga = load_meta_ga()
    df_meta_pt = load_meta_pt()
    m_bd = meta_breakdowns(df_meta_ga, df_meta_pt, m_all_days, all_months_meta)

    # ── GOOGLE ────────────────────────────────────────────────
    print("\n[GOOGLE ADS]")
    df_google = load_google()

    print("  Diário...")
    g_daily, g_last, g_all_days = google_daily(df_google)
    print(f"     {len(g_daily['days'])} dias | último: {g_last}")

    print("  KPIs...")
    g_kpis = google_kpis(df_google, g_all_days)

    print("  Mensal...")
    g_monthly = google_monthly(df_google)

    print("  Campanhas + palavras-chave...")
    g_camps = google_camps(df_google, g_all_days)
    g_kw    = google_keywords(df_google, g_all_days)

    g_mes_days = google_mes_days(df_google)

    print("  Breakdowns...")
    df_google_age, df_google_gen = load_google_ga()
    g_bd = google_breakdowns(df_google_age, df_google_gen, g_all_days)

    # ── GERAR HTML ────────────────────────────────────────────
    print("\n[HTML]")
    if not Path(TEMPLATE_FILE).exists():
        print(f"  ERRO: template não encontrado: {TEMPLATE_FILE}")
        return

    html = inject_all(
        TEMPLATE_FILE,
        m_daily, m_last, m_monthly, m_camps, m_mes_days, m_kpis, m_ads, m_bd,
        g_daily, g_last, g_monthly, g_camps, g_mes_days, g_kpis, g_kw, g_bd,
    )

    Path(OUTPUT_FILE).write_text(html, encoding="utf-8")
    print(f"  ✓ {OUTPUT_FILE} gerado ({len(html)//1024}KB)")
    print("=" * 60)


if __name__ == "__main__":
    main()
