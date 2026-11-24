#!/usr/bin/env python3
"""
Dashboard Standalone — Santo Criativo
Gera index.html completo (sem loader, sem data.json separado)
"""

import pandas as pd, json, re, hashlib, requests
from datetime import date
from pathlib import Path

# ══════════════════════════════════════════════════════
# CONFIG DO CLIENTE — edite apenas esta seção
# ══════════════════════════════════════════════════════
SHEET_ID         = "1rNDwriAHBml5Qv7tK99nRzZrKB3N1VF9r8zZGt7eRag"
TEMPLATE_FILE    = "dashboard.html"
OUTPUT_FILE      = "index.html"

NOME_CLIENTE     = "Laboratório Bem Me Quer"
LOGO_LETRA       = "BMQ"
COR_ACENTO       = "#00bfd9"

LANCAMENTO_COD   = ""        # filtra campanhas; "" = ver tudo
USAR_PESQUISA    = False            # False = oculta aba Pesquisa
USAR_GOOGLE      = True             # False = oculta toda a seção Google Ads

# Etapas do funil — coloque False para remover a etapa
# O cálculo das taxas se ajusta automaticamente às etapas ativas
FUNIL_IMPRESSOES  = True
FUNIL_LINK_CLICKS = True
FUNIL_PAGE_VIEW   = False
FUNIL_LEADS       = True

MOEDA            = "BRL"     # BRL | USD | EUR | ARS

_MOEDA_MAP = {
    "BRL": {"simbolo": "R$", "locale": "pt-BR"},
    "USD": {"simbolo": "$",  "locale": "en-US"},
    "EUR": {"simbolo": "€",  "locale": "de-DE"},
    "ARS": {"simbolo": "$",  "locale": "es-AR"},
}
_moeda_cfg    = _MOEDA_MAP.get(MOEDA, _MOEDA_MAP["BRL"])
MOEDA_SIMBOLO = _moeda_cfg["simbolo"]
MOEDA_LOCALE  = _moeda_cfg["locale"]

CPL_BOM          = 11.0
CPL_MEDIO        = 16.0
CTR_BOM          = 0.6
CTR_MEDIO        = 0.4
CR_BOM           = 68.0
CR_MEDIO         = 60.0
TX_CONV_BOM      = 3.0
TX_CONV_MEDIO    = 2.0
CPM_BOM          = 5.0
CPM_MEDIO        = 12.0

# ══════════════════════════════════════════════════════
def sheet_url(t): return f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={t}"
URL_META = sheet_url("meta-ads")
URL_GA   = sheet_url("breakdown-gender-age")
URL_PT   = sheet_url("breakdown-platform")

def to_num(s):
    if pd.api.types.is_numeric_dtype(s): return s.fillna(0)
    clean = s.astype(str).str.strip().str.replace("R$","",regex=False).str.strip()
    if clean.str.contains(r"\d,\d", regex=True).any():
        clean = clean.str.replace(".","",regex=False).str.replace(",",".",regex=False)
    return pd.to_numeric(clean, errors="coerce").fillna(0)

def safe(v):
    if v is None or (isinstance(v,float) and pd.isna(v)): return None
    return round(float(v),2) if float(v)!=0 else None

import re, pathlib
from urllib.parse import urlparse as _urlparse
_THUMBS_USADAS=set(); _THUMB_DIRS=set()
def _thumb_key(url):
    """Nome estável da miniatura: host + caminho do arquivo, sem a query assinada da Meta
    (ela muda todo dia e, com hash da URL inteira, a pasta imgs/ crescia sem parar)."""
    try:
        p=_urlparse(url)
        if re.search(r"\.(jpe?g|png|webp|gif)$", p.path, re.I): return p.netloc+p.path
    except Exception: pass
    return url
def _limpar_thumbs():
    """Apaga miniaturas (<hash16>.jpg/png) que nenhum anúncio usa mais."""
    if not _THUMBS_USADAS: return
    n=0
    for dd in _THUMB_DIRS:
        for f in pathlib.Path(dd).glob("*"):
            if f.is_file() and re.fullmatch(r"[0-9a-f]{16}\.(jpg|png)", f.name) and f.name not in _THUMBS_USADAS:
                f.unlink(); n+=1
    print(f"  imgs/: {len(_THUMBS_USADAS)} em uso | {n} antigas apagadas")

def download_thumb(url, d):
    if not url or str(url)=="nan": return ""
    try:
        ext=".png" if ".png" in url.lower() else ".jpg"
        fname = hashlib.md5(_thumb_key(url).encode()).hexdigest()[:16] + ext
        _THUMBS_USADAS.add(fname); _THUMB_DIRS.add(str(d))
        fp=d/fname
        if not fp.exists():
            r=requests.get(url,timeout=10,headers={"User-Agent":"Mozilla/5.0"})
            if r.status_code==200: fp.write_bytes(r.content)
            else: return ""
        return "imgs/"+fname
    except: return ""

CONV_COLS = ["Conversões"]

def load_meta():
    print("  Lendo meta-ads...")
    df=pd.read_csv(URL_META)
    df=df.rename(columns={
        "Date":"date","Campaign Name":"campaign","Adset Name":"adset",
        "Ad Name":"ad","Thumbnail URL":"thumb","Status":"status",
        "Spend (Cost, Amount Spent)":"spend","Impressions":"impressions",
        "Action Link Clicks":"link_clicks","Action Landing Page View":"page_view",
        "Clicks":"clicks","Reach (Estimated)":"reach",
        "Action Post Engagement":"engagement","Action Post Shares":"shares",
        "Action Post Comments":"comments",
        "Action Post Save (Onsite Conversion)":"saves",
        "Video Thruplay Watched Actions":"thruplay",
    })
    df["date"]=pd.to_datetime(df["date"],errors="coerce")
    if "status" not in df.columns: df["status"]=""
    df["status"]=df["status"].astype(str).str.strip().str.upper()
    for c in ["spend","impressions","link_clicks","page_view","clicks"]:
        if c in df.columns: df[c]=to_num(df[c])
    if "clicks" not in df.columns: df["clicks"]=df["link_clicks"]
    for _col in ["page_view","reach","engagement","shares","comments","saves","thruplay"]:
        if _col not in df.columns: df[_col]=0
        else: df[_col]=to_num(df[_col])
    df["leads"]=sum(to_num(df[c]) for c in CONV_COLS if c in df.columns)
    print(f"     Coluna: {', '.join(c for c in CONV_COLS if c in df.columns)}")
    df["is_lct"]=df["campaign"].str.contains(LANCAMENTO_COD,na=False,case=False) if LANCAMENTO_COD else True
    df=df.dropna(subset=["date"])
    print(f"     {len(df)} linhas | {df['date'].min().date()} → {df['date'].max().date()}")
    print(f"     Total conversões: {df['leads'].sum():.0f}")
    return df

def calc_kpis(p):
    sp=float(p["spend"].sum()); imp=float(p["impressions"].sum())
    lc=float(p["link_clicks"].sum()); pv=float(p["page_view"].sum())
    ld=float(p["leads"].sum()); cl=float(p["clicks"].sum()) if "clicks" in p.columns else lc
    return {"spend":round(sp,2),"impressions":int(imp),"link_clicks":int(lc),
        "clicks":int(cl),"page_view":int(pv),"leads":int(ld),
        "ctr":round(lc/imp*100,2) if imp>0 else None,
        "ctr_all":round(cl/imp*100,2) if imp>0 else None,
        "connect_rate":round(pv/lc*100,2) if lc>0 else None,
        "tx_conv":round(ld/pv*100,2) if pv>0 else None,
        "cpl":round(sp/ld,2) if ld>0 else None,
        "cpm":round(sp/imp*1000,2) if imp>0 else None}

def meta_kpis(df):
    return {"lct":calc_kpis(df[df["is_lct"]]),"all":calc_kpis(df)}

def build_daily(p):
    has_clicks="clicks" in p.columns
    agg_cols=dict(spend=("spend","sum"),impressions=("impressions","sum"),
        link_clicks=("link_clicks","sum"),page_view=("page_view","sum"),leads=("leads","sum"),
        engagement=("engagement","sum"),reach=("reach","sum"),
        shares=("shares","sum"),comments=("comments","sum"),
        saves=("saves","sum"),thruplay=("thruplay","sum"))
    if has_clicks: agg_cols["clicks"]=("clicks","sum")
    agg=p.groupby("date").agg(**agg_cols).reset_index().sort_values("date")
    out={k:[] for k in ["days","spend","impressions","link_clicks","clicks","page_view","leads",
                         "ctr","ctr_all","connect_rate","tx_conv","cpl","cpm",
                         "reach","engagement","cpe","shares","comments","saves","thruplay"]}
    for _,r in agg.iterrows():
        sp=float(r["spend"]); imp=float(r["impressions"]); lc=float(r["link_clicks"])
        pv=float(r["page_view"]); ld=float(r["leads"])
        cl=float(r["clicks"]) if has_clicks else lc
        eng=float(r["engagement"]) if "engagement" in r.index else 0
        out["days"].append(r["date"].strftime("%d/%m/%Y"))
        out["spend"].append(round(sp,2)); out["impressions"].append(int(imp))
        out["link_clicks"].append(int(lc)); out["clicks"].append(int(cl))
        out["page_view"].append(int(pv)); out["leads"].append(int(ld))
        out["reach"].append(int(r["reach"]) if "reach" in r.index else 0)
        out["engagement"].append(int(eng))
        out["cpe"].append(round(sp/eng,2) if eng>0 else None)
        out["shares"].append(int(r["shares"]) if "shares" in r.index else 0)
        out["comments"].append(int(r["comments"]) if "comments" in r.index else 0)
        out["saves"].append(int(r["saves"]) if "saves" in r.index else 0)
        out["thruplay"].append(int(r["thruplay"]) if "thruplay" in r.index else 0)
        out["ctr"].append(round(lc/imp*100,2) if imp>0 else None)
        out["ctr_all"].append(round(cl/imp*100,2) if imp>0 else None)
        out["connect_rate"].append(round(pv/lc*100,2) if lc>0 else None)
        out["tx_conv"].append(round(ld/pv*100,2) if pv>0 else None)
        out["cpl"].append(round(sp/ld,2) if ld>0 else None)
        out["cpm"].append(round(sp/imp*1000,2) if imp>0 else None)
    return out

def meta_daily(df):
    return {"lct":build_daily(df[df["is_lct"]]),"all":build_daily(df)}

def meta_daily_camps(df):
    result={"lct":{},"all":{}}
    for key,subset in [("lct",df[df["is_lct"]]),("all",df)]:
        for camp in subset["campaign"].unique():
            result[key][camp]=build_daily(subset[subset["campaign"]==camp])
    return result

_STATUS_PRIORITY={"ACTIVE":0,"WITH_ISSUES":1,"PAUSED":2,"ADSET_PAUSED":3,"CAMPAIGN_PAUSED":4,"ARCHIVED":5}

def _pick_status(group):
    if "status" not in group.columns: return ""
    g=group[group["status"].notna()&(group["status"]!="")&(group["status"]!="NAN")]
    if len(g)==0: return ""
    last_date=g["date"].max(); last=g[g["date"]==last_date]
    if (last["status"]=="ACTIVE").any(): return "ACTIVE"
    statuses=last["status"].unique().tolist()
    statuses.sort(key=lambda s:_STATUS_PRIORITY.get(s,99))
    return statuses[0]

def meta_raw(df):
    rows=[]; has_status="status" in df.columns
    camp_st={k:_pick_status(g) for k,g in df.groupby("campaign")} if has_status else {}
    adset_st={(c,a):_pick_status(g) for (c,a),g in df.groupby(["campaign","adset"])} if has_status else {}
    agg=df.groupby(["date","campaign","adset","is_lct"]).agg(
        spend=("spend","sum"),leads=("leads","sum"),impressions=("impressions","sum"),
        link_clicks=("link_clicks","sum"),clicks=("clicks","sum"),page_view=("page_view","sum")
    ).reset_index()
    for _,r in agg.iterrows():
        rows.append({"d":r["date"].strftime("%d/%m/%Y"),"c":str(r["campaign"]),"a":str(r["adset"]),
            "lct":bool(r["is_lct"]),"sp":round(float(r["spend"]),2),
            "ld":int(r["leads"]),"imp":int(r["impressions"]),
            "lc":int(r["link_clicks"]),"cl":int(r["clicks"]),"pv":int(r["page_view"]),
            "sc":camp_st.get(str(r["campaign"]),""),
            "sa":adset_st.get((str(r["campaign"]),str(r["adset"])),"")})
    return rows

def meta_tables_period(df, p, img_dir):
    def ag(sub,cols):
        agg_d=dict(spend=("spend","sum"),impressions=("impressions","sum"),
            link_clicks=("link_clicks","sum"),clicks=("clicks","sum"),
            page_view=("page_view","sum"),leads=("leads","sum"))
        if "reach" in sub.columns: agg_d["reach"]=("reach","sum")
        if "engagement" in sub.columns: agg_d["engagement"]=("engagement","sum")
        return sub.groupby(cols).agg(**agg_d).reset_index()
    def calc_row(r):
        sp=round(float(r["spend"]),2); imp=int(r["impressions"]); lc=int(r["link_clicks"])
        cl=int(r["clicks"]) if "clicks" in r.index else lc
        pv=int(r["page_view"]); ld=int(r["leads"])
        eng=int(r["engagement"]) if "engagement" in r.index else 0
        rch=int(r["reach"]) if "reach" in r.index else 0
        return {"spend":sp,"imp":imp,"lc":lc,"cl":cl,"pv":pv,"ld":ld,"reach":rch,"engagement":eng,
            "ctr":round(lc/imp*100,2) if imp>0 else None,
            "ctr_all":round(cl/imp*100,2) if imp>0 else None,
            "cr":round(pv/lc*100,2) if lc>0 else None,
            "tx_cv":round(ld/pv*100,2) if pv>0 else None,
            "cpl":round(sp/ld,2) if ld>0 else None,
            "cpm":round(sp/imp*1000,2) if imp>0 else None,
            "cpe":round(sp/eng,2) if eng>0 else None}
    camp_st={k:_pick_status(g) for k,g in df.groupby("campaign")}
    adset_st={(c,a):_pick_status(g) for (c,a),g in df.groupby(["campaign","adset"])}
    ad_st={(c,a,n):_pick_status(g) for (c,a,n),g in df.groupby(["campaign","adset","ad"])}
    camps=[{"n":str(r["campaign"]),"status":camp_st.get(str(r["campaign"]),""),**calc_row(r)}
           for _,r in ag(p,"campaign").sort_values("leads",ascending=False).iterrows()]
    adsets=[{"n":str(r["adset"]),"camp":str(r["campaign"]),"status":adset_st.get((str(r["campaign"]),str(r["adset"])),""),**calc_row(r)}
            for _,r in ag(p,["campaign","adset"]).sort_values("leads",ascending=False).iterrows()]
    df_full_thumb=df[df["thumb"].notna()&(df["thumb"].astype(str)!="nan")] if "thumb" in df.columns else pd.DataFrame()
    thumb_map={}
    for _,r in df_full_thumb.iterrows():
        k=(str(r["ad"]),str(r["adset"]),str(r["campaign"]))
        if k not in thumb_map: thumb_map[k]=download_thumb(str(r["thumb"]),img_dir)
    ads_extra={_c:(_c,"sum") for _c in ["reach","engagement","shares","comments","saves","thruplay"] if _c in p.columns}
    ads_agg=p.groupby(["ad","adset","campaign"]).agg(spend=("spend","sum"),impressions=("impressions","sum"),
        link_clicks=("link_clicks","sum"),clicks=("clicks","sum"),leads=("leads","sum"),**ads_extra
    ).reset_index().sort_values("leads",ascending=False)
    ads=[]
    for _,r in ads_agg.iterrows():
        sp=round(float(r["spend"]),2); imp=int(r["impressions"])
        lc=int(r["link_clicks"]); cl=int(r["clicks"]) if "clicks" in r.index else lc; ld=int(r["leads"])
        _eng=int(r["engagement"]) if "engagement" in r.index else 0
        k=(str(r["ad"]),str(r["adset"]),str(r["campaign"]))
        ads.append({"n":str(r["ad"]),"adset":str(r["adset"]),"camp":str(r["campaign"]),
            "status":ad_st.get((str(r["campaign"]),str(r["adset"]),str(r["ad"])),""),
            "thumb":thumb_map.get(k,""),"spend":sp,"imp":imp,"lc":lc,"cl":cl,"ld":ld,
            "reach":int(r["reach"]) if "reach" in r.index else 0,"engagement":_eng,
            "shares":int(r["shares"]) if "shares" in r.index else 0,
            "comments":int(r["comments"]) if "comments" in r.index else 0,
            "saves":int(r["saves"]) if "saves" in r.index else 0,
            "thruplay":int(r["thruplay"]) if "thruplay" in r.index else 0,
            "ctr":round(lc/imp*100,2) if imp>0 else None,
            "ctr_all":round(cl/imp*100,2) if imp>0 else None,
            "cpl":round(sp/ld,2) if ld>0 else None,
            "cpm":round(sp/imp*1000,2) if imp>0 else None,
            "cpe":round(sp/_eng,2) if _eng>0 else None})
    return {"camps":camps,"adsets":adsets,"ads":ads}

def meta_tables(df, img_dir):
    hoje=pd.Timestamp(date.today()); ontem=hoje-pd.Timedelta(days=1)
    result={"lct":{},"all":{}}
    period_ranges={"1":(ontem,ontem),"7":(hoje-pd.Timedelta(days=6),hoje),
        "14":(hoje-pd.Timedelta(days=13),hoje),"30":(hoje-pd.Timedelta(days=29),hoje),"all":(None,None)}
    for key,subset in [("lct",df[df["is_lct"]]),("all",df)]:
        for pname,(start,end) in period_ranges.items():
            p=subset if start is None else subset[(subset["date"]>=start)&(subset["date"]<=end)]
            result[key][pname]=meta_tables_period(df,p,img_dir)
            print(f"     [{key}][{pname}]: {len(result[key][pname]['camps'])} camps | {len(result[key][pname]['ads'])} ads")
    return result

def meta_breakdowns(df):
    print("  Lendo breakdowns...")
    hoje_bd=pd.Timestamp(date.today()); AGE_ORDER=["18-24","25-34","35-44","45-54","55-64","65+"]
    CONV_COLS_BD=["Action Messaging Conversations Started (Onsite Conversion)","Action Leads","Action FB Pixel Custom (Offsite Conversion)"]
    def seg(agg,dim):
        agg=agg[agg["spend"]>0].copy()
        agg["cpl"]=(agg["spend"]/agg["leads"]).where(agg["leads"]>0).round(2)
        return [{"n":str(r[dim]),"spend":round(float(r["spend"]),2),"ld":int(r["leads"]),"cpl":safe(r["cpl"])} for _,r in agg.iterrows()]
    try:
        df_ga=pd.read_csv(URL_GA); df_ga["date"]=pd.to_datetime(df_ga["Date"],errors="coerce")
        df_ga["spend"]=to_num(df_ga["Spend (Cost, Amount Spent)"])
        available=[c for c in CONV_COLS_BD if c in df_ga.columns]
        df_ga["leads"]=sum(to_num(df_ga[c]) for c in available) if available else pd.Series(0,index=df_ga.index)
        df_ga["age"]=df_ga["Age (Breakdown)"].astype(str)
        df_ga["gender"]=df_ga["Gender (Breakdown)"].astype(str)
        df_ga["is_lct"]=df_ga["Campaign Name"].str.contains(LANCAMENTO_COD,na=False,case=False) if "Campaign Name" in df_ga.columns and LANCAMENTO_COD else True
        df_ga=df_ga.dropna(subset=["date"])
    except Exception as e: print(f"  Aviso GA: {e}"); df_ga=pd.DataFrame()
    try:
        df_pt=pd.read_csv(URL_PT); df_pt["date"]=pd.to_datetime(df_pt["Date"],errors="coerce")
        df_pt["spend"]=to_num(df_pt["Spend (Cost, Amount Spent)"])
        available_pt=[c for c in CONV_COLS_BD if c in df_pt.columns]
        df_pt["leads"]=sum(to_num(df_pt[c]) for c in available_pt) if available_pt else pd.Series(0,index=df_pt.index)
        df_pt["platform"]=df_pt["Platform Position (Breakdown)"].astype(str)
        df_pt["is_lct"]=df_pt["Campaign Name"].str.contains(LANCAMENTO_COD,na=False,case=False) if "Campaign Name" in df_pt.columns and LANCAMENTO_COD else True
        df_pt=df_pt.dropna(subset=["date"])
    except Exception as e: print(f"  Aviso PT: {e}"); df_pt=pd.DataFrame()
    result={}
    for pname,n in [("all",0)]:
        start=hoje_bd-pd.Timedelta(days=n-1) if n>0 else None
        for lname,lct_filter in [("lct",True),("all",None)]:
            pga=df_ga if lct_filter is None else (df_ga[df_ga["is_lct"]] if len(df_ga)>0 else df_ga)
            ppt=df_pt if lct_filter is None else (df_pt[df_pt["is_lct"]] if len(df_pt)>0 else df_pt)
            if n>0:
                pga=pga[(pga["date"]>=start)&(pga["date"]<=hoje_bd)] if len(pga)>0 else pga
                ppt=ppt[(ppt["date"]>=start)&(ppt["date"]<=hoje_bd)] if len(ppt)>0 else ppt
            age_d=[]; gen_d=[]; plat_d=[]
            if len(pga)>0:
                ag_age=pga[pga["age"].isin(AGE_ORDER)].groupby("age").agg(spend=("spend","sum"),leads=("leads","sum")).reset_index()
                ag_age["_o"]=ag_age["age"].apply(lambda x:AGE_ORDER.index(x) if x in AGE_ORDER else 99)
                age_d=seg(ag_age.sort_values("_o"),"age")
                ag_gen=pga[pga["gender"].isin(["female","male"])].groupby("gender").agg(spend=("spend","sum"),leads=("leads","sum")).reset_index().sort_values("leads",ascending=False)
                gen_d=seg(ag_gen,"gender")
            if len(ppt)>0:
                ag_pt=ppt.groupby("platform").agg(spend=("spend","sum"),leads=("leads","sum")).reset_index().sort_values("leads",ascending=False).head(8)
                plat_d=seg(ag_pt,"platform")
            if lname not in result: result[lname]={}
            result[lname][pname]={"age":age_d,"gender":gen_d,"platform":plat_d}
    # Dados raw para filtros dinâmicos no JS (período, campanha)
    raw_ga=[]
    if len(df_ga)>0:
        for _,r in df_ga.iterrows():
            if pd.isna(r['date']): continue
            raw_ga.append({'d':r['date'].strftime('%d/%m/%Y'),'age':str(r['age']),'gen':str(r['gender']),
                           'sp':round(float(r['spend']),2),'ld':int(r['leads']),
                           'eng':int(r.get('Action Post Engagement',0)) if 'Action Post Engagement' in r.index else 0,
                           'rch':int(r.get('Reach (Estimated)',0)) if 'Reach (Estimated)' in r.index else 0,
                           'lct':bool(r['is_lct']),
                           'camp':str(r['Campaign Name']) if 'Campaign Name' in r.index else ''})
    raw_pt=[]
    if len(df_pt)>0:
        for _,r in df_pt.iterrows():
            if pd.isna(r['date']): continue
            raw_pt.append({'d':r['date'].strftime('%d/%m/%Y'),'plat':str(r['platform']),
                           'sp':round(float(r['spend']),2),'ld':int(r['leads']),
                           'eng':int(r.get('Action Post Engagement',0)) if 'Action Post Engagement' in r.index else 0,
                           'rch':int(r.get('Reach (Estimated)',0)) if 'Reach (Estimated)' in r.index else 0,
                           'lct':bool(r['is_lct']),
                           'camp':str(r['Campaign Name']) if 'Campaign Name' in r.index else ''})
    result['_raw_ga']=raw_ga
    result['_raw_pt']=raw_pt
    return result

def meta_monthly(df):
    PT_MONTHS={"Jan":"Jan","Feb":"Fev","Mar":"Mar","Apr":"Abr","May":"Mai","Jun":"Jun",
               "Jul":"Jul","Aug":"Ago","Sep":"Set","Oct":"Out","Nov":"Nov","Dec":"Dez"}
    df=df.copy(); df["ym"]=df["date"].dt.to_period("M"); months=sorted(df["ym"].unique())
    out={"lbl":[],"totalS":[],"totalL":[],"cplG":[],"cpmG":[],"ctrG":[],"camps":[]}
    for m in months:
        p=df[df["ym"]==m]; sp=round(float(p["spend"].sum()),2); ld=int(p["leads"].sum())
        imp=float(p["impressions"].sum()); lc=float(p["link_clicks"].sum())
        raw_lbl=pd.Period(m,"M").strftime("%b/%y"); pt_lbl=PT_MONTHS.get(raw_lbl[:3],raw_lbl[:3])+raw_lbl[3:]
        out["lbl"].append(pt_lbl); out["totalS"].append(sp); out["totalL"].append(ld)
        out["cplG"].append(round(sp/ld,2) if ld>0 else None)
        out["cpmG"].append(round(sp/imp*1000,2) if imp>0 else None)
        out["ctrG"].append(round(lc/imp*100,2) if imp>0 else None)
        ag=p.groupby("campaign").agg(spend=("spend","sum"),leads=("leads","sum"),
            impressions=("impressions","sum"),link_clicks=("link_clicks","sum")).reset_index()
        for _,r in ag.iterrows():
            out["camps"].append({"n":str(r["campaign"]),"spend":round(float(r["spend"]),2),
                "leads":int(r["leads"]),"imp":int(r["impressions"]),"lc":int(r["link_clicks"])})
    print(f"     Meta Mensal: {len(months)} meses"); return out

# ══ GOOGLE ADS ════════════════════════════════════════
URL_GOOGLE        = sheet_url("google-ads")
URL_GOOGLE_PESQ   = sheet_url("google-ads-pesquisa")
URL_GOOGLE_OUTROS = sheet_url("google-ads-outros")
URL_GOOGLE_GE     = sheet_url("google-breakdown-gender")
URL_GOOGLE_AG     = sheet_url("google-breakdown-age")
AGE_MAP={"AGE_RANGE_18_24":"18-24","AGE_RANGE_25_34":"25-34","AGE_RANGE_35_44":"35-44",
         "AGE_RANGE_45_54":"45-54","AGE_RANGE_55_64":"55-64","AGE_RANGE_65_UP":"65+"}
_dfp_pesquisa=pd.DataFrame()

def load_google():
    global _dfp_pesquisa
    print("  Lendo google-ads...")
    COLS=["date","campaign","adgroup","keyword","match_type","spend","conversions","clicks","impressions","is_search"]
    try:
        df=pd.read_csv(URL_GOOGLE)
        df["date"]=pd.to_datetime(df["Date (Segment)"],errors="coerce")
        df["spend"]=to_num(df["Cost (Spend, Amount Spent)"])
        df["conversions"]=to_num(df["All Conversions"])
        df["clicks"]=to_num(df["Clicks"]); df["impressions"]=to_num(df["Impressions"])
        df["campaign"]=df["Campaign Name"]; df["adgroup"]=df["Ad Group Name"]
        df["keyword"]=df["Keyword (Ad Group Criterion)"]; df["match_type"]=df["Match Type (Segment)"]
        df["is_search"]=True; df=df.dropna(subset=["date"]); df=df[df["spend"]>0]
        if len(df)>0: print(f"     Search: {len(df)} linhas")
        else: print("     Search: vazio"); df=pd.DataFrame(columns=COLS)
    except Exception as e: print(f"     Aviso google-ads: {e}"); df=pd.DataFrame(columns=COLS)
    try:
        dfp=pd.read_csv(URL_GOOGLE_PESQ)
        dfp["date"]=pd.to_datetime(dfp["Date (Segment)"],errors="coerce")
        dfp["spend"]=to_num(dfp["Cost (Spend, Amount Spent)"])
        dfp["conversions"]=to_num(dfp["All Conversions"])
        dfp["clicks"]=to_num(dfp["Clicks"]); dfp["impressions"]=to_num(dfp["Impressions"])
        dfp["campaign"]=dfp["Campaign Name"]; dfp["adgroup"]=dfp["Ad Group Name"]
        dfp=dfp.dropna(subset=["date"]); _dfp_pesquisa=dfp
        print(f"     Pesquisa: {len(dfp)} linhas")
    except Exception as e: print(f"     Aviso pesquisa: {e}"); _dfp_pesquisa=pd.DataFrame()
    try:
        df2=pd.read_csv(URL_GOOGLE_OUTROS)
        df2["date"]=pd.to_datetime(df2["Date (Segment)"],errors="coerce")
        df2["spend"]=to_num(df2["Cost (Spend, Amount Spent)"])
        df2["conversions"]=to_num(df2["All Conversions"])
        df2["clicks"]=to_num(df2["Clicks"]); df2["impressions"]=to_num(df2["Impressions"])
        df2["campaign"]=df2["Campaign Name"]
        df2["adgroup"]=df2["Ad Group Name"] if "Ad Group Name" in df2.columns else df2["Campaign Name"]
        df2["keyword"]=""; df2["match_type"]=""; df2["is_search"]=False; df2=df2.dropna(subset=["date"])
        print(f"     Outros: {len(df2)} linhas")
        df=pd.concat([df[COLS],df2[COLS]],ignore_index=True)
    except Exception as e: print(f"     Aviso outros: {e}")
    if len(df)>0: print(f"     Total: {len(df)} linhas | {df['date'].min().date()} → {df['date'].max().date()}")
    return df

def apply_pesq_spend(df):
    global _dfp_pesquisa
    if _dfp_pesquisa.empty: return df
    df=df.copy(); df["date"]=pd.to_datetime(df["date"],errors="coerce"); extras=[]
    for camp in _dfp_pesquisa["campaign"].unique():
        for dt in _dfp_pesquisa[_dfp_pesquisa["campaign"]==camp]["date"].unique():
            sp_pesq=float(_dfp_pesquisa[(_dfp_pesquisa["campaign"]==camp)&(_dfp_pesquisa["date"]==dt)]["spend"].sum())
            mask=(df["campaign"]==camp)&(df["date"]==dt); sp_kw=float(df[mask]["spend"].sum())
            diff=round(sp_pesq-sp_kw,4)
            if diff>0.01: extras.append({"date":dt,"campaign":camp,"adgroup":"","keyword":"__pesq_diff__",
                "match_type":"","spend":diff,"conversions":0,"clicks":0,"impressions":0,"is_search":True})
    if not extras: return df
    return pd.concat([df,pd.DataFrame(extras)],ignore_index=True)

def google_daily(df):
    agg=df.groupby("date").agg(spend=("spend","sum"),conversions=("conversions","sum"),
        clicks=("clicks","sum"),impressions=("impressions","sum")).reset_index().sort_values("date")
    out={k:[] for k in ["days","spend","conversions","cpa","ctr","cpc"]}
    for _,r in agg.iterrows():
        sp=round(float(r["spend"]),2); cv=round(float(r["conversions"]),2)
        cl=int(r["clicks"]); imp=int(r["impressions"])
        out["days"].append(r["date"].strftime("%d/%m/%Y")); out["spend"].append(sp); out["conversions"].append(cv)
        out["cpa"].append(round(sp/cv,2) if cv>0 else None)
        out["ctr"].append(round(cl/imp*100,2) if imp>0 else None)
        out["cpc"].append(round(sp/cl,2) if cl>0 else None)
    return out

def google_kpis(df):
    hoje=pd.Timestamp(date.today()); ontem=hoje-pd.Timedelta(days=1)
    def kpi(p):
        if not len(p): return {"spend":0,"conversions":0,"clicks":0,"impressions":0,"cpa":None,"ctr":None,"cpc":None}
        sp=float(p["spend"].sum()); cv=float(p["conversions"].sum()); cl=int(p["clicks"].sum()); imp=int(p["impressions"].sum())
        return {"spend":round(sp,2),"conversions":round(cv,2),"clicks":cl,"impressions":imp,
            "cpa":round(sp/cv,2) if cv>0 else None,"ctr":round(cl/imp*100,2) if imp>0 else None,"cpc":round(sp/cl,2) if cl>0 else None}
    result={"1":kpi(df[(df["date"]>=ontem)&(df["date"]<=ontem)])}
    for n in [7,14,30]: result[str(n)]=kpi(df[df["date"]>=hoje-pd.Timedelta(days=n-1)])
    result["all"]=kpi(df); return result

def google_camps(df):
    hoje=pd.Timestamp(date.today()); ontem=hoje-pd.Timedelta(days=1)
    global _dfp_pesquisa; dfp_full=_dfp_pesquisa if not _dfp_pesquisa.empty else pd.DataFrame()
    def get_pesq_spend(dfp_p,camp,adgroup=None):
        if dfp_p.empty: return None
        mask=dfp_p["campaign"]==camp
        if adgroup: mask=mask&(dfp_p["adgroup"]==adgroup)
        return round(float(dfp_p[mask]["spend"].sum()),2) if mask.any() else None
    def camps_period(p,dfp_p):
        if not len(p): return []
        ag=p.groupby("campaign").agg(spend=("spend","sum"),conversions=("conversions","sum"),
            clicks=("clicks","sum"),impressions=("impressions","sum")).reset_index()
        rows=[]
        for _,r in ag.sort_values("conversions",ascending=False).iterrows():
            camp=str(r["campaign"]); is_sc=p[p["campaign"]==camp]["is_search"].any()
            sp_real=get_pesq_spend(dfp_p,camp) if is_sc else None
            sp=sp_real if sp_real is not None else round(float(r["spend"]),2)
            cv=round(float(r["conversions"]),2); cl=int(r["clicks"]); imp=int(r["impressions"])
            adg=p[p["campaign"]==camp].groupby("adgroup").agg(spend=("spend","sum"),conversions=("conversions","sum"),
                clicks=("clicks","sum"),impressions=("impressions","sum")).reset_index()
            adgroups=[]
            for _,ag2 in adg.sort_values("conversions",ascending=False).iterrows():
                adn=str(ag2["adgroup"]); sp2r=get_pesq_spend(dfp_p,camp,adn) if is_sc else None
                sp2=sp2r if sp2r is not None else round(float(ag2["spend"]),2)
                cv2=round(float(ag2["conversions"]),2); cl2=int(ag2["clicks"]); imp2=int(ag2["impressions"])
                kws=p[(p["campaign"]==camp)&(p["adgroup"]==adn)].groupby("keyword").agg(
                    spend=("spend","sum"),conversions=("conversions","sum"),
                    clicks=("clicks","sum"),impressions=("impressions","sum")).reset_index()
                kw_list=[]; sp_kw_total=0.0
                for _,k in kws.sort_values("conversions",ascending=False).iterrows():
                    if not str(k["keyword"]).strip(): continue
                    sp_k=round(float(k["spend"]),2); cv_k=round(float(k["conversions"]),2)
                    cl_k=int(k["clicks"]); imp_k=int(k["impressions"]); sp_kw_total+=sp_k
                    mt=p[(p["campaign"]==camp)&(p["adgroup"]==adn)&(p["keyword"]==k["keyword"])]["match_type"]
                    kw_list.append({"n":str(k["keyword"]),"match":str(mt.mode()[0]) if len(mt)>0 else "",
                        "spend":sp_k,"conv":cv_k,"cpa":round(sp_k/cv_k,2) if cv_k>0 else None,
                        "cpc":round(sp_k/cl_k,2) if cl_k>0 else None,
                        "ctr":round(cl_k/imp_k*100,2) if imp_k>0 else None,"clicks":cl_k,"imp":imp_k})
                if is_sc:
                    sp_na=round(sp2-sp_kw_total,2) if sp2r is not None else round(float(ag2["spend"])-sp_kw_total,2)
                    if sp_na>0.01: kw_list.append({"n":"N/A","match":"—","spend":sp_na,"conv":None,
                        "cpa":None,"cpc":None,"ctr":None,"clicks":0,"imp":0})
                adgroups.append({"n":adn,"spend":sp2,"conv":cv2,"cpa":round(sp2/cv2,2) if cv2>0 else None,
                    "cpc":round(sp2/cl2,2) if cl2>0 else None,"ctr":round(cl2/imp2*100,2) if imp2>0 else None,
                    "clicks":cl2,"imp":imp2,"keywords":kw_list})
            rows.append({"n":camp,"spend":sp,"conv":cv,"cpa":round(sp/cv,2) if cv>0 else None,
                "cpc":round(sp/cl,2) if cl>0 else None,"ctr":round(cl/imp*100,2) if imp>0 else None,
                "clicks":cl,"imp":imp,"adgroups":adgroups})
        return rows
    def fp(start,end=None):
        if dfp_full.empty: return dfp_full
        m=dfp_full["date"]>=start
        if end is not None: m=m&(dfp_full["date"]<=end)
        return dfp_full[m]
    result={"1":camps_period(df[(df["date"]>=ontem)&(df["date"]<=ontem)],fp(ontem,ontem))}
    for n in [7,14,30]: result[str(n)]=camps_period(df[df["date"]>=hoje-pd.Timedelta(days=n-1)],fp(hoje-pd.Timedelta(days=n-1)))
    result["all"]=camps_period(df,dfp_full); return result

def google_keywords(df):
    df_search=df[df["is_search"]==True] if "is_search" in df.columns else df
    hoje=pd.Timestamp(date.today()); ontem=hoje-pd.Timedelta(days=1)
    def kws_period(p):
        ag=p.groupby("keyword").agg(spend=("spend","sum"),conversions=("conversions","sum"),
            clicks=("clicks","sum"),impressions=("impressions","sum")).reset_index()
        ag=ag[~ag["keyword"].astype(str).str.strip().isin(["","__pesq_diff__"])]
        ag=ag[ag["spend"]>0].sort_values("conversions",ascending=False).head(25)
        rows=[]
        for _,k in ag.iterrows():
            sp=round(float(k["spend"]),2); cv=round(float(k["conversions"]),2); cl=int(k["clicks"]); imp=int(k["impressions"])
            mt=p[p["keyword"]==k["keyword"]]["match_type"]
            rows.append({"n":str(k["keyword"]),"match":str(mt.mode()[0]) if len(mt)>0 else "",
                "spend":sp,"conv":cv,"cpa":round(sp/cv,2) if cv>0 else None,
                "cpc":round(sp/cl,2) if cl>0 else None,"ctr":round(cl/imp*100,2) if imp>0 else None,
                "clicks":cl,"imp":imp})
        return rows
    result={"1":kws_period(df_search[(df_search["date"]>=ontem)&(df_search["date"]<=ontem)])}
    for n in [7,14,30]: result[str(n)]=kws_period(df_search[df_search["date"]>=hoje-pd.Timedelta(days=n-1)])
    result["all"]=kws_period(df_search); return result

def google_raw(df):
    df=df.copy(); df["date"]=pd.to_datetime(df["date"],errors="coerce"); df=df.dropna(subset=["date"]); rows=[]
    df_search=df[df["is_search"]==True] if "is_search" in df.columns else df
    agg=df_search.groupby(["date","campaign","adgroup","keyword","match_type"]).agg(
        spend=("spend","sum"),conversions=("conversions","sum"),clicks=("clicks","sum"),impressions=("impressions","sum")).reset_index()
    for _,r in agg.iterrows():
        rows.append({"d":r["date"].strftime("%d/%m/%Y"),"c":str(r["campaign"]),"a":str(r["adgroup"]),
            "kw":str(r["keyword"]),"mt":str(r["match_type"]),"sp":round(float(r["spend"]),2),
            "cv":round(float(r["conversions"]),2),"cl":int(r["clicks"]),"imp":int(r["impressions"])})
    df_outros=df[df["is_search"]==False] if "is_search" in df.columns else pd.DataFrame()
    if len(df_outros)>0:
        agg2=df_outros.groupby(["date","campaign","adgroup"]).agg(spend=("spend","sum"),conversions=("conversions","sum"),
            clicks=("clicks","sum"),impressions=("impressions","sum")).reset_index()
        for _,r in agg2.iterrows():
            rows.append({"d":r["date"].strftime("%d/%m/%Y"),"c":str(r["campaign"]),"a":str(r["adgroup"]),
                "kw":"","mt":"","sp":round(float(r["spend"]),2),"cv":round(float(r["conversions"]),2),
                "cl":int(r["clicks"]),"imp":int(r["impressions"])})
    return rows

def google_breakdowns(df):
    print("  Lendo breakdowns Google...")
    hoje=pd.Timestamp(date.today()); ontem=hoje-pd.Timedelta(days=1)
    try:
        df_a=pd.read_csv(URL_GOOGLE_AG); df_a["date"]=pd.to_datetime(df_a["Date (Segment)"],errors="coerce")
        df_a["spend"]=to_num(df_a["Cost (Spend, Amount Spent)"]); df_a["conv"]=to_num(df_a["All Conversions"])
        df_a["age"]=df_a["Age (Ad Group Criterion)"].map(AGE_MAP).fillna(df_a["Age (Ad Group Criterion)"].astype(str))
        df_a=df_a.dropna(subset=["date"])
    except Exception as e: print(f"  Aviso Age: {e}"); df_a=pd.DataFrame()
    try:
        df_g=pd.read_csv(URL_GOOGLE_GE); df_g["date"]=pd.to_datetime(df_g["Date (Segment)"],errors="coerce")
        df_g["spend"]=to_num(df_g["Cost (Spend, Amount Spent)"]); df_g["conv"]=to_num(df_g["All Conversions"])
        df_g["gender"]=df_g["Gender (Ad Group Criterion)"].astype(str).str.lower(); df_g=df_g.dropna(subset=["date"])
    except Exception as e: print(f"  Aviso Gender: {e}"); df_g=pd.DataFrame()
    AGE_ORDER=["18-24","25-34","35-44","45-54","55-64","65+"]
    def bd(pa,pg):
        age_d=[]; gen_d=[]
        try:
            if len(pa)>0:
                aa=pa[pa["age"].isin(AGE_ORDER)].groupby("age").agg(spend=("spend","sum"),conv=("conv","sum")).reset_index()
                aa["_o"]=aa["age"].apply(lambda x:AGE_ORDER.index(x) if x in AGE_ORDER else 99)
                aa=aa[aa["spend"]>0].sort_values("_o"); aa["cpl"]=(aa["spend"]/aa["conv"]).where(aa["conv"]>0).round(2)
                age_d=[{"n":str(r["age"]),"spend":round(float(r["spend"]),2),"conv":round(float(r["conv"]),2),"cpl":safe(r["cpl"])} for _,r in aa.iterrows()]
        except: pass
        try:
            if len(pg)>0:
                ga=pg[pg["gender"].isin(["female","male"])].groupby("gender").agg(spend=("spend","sum"),conv=("conv","sum")).reset_index()
                ga=ga[ga["spend"]>0].sort_values("conv",ascending=False); ga["cpl"]=(ga["spend"]/ga["conv"]).where(ga["conv"]>0).round(2)
                gen_d=[{"n":str(r["gender"]),"spend":round(float(r["spend"]),2),"conv":round(float(r["conv"]),2),"cpl":safe(r["cpl"])} for _,r in ga.iterrows()]
        except: pass
        return {"age":age_d,"gender":gen_d}
    def filt(dfa,dfg,start,end):
        pa=dfa[(dfa["date"]>=start)&(dfa["date"]<=end)] if len(dfa)>0 else dfa
        pg=dfg[(dfg["date"]>=start)&(dfg["date"]<=end)] if len(dfg)>0 else dfg
        return bd(pa,pg)
    result={"1":filt(df_a,df_g,ontem,ontem)}
    for n in [7,14,30]: result[str(n)]=filt(df_a,df_g,hoje-pd.Timedelta(days=n-1),hoje)
    result["all"]=bd(df_a,df_g); return result

def google_monthly(df):
    PT_MONTHS={"Jan":"Jan","Feb":"Fev","Mar":"Mar","Apr":"Abr","May":"Mai","Jun":"Jun",
               "Jul":"Jul","Aug":"Ago","Sep":"Set","Oct":"Out","Nov":"Nov","Dec":"Dez"}
    df=df.copy(); df["date"]=pd.to_datetime(df["date"],errors="coerce"); df=df.dropna(subset=["date"])
    if not len(df): return {"lbl":[],"totalS":[],"totalConv":[],"cpaG":[],"cpcG":[],"ctrG":[],"camps":[]}
    df["ym"]=df["date"].dt.to_period("M"); months=sorted(df["ym"].unique())
    out={"lbl":[],"totalS":[],"totalConv":[],"cpaG":[],"cpcG":[],"ctrG":[],"camps":[]}
    for m in months:
        p=df[df["ym"]==m]; sp=round(float(p["spend"].sum()),2); cv=round(float(p["conversions"].sum()),2)
        cl=int(p["clicks"].sum()); imp=int(p["impressions"].sum())
        raw_lbl=pd.Period(m,"M").strftime("%b/%y"); pt_lbl=PT_MONTHS.get(raw_lbl[:3],raw_lbl[:3])+raw_lbl[3:]
        out["lbl"].append(pt_lbl); out["totalS"].append(sp); out["totalConv"].append(cv)
        out["cpaG"].append(round(sp/cv,2) if cv>0 else None)
        out["cpcG"].append(round(sp/cl,2) if cl>0 else None)
        out["ctrG"].append(round(cl/imp*100,2) if imp>0 else None)
        ag=p.groupby("campaign").agg(spend=("spend","sum"),conversions=("conversions","sum"),
            clicks=("clicks","sum"),impressions=("impressions","sum")).reset_index()
        for _,r in ag.iterrows():
            out["camps"].append({"n":str(r["campaign"]),"spend":round(float(r["spend"]),2),
                "conv":round(float(r["conversions"]),2),"clicks":int(r["clicks"]),"imp":int(r["impressions"])})
    print(f"     Google Mensal: {len(months)} meses"); return out

# ══ INJEÇÃO ════════════════════════════════════════════
def replace_js_const(html, name, value):
    replacement = f"const {name} = {json.dumps(value, ensure_ascii=False)};"
    pattern_start = re.compile(rf"const {name}\s*=\s*")
    m = pattern_start.search(html)
    if not m:
        print(f"  AVISO: não encontrou const {name}")
        return html
    start = m.start(); val_start = m.end()
    i = val_start; depth = 0; in_str = False; str_char = None
    while i < len(html):
        ch = html[i]
        if in_str:
            if ch == '\\': i += 2; continue
            if ch == str_char: in_str = False
        else:
            if ch in ('"', "'", '`'): in_str = True; str_char = ch
            elif ch in ('{', '['): depth += 1
            elif ch in ('}', ']'): depth -= 1
            elif ch == ';' and depth == 0: break
        i += 1
    html = html[:start] + replacement + html[i+1:]
    return html

def main():
    print("="*60)
    print(f"Dashboard Standalone — {NOME_CLIENTE}")
    print("="*60)
    img_dir=Path("imgs"); img_dir.mkdir(exist_ok=True)

    print("\n[META ADS]")
    df_meta=load_meta()
    m_k=meta_kpis(df_meta); m_d=meta_daily(df_meta)
    m_dc=meta_daily_camps(df_meta); m_raw=meta_raw(df_meta)
    m_t=meta_tables(df_meta,img_dir); m_bd=meta_breakdowns(df_meta)
    m_month=meta_monthly(df_meta)
    total_leads=m_k["lct"]["leads"] if LANCAMENTO_COD else m_k["all"]["leads"]
    print(f"  ✓ {total_leads} leads | {MOEDA_SIMBOLO} {m_k['lct']['spend']:,.2f} invest.")

    print("\n[GOOGLE ADS]")
    if USAR_GOOGLE:
        try:
            df_google=load_google()
            df_google_corr=apply_pesq_spend(df_google)
            g_daily=google_daily(df_google_corr); g_kpis=google_kpis(df_google_corr)
            g_camps=google_camps(df_google); g_kw=google_keywords(df_google)
            g_bd=google_breakdowns(df_google); g_month=google_monthly(df_google_corr)
            g_raw=google_raw(df_google_corr)
            print(f"  ✓ {df_google['conversions'].sum():.0f} conv. | {MOEDA_SIMBOLO} {df_google['spend'].sum():,.2f} invest.")
        except Exception as e:
            print(f"  Aviso Google: {e}")
            g_daily={"days":[],"spend":[],"conversions":[],"cpa":[],"ctr":[],"cpc":[]}
            g_kpis={}; g_camps={}; g_kw={}; g_bd={}
            g_month={"lbl":[],"totalS":[],"totalConv":[],"cpaG":[],"cpcG":[],"ctrG":[],"camps":[]}
            g_raw=[]
    else:
        print("  (desativado)")
        g_daily={"days":[],"spend":[],"conversions":[],"cpa":[],"ctr":[],"cpc":[]}
        g_kpis={}; g_camps={}; g_kw={}; g_bd={}
        g_month={"lbl":[],"totalS":[],"totalConv":[],"cpaG":[],"cpcG":[],"ctrG":[],"camps":[]}
        g_raw=[]

    print("\n[HTML]")
    if not Path(TEMPLATE_FILE).exists():
        print(f"  ERRO: {TEMPLATE_FILE} não encontrado"); return

    html=Path(TEMPLATE_FILE).read_text(encoding="utf-8")
    html=replace_js_const(html,"META_KPIS",      m_k)
    html=replace_js_const(html,"META_DAILY",      m_d)
    html=replace_js_const(html,"META_DAILY_CAMPS",m_dc)
    html=replace_js_const(html,"META_RAW_CAMP",   m_raw)
    html=replace_js_const(html,"META_TABLES",     m_t)
    html=replace_js_const(html,"META_BD",         m_bd)
    html=replace_js_const(html,"META_MONTHLY",    m_month)
    html=replace_js_const(html,"PESQUISA",        False)
    html=replace_js_const(html,"DATA_GERACAO",    date.today().strftime("%Y-%m-%d"))
    html=replace_js_const(html,"GOOGLE_DAILY",    g_daily)
    html=replace_js_const(html,"GOOGLE_KPIS",     g_kpis)
    html=replace_js_const(html,"GOOGLE_CAMPS",    g_camps)
    html=replace_js_const(html,"GOOGLE_KW",       g_kw)
    html=replace_js_const(html,"GOOGLE_BD",       g_bd)
    html=replace_js_const(html,"GOOGLE_MONTHLY",  g_month)
    html=replace_js_const(html,"GOOGLE_RAW",      g_raw)
    html=replace_js_const(html,"USAR_GOOGLE",     USAR_GOOGLE)
    html=replace_js_const(html,"FUNIL_IMPRESSOES", FUNIL_IMPRESSOES)
    html=replace_js_const(html,"FUNIL_LINK_CLICKS",FUNIL_LINK_CLICKS)
    html=replace_js_const(html,"FUNIL_PAGE_VIEW",  FUNIL_PAGE_VIEW)
    html=replace_js_const(html,"FUNIL_LEADS",      FUNIL_LEADS)
    html=replace_js_const(html,"MOEDA_SIMBOLO",   MOEDA_SIMBOLO)
    html=replace_js_const(html,"MOEDA_COD",       MOEDA)
    html=replace_js_const(html,"LANCAMENTO_COD",  LANCAMENTO_COD)
    html=replace_js_const(html,"NOME_CLIENTE",    NOME_CLIENTE)
    html=replace_js_const(html,"LOGO_LETRA",      LOGO_LETRA)
    html=replace_js_const(html,"COR_ACENTO",      COR_ACENTO)
    html=replace_js_const(html,"CPL_BOM",         CPL_BOM)
    html=replace_js_const(html,"CPL_MEDIO",       CPL_MEDIO)
    html=replace_js_const(html,"CTR_BOM",         CTR_BOM)
    html=replace_js_const(html,"CTR_MEDIO",       CTR_MEDIO)
    html=replace_js_const(html,"CR_BOM",          CR_BOM)
    html=replace_js_const(html,"CR_MEDIO",        CR_MEDIO)
    html=replace_js_const(html,"TX_CONV_BOM",     TX_CONV_BOM)
    html=replace_js_const(html,"TX_CONV_MEDIO",   TX_CONV_MEDIO)
    html=replace_js_const(html,"CPM_BOM",         CPM_BOM)
    html=replace_js_const(html,"CPM_MEDIO",       CPM_MEDIO)

    Path(OUTPUT_FILE).write_text(html, encoding="utf-8")

    # data.json para o painel da agência (santo_criativo)
    def _snap(n):
        last=df_meta["date"].max(); p=df_meta[df_meta["date"]>last-pd.Timedelta(days=n)]
        s=round(float(p["spend"].sum()),2); l=int(p["leads"].sum())
        return {"spend":s,"leads":l,"cpl":round(s/l,2) if l else None}
    Path("data.json").write_text(json.dumps({"cliente":NOME_CLIENTE,"cor":COR_ACENTO,"letra":LOGO_LETRA,
        "atualizado":date.today().strftime("%d/%m/%Y"),
        "meta":{"cplBom":CPL_BOM,"cplMedio":CPL_MEDIO,**{f"d{n}":_snap(n) for n in (1,7,14,30)}},
        "google":{"ativo":False}},ensure_ascii=False,indent=2),encoding="utf-8")
    print("  ✓ data.json")
    print(f"  ✓ {OUTPUT_FILE} ({len(html)//1024}KB)")
    print("="*60)

if __name__ == "__main__":
    main()
    _limpar_thumbs()
