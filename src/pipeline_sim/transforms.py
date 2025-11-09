import json, hashlib
from typing import Iterable, Tuple
from datetime import timezone
import pandas as pd
from .schemas import ChartIncident, RwisObservation, WzdxFeature

def hash_id(*parts: Iterable[str]) -> str:
    s = "|".join(str(p) for p in parts)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]

def bronze_chart_to_silver(path: str) -> pd.DataFrame:
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            try:
                rec = ChartIncident(**obj).model_dump()
                rows.append(rec)
            except Exception:
                continue
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    # Normalize timestamps to UTC ISO strings
    for col in ["event_start_ts","event_end_ts"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True).astype("datetime64[ms]")
    df["incident_sk"] = df.apply(lambda r: hash_id(r.get("incident_id")), axis=1)
    return df[["incident_sk","incident_id","event_start_ts","event_end_ts","route","direction","lat","lon","description","severity"]]

def bronze_rwis_to_silver(path: str) -> pd.DataFrame:
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            try:
                rec = RwisObservation(**obj).model_dump()
                rows.append(rec)
            except Exception:
                continue
    df = pd.DataFrame(rows)
    if df.empty: return df
    df["obs_ts"] = pd.to_datetime(df["obs_ts"], utc=True).astype("datetime64[ms]")
    return df

def bronze_wzdx_to_silver(path: str) -> pd.DataFrame:
    gj = json.load(open(path,"r",encoding="utf-8"))
    feats = gj.get("features",[])
    rows = []
    for feat in feats:
        try:
            row = WzdxFeature.from_geojson(feat).model_dump()
            rows.append(row)
        except Exception:
            continue
    df = pd.DataFrame(rows)
    if df.empty: return df
    for c in ["start_ts","end_ts"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], utc=True).astype("datetime64[ms]")
    df["wzdx_sk"] = df.apply(lambda r: hash_id(r.get("wzdx_id")), axis=1)
    return df[["wzdx_sk","wzdx_id","road_name","start_ts","end_ts","geom"]]

def build_gold_fact(incidents: pd.DataFrame, rwis: pd.DataFrame, wzdx: pd.DataFrame) -> pd.DataFrame:
    # Very simple demo join: nearest‑time RWIS to each incident; attach any active WZDx by time overlap
    fact = incidents.copy()
    if not rwis.empty:
        # attach nearest obs_ts per station (simplified; real impl would be geo+time)
        rwis_sorted = rwis.sort_values("obs_ts")
        fact["nearest_obs_ts"] = fact["event_start_ts"].apply(
            lambda ts: rwis_sorted.iloc[(rwis_sorted["obs_ts"]-ts).abs().argsort()].iloc[0]["obs_ts"] if not rwis_sorted.empty else pd.NaT
        )
    if not wzdx.empty:
        # time overlap flag
        def overlap(row):
            st, et = row["event_start_ts"], row["event_end_ts"]
            if pd.isna(et): et = st
            active = wzdx[(wzdx["start_ts"]<=et) & ((wzdx["end_ts"].isna()) | (wzdx["end_ts"]>=st))]
            return len(active)>0
        fact["wzdx_active"] = fact.apply(overlap, axis=1)
    return fact
