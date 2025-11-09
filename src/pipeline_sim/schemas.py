from pydantic import BaseModel, Field, field_validator
from typing import Optional, List
from datetime import datetime
import re

class ChartIncident(BaseModel):
    incident_id: str = Field(..., alias="incidentId")
    event_start_ts: datetime = Field(..., alias="startTime")
    event_end_ts: Optional[datetime] = Field(None, alias="endTime")
    route: str
    direction: str
    lat: float = Field(..., alias="latitude")
    lon: float = Field(..., alias="longitude")
    description: Optional[str] = None
    severity: Optional[int] = None

    @field_validator("route", mode="before")
    @classmethod
    def norm_route(cls, v):
        return str(v).upper().strip()

    @field_validator("direction", mode="before")
    @classmethod
    def norm_direction(cls, v):
        v = str(v).upper().strip()
        # Map words to initials if needed
        mapping = {"NORTH":"N","EAST":"E","SOUTH":"S","WEST":"W"}
        return mapping.get(v, v)

    @field_validator("severity", mode="before")
    @classmethod
    def to_int(cls, v):
        if v is None: return None
        try: return int(v)
        except: return None

class RwisObservation(BaseModel):
    station_id: str = Field(..., alias="stationId")
    obs_ts: datetime = Field(..., alias="obsTime")
    air_temp_c: Optional[float] = Field(None, alias="airTempC")
    road_temp_c: Optional[float] = Field(None, alias="roadTempC")
    precip_type: Optional[str] = Field(None, alias="precipType")
    wind_speed_kph: Optional[float] = Field(None, alias="windSpeedKph")

class WzdxFeature(BaseModel):
    wzdx_id: str = Field(..., alias="id")
    road_name: Optional[str] = None
    start_ts: datetime
    end_ts: Optional[datetime] = None
    geom: List[float]

    @classmethod
    def from_geojson(cls, feat: dict):
        props = feat.get("properties",{}).get("core_details",{})
        road_name = None
        rn = props.get("road_names", [])
        if isinstance(rn, list) and rn:
            road_name = rn[0]
        return cls(
            id=feat.get("id"),
            road_name=road_name,
            start_ts=props.get("start_date"),
            end_ts=props.get("end_date"),
            geom=feat.get("geometry",{}).get("coordinates",[])
        )
