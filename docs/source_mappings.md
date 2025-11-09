# Source Mappings

This document tracks how each field flows across zones and any transforms applied.

## CHART Incidents/Events (JSON → Bronze/Silver)

| Source Field | Bronze Column | Silver Column | Type | Notes |
|---|---|---|---|---|
| incidentId | incident_id | incident_id | STRING | Primary key from source |
| startTime | start_time | event_start_ts | TIMESTAMP | Converted to UTC |
| endTime | end_time | event_end_ts | TIMESTAMP | May be null |
| route | route | route | STRING | Normalized uppercase |
| direction | direction | direction | STRING | N/E/S/W normalized |
| latitude | latitude | lat | DOUBLE | WGS84 |
| longitude | longitude | lon | DOUBLE | WGS84 |
| description | description | description | STRING | Trimmed |
| severity | severity | severity | INT | Parsed from string enum |

## RWIS Observations (JSON)

| Source Field | Bronze | Silver | Type | Notes |
| stationId | station_id | station_id | STRING | |
| obsTime | obs_time | obs_ts | TIMESTAMP | UTC |
| airTempC | air_temp_c | air_temp_c | DOUBLE | |
| roadTempC | road_temp_c | road_temp_c | DOUBLE | |
| precipType | precip_type | precip_type | STRING | ENUM set |
| windSpeedKph | wind_speed_kph | wind_speed_kph | DOUBLE | |

## WZDx Features (GeoJSON)

| Source Field | Bronze | Silver | Type | Notes |
| id | feature_id | wzdx_id | STRING | |
| properties.core_details.road_names[0] | road_name | road_name | STRING | |
| properties.core_details.start_date | start_date | start_ts | TIMESTAMP | |
| properties.core_details.end_date | end_date | end_ts | TIMESTAMP | |
| geometry.coordinates | geometry | geom | GEOMETRY | Stored as lon/lat |

## AADT (CSV)

| Source Field | Bronze | Silver | Type | Notes |
| segment_id | segment_id | segment_id | STRING | key |
| year | year | year | INT | |
| aadt | aadt | aadt | INT | |

See `src/pipeline_sim/transforms.py` for implementation details.
