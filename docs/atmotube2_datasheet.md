# Atmotube Datasheet

The Atmostube data dictonary (`dict`) is generated from the parsing logic in the current `src/parsers/atmotube.py` (the `parse()` function and its `build_*_df` helpers). 

This document describes every output variable produced by the parser: its meaning, unit, source sensor, and any notes on interpretation.

---

## Time

Added by `add_timezone_col()` in `src/utils.py`, which standardizes the datetime column and derives timezone from latitude/longitude. Auto-detects the datetime column (`detect_utc_col`) and lat/lon columns (`detect_latlon_cols`) if not specified.

| Variable | Unit / Format | Description |
|---|---|---|
| `datetime` | ISO 8601, UTC-aware | Standardized timestamp. Acts as the index key used to merge all category tables together. |
| `timezone` | IANA database format (e.g. `America/Toronto`) | Local timezone derived from latitude/longitude. Only assigned to rows with valid coordinates — see data-quality rule below. Rows with missing/invalid lat-lon are left as `NaN`. |

> **Data-quality rule — "null island" exclusion:** A coordinate pair is only considered valid (and thus eligible for a timezone lookup) if both latitude and longitude are non-null **and not exactly `(0.0, 0.0)`**. `(0, 0)` — known as "null island" — is a common default/error value from GPS sensors and is treated as invalid. In cartography, null island is the location where the equator meets the prime meridian at the Gulf of Guinea off the coast of West Africa!

> **Column ordering:** After this step, columns are reordered so that `datetime`, `longitude`, `latitude`, `timezone` appear first, followed by all remaining columns.

---

## GIS / Location

**Builders:** `build_gis_df()` and `build_raw_gis_df()` in `src/utils.py`.
**Sensor:** Phone GPS (the Atmotube PRO/PRO 2 has no internal GPS module).
**Altitude:** Derived from barometric pressure (Bosch BME280), when present.

| Variable | Unit / Format | Description |
|---|---|---|
| `latitude` | degrees | Latitude (0° = equator). |
| `longitude` | degrees | Longitude (0° = prime meridian). |
| `altitude` | meters | Current elevation, derived from barometric pressure. **Optional** — included only if an altitude-like column is detected in the source data; if absent, the function logs "No altitude data available." and the column is simply omitted. |

> **`gis` vs `raw_gis` — different row-filtering, same columns:**
> - **`gis`** (`build_gis_df`) — drops any row missing `datetime`, `latitude`, `longitude`, or `timezone` Intended for map visualizations.
> - **`raw_gis`** (`build_raw_gis_df`) — keeps every row regardless of missing coordinates. Intended for data-loss reporting (e.g. `report_loss()` in `stats.py`), to quantify how many rows were dropped by the cleaning step above.

> **Why the PRO can't measure true GPS accuracy:** Accuracy depends on the phone's own GNSS chip, which is a function of satellite geometry, satellite count, and signal strength. The Atmotube PRO 2 does **not** record satellite geometry (DOP — HDOP/PDOP/VDOP), but it **does** record signal strength (SNR, across four bands) — see **Satellite** section below.

---

## Particulate Matter (PM)

**Sensor:** Sensirion SPS30
**Builder:** `build_pm_df()`

| Variable | Unit | Description |
|---|---|---|
| `pm1_0_ugm3_atm` | µg/m³ | Mass concentration of particulate matter ≤1.0 µm in a cubic meter of air. |
| `pm2_5_ugm3_atm` | µg/m³ | Mass concentration of particulate matter ≤2.5 µm in a cubic meter of air. |
| `pm10_ugm3_atm` | µg/m³ | Mass concentration of particulate matter ≤10 µm in a cubic meter of air. |

### PM Extension (Atmotube PRO 2 only — raw particle counts)

| Variable | Unit | Description |
|---|---|---|
| `pm0_5_um_count` | count | Number of particles with diameter beyond 0.5 µm, per unit volume. |
| `pm1_0_um_count` | count | Number of particles with diameter beyond 1.0 µm. |
| `pm2_5_um_count` | count | Number of particles with diameter beyond 2.5 µm. |
| `pm10_um_count` | count | Number of particles with diameter beyond 10 µm. |
| `pmsize_nm_avg` | nm | Average particle size, as originally reported. |
| `pmsize_um_avg` | µm | Average particle size, converted from nanometers (nm ÷ 1000). |

> Note: PM extension columns are only present on Atmotube PRO 2 exports. If absent, the script logs "No raw particle count data available."

---

## Weather

**Sensor:** Bosch BME280
**Builder:** `build_weather_df()`

| Variable | Unit | Description |
|---|---|---|
| `temp_c` | °C | Ambient temperature. |
| `hum_pct` | % | Relative humidity — how much water vapor is in the air relative to the maximum it can hold at that temperature (0% = dry, 100% = saturated). |
| `press_hpa` | hPa (hectopascals) | Atmospheric pressure at the current elevation (not adjusted to sea level). |
| `aqs_total` | score out of 100, integer (`Int64`) | Aggregate Air Quality Score combining PM, TVOC, CO₂, and NOx. 0 = polluted, 100 = clean. |

> **Note on placement:** `aqs_total` now lives in the weather table rather than its own AQS table, since it's pulled from the same raw export columns as temp/humidity/pressure in this version. It is still an aggregate of PM + TVOC + CO₂ + NOx, not a weather measurement itself — listed here purely for table-of-origin, not category.

Temp/humidity/pressure are used downstream for heat index and dew point calculations.

---

## Gas (TVOC, NOx, CO₂)

**Builder:** `build_gas_df()`
**Sensor (PRO 2):** Sensirion SPS30


| Variable | Unit | Description |
|---|---|---|
| `tvoc_ppm` | ppm | Absolute concentration of Total Volatile Organic Compounds in one volume of air. |
| `tvoc_index` | index out of 500 | Normalized TVOC score relative to a rolling 24-hour baseline (100 = baseline; >100 = becoming more polluted; <100 = becoming cleaner). |
| `nox_index` | index out of 500 | Normalized score for nitric oxide (NO) and nitrogen dioxide (NO₂), relative to a rolling 24-hour baseline (100 = baseline). |
| `co2_ppm` | ppm | Absolute concentration of carbon dioxide in one volume of air. |

> **Correction:** The script comments this builder as "VOC sensor = Sensirion SGP40," but the SGP40 only measures TVOC and has no NOx capability. The correct part number is the **Sensirion SGP41**, which measures both TVOC and NOx and is used in the **Atmotube PRO 2** specifically. The original Atmotube PRO uses a different TVOC-only sensor (Sensirion SGPC3), not the SGP40 or SGP41. In short: SGP41 = PRO 2 only.
>
> CO₂ (`co2_ppm`) comes from a separate sensor (Sensirion SCD41 per the original script's annotation), bundled into this builder simply because both are queried together from the raw export.

---

## Satellite

**Builder:** `build_sat_df()`

| Variable | Unit | Description |
|---|---|---|
| `position_error_m` | meters | Estimated accuracy of the phone's GPS fix. |
| `sat_view_count` | count | Number of satellites currently in view. |
| `sat_used_count` | count | Number of satellites used to calculate the location fix. (Minimum 4 for a 3D fix [lon, lat, alt]; minimum 3 for a 2D fix [lon, lat].) *(Renamed from `sat_fix_count` in the original script — same meaning.)* |
| `sat_lowsignal_count` | count | Satellites in view with SNR in the 0–19 range (low signal strength). |
| `sat_medsignal_count` | count | Satellites in view with SNR in the 20–49 range (medium signal strength). |
| `sat_highsignal_count` | count | Satellites in view with SNR in the 50–99 range (high signal strength). |
| `sat_signal_avg` | SNR | Average satellite signal strength relative to surrounding electronic noise. |

> Note: Satellite columns are only present on Atmotube PRO 2 exports. If absent, the script logs "No satellite data available."

---

## Phone

**Builder:** `build_phone_df()`
**Source:** Phone sensors / OS (motion, battery, charging state) and phone-vs-device GPS source flag.

| Variable | Type | Description |
|---|---|---|
| `gps_phone_bool` | boolean | Whether this data point's GPS fix came live from the phone (`True`) versus pulled from the phone's local storage as historical data (`False`). *(Replaces the original script's pair `gps_now_bool` / `gps_past_bool`, which were inverses of each other — collapsed into a single boolean.)* |
| `motion_phone_bool` | boolean | Whether the device was moving at the time of the reading. *(Renamed from `motion_now_bool`.)* |
| `battery_phone_pct` | % | Remaining battery charge on the phone. |
| `charge_phone_bool` | boolean | Whether the phone was actively charging during data collection. |
| `cooldown_phone_bool` | boolean | Whether the phone was in a post-charge cooldown state. Kept as a separate boolean (rather than folded into `charge_phone_bool`) since cooldown may still affect phone/sensor performance even though the phone isn't actively charging. |

> **Logic change from original script:** The original encoded charging state as a single ordinal column (`charg_phone_state`: 0 = not charging, 1 = cooldown, 2 = charging) plus one derived boolean. The current version splits this directly into two independent booleans (`charge_phone_bool`, `cooldown_phone_bool`) from the same raw `no` / `cd` / `yes` values, with no intermediate ordinal column retained in the output.

---

## Text

**Builder:** `build_txt_df()`

| Variable | Description |
|---|---|
| `user_notes` | Free-text notes entered by the user. |

---

## Merged Output

`parse()` returns a dict of all sub-tables (`gis`, `raw_gis`, `pm`, `weather`, `gas`, `phone`, `sat`, `txt`) plus `all` — a left-merge of `gis`, `pm`, `weather`, `gas`, `phone`, `sat`, and `txt` on `datetime`. (`raw_gis` is excluded from the `all` merge — it's a separate diagnostic table, not part of the analysis-ready output.)

```python
from src.parsers.atmotube import parse
dfs = parse(df)
dfs["pm"]    # PM sub-dataframe
dfs["all"]   # fully merged dataframe
```

---

## Source References

- [Atmotube — Sensor Accuracy & Technical Specifications](https://support.atmotube.com/en/articles/10450299-sensor-accuracy-and-technical-specifications)
- [Atmotube — Understanding the Atmotube PRO 2 Air Quality Score (AQS)](https://support.atmotube.com/en/articles/12621821-understanding-atmotube-pro-2-air-quality-score-aqs)
- [Atmotube — Data Storage and Collection](https://support.atmotube.com/en/articles/10365067-data-storage-and-collection)
- [Atmotube — History Mode Overview](https://support.atmotube.com/en/articles/13002682-history-mode-overview)
- [Atmotube — PRO 2 Technical Specifications](https://support.atmotube.com/en/articles/12629420-atmotube-pro-2-technical-specifications)
- [Atmotube PRO 2 Manual (manuals.plus)](https://manuals.plus/m/5680a4efbd277d95da9d611da9e9e4d4f685a0f3a5e72d9c7c815662d60fb8db)
- [AirGradient — Explaining VOC, TVOC, and VOC Index](https://www.airgradient.com/blog/explaining-voc-tvoc-and-voc-index/)
- [Sensirion — SGP4x VOC/NOx & Building Standards](https://sensirion.com/media/documents/4B4D0E67/6520038C/GAS_AN_SGP4x_BuildingStandards_D1_1.pdf)
- [Sensirion — NOx Index Info Note](https://sensirion.com/media/documents/9F289B95/6294DFFC/Info_Note_NOx_Index.pdf)
- [GNSS Accuracy Metrics: DOP, C/N0, TTFF](https://gnsssimulator.com/gnss-accuracy-metrics-dop-cn0-ttff/)