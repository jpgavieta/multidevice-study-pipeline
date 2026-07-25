from functools import reduce
import pandas as pd

from ..DEPRECATEDutils import (
    build_gis_df,
    add_timezone_col,
    get_cols,
    rename_cols,
    parse_jsonblob_csv,
    detect_json_col,
)

# ============================================================================================================
# Df builders
print("hello from Ponyopi parser")

def build_pm_df(df):  # PM sensor = Plantower PMS7008/PMS5003
    pm_df = get_cols(df, ["datetime", "pm.pm", "atm", "ug", "m3"], ["cf", "count", "um", "ppm"])
    pm_df = rename_cols(pm_df,
        ["pm", "2", "5"],   "pm2_5_ugm3_atm",
        ["pm", "10"],       "pm10_ugm3_atm",
        ["pm", "1"],        "pm1_0_ugm3_atm",
        silent=True
    )

    pm_count_df = get_cols(df, ["datetime", "pm.pm", "count", "um"], ["atm", "ug", "m3", "cf", "ppm", "hum"])
    if pm_count_df.shape[1] <= 1:
        print("No raw particle count data available.")
    else:
        pm_count_df = rename_cols(pm_count_df,
            ["0", "3"],  "pm0_3_um_count",
            ["0", "5"],  "pm0_5_um_count",
            ["2", "5"],  "pm2_5_um_count",
            ["10"],      "pm10_um_count",
            ["1"],       "pm1_0_um_count",
            ["5"],       "pm5_0_um_count",
            silent=True
        )
        pm_df = pd.merge(pm_df, pm_count_df, on="datetime", how="left")

    pm_cf_df = get_cols(df, ["datetime", "pm.pm", "cf"], ["atm", "ug", "m3", "count", "um", "ppm"])
    if pm_cf_df.shape[1] <= 1:
        print("No Standard Particle (CF=1) concentration data available.")
    else:
        pm_cf_df = rename_cols(pm_cf_df,
            ["0", "5"],  "pm0_5_ugm3_cf",
            ["2", "5"],  "pm2_5_ugm3_cf",
            ["10"],      "pm10_ugm3_cf",
            ["1"],       "pm1_0_ugm3_cf",
            silent=True
        )
        pm_df = pd.merge(pm_df, pm_cf_df, on="datetime", how="left")

    return pm_df


def build_weather_df(df): 
    weather_df = get_cols(df, ["datetime", "temp", "hum", "press"], ["net", "sys", "power", "errors"]) # not DTH reliant
    return rename_cols(weather_df,
        ["temp"],  "temp_c",
        ["hum"],   "hum_pct",
        ["press"], "press_hpa",
    )


def build_gas_df(df):  # VOC & NOx sensor = Sensirion SGP4x (coming soon)
    gas_df = get_cols(df, ["datetime", "tvoc", "nox", "co2"])

    if gas_df.shape[1] <= 1:
        print("No gas sensor data available.")
        return gas_df

    gas_df = rename_cols(gas_df,
        ["tvoc", "ppm"],    "tvoc_ppm",
        ["tvoc", "index"],  "tvoc_index",
        ["nox", "index"],   "nox_index",
        ["co2", "ppm"],     "co2_ppm",
        silent=True
    )
    return gas_df

def build_sat_df(df): 
    sat_df = get_cols(df, ["datetime", "sat", "position"], ["net", "sys", "power", "errors"])
    sat_df = rename_cols(sat_df,
        ["position"],               "position_error_m",
        ["sat", "view"],            "sat_view_count",
        ["sat", "fix"],             "sat_used_count",
    ) # Will need to add extension for parse whatever NMEA sentences can be extracted if we want more sat data (e.g. signal strength, SNR, etc.)
    return sat_df


def build_sys_df(df):  # System = CPU, memory, load, uptime + Power = voltage rails, throttle flags
    sys_df = get_cols(df, ["datetime", "sys", "power"], ["net", "dht", "gps", "pm", "errors"])
    return rename_cols(sys_df,
        ["cpu", "pct"],     "cpu_pct",
        ["cpu", "temp"],    "cpu_temp_c",
        ["load", "1"],      "load_1min",
        ["load", "5"],      "load_5min",
        ["load", "15"],     "load_15min",
        ["mem", "total"],   "mem_total_mb",
        ["mem", "avail"],   "mem_avail_mb",
        ["mem", "used"],    "mem_used_mb",
        ["uptime"],         "uptime_s",
        ["vcore"],          "vcore_v",
        ["vsdram", "c"],    "vsdram_c_v",
        ["vsdram", "i"],    "vsdram_i_v",
        ["vsdram", "p"],    "vsdram_p_v",
        ["undervolt", "now"],       "undervolt_now_bool",
        ["freq", "capped", "now"],  "freq_capped_now_bool",
        ["throttled", "now"],       "throttled_now_bool",
        ["soft", "temp", "now"],    "soft_temp_now_bool",
        ["undervolt", "occurred"],      "undervolt_occurred_bool",
        ["freq", "capped", "occurred"], "freq_capped_occurred_bool",
        ["throttled", "occurred"],      "throttled_occurred_bool",
        ["soft", "temp", "occurred"],   "soft_temp_occurred_bool",
        silent=True
    )


def build_net_df(df):  # Network
    net_df = get_cols(df, ["datetime", "net"])
    return rename_cols(net_df,
        ["local"],   "local_ip",
        ["public"],  "public_ip",
        silent=True
    )


# ============================================================================================================
# Top-level parser

def parse(df: pd.DataFrame) -> dict:
    """
    Parse a raw PonyoPi DataFrame and return a dict of clean DataFrames.

    Parameters
    ----------
    df : pd.DataFrame
        Raw CSV loaded as a DataFrame (from etl.py).
        Accepts either JSON-blob format (2 columns) or pre-flattened format (many columns).

    Returns
    -------
    dict with keys:
        "gis", "pm", "weather", "sat", "sys", "net", "gas", "all"
    """
    df = df.copy()

    # If JSON-blob format, flatten first
    if detect_json_col(df) is not None:
        df = parse_jsonblob_csv(df)

    # Standardize datetime, latitude, longitude and add timezone column
    df = add_timezone_col(df)

    dfs = {
        "gis":      build_gis_df(df),       # Actually from utils.py
        "pm":       build_pm_df(df),
        "weather":  build_weather_df(df),
        "gas":      build_gas_df(df),  
        "sat":      build_sat_df(df),
        "sys":      build_sys_df(df),
        "net":      build_net_df(df),
    }
    
    return dfs

# Example: from src.parsers.ponyopi import parse
#          dfs = parse(df)
#          dfs["pm"]    # PM sub-dataframe
