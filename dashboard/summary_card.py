import pandas as pd
from dash import html
from dashboard.constants import get_safety_label
from dashboard.db_helpers import collection


def generate_summary_card(location_name, parameter):
    doc = collection.find_one({"location_name": location_name})
    records = []

    units = "unknown"
    for sensor in doc.get("sensors", []):
        if sensor["parameter"] == parameter:
            units = sensor.get("units", "unknown")
            for m in sensor.get("measurements", []):
                dt = pd.to_datetime(m["date"]) + pd.to_timedelta(m["hour"], unit="h")
                records.append({"datetime": dt, "value": m["value"]})

    df = pd.DataFrame(records)
    if df.empty:
        return html.P("No data available.")

    df.sort_values("datetime", inplace=True)
    latest = df.iloc[-1]
    max_row = df.loc[df["value"].idxmax()]
    min_row = df.loc[df["value"].idxmin()]

    return html.Div(
        [
            html.H5(f"{parameter.upper()} Summary at {location_name}"),
            html.P(f"Latest: {latest['value']} {units} on {latest['datetime']}"),
            html.P(f"Status: {get_safety_label(latest['value'], parameter)}"),
            html.P(f"Max: {max_row['value']} {units} on {max_row['datetime']}"),
            html.P(f"Min: {min_row['value']} {units} on {min_row['datetime']}"),
        ]
    )
