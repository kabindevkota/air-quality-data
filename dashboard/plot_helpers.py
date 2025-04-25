import pandas as pd
import plotly.express as px
from dashboard.constants import PARAMETER_BANDS


def generate_line_plot(df, parameter, agg_level):
    df_resampled = df.resample(agg_level).mean().reset_index()
    fig = px.line(
        df_resampled, x="datetime", y="value", title=f"{parameter.upper()} Trend"
    )
    fig.update_traces(
        line=dict(color="#007ACC", width=2), connectgaps=True, mode="lines+markers"
    )

    bands = PARAMETER_BANDS.get(parameter)
    if bands:
        for y0, y1, color, label in bands:
            fig.add_hrect(
                y0=y0,
                y1=y1,
                fillcolor=color,
                line_width=0,
                opacity=0.2,
                annotation_text=label,
                annotation_position="top left",
            )
    return fig


def generate_calendar_heatmap(df, parameter, year=None):
    df_daily = df[["value"]].resample("D").mean().reset_index()
    df_daily["day"] = df_daily["datetime"].dt.day
    df_daily["month"] = df_daily["datetime"].dt.month
    df_daily["year"] = df_daily["datetime"].dt.year

    title = f"{parameter.upper()} Calendar Heatmap ({year or 'All'})"
    return px.density_heatmap(
        df_daily,
        x="day",
        y="month",
        z="value",
        color_continuous_scale="YlOrRd",
        title=title,
    )


def generate_hourly_heatmap(df, parameter):
    df["hour"] = df.index.hour
    df["dayofweek"] = df.index.day_name()
    df_hourly = df.groupby(["dayofweek", "hour"]).mean().reset_index()

    return px.density_heatmap(
        df_hourly,
        x="hour",
        y="dayofweek",
        z="value",
        color_continuous_scale="Blues",
        title=f"{parameter.upper()} by Hour & Day of Week",
    )


def generate_distribution_plot(df, parameter):
    return px.histogram(
        df.reset_index(), x="value", nbins=50, title=f"{parameter.upper()} Distribution"
    )
