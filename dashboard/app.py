import os
import pandas as pd
from dash import Dash, dcc, html, Input, Output
from dotenv import load_dotenv
import plotly.express as px

from dashboard.db_helpers import (
    get_location_markers, get_parameters_for_location,
    get_parameter_records
)
from dashboard.plot_helpers import (
    generate_line_plot, generate_calendar_heatmap,
    generate_hourly_heatmap, generate_distribution_plot
)
from dashboard.summary_card import generate_summary_card

# Load Mapbox token
load_dotenv()
px.set_mapbox_access_token(os.getenv("mapbox_token"))

# Dash App
app = Dash(__name__)
app.title = "United States Air Quality Dashboard"

# App Layout
app.layout = html.Div([
    html.H2("🇺🇸 Air Quality Dashboard (US)"),

    dcc.Graph(id="map", figure=px.scatter_mapbox(
        get_location_markers(),
        lat="lat",
        lon="lon",
        hover_name="location_name",
        hover_data=["locality"],
        zoom=3.5,
        height=500,
        color_discrete_sequence=["#008080"]
    ).update_layout(
        mapbox_style="light",
        margin={"r": 0, "t": 0, "l": 0, "b": 0}
    )),

    html.Div([
        html.Label("Select Parameter"),
        dcc.RadioItems(id="parameter-radio", labelStyle={"display": "inline-block", "marginRight": "15px"})
    ], style={"marginTop": "10px", "marginBottom": "20px"}),

    html.Div(id="summary-card", style={
        "padding": "15px",
        "backgroundColor": "#f9f9f9",
        "border": "1px solid #ddd",
        "borderRadius": "8px",
        "marginBottom": "20px"
    }),

    html.Div([
        html.Div([
            html.Label("Select Year (optional)"),
            dcc.Dropdown(id="year-dropdown", placeholder="All Years", clearable=True)
        ], style={"width": "45%", "display": "inline-block", "marginRight": "5%"}),

        html.Div([
            html.Label("Select Plot Type"),
            dcc.RadioItems(
                id="plot-type-radio",
                options=[
                    {"label": "Time Series (Line)", "value": "line"},
                    {"label": "Calendar Heatmap", "value": "calendar"},
                    {"label": "Hourly Heatmap", "value": "hourly"},
                    {"label": "Distribution", "value": "distribution"}
                ],
                value="line",
                labelStyle={"display": "block", "marginBottom": "5px"}
            )
        ], style={"width": "45%", "display": "inline-block", "verticalAlign": "top"})
    ], style={"marginTop": "30px", "marginBottom": "20px"}),

    html.Div(id="agg-toggle-container", children=[
        html.Label("Aggregation Level"),
        dcc.RadioItems(
            id="agg-toggle",
            options=[
                {"label": "Hourly", "value": "H"},
                {"label": "Daily", "value": "D"},
                {"label": "Weekly", "value": "W"},
                {"label": "Monthly", "value": "M"}
            ],
            value="H",
            labelStyle={"display": "inline-block", "marginRight": "15px"}
        )
    ], style={"marginBottom": "20px"}),

    dcc.Graph(id="timeseries-graph")
])

# Callbacks

@app.callback(
    Output("parameter-radio", "options"),
    Input("map", "clickData")
)
def update_parameters(clickData):
    if clickData:
        location_name = clickData["points"][0]["hovertext"]
        return get_parameters_for_location(location_name)
    return []

@app.callback(
    Output("summary-card", "children"),
    Input("map", "clickData"),
    Input("parameter-radio", "value")
)
def update_summary(clickData, parameter):
    if clickData and parameter:
        location_name = clickData["points"][0]["hovertext"]
        return generate_summary_card(location_name, parameter)
    return html.P("Click a location and choose a parameter to see stats.")

@app.callback(
    Output("year-dropdown", "options"),
    Input("map", "clickData"),
    Input("parameter-radio", "value")
)
def update_year_dropdown(clickData, parameter):
    if clickData and parameter:
        location_name = clickData["points"][0]["hovertext"]
        df, _ = get_parameter_records(location_name, parameter)
        if not df.empty:
            years = sorted(df["datetime"].dt.year.unique())
            return [{"label": str(y), "value": y} for y in years]
    return []

@app.callback(
    Output("agg-toggle-container", "style"),
    Input("plot-type-radio", "value")
)
def toggle_agg_visibility(plot_type):
    if plot_type == "line":
        return {"marginBottom": "20px"}
    return {"display": "none"}

@app.callback(
    Output("timeseries-graph", "figure"),
    Input("map", "clickData"),
    Input("parameter-radio", "value"),
    Input("agg-toggle", "value"),
    Input("year-dropdown", "value"),
    Input("plot-type-radio", "value")
)
def update_plot(clickData, parameter, agg_level, selected_year, plot_type):
    if clickData and parameter:
        location_name = clickData["points"][0]["hovertext"]
        df, _ = get_parameter_records(location_name, parameter)

        if df.empty:
            return px.scatter(title="No data available.")

        if selected_year:
            df = df[df["datetime"].dt.year == selected_year]

        df.set_index("datetime", inplace=True)

        if plot_type == "line":
            return generate_line_plot(df, parameter, agg_level)
        elif plot_type == "calendar":
            return generate_calendar_heatmap(df, parameter, selected_year)
        elif plot_type == "hourly":
            return generate_hourly_heatmap(df, parameter)
        elif plot_type == "distribution":
            return generate_distribution_plot(df, parameter)

    return px.scatter(title="Click a location and select a parameter.")

if __name__ == "__main__":
    app.run(debug=True)
