PARAMETER_BANDS = {
    "pm25": [
        (0.0, 12.0, "#d2f8d2", "Good"),
        (12.1, 35.4, "#fff5cc", "Moderate"),
        (35.5, 55.4, "#ffdddd", "USG"),
        (55.5, 150.4, "#f08080", "Unhealthy"),
        (150.5, 250.4, "#c71585", "Hazardous"),
    ],
    "pm10": [
        (0, 50, "#d2f8d2", "Good"),
        (50, 100, "#fff5cc", "Moderate"),
        (100, 250, "#ffcccc", "Unhealthy"),
    ],
    "o3": [
        (0, 0.070, "#d2f8d2", "Good"),
        (0.070, 0.100, "#fff5cc", "Moderate"),
        (0.100, 0.200, "#ffcccc", "Unhealthy"),
    ],
    "no2": [
        (0, 0.053, "#d2f8d2", "Good"),
        (0.053, 0.1, "#fff5cc", "Moderate"),
        (0.1, 0.2, "#ffcccc", "Unhealthy"),
    ],
    "so2": [
        (0, 0.075, "#d2f8d2", "Good"),
        (0.075, 0.15, "#fff5cc", "Moderate"),
        (0.15, 0.3, "#ffcccc", "Unhealthy"),
    ],
    "co": [
        (0, 9, "#d2f8d2", "Good"),
        (9, 15, "#fff5cc", "Moderate"),
        (15, 30, "#ffcccc", "Unhealthy"),
    ],
    "relativehumidity": [
        (0.0, 30.0, "#ffe6e6", "Very Low / Dry"),
        (30.0, 40.0, "#fff5cc", "Low"),
        (40.0, 60.0, "#d2f8d2", "Comfortable / Ideal"),
        (60.0, 70.0, "#fff0b3", "Moderate / Slightly High"),
        (70.0, 90.0, "#ffcccc", "High"),
        (90.0, 100.0, "#f08080", "Very High / Saturated"),
    ],
    "temperature": [
        (-50.0, 0.0, "#e0f7fa", "Freezing / Very Cold"),
        (0.0, 10.0, "#cce5ff", "Cold"),
        (10.0, 18.0, "#ccffff", "Cool"),
        (18.0, 24.0, "#d2f8d2", "Comfortable / Mild"),
        (24.0, 30.0, "#fff5cc", "Warm"),
        (30.0, 35.0, "#ffdddd", "Hot"),
        (35.0, 100.0, "#f08080", "Very Hot / Extreme"),
    ],
}


def get_safety_label(value, parameter):
    bands = PARAMETER_BANDS.get(parameter, [])
    for y0, y1, _, label in bands:
        if y0 <= value <= y1:
            return label
    return "Unknown"
