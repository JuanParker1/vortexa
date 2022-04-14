from datetime import datetime
from collections import defaultdict

date_from = datetime(2022, 1, 1)
date_to = datetime(2022, 5, 1)


from vortexasdk import Products, CargoTimeSeries, CargoMovements, VesselMovements, Geographies, Vessels
import pandas as pd
import os
import xlsxwriter

needed_columns = [
    "events.cargo_port_load_event.0.end_timestamp",
    "events.cargo_port_unload_event.0.start_timestamp",
    "vessels.0.name",
    "vessels.0.vessel_class",
    "vessels.0.corporate_entities.charterer.label",
    "vessels.1.name",
    "vessels.1.vessel_class",
    "vessels.1.corporate_entities.charterer.label",
    "vessels.2.name",
    "vessels.2.vessel_class",
    "vessels.2.corporate_entities.charterer.label",
    "quantity",
    "events.cargo_port_unload_event.0.location.port.label",
    "product.group.label",
    "product.grade.label",
    "events.cargo_sts_event.0.event_type",
]

needed_labels = [
    "Skarv",
    "Goliat",
    "Statfjord",
    "Gullfaks Blend",
    "Alvheim",
    "Brent Blend",
    "Johan Sverdrup",
    "Gudrun",
    "Heidrun",
    "Forties",
    "Troll Blend",
    "Grane",
    "Oseberg Blend",
    "Gina Krog",
    "Flotta Gold",
    "Norne",
    "Draugen",
    "Clair",
    "Ekofisk Blend",
    "West Texas Intermediate (WTI)",
    "Mariner",
    "Danish Blend (DUC)",
    "Asgard Blend",
    "Harding",
    "Kraken",
]

xlsx_columns = {
    "From": "events.cargo_port_load_event.0.end_timestamp",
    "To": "events.cargo_port_unload_event.0.start_timestamp",
    "Ship": None,
    "Ship Class": None,
    "Charterer": None,
    "Volume": "quantity",
    "Estimated Volume": None,
    "Destination": "events.cargo_port_unload_event.0.location.port.label",
}

estimated_volume = {
    "suezmax": 1000000,
    "vlcc_plus": 2000000,
    "aframax": 600000,
}
estimated_volume = defaultdict(str, estimated_volume)

def set_api_key(api_key_file="api_key.txt"):
    with open(api_key_file, "rt") as file:
        os.environ["VORTEXA_API_KEY"] = file.read().strip()


def format_time(ts):
    date_str = str(ts).split()[0]
    return '.'.join(reversed(date_str.split('-')))


def format_value(value):
    value_str = str(value)
    if value_str.lower() == "nan":
        value_str = ""
    return value_str


if __name__ == '__main__':
    set_api_key()
    
    cargo_movements = CargoMovements().search(
        filter_activity="any_activity",
        filter_time_min=date_from,
        filter_time_max=date_to,
        cm_unit='b',
    ).to_df(columns=needed_columns)

    cargo_movements = cargo_movements.sort_values(
        by="events.cargo_port_load_event.0.end_timestamp",
        ascending=True,
    )
    cargo_movements.loc[:, "events.cargo_port_load_event.0.end_timestamp"] = \
        cargo_movements["events.cargo_port_load_event.0.end_timestamp"].apply(format_time)
    cargo_movements.loc[:, "events.cargo_port_unload_event.0.start_timestamp"] = \
        cargo_movements["events.cargo_port_unload_event.0.start_timestamp"].apply(format_time)
    crude_movements = cargo_movements[cargo_movements["product.group.label"] == "Crude/Condensates"].copy()
    
    workbook = xlsxwriter.Workbook("tracking.xlsx")
    bold = workbook.add_format({'bold': True})

    for key, item in crude_movements[crude_movements["product.grade.label"].isin(needed_labels)].groupby("product.grade.label"):
        worksheet = workbook.add_worksheet(key)
        for col, title in enumerate(xlsx_columns.keys()):
            worksheet.write(0, col, title, bold)
        column_widths = [len(key) for key in xlsx_columns.keys()] + [15]
        row = 1
        for i, item_row in item.iterrows():
            print(item_row["events.cargo_sts_event.0.event_type"])
            ships = list(map(str, [
                item_row["vessels.0.name"],
                item_row["vessels.1.name"],
                item_row["vessels.2.name"],
            ]))
            ships_classes = list(map(str, [
                item_row["vessels.0.vessel_class"],
                item_row["vessels.1.vessel_class"],
                item_row["vessels.2.vessel_class"],
            ]))
            ships_charterers = list(map(str, [
                item_row["vessels.0.corporate_entities.charterer.label"],
                item_row["vessels.1.corporate_entities.charterer.label"],
                item_row["vessels.2.corporate_entities.charterer.label"],
            ]))
            ships = [s for s in ships if s != "nan"]
            ships_classes = [s for s in ships_classes if s != "nan"]
            ships_charterers = [s for s in ships_charterers if s != "nan"] 
            row_to_write = [
                format_value(item_row[xlsx_columns["From"]]),
                format_value(item_row[xlsx_columns["To"]]),
                " → ".join(ships),
                " → ".join(ships_classes),
                " → ".join(ships_charterers),
                format_value(item_row[xlsx_columns["Volume"]]),
                estimated_volume[item_row["vessels.0.vessel_class"]],
                format_value(item_row[xlsx_columns["Destination"]]),
                format_value(item_row["events.cargo_sts_event.0.event_type"]),
            ]
            for i, value in enumerate(row_to_write):
                worksheet.write(row, i, value)
                column_widths[i] = max(column_widths[i], len(str(value)) + 2)
            row += 1
        for i, width in enumerate(column_widths):
            worksheet.set_column(i, i, width)
    workbook.close()
