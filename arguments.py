import argparse
import os

def args_reader():
    parser = argparse.ArgumentParser(description='Process some paths.')

    parser.add_argument (
        "-module",
        dest='module',
        default="./lab100_json_shipment/jsonShipmentDisplayApp.py",
        help="Папка, из которой запустить пример кода.",
        type=str
    )

    parser.add_argument (
        "-datapath",
        dest='datapath',
        default="./data/json/shipment.json",
        help="Папка, из которой загрузить данные.",
        type=str
    )

    parser.add_argument (
        "-datapath1",
        dest='datapath1',
        default="./data/dapip/InstitutionCampus.csv",
        help="Папка, из которой загрузить данные.",
        type=str
    )

    parser.add_argument (
        "-datapath2",
        dest='datapath2',
        default="./data/hud/COUNTY_ZIP_092018.csv",
        help="Папка, из которой загрузить данные.",
        type=str
    )

    return parser.parse_args()

def get_absolute_file_path(path, filename):
    current_dir = os.path.dirname(__file__)
    relative_path = f"{path}{filename}"
    absolute_file_path = os.path.join(current_dir, relative_path)
    return absolute_file_path
