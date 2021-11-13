import glob
import os
import shutil
from datetime import datetime

import pandas
import pandas as pd
import yaml
from sqlalchemy import create_engine


def find_files(input_dir: str, file_formats: list):
    result = []
    for file_format in file_formats:
        result += glob.glob(f'{input_dir}/*.{file_format}')
    return result


def parse_excel(file_path: str, columns: str):
    excel = pd.read_excel(file_path, header=None, keep_default_na=True, sheet_name=None, usecols=columns)
    return list(map(lambda sheet_name: excel[sheet_name], excel))


def prepare_sheets(sheets: list, first_row_index: int, sorted_columns: list):
    result = []
    sheet: pandas.DataFrame
    for sheet in sheets:
        sheet = sheet[first_row_index:].dropna().rename(lambda x: sorted_columns[x - 1], axis='columns')
        result.append(sheet)
    return result


def send_sheets(sheets: list, engine, table: str):
    for sheet in sheets:
        sheet.to_sql(table, con=engine, index=False, if_exists='append')


def get_sorted_columns(mapping: dict):
    return list(map(lambda k: mapping[k], sorted(mapping.keys())))


def read_config(path: str):
    file = open(path, 'r')
    return yaml.load(file, Loader=yaml.FullLoader)


def main():
    config = read_config('./configs/config.yaml')
    input_files = find_files(config['input_dir'], config['file_formats'])
    error_dir = config['error_dir']
    done_dir = config['done_dir']
    sorted_columns = get_sorted_columns(config['markup']['mapping'])
    columns = ','.join(config['markup']['mapping'].keys())
    first_row_index = config['markup']['first_row_index']
    engine = create_engine(config['db']['connection'])
    table = config['db']['table']

    os.makedirs(config['input_dir'], exist_ok=True)

    for file in input_files:
        try:
            sheets = parse_excel(file, columns)
            prepared_sheets = prepare_sheets(sheets, first_row_index, sorted_columns)
            send_sheets(prepared_sheets, engine, table)
            os.makedirs(done_dir, exist_ok=True)
            shutil.move(file, done_dir + datetime.now().strftime("%d-%m-%Y_%H:%M:%S") + '_' + os.path.basename(file))
        except Exception as e:
            print(e)
            os.makedirs(error_dir, exist_ok=True)
            filename = datetime.now().strftime("%d-%m-%Y_%H:%M:%S") + '_' + os.path.basename(file)
            shutil.move(file, error_dir + filename)
            log_file = open(error_dir + filename + '.log', 'w')
            log_file.write(str(e))
            log_file.close()


if __name__ == '__main__':
    main()
