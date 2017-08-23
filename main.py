import os
from functools import partial
from multiprocessing import Pool, Manager
from collections import defaultdict
from db import Database
from constants import (META_INDEX, COST_INDEX, OBJECT_TYPES, CSV_FILES_PATH,
                        ZIP_FILES_PATH, URLS, PROCESSES)
from utils import get_filename_from_url, download_zip_file, get_csv_file_data



def process_csv_files():
    # shared dict between processes
    result_dict = manager.dict()
    csv_files = []
    for _file in os.listdir(CSV_FILES_PATH):
        if _file.endswith(".csv"):
            csv_path = os.path.join(CSV_FILES_PATH, _file)
            csv_files.append(csv_path)
    pool.map(partial(process_csv_file_data, result_dict=result_dict), csv_files)
    with Database() as db:
        db.save_billing_data(result_dict)


def process_urls():
    urls = []
    if not os.path.exists(ZIP_FILES_PATH):
        os.makedirs(ZIP_FILES_PATH)
    for url in URLS:
        file_name = get_filename_from_url(url)
        if os.path.exists(os.path.join(ZIP_FILES_PATH, file_name)):
            continue
        urls.append(url)
    if urls:
        pool.map(download_zip_file, urls)


def process_csv_file_data(csv_file_path, result_dict):
    try:
        collected_info = defaultdict(float)
        iter_rows = iter(get_csv_file_data(csv_file_path))
        column_names = next(iter_rows)
        for row in iter_rows:
            meta = row[META_INDEX]
            cost = row[COST_INDEX]
            object_id = meta.split(':')[1:]
            try:
                cost = float(cost)
            except ValueError as e:
                continue
            if not meta or not cost or len(object_id) != 4:
                continue
            for el in zip(OBJECT_TYPES, object_id):
                if el[1]:
                    collected_info[el] += cost
        print("Processed csv file")
    except Exception as e:
        print('Problem with processing csv file', e)
    join_collected_info(result_dict, collected_info)


def join_collected_info(result_dict, collected_info):
    if len(result_dict) == 0:
        result_dict.update(collected_info)
    else:
        temp_dict = sum_merge_collected_info(result_dict, collected_info)
        result_dict.clear()
        result_dict.update(temp_dict)
    print("Merged collected info")


def sum_merge_collected_info(x, y):
    return {k: x.get(k, 0) + y.get(k, 0) for k in set(x.keys()) | set(y.keys())}


if __name__ == '__main__':
    pool = Pool(PROCESSES)
    manager = Manager()
    process_urls()
    process_csv_files()
    pool.close()
    pool.join()