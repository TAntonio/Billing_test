import os
import csv
import shutil
import zipfile
from urllib import request
from urllib.parse import urlsplit
from constants import ZIP_FILES_PATH, CSV_FILES_PATH


def download_zip_file(url):
    try:
        filename = get_filename_from_url(url)
        with request.urlopen(url) as response:
            output_path = os.path.join(ZIP_FILES_PATH, filename)
            with open(output_path, 'wb') as out_file:
                shutil.copyfileobj(response, out_file)
        print("Downloaded zip file")
    except Exception as e:
        print("Problem with downloading zip file", e)
    decompress_zip_file(output_path)
    return output_path


def decompress_zip_file(zip_path):
    if not os.path.exists(CSV_FILES_PATH):
        os.makedirs(CSV_FILES_PATH)
    with zipfile.ZipFile(zip_path, "r") as zip_file:
        for filename in zip_file.namelist():
            if not os.path.exists(os.path.join(CSV_FILES_PATH, filename)):
                zip_file.extract(filename, CSV_FILES_PATH)
    print("Decompressed zip file")


def get_csv_file_data(csv_file_path):
    with open(csv_file_path, "r") as csv_file:
        csv_data = csv.reader(csv_file)
        for row in csv_data:
            yield row


def get_filename_from_url(url):
    return urlsplit(url).path.split('/')[-1]
