

BASE_URL = 'https://s3.amazonaws.com/detailed-billing-test/615271354814-aws-billing-detailed-line-items-with-resources-and-tags-{}.csv.zip'
URLS = [BASE_URL.format('2016-05'), BASE_URL.format('2016-06'), BASE_URL.format('2016-07'), BASE_URL.format('2016-08')]
ZIP_FILES_PATH = 'zips_folder'
CSV_FILES_PATH = 'csv_files'
OBJECT_TYPES = ('env', 'farm', 'farm_role', 'server')
META_INDEX = 20
COST_INDEX = 18
PROCESSES = 5
