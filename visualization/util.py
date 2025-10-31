import os
from dotenv import load_dotenv

load_dotenv()
load_dotenv('../.env')

VISUALIZATION_RESULTS_DIR = os.getenv('VISUALIZATION_RESULTS_DIR')


def initialize_directory(base_directory_name):
    base_dir = os.path.join(VISUALIZATION_RESULTS_DIR, base_directory_name)
    os.makedirs(base_dir, exist_ok=True)

    for filename in os.listdir(base_dir):
        item_path = os.path.join(base_dir, filename)
        if os.path.isfile(item_path):
            os.remove(item_path)
