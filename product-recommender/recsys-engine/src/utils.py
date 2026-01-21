import csv
import logging
from pathlib import Path
from typing import List, Dict, Union

from src.logger_config import setup_logger

setup_logger()
logger = logging.getLogger(__name__)


def generate_from_csv(file_name: Union[str, Path]) -> List[dict]:
    records = []
    with open(file_name, encoding="utf-8") as csv_file:
        csvReader = csv.DictReader(csv_file)
        for rows in csvReader:
            records.append(rows)
    return records


def write_to_csv(objs: List[Dict], file_name: Union[str, Path], excls: List[str] = []):
    if not objs:
        return
    fieldnames = [k for k in objs[0].keys() if k not in excls]
    with open(file_name, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for obj in objs:
            writer.writerow({k: v for k, v in obj.items() if k not in excls})


def get_product_map(file_name: Union[str, Path]):
    prod_map = {}
    product_id = 0
    for product in generate_from_csv(file_name):
        prod_map[product_id] = {
            "name": product["name"],
            "description": product["description"],
            "price": product["price"],
            "category": product["category"],
        }
        product_id += 1
    return prod_map
