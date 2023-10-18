import os
import csv
import random
from s00_create_resources import Transaction

DATA_PATH = os.getenv("DATA_PATH", "../.external/data")


def read(file_path: str):
    lines = []
    with open(file_path) as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            lines.append(row)
    return lines


def write(items: list, file_path: str):
    with open(file_path, "w") as f:
        writer = csv.writer(f)
        for item in items:
            writer.writerow(item)


if __name__ == "__main__":
    items = []
    trans = read(os.path.join(DATA_PATH, "transactions.csv"))
    items.append(trans[0])
    items = items + random.sample(trans[1 : len(trans)], int(len(trans) / 10))
    write(items, os.path.join(DATA_PATH, "transactions-sample.csv"))
