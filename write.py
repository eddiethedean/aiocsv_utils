from csv import DictWriter


def create_errors_csv(path: str, column_names: list[str]) -> None:
    """Replaces file at path with new csv."""
    with open(path, 'w') as f:
        dictwriter = DictWriter(f, fieldnames=column_names)
        dictwriter.writeheader()


def write_csv_row(path: str, record: dict) -> None:
    """Appends a record to the csv."""
    with open(path, 'a') as f:
        dictwriter = DictWriter(f, fieldnames=record.keys())
        dictwriter.writerow(record)