from csv import DictWriter
import aiofiles
from aiocsv import AsyncDictWriter


def create_csv(path: str, column_names: list[str]) -> None:
    """Replaces file at path with new csv."""
    with open(path, 'w') as f:
        dictwriter = DictWriter(f, fieldnames=column_names)
        dictwriter.writeheader()


def write_csv_row(path: str, record: dict) -> None:
    """Appends a record to the csv."""
    with open(path, 'a') as f:
        dictwriter = DictWriter(f, fieldnames=record.keys())
        dictwriter.writerow(record)
        
        
def write_csv_rows(path: str, records: list[dict]) -> None:
    """Appends a record to the csv."""
    with open(path, 'a') as f:
        dictwriter = DictWriter(f, fieldnames=records[0].keys())
        dictwriter.writerows(records)
        
        
async def async_create_csv(path: str, column_names: list[str]) -> None:
    """Replaces file at path with new csv."""
    async with aiofiles.open(path, 'w') as f:
        dictwriter = AsyncDictWriter(f, fieldnames=column_names)
        await dictwriter.writeheader()


async def async_write_csv_row(path: str, record: dict) -> None:
    """Appends a record to the csv."""
    async with aiofiles.open(path, 'a') as f:
        dictwriter = AsyncDictWriter(f, fieldnames=record.keys())
        await dictwriter.writerow(record)
        
        
async def async_write_csv_rows(path: str, records: list[dict]) -> None:
    """Appends a record to the csv."""
    async with aiofiles.open(path, 'a') as f:
        dictwriter = AsyncDictWriter(f, fieldnames=records[0].keys())
        await dictwriter.writerows(records)