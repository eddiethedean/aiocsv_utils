from typing import AsyncGenerator, Iterable, Any

import aiofiles
from aiocsv import AsyncDictReader
import pandas as pd
from aioitertools.more_itertools import chunked

from .convert import convert_str

Record = dict[str, Any]


async def csv_to_records(
    path: str,
    mode="r",
    encoding="utf-8",
    newline="",
    delimiter=','
) -> AsyncGenerator[Record, None]:
    """Asynchronously read a csv and async yield each record.
    
    Example
    -------
    >>> import asyncio
    >>>
    >>> from csv_utils import csv_to_records
    >>>
    >>> async def read_first_record() -> dict | None:
    >>>     async for row in csv_to_records('data/cities.csv'):
    >>>         return row
    >>>
    >>> asyncio.run(read_first_record())
    {'LatD': 41, 'LatM': 5, 'LatS': 59, 'NS': 'N', 'LonD': 80, 'LonM': 39,
     'LonS': 0, 'EW': 'W', 'City': 'Youngstown', 'State': 'OH'}
    """
    async with aiofiles.open(path, mode=mode, encoding=encoding, newline=newline) as afp:
        async for row in AsyncDictReader(afp, delimiter=delimiter):
            yield {col: convert_str(val) for col, val in row.items()}
            
            
async def csv_to_records_chunks(
    path: str,
    chunk_size: int,
    mode="r",
    encoding="utf-8",
    newline="",
    delimiter=','
) -> AsyncGenerator[list[Record], None]:
    records = csv_to_records(path, mode, encoding, newline, delimiter)
    async for chunk in chunked(records, chunk_size):
        yield chunk

            
async def async_records_to_df(records: AsyncGenerator[Record, None]) -> pd.DataFrame:
    return pd.DataFrame([row async for row in records])


def records_to_df(records: Iterable[Record]) -> pd.DataFrame:
    return pd.DataFrame(records)


async def csv_to_df(
    path: str,
    mode="r",
    encoding="utf-8",
    newline="",
    delimiter=','
) -> pd.DataFrame:
    records = csv_to_records(path, mode, encoding, newline, delimiter)
    return await async_records_to_df(records)
        
        
async def csv_to_df_chunks(
    path: str,
    chunk_size: int,
    mode="r",
    encoding="utf-8",
    newline="",
    delimiter=','
) -> AsyncGenerator[pd.DataFrame, None]:
    async for chunk in csv_to_records_chunks(path, chunk_size, mode, encoding, newline, delimiter):
        yield records_to_df(chunk)