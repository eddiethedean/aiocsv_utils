import typing as _typing

import aiofiles as _aiofiles
import aiocsv as _aiocsv
from aioitertools.more_itertools import chunked as _chunked

from .convert import convert_str as _convert_str


async def csv_headers(
    path: str,
    mode='r',
    encoding='utf-8',
    newline='',
    delimiter=','
) -> list[str]:
    """Asynchronously read the header names of a csv file.
    
    Example
    -------
    >>> import asyncio
    >>>
    >>> from aiocsv_utils.read import csv_headers
    >>>
    >>> asyncio.run(csv_headers('data/cities.csv'))
    ['LatD', 'LatM', 'LatS', 'NS', 'LonD', 'LonM', 'LonS', 'EW', 'City', 'State']
    """
    async with _aiofiles.open(path, mode=mode, encoding=encoding, newline=newline) as afp:
        reader = _aiocsv.AsyncReader(afp, delimiter=delimiter)
        return await anext(reader)


async def csv_to_records(
    path: str,
    mode='r',
    encoding='utf-8',
    newline='',
    delimiter=','
) -> _typing.AsyncGenerator[dict[str, _typing.Any], None]:
    """Asynchronously read a csv and async yield each record.
    
    Example
    -------
    >>> import asyncio
    >>>
    >>> from aiocsv_utils.read import csv_to_records
    >>>
    >>> async def read_first_record() -> list[dict] | None:
    >>>     async for row in csv_to_records('data/cities.csv'):
    >>>         return row
    >>>
    >>> asyncio.run(read_first_record())
    {'LatD': 41, 'LatM': 5, 'LatS': 59, 'NS': 'N', 'LonD': 80, 'LonM': 39,
     'LonS': 0, 'EW': 'W', 'City': 'Youngstown', 'State': 'OH'}
    """
    async with _aiofiles.open(path, mode=mode, encoding=encoding, newline=newline) as afp:
        async for row in _aiocsv.AsyncDictReader(afp, delimiter=delimiter):
            yield {col: _convert_str(val) for col, val in row.items()}
            
            
async def csv_to_records_chunks(
    path: str,
    chunk_size: int,
    mode='r',
    encoding='utf-8',
    newline='',
    delimiter=','
) -> _typing.AsyncGenerator[list[dict[str, _typing.Any]], None]:
    """Asynchronously read a csv and async yield chunks of records.
    
    Example
    -------
    >>> import asyncio
    >>>
    >>> from aiocsv_utils.read import csv_to_records_chunks
    >>>
    >>> async def read_first_two_record() -> dict | None:
    >>>     async for chunk in csv_to_records_chunks('data/cities.csv', 2):
    >>>         return chunk
    >>>
    >>> asyncio.run(read_first_two_record())
    [{'LatD': 41, 'LatM': 5, 'LatS': 59, 'NS': 'N', 'LonD': 80, 'LonM': 39,
    'LonS': 0, 'EW': 'W', 'City': 'Youngstown', 'State': 'OH'},
    {'LatD': 42, 'LatM': 52, 'LatS': 48, 'NS': 'N', 'LonD': 97, 'LonM': 23,
    'LonS': 23, 'EW': 'W', 'City': 'Yankton', 'State': 'SD'}]
    """
    records = csv_to_records(path, mode, encoding, newline, delimiter)
    async for chunk in _chunked(records, chunk_size):
        yield chunk