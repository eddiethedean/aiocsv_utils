import typing as _typing

import aiofiles as _aiofiles
from aiofiles.threadpool.text import AsyncTextIOWrapper as _AsyncTextIOWrapper
import aiocsv as _aiocsv
from aioitertools.more_itertools import chunked as _chunked

from .convert import convert_str as _convert_str


async def csv_file_headers(
    async_file: _AsyncTextIOWrapper,
    delimiter=','
) -> list[str]:
    """Asynchronously read the header names of a csv file.
    
    Parameters
    ----------
    async_file : AsyncTextIOWrapper
        Async file object from aiofiles.read
    delimiter : str, optional
        CSV file delimiter character.

    Returns
    -------
    list[str]
        A list of the CSV column headers.
        
    Example
    -------
    >>> import asyncio
    >>> import aiofiles
    >>>
    >>> from aiocsv_utils.read import csv_file_headers
    >>>
    >>> async def get_headers(path: str) -> list[str]:
    >>>     async with aiofiles.open(path, mode='r', encoding='utf-8', newline='') as f:
    >>>         return csv_file_headers(f)
    >>>
    >>> asyncio.run(get_headers('data/cities.csv'))
    ['LatD', 'LatM', 'LatS', 'NS', 'LonD', 'LonM', 'LonS', 'EW', 'City', 'State']
    """
    reader = _aiocsv.AsyncReader(async_file, delimiter=delimiter)
    return await anext(reader)
        

async def csv_headers(
    path: str,
    mode='r',
    encoding='utf-8',
    newline='',
    delimiter=','
) -> list[str]:
    """Asynchronously read the header names of a csv by path.
    
    Parameters
    ----------
    path : str
        File path to CSV file.
    mode : str, optional
        Mode while opening a file.
    encoding : str, optional
        The encoding format.
    newline : str, optional
        How newlines mode works.
    delimiter : str, optional
        CSV file delimiter character.

    Returns
    -------
    list[str]
        A list of the CSV column headers.
        
    Raises
    ------
    FileNotFoundError
        If file does not exist.
        
    Example
    -------
    >>> import asyncio
    >>>
    >>> from aiocsv_utils.read import csv_headers
    >>>
    >>> asyncio.run(csv_headers('data/cities.csv'))
    ['LatD', 'LatM', 'LatS', 'NS', 'LonD', 'LonM', 'LonS', 'EW', 'City', 'State']
    """
    async with _aiofiles.open(path, mode=mode, buffering=-1, encoding=encoding, errors=None, newline=newline) as afp:
        return await csv_file_headers(afp, delimiter)


async def csv_file_to_records(
    async_file: _AsyncTextIOWrapper,
    delimiter=','
) -> _typing.AsyncGenerator[dict[str, _typing.Any], None]:
    """Asynchronously read a CSV file and async yield each record.
    
    Parameters
    ----------
    async_file : AsyncTextIOWrapper
        Async file object from aiofiles.read
    delimiter : str, optional
        CSV file delimiter character.

    Returns
    -------
    AsyncGenerator[dict[str, Any]
        An async generator that yields each CSV row as a dict.
        
    Example
    -------
    >>> import asyncio
    >>> import aiofiles
    >>>
    >>> from aiocsv_utils.read import csv_file_to_records
    >>>
    >>> async def read_first_record() -> list[dict] | None:
    >>>     async with aiofiles.open(path, mode='r', encoding='utf-8', newline='') as f:
    >>>         async for row in csv_file_to_records(f):
    >>>             return row
    >>>
    >>> asyncio.run(read_first_record())
    {'LatD': 41, 'LatM': 5, 'LatS': 59, 'NS': 'N', 'LonD': 80, 'LonM': 39,
     'LonS': 0, 'EW': 'W', 'City': 'Youngstown', 'State': 'OH'}
    """
    async for row in _aiocsv.AsyncDictReader(async_file, delimiter=delimiter):
        yield {col: _convert_str(val) for col, val in row.items()}
            

async def csv_to_records(
    path: str,
    mode='r',
    encoding='utf-8',
    newline='',
    delimiter=','
) -> _typing.AsyncGenerator[dict[str, _typing.Any], None]:
    """Asynchronously read a CSV by path and async yield each record.
    
    Parameters
    ----------
    path : str
        File path to CSV file.
    mode : str, optional
        Mode while opening a file.
    encoding : str, optional
        The encoding format.
    newline : str, optional
        How newlines mode works.
    delimiter : str, optional
        CSV file delimiter character.

    Returns
    -------
    AsyncGenerator[dict[str, Any]
        An async generator that yields each CSV row as a dict.
        
    Raises
    ------
    FileNotFoundError
        If file does not exist.
        
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
        async for row in csv_file_to_records(afp, delimiter):
            yield row
            

async def csv_file_to_records_chunks(
    async_file: _AsyncTextIOWrapper,
    chunk_size: int,
    delimiter=','
) -> _typing.AsyncGenerator[list[dict[str, _typing.Any]], None]:
    """Asynchronously read a CSV file and async yield chunks of records.
    
    Parameters
    ----------
    async_file : AsyncTextIOWrapper
        Async file object from aiofiles.read
    delimiter : str, optional
        CSV file delimiter character.

    Returns
    -------
    AsyncGenerator[list[dict[str, Any]]]
        An async generator that yields chunks of CSV rows as lists of dicts.
        
    Example
    -------
    >>> import asyncio
    >>>
    >>> from aiocsv_utils.read import csv_file_to_records_chunks
    >>>
    >>> async def read_first_two_record() -> dict | None:
    >>>     async with aiofiles.open(path, mode='r', encoding='utf-8', newline='') as f:
    >>>         async for chunk in csv_file_to_records_chunks(f, 2):
    >>>             return chunk
    >>>
    >>> asyncio.run(read_first_two_record())
    [{'LatD': 41, 'LatM': 5, 'LatS': 59, 'NS': 'N', 'LonD': 80, 'LonM': 39,
    'LonS': 0, 'EW': 'W', 'City': 'Youngstown', 'State': 'OH'},
    {'LatD': 42, 'LatM': 52, 'LatS': 48, 'NS': 'N', 'LonD': 97, 'LonM': 23,
    'LonS': 23, 'EW': 'W', 'City': 'Yankton', 'State': 'SD'}]
    """
    records = csv_file_to_records(async_file, delimiter)
    async for chunk in _chunked(records, chunk_size):
        yield chunk
        
async def csv_to_records_chunks(
    path: str,
    chunk_size: int,
    mode='r',
    encoding='utf-8',
    newline='',
    delimiter=','
) -> _typing.AsyncGenerator[list[dict[str, _typing.Any]], None]:
    """Asynchronously read a CSV by path and async yield chunks of records.
    
    Parameters
    ----------
    path : str
        File path to CSV file.
    mode : str, optional
        Mode while opening a file.
    encoding : str, optional
        The encoding format.
    newline : str, optional
        How newlines mode works.
    delimiter : str, optional
        CSV file delimiter character.

    Returns
    -------
    AsyncGenerator[list[dict[str, Any]]]
        An async generator that yields chunks of CSV rows as lists of dicts.
    
    Raises
    ------
    FileNotFoundError
        If file does not exist.
        
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
    async with _aiofiles.open(path, mode=mode, encoding=encoding, newline=newline) as f:
        async for chunk in csv_file_to_records_chunks(f, chunk_size, delimiter):
            yield chunk