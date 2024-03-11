import typing as _typing

import aiofiles as _aiofiles
from aiofiles.threadpool.text import AsyncTextIOWrapper as _AsyncTextIOWrapper
from aiocsv import AsyncDictWriter as _AsyncDictWriter


async def create_csv(
    path: str,
    headers: _typing.Sequence[str]
) -> None:
    """Replace file at path with new empty csv.
    
    Example
    -------
    >>> import asyncio
    >>>
    >>> from aiocsv_utils.write import create_csv
    >>>
    >>> asyncio.run(create_csv('data/people.csv', ['id', 'name', 'age', 'ssn']))
    >>>
    >>> with open('cities.csv', mode='r', encoding='utf-8', newline='') as f:
    >>>     print(f.read())
    id,name,age,ssn
    """
    async with _aiofiles.open(path, 'w') as f:
        dictwriter = _AsyncDictWriter(f, fieldnames=headers)
        await dictwriter.writeheader()


async def write_csv_file_row(
    async_file: _AsyncTextIOWrapper,
    record: dict,
    headers: _typing.Sequence[str]
) -> None:
    """Appends a record to a csv file.
    
    Example
    -------
    >>> import asyncio
    >>> import aiofiles
    >>>
    >>> from aiocsv_utils.write import write_csv_file_row
    >>>
    >>> record = {'id': 1, 'name': 'John', 'age': 30, 'ssn': '111-22-3333'}
    >>> headers = ['id', 'name', 'age', 'ssn']
    >>> 
    >>> async def write_line(path, record, headers) -> None:
    >>>     async with aiofiles.open(path, 'w') as f:
    >>>         write_csv_file_row(f, record, headers)
    >>>
    >>> asyncio.run(write_line('data/people.csv', record, headers))
    >>>
    >>> with open('cities.csv', mode='r', encoding='utf-8', newline='') as f:
    >>>     print(f.read())
    id,name,age,ssn
    1,John,30,111-22-3333
    """
    dictwriter = _AsyncDictWriter(async_file, fieldnames=headers)
    await dictwriter.writerow(record)
        

async def write_csv_row(
    path: str,
    record: dict,
    headers: _typing.Sequence[str]
) -> None:
    """Appends a record to a csv file.
    
    Example
    -------
    >>> import asyncio
    >>>
    >>> from aiocsv_utils.write import write_csv_row
    >>>
    >>> record = {'id': 1, 'name': 'John', 'age': 30, 'ssn': '111-22-3333'}
    >>> headers = ['id', 'name', 'age', 'ssn']
    >>> asyncio.run(write_csv_row('data/people.csv', record, headers))
    >>>
    >>> with open('cities.csv', mode='r', encoding='utf-8', newline='') as f:
    >>>     print(f.read())
    id,name,age,ssn
    1,John,30,111-22-3333
    """
    async with _aiofiles.open(path, 'a') as f:
        await write_csv_file_row(f, record, headers)
        
        
async def write_csv_rows(
    path: str,
    records: list[dict],
    headers: _typing.Sequence[str]
) -> None:
    """Appends a list of records to a csv file.
    
    Example
    -------
    >>> import asyncio
    >>>
    >>> from aiocsv_utils.write import write_csv_row
    >>>
    >>> records = [{'id': 2, 'name': 'Jane', 'age': 25, 'ssn': '222-33-4444'},
    >>>            {'id': 3, 'name': 'Mike', 'age': 47, 'ssn': '000-11-2222'}]
    >>> headers = ['id', 'name', 'age', 'ssn']
    >>> asyncio.run(write_csv_row('data/people.csv', records, headers))
    >>>
    >>> with open('cities.csv', mode='r', encoding='utf-8', newline='') as f:
    >>>     print(f.read())
    id,name,age,ssn
    1,John,30,111-22-3333
    2,Jane,25,222-33-4444
    3,Mike,47,000-11-2222
    """
    async with _aiofiles.open(path, 'a') as f:
        dictwriter = _AsyncDictWriter(f, fieldnames=headers)
        await dictwriter.writerows(records)