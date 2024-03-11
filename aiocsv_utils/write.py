from typing import Optional, Sequence

import aiofiles
from aiocsv import AsyncDictWriter
        
        
async def create_csv(
    path: str,
    headers: Sequence[str]
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
    async with aiofiles.open(path, 'w') as f:
        dictwriter = AsyncDictWriter(f, fieldnames=headers)
        await dictwriter.writeheader()


async def write_csv_row(
    path: str,
    record: dict,
    headers: Sequence[str]
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
    async with aiofiles.open(path, 'a') as f:
        dictwriter = AsyncDictWriter(f, fieldnames=headers)
        await dictwriter.writerow(record)
        
        
async def write_csv_rows(
    path: str,
    records: list[dict],
    headers: Sequence[str]
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
    async with aiofiles.open(path, 'a') as f:
        dictwriter = AsyncDictWriter(f, fieldnames=headers)
        await dictwriter.writerows(records)