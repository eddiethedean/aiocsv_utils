import pytest
from aiofiles.tempfile import NamedTemporaryFile

from aiocsv_utils.write import create_csv
from aiocsv_utils.write import write_csv_row, write_csv_rows


@pytest.mark.asyncio
async def test_create_csv():
    async with NamedTemporaryFile('w') as tmp:
        await create_csv(tmp.name, ['id', 'name', 'age', 'ssn'])
        with open(tmp.name, 'r') as f:
            results = f.read()
    assert results == 'id,name,age,ssn\n'
    
    
@pytest.mark.asyncio
async def test_write_csv_row():
    record = {'id': 1, 'name': 'John', 'age': 30, 'ssn': '111-22-3333'}
    headers = record.keys()
    async with NamedTemporaryFile('w') as tmp:
        await write_csv_row(tmp.name, record, headers)
        with open(tmp.name, 'r') as f:
            results = f.read()
    assert results == "1,John,30,111-22-3333\n"
    
    
@pytest.mark.asyncio
async def test_write_csv_rows():
    records = [{'id': 2, 'name': 'Jane', 'age': 25, 'ssn': '222-33-4444'},
               {'id': 3, 'name': 'Mike', 'age': 47, 'ssn': '000-11-2222'}]
    headers = records[0].keys()
    async with NamedTemporaryFile('w') as tmp:
        await write_csv_rows(tmp.name, records, headers)
        with open(tmp.name, 'r') as f:
            results = f.read()
    assert results == "2,Jane,25,222-33-4444\n3,Mike,47,000-11-2222\n"
    
    
@pytest.mark.asyncio
async def test_write_csv_row_to_headers():
    headers = ['id', 'name', 'age', 'ssn']
    record = {'age': 30, 'id': 1, 'name': 'John', 'ssn': '111-22-3333'}
    async with NamedTemporaryFile('w') as tmp:
        await create_csv(tmp.name, headers)
        await write_csv_row(tmp.name, record, headers)
        with open(tmp.name, 'r') as f:
            results = f.read()
    assert results == "id,name,age,ssn\n1,John,30,111-22-3333\n"
    

@pytest.mark.asyncio
async def test_write_csv_rows_to_headers():
    headers = ['id', 'name', 'age', 'ssn']
    records = [{'age': 25, 'id': 2, 'name': 'Jane', 'ssn': '222-33-4444'},
               {'id': 3, 'ssn': '000-11-2222', 'name': 'Mike', 'age': 47, }]
    async with NamedTemporaryFile('w') as tmp:
        await write_csv_rows(tmp.name, records, headers)
        with open(tmp.name, 'r') as f:
            results = f.read()
    assert results == "2,Jane,25,222-33-4444\n3,Mike,47,000-11-2222\n"