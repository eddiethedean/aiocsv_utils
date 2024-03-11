import pytest

from aiocsv_utils.read import csv_headers
from aiocsv_utils.read import csv_to_records
from aiocsv_utils.read import csv_to_records_chunks


@pytest.mark.asyncio
async def test_csv_headers():
    expected = ['LatD', 'LatM', 'LatS', 'NS', 'LonD',
                'LonM', 'LonS', 'EW', 'City', 'State']
    assert await csv_headers('data/cities.csv') == expected
    
    
@pytest.mark.asyncio
async def test_empty_csv_headers():
    expected = ['LatD', 'LatM', 'LatS', 'NS', 'LonD',
                'LonM', 'LonS', 'EW', 'City', 'State']
    assert await csv_headers('data/cities_empty.csv') == expected
    

@pytest.mark.asyncio
async def test_csv_to_records():
    async for row in csv_to_records('data/cities.csv'):
        assert row == {'LatD': 41, 'LatM': 5, 'LatS': 59, 'NS': 'N', 'LonD': 80, 'LonM': 39,
                       'LonS': 0, 'EW': 'W', 'City': 'Youngstown', 'State': 'OH'}
        break
    
    
@pytest.mark.asyncio
async def test_empty_csv_to_records():
    with pytest.raises(StopAsyncIteration) as e_info:
        await anext(csv_to_records('data/cities_empty.csv'))
        
        
@pytest.mark.asyncio
async def test_csv_to_records_chunks():
    async for chunk in csv_to_records_chunks('data/cities.csv', 2):
        assert chunk == [
            {'LatD': 41, 'LatM': 5, 'LatS': 59, 'NS': 'N', 'LonD': 80, 'LonM': 39,
             'LonS': 0, 'EW': 'W', 'City': 'Youngstown', 'State': 'OH'},
            {'LatD': 42, 'LatM': 52, 'LatS': 48, 'NS': 'N', 'LonD': 97, 'LonM': 23,
             'LonS': 23, 'EW': 'W', 'City': 'Yankton', 'State': 'SD'}
        ]
        break


@pytest.mark.asyncio
async def test_empty_csv_to_records_chunks():
    with pytest.raises(StopAsyncIteration) as e_info:
        await anext(csv_to_records_chunks('data/cities_empty.csv', 2))