import pytest
import pandas as pd

from aiocsv_utils.read import csv_to_records, csv_to_records_chunks
from aiocsv_utils.read import csv_to_df, csv_to_df_chunks


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
        
    
@pytest.mark.asyncio
async def test_csv_to_df() -> None:
    df = await csv_to_df("data/cities.csv")
    expected = {'LatD': 41, 'LatM': 5, 'LatS': 59,
            'NS': 'N', 'LonD': 80, 'LonM': 39,
            'LonS': 0, 'EW': 'W',
            'City': 'Youngstown', 'State': 'OH'}
    assert dict(df.iloc[0]) == expected


@pytest.mark.asyncio
async def test_empty_csv_to_df() -> None:
    df = await csv_to_df("data/cities_empty.csv")
    assert len(df) == 0
    assert pd.DataFrame({
        'LatD': [], 'LatM': [],
        'LatS': [], 'NS': [],
        'LonD': [], 'LonM': [],
        'LonS': [], 'EW': [],
        'City': [],
        'State': []}).equals(df)
    
    
@pytest.mark.asyncio
async def test_csv_to_df_chunks() -> None:
    dfs = csv_to_df_chunks("data/cities.csv", 2)
    expected = pd.DataFrame(
        {
            'LatD': [41, 42], 'LatM': [5, 52],
            'LatS': [59, 48], 'NS': ['N', 'N'],
            'LonD': [80, 97], 'LonM': [39, 23],
            'LonS': [0, 23], 'EW': ['W', 'W'],
            'City': ['Youngstown', 'Yankton'],
            'State': ['OH', 'SD']
        }
    )
    df = await anext(dfs)
    assert df.equals(expected)
    
    
@pytest.mark.asyncio
async def test_empty_csv_to_df_chunks() -> None:
    dfs = csv_to_df_chunks("data/cities_empty.csv", 2)
    with pytest.raises(StopAsyncIteration) as e_info:
        df = await anext(dfs)