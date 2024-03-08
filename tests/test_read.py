import pytest

from aiocsv_utils.src.read import csv_to_df


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