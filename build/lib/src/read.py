from typing import AsyncGenerator

import aiofiles
import pandas as pd
from aiocsv import AsyncDictReader

from aioitertools import chunked


async def csv_to_df(
    path: str,
    mode="r",
    encoding="utf-8",
    newline="",
    delimiter=','
) -> pd.DataFrame:
    async with aiofiles.open(path, mode=mode, encoding=encoding, newline=newline) as afp:
        rows = [row async for row in AsyncDictReader(afp, delimiter=delimiter)]
        return pd.DataFrame.from_records(rows)
        
        
async def csv_to_df_chunks(
    path: str,
    chunk_size: int,
    mode="r",
    encoding="utf-8",
    newline="",
    delimiter=','
) -> AsyncGenerator[pd.DataFrame, None]:
    async with aiofiles.open(path, mode=mode, encoding=encoding, newline=newline) as afp:
        async for chunk in chunked(AsyncDictReader(afp, delimiter=delimiter), chunk_size):
            yield pd.DataFrame.from_records(chunk)