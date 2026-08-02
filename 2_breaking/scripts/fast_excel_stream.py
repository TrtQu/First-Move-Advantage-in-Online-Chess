
import csv

import io

import ssl

import time

import urllib.request

from pathlib import Path



import zstandard as zstd



URL = "https://database.lichess.org/standard/lichess_db_standard_rated_2026-06.pgn.zst"

OUTPUT = Path("excel_over_limit.csv")

LIMIT = 10_050_000 



start = time.perf_counter()



request = urllib.request.Request(

    URL,

    headers={"User-Agent": "Mozilla/5.0"},

)



ssl_context = ssl._create_unverified_context()



print("Connecting to Lichess...", flush=True)



with urllib.request.urlopen(

    request,

    timeout=120,

    context=ssl_context,

) as response:



    decompressor = zstd.ZstdDecompressor()



    with decompressor.stream_reader(response) as binary_stream:

        text_stream = io.TextIOWrapper(

            binary_stream,

            encoding="utf-8",

            errors="replace",

            newline="",

        )



        with OUTPUT.open(

            "w",

            encoding="utf-8",

            newline="",

        ) as destination:



            writer = csv.writer(destination)

            writer.writerow(

                ["source_line_number", "raw_pgn_preview"]

            )



            count = 0



            for count, line in enumerate(text_stream, start=1):

                writer.writerow(

                    [

                        count,

                        line.rstrip("\r\n")[:200],

                    ]

                )



                if count % 100_000 == 0:

                    elapsed = time.perf_counter() - start

                    print(

                        f"Wrote {count:,} rows in "

                        f"{elapsed:.1f} seconds",

                        flush=True,

                    )



                if count >= LIMIT:

                    break



elapsed = time.perf_counter() - start



print()

print(f"Data rows written: {count:,}")

print(f"Total CSV rows: {count + 1:,}")

print(f"Elapsed time: {elapsed:.2f} seconds")

print(f"Output: {OUTPUT.resolve()}")

