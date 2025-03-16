import sqlite3
import zstandard as zstd
import io
from front_month_validator import FrontMonthValidator


def decompress_to_sqlite(zst_file, db_file):
    """
    Decompress a .zst file and load front-month NQ futures data into an SQLite database,
    using the FrontMonthValidator to check if a record is for the front-month contract.
    """
    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    c.execute('''
        CREATE TABLE IF NOT EXISTS data (
            ts_event TEXT,
            rtype INTEGER,
            publisher_id INTEGER,
            instrument_id INTEGER,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            symbol TEXT
        )
    ''')

    insert_sql = """
        INSERT INTO data (
            ts_event, rtype, publisher_id, instrument_id,
            open, high, low, close, volume, symbol
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    with open(zst_file, 'rb') as f:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(f) as reader:
            text_stream = io.TextIOWrapper(reader, encoding='utf-8')

            # If CSV has a header line, skip it:
            text_stream.readline()

            for line in text_stream:
                line = line.strip()
                if not line:
                    continue

                # Ensure we have exactly 10 comma-separated fields
                parts = line.split(',')
                if len(parts) != 10:
                    print("Skipping line with unexpected number of columns:", line)
                    continue

                # Use FrontMonthValidator to check the row
                validator = FrontMonthValidator(line)
                if not validator.is_valid_front_month():
                    continue

                try:
                    dt = parts[0]
                    rtype = int(parts[1])
                    publisher_id = int(parts[2])
                    instrument_id = int(parts[3])
                    open_price = float(parts[4])
                    high_price = float(parts[5])
                    low_price = float(parts[6])
                    close_price = float(parts[7])
                    volume = int(parts[8])
                    symbol = parts[9]

                    c.execute(insert_sql, (
                        dt, rtype, publisher_id, instrument_id,
                        open_price, high_price, low_price, close_price,
                        volume, symbol
                    ))
                except Exception as e:
                    print(f"Error processing line: {line}, Error: {e}")

    conn.commit()
    conn.close()
    print("Front-month data loaded into SQLite database successfully.")


if __name__ == '__main__':
    # Modify the paths and db file as needed
    decompress_to_sqlite('nq-csv/data.zst', 'data.db')
