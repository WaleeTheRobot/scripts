import sqlite3
import zstandard as zstd
import io
from front_month_validator import FrontMonthValidator
from dateutil.parser import isoparse
import pytz

def decompress_to_sqlite(zst_file, db_file):
    """
    Decompress DataBento's futures OHLCV .zst file and load front-month NQ futures data into an SQLite database,
    using the FrontMonthValidator to check if a record is for the front-month contract.
    Only selected fields (timestamp, open, high, low, close, volume, symbol) are written into the database.
    The timestamp is converted from UTC to Eastern Time (ET) with the correct offset for EST/EDT.
    
    Args:
        zst_file (str): Path to the .zst compressed file.
        db_file (str): Path to the SQLite database file.
    """
    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    # Create table if it doesn't exist
    c.execute('''
        CREATE TABLE IF NOT EXISTS data (
            timestamp TEXT,
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
            timestamp, open, high, low, close, volume, symbol
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    # Define Eastern Time zone (handles EST/EDT automatically)
    eastern = pytz.timezone('US/Eastern')

    # Open and decompress the .zst file
    with open(zst_file, 'rb') as f:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(f) as reader:
            text_stream = io.TextIOWrapper(reader, encoding='utf-8')

            # Skip the header line
            text_stream.readline()

            # Process each line
            for line in text_stream:
                line = line.strip()
                if not line:
                    continue

                # Split line into fields and verify it has exactly 10 columns
                parts = line.split(',')
                if len(parts) != 10:
                    print("Skipping line with unexpected number of columns:", line)
                    continue

                # Filter for front-month contracts
                validator = FrontMonthValidator(line)
                if not validator.is_valid_front_month():
                    continue

                try:
                    dt = parts[0]
                    open_price = float(parts[4])
                    high_price = float(parts[5])
                    low_price = float(parts[6])
                    close_price = float(parts[7])
                    volume = int(parts[8])
                    symbol = parts[9]

                    # Parse UTC timestamp
                    dt_utc = isoparse(dt)
                    # Convert to Eastern Time
                    dt_eastern = dt_utc.astimezone(eastern)
                    # Format with correct offset (e.g., -0500 or -0400)
                    dt_eastern_str = dt_eastern.strftime("%Y-%m-%dT%H:%M:%S.%f")[:26] + dt_eastern.strftime("%z")

                    c.execute(insert_sql, (
                        dt_eastern_str, open_price, high_price, low_price,
                        close_price, volume, symbol
                    ))
                except Exception as e:
                    print(f"Error processing line: {line}, Error: {e}")

    conn.commit()
    conn.close()
    print("Front-month data loaded into SQLite database successfully.")

if __name__ == '__main__':
    decompress_to_sqlite('nq-csv/data.zst', 'data.db')
