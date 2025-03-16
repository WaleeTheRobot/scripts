import unittest
from datetime import datetime, date, timedelta
from typing import Optional, Tuple


class FrontMonthValidator:
    """
    A validator class that processes a single record (string), checks if it
    represents a correct front-month futures contract, and stores intermediate
    results.
    """

    MONTH_CODES = {
        'H': 3,
        'M': 6,
        'U': 9,
        'Z': 12
    }

    @classmethod
    def get_third_friday(cls, year: int, month: int) -> date:
        d = date(year, month, 1)
        # weekday(): Monday=0, ..., Friday=4
        days_until_friday = (4 - d.weekday() + 7) % 7
        first_friday = d + timedelta(days=days_until_friday)
        return first_friday + timedelta(weeks=2)

    def __init__(self, record: str) -> None:
        self.record = record

        # Parsed from the record
        self.dt: Optional[datetime] = None
        self.symbol: Optional[str] = None

        # Candidate month/year derived from the timestamp (expiration logic)
        self.candidate_year: Optional[int] = None
        self.candidate_month: Optional[int] = None

        # Contract month/year derived from the symbol
        self.sym_month: Optional[int] = None
        self.sym_year: Optional[int] = None

    def parse_record(self) -> bool:
        """
        Extracts the timestamp from the first comma-separated field,
        and the symbol from the last field. Rejects spreads (any symbol
        containing a hyphen).
        Returns True if parsing succeeded; False if invalid.
        """
        parts = self.record.split(',')
        if len(parts) < 2:
            return False

        timestamp_str = parts[0].strip()
        symbol_candidate = parts[-1].strip()

        # Reject spreads
        if '-' in symbol_candidate:
            return False

        # Clean up the timestamp (remove trailing 'Z' and truncate fractional seconds)
        ts_str = timestamp_str.rstrip('Z')
        if '.' in ts_str:
            main_part, fractional = ts_str.split('.', 1)
            fractional = fractional[:6]
            ts_str = main_part + '.' + fractional

        # Attempt to parse into a datetime
        try:
            self.dt = datetime.fromisoformat(ts_str)
        except ValueError:
            return False

        self.symbol = symbol_candidate
        return True

    def next_valid_month_and_year(self, year: int, current_month: int) -> Tuple[int, int]:
        """
        Returns the next valid contract (year, month) after 'current_month' in the same year,
        or rolls over to the following year if 'current_month' is at or past the last valid month.
        """
        valid_months = sorted(self.MONTH_CODES.values())  # [3, 6, 9, 12]

        for m in valid_months:
            if m > current_month:
                return (year, m)
        # If none is greater, wrap to next year's first valid month
        return (year + 1, valid_months[0])

    def compute_candidate_month(self) -> None:
        """
        Determines the front-month contract (month/year) based on self.dt
        and assigns self.candidate_year, self.candidate_month.
        """
        if not self.dt:
            return

        ts_date = self.dt.date()
        current_year = ts_date.year
        current_month = ts_date.month

        valid_months = sorted(self.MONTH_CODES.values())  # [3, 6, 9, 12]

        # If the current month is one of the contract months...
        if current_month in valid_months:
            expiration = self.get_third_friday(current_year, current_month)
            if ts_date <= expiration:
                # Still in the current month's contract
                self.candidate_year = current_year
                self.candidate_month = current_month
            else:
                # We must roll to the next valid contract
                y, m = self.next_valid_month_and_year(
                    current_year, current_month)
                self.candidate_year = y
                self.candidate_month = m
        else:
            # If the current month is NOT a valid contract month,
            # jump directly to the next valid month/year
            y, m = self.next_valid_month_and_year(current_year, current_month)
            self.candidate_year = y
            self.candidate_month = m

    def parse_symbol(self) -> bool:
        """
        Extracts the month code and year digit from the end of self.symbol,
        interpreting them in the context of self.candidate_year (for decade).
        Populates self.sym_month, self.sym_year.
        Returns True if successful; False if invalid.
        """
        if not self.symbol or len(self.symbol) < 3:
            return False

        month_code = self.symbol[-2]
        year_digit = self.symbol[-1]

        if month_code not in self.MONTH_CODES:
            return False

        try:
            digit = int(year_digit)
        except ValueError:
            return False

        self.sym_month = self.MONTH_CODES[month_code]

        # Must have candidate_year to figure out the contract's decade
        if self.candidate_year is None:
            return False

        candidate_decade = (self.candidate_year // 10) * 10
        contract_year = candidate_decade + digit
        if contract_year < self.candidate_year:
            contract_year += 10

        self.sym_year = contract_year
        return True

    def is_valid_front_month(self) -> bool:
        """
        Orchestrates the validation steps:
          1. parse_record
          2. compute_candidate_month
          3. parse_symbol
          4. compare final month/year
        Returns True if the symbol matches the computed front-month logic.
        """
        if not self.parse_record():
            return False

        self.compute_candidate_month()

        if not self.parse_symbol():
            return False

        return (
            self.candidate_month == self.sym_month
            and self.candidate_year == self.sym_year
        )


def is_valid_front_month(item: str) -> bool:
    """
    A convenience function that creates a FrontMonthValidator instance
    and checks is_valid_front_month. This preserves the existing
    'is_valid_front_month' function signature for easy drop-in replacement.
    """
    validator = FrontMonthValidator(item)
    return validator.is_valid_front_month()


# -------------------------------------------------------------------
# Test Cases
# Format for record is from DataBento's OHLCV futures data.
# -------------------------------------------------------------------
class TestFrontMonthExpiration(unittest.TestCase):
    def test_valid_front_month_before_expiration(self):
        record = '2018-03-12T06:12:00.000000000Z,33,1,23520,7167.25,7167.50,7167.25,7167.50,10,NQH8'
        self.assertTrue(is_valid_front_month(record))

    def test_valid_front_month_after_expiration(self):
        record = '2018-03-17T06:12:00.000000000Z,33,1,23520,7167.25,7167.50,7167.25,7167.50,10,NQM8'
        self.assertTrue(is_valid_front_month(record))

    def test_invalid_symbol_spread(self):
        record = '2018-03-12T06:12:00.000000000Z,33,1,23520,7167.25,7167.50,7167.25,7167.50,10,NQH8-NQM8'
        self.assertFalse(is_valid_front_month(record))

    def test_invalid_contract_month(self):
        record = '2018-05-10T10:00:00.000000000Z,33,1,23520,7167.25,7167.50,7167.25,7167.50,10,NQH8'
        self.assertFalse(is_valid_front_month(record))

    def test_year_rollover(self):
        record = '2018-12-22T10:00:00.000000000Z,33,1,23520,7167.25,7167.50,7167.25,7167.50,10,NQH9'
        self.assertTrue(is_valid_front_month(record))

    def test_invalid_timestamp(self):
        record = 'invalid_timestamp,33,1,23520,7167.25,7167.50,7167.25,7167.50,10,NQH8'
        self.assertFalse(is_valid_front_month(record))

    def test_front_month_on_expiration_day(self):
        record = '2019-03-15T23:59:59.999999Z,33,1,23520,7167.25,7167.50,7167.25,7167.50,10,NQH9'
        self.assertTrue(is_valid_front_month(record))

    def test_front_month_next_month_after_expiration_day(self):
        record = '2019-03-16T00:00:00.000000Z,33,1,23520,7167.25,7167.50,7167.25,7167.50,10,NQM9'
        self.assertTrue(is_valid_front_month(record))

    def test_front_month_mid_summer_non_expiration_month(self):
        record = '2021-07-10T10:00:00.000000Z,33,1,23520,7167.25,7167.50,7167.25,7167.50,10,NQU1'
        self.assertTrue(is_valid_front_month(record))

    def test_front_month_end_of_quarter_before_expiration(self):
        record = '2022-09-14T12:00:00.000Z,33,1,23520,7167.25,7167.50,7167.25,7167.50,10,NQU2'
        self.assertTrue(is_valid_front_month(record))

    def test_front_month_end_of_quarter_after_expiration(self):
        record = '2022-09-17T12:00:00.000Z,33,1,23520,7167.25,7167.50,7167.25,7167.50,10,NQZ2'
        self.assertTrue(is_valid_front_month(record))

    def test_front_month_late_december_rolling_to_next_year(self):
        record = '2023-12-20T10:00:00.000Z,33,1,23520,7167.25,7167.50,7167.25,7167.50,10,NQH4'
        self.assertTrue(is_valid_front_month(record))

    def test_front_month_january_rolling_to_march_same_year(self):
        record = '2025-01-10T08:00:00.000Z,33,1,23520,7167.25,7167.50,7167.25,7167.50,10,NQH5'
        self.assertTrue(is_valid_front_month(record))

    def test_front_month_february_just_before_march_contract_starts(self):
        record = '2025-02-28T09:30:00.000Z,33,1,23520,7167.25,7167.50,7167.25,7167.50,10,NQH5'
        self.assertTrue(is_valid_front_month(record))

    def test_on_third_friday_exactly_at_midnight(self):
        record = '2026-03-20T00:00:00.000000Z,33,1,23520,7167.25,7167.50,7167.25,7167.50,10,NQH6'
        self.assertTrue(is_valid_front_month(record))

    def test_timestamp_without_fractional_seconds(self):
        record = '2027-06-15T12:00:00Z,33,1,23520,7167.25,7167.50,7167.25,7167.50,10,NQM7'
        self.assertTrue(is_valid_front_month(record))

    def test_invalid_symbol_too_short(self):
        record = '2018-03-12T06:12:00.000000000Z,33,1,23520,7167.25,7167.50,7167.25,7167.50,10,NQ'
        self.assertFalse(is_valid_front_month(record))

    def test_invalid_symbol_unknown_month_code(self):
        record = '2018-03-12T06:12:00.000Z,33,1,23520,7167.25,7167.50,7167.25,7167.50,10,NQX8'
        self.assertFalse(is_valid_front_month(record))

    def test_invalid_symbol_year_not_digit(self):
        record = '2018-03-12T06:12:00.000Z,33,1,23520,7167.25,7167.50,7167.25,7167.50,10,NQH?'
        self.assertFalse(is_valid_front_month(record))

    def test_non_front_month_after_expiration(self):
        record = '2028-03-18T00:00:00.000Z,33,1,23520,7167.25,7167.50,7167.25,7167.50,10,NQH8'
        self.assertFalse(is_valid_front_month(record))

    def test_decade_rollover(self):
        record = '2029-12-22T00:00:00.000Z,33,1,23520,7167.25,7167.50,7167.25,7167.50,10,NQH0'
        self.assertTrue(is_valid_front_month(record))

    def test_day_after_expiration_midnight(self):
        record = '2037-06-20T00:00:00Z,33,1,23520,7167.25,7167.50,7167.25,7167.50,10,NQU7'
        self.assertTrue(is_valid_front_month(record))


if __name__ == '__main__':
    unittest.main()
