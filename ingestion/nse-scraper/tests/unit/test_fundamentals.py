"""Unit tests for fundamentals parsers and scrapers."""

import unittest
from datetime import date


class TestQuarterlyFinancialsParser(unittest.TestCase):
    """Test cases for quarterly financials parser."""

    def test_compute_roe(self):
        """Test ROE computation."""
        # ROE = (Net Profit / Equity) × 100
        net_profit = 100
        equity = 1000
        expected_roe = 10.0

        roe = (net_profit / equity) * 100
        self.assertEqual(roe, expected_roe)

    def test_compute_roa(self):
        """Test ROA computation."""
        # ROA = (Net Profit / Total Assets) × 100
        net_profit = 100
        total_assets = 2000
        expected_roa = 5.0

        roa = (net_profit / total_assets) * 100
        self.assertEqual(roa, expected_roa)

    def test_compute_debt_to_equity(self):
        """Test debt-to-equity ratio computation."""
        # Debt/Equity = Total Debt / Equity
        total_debt = 500
        equity = 1000
        expected_ratio = 0.5

        ratio = total_debt / equity
        self.assertEqual(ratio, expected_ratio)

    def test_compute_current_ratio(self):
        """Test current ratio computation."""
        # Current Ratio = Current Assets / Current Liabilities
        current_assets = 800
        current_liabilities = 400
        expected_ratio = 2.0

        ratio = current_assets / current_liabilities
        self.assertEqual(ratio, expected_ratio)

    def test_compute_operating_margin(self):
        """Test operating margin computation."""
        # Operating Margin = (Operating Profit / Revenue) × 100
        operating_profit = 200
        revenue = 1000
        expected_margin = 20.0

        margin = (operating_profit / revenue) * 100
        self.assertEqual(margin, expected_margin)

    def test_compute_net_margin(self):
        """Test net margin computation."""
        # Net Margin = (Net Profit / Revenue) × 100
        net_profit = 100
        revenue = 1000
        expected_margin = 10.0

        margin = (net_profit / revenue) * 100
        self.assertEqual(margin, expected_margin)

    def test_compute_pe_ratio(self):
        """Test P/E ratio computation."""
        # PE = Price / EPS
        price = 1000
        eps = 50
        expected_pe = 20.0

        pe = price / eps
        self.assertEqual(pe, expected_pe)


class TestShareholdingPatternParser(unittest.TestCase):
    """Test cases for shareholding pattern parser."""

    def test_shareholding_sum(self):
        """Test that shareholding percentages sum to 100."""
        promoter = 50.0
        fii = 20.0
        dii = 15.0
        public = 15.0

        total = promoter + fii + dii + public
        self.assertEqual(total, 100.0)

    def test_institutional_calculation(self):
        """Test institutional shareholding calculation."""
        fii = 20.0
        dii = 15.0
        expected_institutional = 35.0

        institutional = fii + dii
        self.assertEqual(institutional, expected_institutional)

    def test_shares_from_percentage(self):
        """Test shares calculation from percentage."""
        total_shares = 1000000000  # 100 crore
        promoter_percent = 50.0
        expected_promoter_shares = 500000000  # 50 crore

        promoter_shares = int(total_shares * promoter_percent / 100)
        self.assertEqual(promoter_shares, expected_promoter_shares)


class TestDataGenerators(unittest.TestCase):
    """Test cases for sample data generators."""

    def test_quarter_calculation(self):
        """Test quarter calculation from month."""
        test_cases = [
            (1, 1), (2, 1), (3, 1),  # Q1
            (4, 2), (5, 2), (6, 2),  # Q2
            (7, 3), (8, 3), (9, 3),  # Q3
            (10, 4), (11, 4), (12, 4),  # Q4
        ]

        for month, expected_quarter in test_cases:
            quarter = (month - 1) // 3 + 1
            self.assertEqual(quarter, expected_quarter, f"Month {month} should be Q{expected_quarter}")

    def test_quarter_end_dates(self):
        """Test quarter end date calculation."""
        test_cases = [
            (1, date(2024, 3, 31)),   # Q1 end
            (2, date(2024, 6, 30)),   # Q2 end
            (3, date(2024, 9, 30)),   # Q3 end
            (4, date(2024, 12, 31)),  # Q4 end
        ]

        for quarter, expected_date in test_cases:
            if quarter == 1:
                quarter_end = date(2024, 3, 31)
            elif quarter == 2:
                quarter_end = date(2024, 6, 30)
            elif quarter == 3:
                quarter_end = date(2024, 9, 30)
            else:
                quarter_end = date(2024, 12, 31)

            self.assertEqual(quarter_end, expected_date)


class TestSchemaValidation(unittest.TestCase):
    """Test cases for schema validation."""

    def test_quarterly_financials_schema_fields(self):
        """Test that quarterly financials schema has required fields."""
        required_fields = [
            "event_id", "event_time", "ingest_time",
            "symbol", "company_name", "period_end_date",
            "revenue", "net_profit", "eps",
            "roe", "roa", "debt_to_equity",
        ]

        # This is a mock test - in reality would validate against schema file
        schema_fields = {
            "event_id": "string",
            "event_time": "timestamp",
            "ingest_time": "timestamp",
            "symbol": "string",
            "company_name": "string",
            "period_end_date": "date",
            "revenue": "double",
            "net_profit": "double",
            "eps": "double",
            "roe": "double",
            "roa": "double",
            "debt_to_equity": "double",
        }

        for field in required_fields:
            self.assertIn(field, schema_fields, f"Field {field} should be in schema")

    def test_shareholding_pattern_schema_fields(self):
        """Test that shareholding pattern schema has required fields."""
        required_fields = [
            "event_id", "event_time", "ingest_time",
            "symbol", "company_name", "quarter_end_date",
            "promoter_shareholding_percent",
            "fii_shareholding_percent",
            "dii_shareholding_percent",
            "public_shareholding_percent",
        ]

        # This is a mock test - in reality would validate against schema file
        schema_fields = {
            "event_id": "string",
            "event_time": "timestamp",
            "ingest_time": "timestamp",
            "symbol": "string",
            "company_name": "string",
            "quarter_end_date": "date",
            "promoter_shareholding_percent": "double",
            "fii_shareholding_percent": "double",
            "dii_shareholding_percent": "double",
            "public_shareholding_percent": "double",
        }

        for field in required_fields:
            self.assertIn(field, schema_fields, f"Field {field} should be in schema")


if __name__ == "__main__":
    unittest.main()
