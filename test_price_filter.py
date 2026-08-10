import os
import unittest

from price_filter import DEFAULT_MIN_STOCK_PRICE, filter_matches_by_min_price, get_min_stock_price


class PriceFilterTests(unittest.TestCase):
    def tearDown(self):
        os.environ.pop("MIN_STOCK_PRICE", None)

    def test_default_threshold_is_1000(self):
        self.assertEqual(get_min_stock_price(), DEFAULT_MIN_STOCK_PRICE)

    def test_keeps_prices_at_or_above_1000(self):
        matches = [
            {"symbol": "A", "price": 999.99},
            {"symbol": "B", "price": 1000},
            {"symbol": "C", "price": 1250.50},
        ]
        filtered = filter_matches_by_min_price(matches)
        self.assertEqual([m["symbol"] for m in filtered], ["B", "C"])

    def test_threshold_is_configurable(self):
        os.environ["MIN_STOCK_PRICE"] = "1500"
        matches = [
            {"symbol": "A", "price": 1499},
            {"symbol": "B", "price": 1500},
            {"symbol": "C", "price": 2000},
        ]
        filtered = filter_matches_by_min_price(matches)
        self.assertEqual([m["symbol"] for m in filtered], ["B", "C"])

    def test_missing_or_invalid_price_is_excluded(self):
        matches = [
            {"symbol": "A", "price": None},
            {"symbol": "B", "price": "not-a-number"},
            {"symbol": "C", "price": 1200},
        ]
        filtered = filter_matches_by_min_price(matches)
        self.assertEqual([m["symbol"] for m in filtered], ["C"])


if __name__ == "__main__":
    unittest.main()
