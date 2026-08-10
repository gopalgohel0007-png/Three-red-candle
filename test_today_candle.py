import unittest
from unittest.mock import patch

import server


class TodayCandleTests(unittest.TestCase):
    def _data(self, date):
        return {
            "price": 950.0,
            "prevClose": 1000.0,
            "candles": [
                {"date": date, "open": 1000.0, "close": 990.0, "high": 1005.0, "low": 985.0, "red": True}
            ],
        }

    @patch("server._nse_session_is_open_now", return_value=True)
    @patch("server._original_fetch_candles")
    def test_today_candle_uses_live_price_as_close(self, fetch, market_open):
        today = server._today_label()
        fetch.return_value = (self._data(today), None)

        data, err = server.fetch_candles_with_today("TEST")

        self.assertIsNone(err)
        self.assertEqual(data["candles"][-1]["close"], 950.0)
        self.assertTrue(data["candles"][-1]["red"])

    @patch("server._nse_session_is_open_now", return_value=True)
    @patch("server._original_fetch_candles")
    def test_old_candle_is_not_relabelled_as_today(self, fetch, market_open):
        fetch.return_value = (self._data("07 Aug"), None)

        data, err = server.fetch_candles_with_today("TEST")

        self.assertIsNone(err)
        self.assertEqual(data["candles"][-1]["close"], 990.0)

    @patch("server._nse_session_is_open_now", return_value=False)
    @patch("server._original_fetch_candles")
    def test_outside_market_hours_leaves_candle_untouched(self, fetch, market_open):
        today = server._today_label()
        fetch.return_value = (self._data(today), None)

        data, err = server.fetch_candles_with_today("TEST")

        self.assertIsNone(err)
        self.assertEqual(data["candles"][-1]["close"], 990.0)


if __name__ == "__main__":
    unittest.main()
