import unittest
from datetime import date, datetime, timezone, timedelta

from hec.core.models import NetElectricityPriceInterval
from hec.utils.utils import calculate_easter, is_a_holiday, get_interval_from_list


class Test(unittest.TestCase):
    def setUp(self):
        """Setup common test data."""
        self.cet = timezone(timedelta(hours=1), "CET")  # Winter time
        self.cest = timezone(timedelta(hours=2), "CEST")  # Summer time

        # Sample NetElectricityPriceInterval data
        dummy_prices = {"dynamic": {"buy": 0.22, "sell": 0.06}, "fixed": {"buy": 0.27, "sell": 0.02}}

        self.sample_intervals_hourly = [
            NetElectricityPriceInterval(datetime(2024, 1, 15, 10, 0, 0, tzinfo=self.cet), 60, "dynamic", dummy_prices),
            NetElectricityPriceInterval(datetime(2024, 1, 15, 11, 0, 0, tzinfo=self.cet), 60, "dynamic", dummy_prices),
            NetElectricityPriceInterval(datetime(2024, 1, 15, 12, 0, 0, tzinfo=self.cet), 60, "dynamic", dummy_prices),
        ]

        self.sample_intervals_15min = [
            NetElectricityPriceInterval(datetime(2024, 7, 15, 14, 0, 0, tzinfo=self.cest), 15, "fixed", dummy_prices),
            NetElectricityPriceInterval(datetime(2024, 7, 15, 14, 15, 0, tzinfo=self.cest), 15, "fixed", dummy_prices),
            NetElectricityPriceInterval(datetime(2024, 7, 15, 14, 30, 0, tzinfo=self.cest), 15, "fixed", dummy_prices),
            NetElectricityPriceInterval(datetime(2024, 7, 15, 14, 45, 0, tzinfo=self.cest), 15, "fixed", dummy_prices),
            NetElectricityPriceInterval(datetime(2024, 7, 15, 15, 0, 0, tzinfo=self.cest), 15, "fixed", dummy_prices),
        ]

        self.intervals_around_dst_spring_2024 = [  # March 31 2024
            # Before DST: 01:00-01:59 CET (UTC: 00:00-00:59)
            NetElectricityPriceInterval(datetime(2024, 3, 31, 1, 0, 0, tzinfo=self.cet), 60, "dynamic", dummy_prices),
            # After DST: 03:00-03:59 CEST (UTC: 01:00-01:59, after the jump)
            NetElectricityPriceInterval(datetime(2024, 3, 31, 3, 0, 0, tzinfo=self.cest), 60, "dynamic", dummy_prices),
        ]

    def test_target_within_hourly_interval(self):
        target = datetime(2024, 1, 15, 10, 30, 0, tzinfo=self.cet)
        result = get_interval_from_list(target, self.sample_intervals_hourly)
        self.assertIsNotNone(result)
        self.assertEqual(result, self.sample_intervals_hourly[0])

    def test_target_at_start_of_hourly_interval(self):
        target = datetime(2024, 1, 15, 11, 0, 0, tzinfo=self.cet)
        result = get_interval_from_list(target, self.sample_intervals_hourly)
        self.assertIsNotNone(result)
        self.assertEqual(result, self.sample_intervals_hourly[1])

    def test_target_at_end_of_hourly_interval_exclusive(self):
        # Target is exactly when the next interval starts, so it should NOT find the previous one
        target = datetime(2024, 1, 15, 12, 0, 0, tzinfo=self.cet)
        result = get_interval_from_list(target, self.sample_intervals_hourly)
        self.assertIsNotNone(result)
        self.assertEqual(result, self.sample_intervals_hourly[2])  # Should find the 12:00 interval

    def test_target_just_before_end_of_hourly_interval(self):
        target = datetime(2024, 1, 15, 11, 59, 59, tzinfo=self.cet)
        result = get_interval_from_list(target, self.sample_intervals_hourly)
        self.assertIsNotNone(result)
        self.assertEqual(result, self.sample_intervals_hourly[1])

    def test_target_within_15min_interval(self):
        target = datetime(2024, 7, 15, 14, 20, 0, tzinfo=self.cest)
        result = get_interval_from_list(target, self.sample_intervals_15min)
        self.assertIsNotNone(result)
        self.assertEqual(result, self.sample_intervals_15min[1])  # Should be 14:15 interval

    def test_target_at_start_of_15min_interval(self):
        target = datetime(2024, 7, 15, 14, 45, 0, tzinfo=self.cest)
        result = get_interval_from_list(target, self.sample_intervals_15min)
        self.assertIsNotNone(result)
        self.assertEqual(result, self.sample_intervals_15min[3])

    def test_target_before_first_interval(self):
        target = datetime(2024, 1, 15, 9, 0, 0, tzinfo=self.cet)
        result = get_interval_from_list(target, self.sample_intervals_hourly)
        self.assertIsNone(result)

    def test_target_after_last_interval(self):
        target = datetime(2024, 1, 15, 13, 30, 0, tzinfo=self.cet)
        result = get_interval_from_list(target, self.sample_intervals_hourly)
        self.assertIsNone(result)

    def test_empty_interval_list(self):
        target = datetime(2024, 1, 15, 10, 0, 0, tzinfo=self.cet)
        result = get_interval_from_list(target, [])
        self.assertIsNone(result)

    def test_target_with_different_timezone_match(self):
        # Intervals are in CET (UTC+1)
        # Target time is specified in UTC, but represents a time within one of the CET intervals
        target_utc = datetime(2024, 1, 15, 9, 30, 0, tzinfo=timezone.utc)  # This is 10:30 CET
        result = get_interval_from_list(target_utc, self.sample_intervals_hourly)
        self.assertIsNotNone(result)
        self.assertEqual(result, self.sample_intervals_hourly[0])  # Should find the 10:00 CET interval

    def test_target_with_naive_datetime_and_aware_intervals(self):
        target_naive = datetime(2024, 1, 15, 10, 30, 0)  # Naive datetime
        with self.assertLogs(logger='hec.utils.utils', level='ERROR') as cm:
            result = get_interval_from_list(target_naive, self.sample_intervals_hourly)
        self.assertIsNone(result)
        self.assertTrue(any("Error" in log_msg for log_msg in cm.output))

    def test_dst_transition_before_change(self):
        # Target time 01:30 CET (before DST jump)
        target = datetime(2024, 3, 31, 1, 30, 0, tzinfo=self.cet)
        result = get_interval_from_list(target, self.intervals_around_dst_spring_2024)
        self.assertIsNotNone(result)
        self.assertEqual(result.interval_start_local, datetime(2024, 3, 31, 1, 0, 0, tzinfo=self.cet))

    def test_dst_transition_after_change(self):
        # Target time 03:30 CEST (after DST jump)
        target = datetime(2024, 3, 31, 3, 30, 0, tzinfo=self.cest)
        result = get_interval_from_list(target, self.intervals_around_dst_spring_2024)
        self.assertIsNotNone(result)
        self.assertEqual(result.interval_start_local, datetime(2024, 3, 31, 3, 0, 0, tzinfo=self.cest))

    def test_dst_transition_target_in_skipped_hour_cet(self):
        target_in_skipped_hour_utc_equivalent = datetime(2024, 3, 31, 1, 30, 0, tzinfo=timezone.utc)  # 02:30 CET
        result = get_interval_from_list(target_in_skipped_hour_utc_equivalent, self.intervals_around_dst_spring_2024)
        self.assertIsNotNone(result)
        self.assertEqual(result.interval_start_local, datetime(2024, 3, 31, 3, 0, 0, tzinfo=self.cest))

    def test_calculate_easter_known_years(self):
        self.assertEqual(calculate_easter(2023), date(2023, 4, 9), "Easter 2023")
        self.assertEqual(calculate_easter(2024), date(2024, 3, 31), "Easter 2024 (Leap Year)")
        self.assertEqual(calculate_easter(2025), date(2025, 4, 20), "Easter 2025")
        self.assertEqual(calculate_easter(1990), date(1990, 4, 15), "Easter 1990")
        self.assertEqual(calculate_easter(2000), date(2000, 4, 23), "Easter 2000 (Century Leap Year)")

    def test_fixed_holidays(self):
        self.assertTrue(is_a_holiday(date(2024, 1, 1)), "New Year's Day")
        self.assertTrue(is_a_holiday(date(2024, 5, 1)), "Labor Day")
        self.assertTrue(is_a_holiday(date(2024, 7, 21)), "Belgian National Day")
        self.assertTrue(is_a_holiday(date(2024, 8, 15)), "Assumption Day")
        self.assertTrue(is_a_holiday(date(2024, 11, 1)), "All Saints' Day")
        self.assertTrue(is_a_holiday(date(2024, 11, 11)), "Armistice Day")
        self.assertTrue(is_a_holiday(date(2024, 12, 25)), "Christmas Day")

    def test_easter_dependent_holidays(self):
        self.assertTrue(is_a_holiday(date(2024, 4, 1)), "Easter Monday 2024")
        self.assertTrue(is_a_holiday(date(2024, 5, 9)), "Ascension Day 2024")
        self.assertTrue(is_a_holiday(date(2024, 5, 20)), "Whit Monday 2024")

        self.assertTrue(is_a_holiday(date(2025, 4, 21)), "Easter Monday 2025")
        self.assertTrue(is_a_holiday(date(2025, 5, 29)), "Ascension Day 2025")
        self.assertTrue(is_a_holiday(date(2025, 6, 9)), "Whit Monday 2025")

    def test_not_holidays(self):
        self.assertFalse(is_a_holiday(date(2024, 1, 2)), "Jan 2nd is not a holiday")
        self.assertFalse(is_a_holiday(date(2024, 3, 30)),
                         "Day before Easter 2024 is not a holiday")
        self.assertFalse(is_a_holiday(date(2024, 10, 10)), "Random October day")

    def test_is_a_holiday_with_datetime_input(self):
        self.assertTrue(is_a_holiday(datetime(2024, 1, 1, 10, 30, 0)), "New Year's Day (datetime)")
        self.assertFalse(is_a_holiday(datetime(2024, 1, 2, 10, 30, 0)), "Jan 2nd (datetime)")

    def test_is_a_holiday_invalid_input(self):
        with self.assertRaises(TypeError):
            is_a_holiday("2024-01-01")
        with self.assertRaises(TypeError):
            is_a_holiday(123)


if __name__ == '__main__':
    unittest.main()
