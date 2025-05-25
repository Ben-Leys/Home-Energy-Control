import logging
import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock

import requests

from hec.core.models import PricePoint
from hec.data_sources import api_entsoe

logger_api_entsoe = logging.getLogger('hec.data_sources.api_entsoe')
# logger_api_entsoe.setLevel(logging.DEBUG) 
# stream_handler = logging.StreamHandler()
# logger_api_entsoe.addHandler(stream_handler)


# --- Sample XML Responses ---

# Successful response, PT60M, 4 points
XML_SUCCESS_PT60M_4POINTS = b"""<?xml version="1.0" encoding="UTF-8"?>
<Publication_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3">
    <mRID>b6c17ca509f0420485b95e9a43dafd14</mRID>
    <revisionNumber>1</revisionNumber>
    <type>A44</type>
    <sender_MarketParticipant.mRID codingScheme="A01">10X1001A1001A450</sender_MarketParticipant.mRID>
    <sender_MarketParticipant.marketRole.type>A32</sender_MarketParticipant.marketRole.type>
    <receiver_MarketParticipant.mRID codingScheme="A01">10X1001A1001A450</receiver_MarketParticipant.mRID>
    <receiver_MarketParticipant.marketRole.type>A33</receiver_MarketParticipant.marketRole.type>
    <createdDateTime>2025-05-22T18:23:41Z</createdDateTime>
    <period.timeInterval>
        <start>2025-05-20T22:00Z</start>
        <end>2025-05-21T22:00Z</end>
    </period.timeInterval>
    <TimeSeries>
        <mRID>1</mRID>
        <auction.type>A01</auction.type>
        <businessType>A62</businessType>
        <in_Domain.mRID codingScheme="A01">10YBE----------2</in_Domain.mRID>
        <out_Domain.mRID codingScheme="A01">10YBE----------2</out_Domain.mRID>
        <contract_MarketAgreement.type>A01</contract_MarketAgreement.type>
        <currency_Unit.name>EUR</currency_Unit.name>
        <price_Measure_Unit.name>MWH</price_Measure_Unit.name>
        <curveType>A03</curveType>
        <Period>
            <timeInterval>
                <start>2025-05-20T22:00Z</start>
                <end>2025-05-21T22:00Z</end>
            </timeInterval>
            <resolution>PT60M</resolution>
            <Point>
                <position>1</position>
                <price.amount>110</price.amount>
            </Point>
            <Point>
                <position>2</position>
                <price.amount>102.33</price.amount>
            </Point>
            <Point>
                <position>3</position>
                <price.amount>97.9</price.amount>
            </Point>
            <Point>
                <position>4</position>
                <price.amount>94.9</price.amount>
            </Point>
        </Period>
    </TimeSeries>
</Publication_MarketDocument>
"""

# Successful response, PT15M, 3 points, expecting 4 (gap filling)
XML_SUCCESS_PT15M_GAP_FILLING = b"""<?xml version="1.0" encoding="UTF-8"?>
<Publication_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3">
    <mRID>b6c17ca509f0420485b95e9a43dafd14</mRID>
    <revisionNumber>1</revisionNumber>
    <type>A44</type>
    <sender_MarketParticipant.mRID codingScheme="A01">10X1001A1001A450</sender_MarketParticipant.mRID>
    <sender_MarketParticipant.marketRole.type>A32</sender_MarketParticipant.marketRole.type>
    <receiver_MarketParticipant.mRID codingScheme="A01">10X1001A1001A450</receiver_MarketParticipant.mRID>
    <receiver_MarketParticipant.marketRole.type>A33</receiver_MarketParticipant.marketRole.type>
    <createdDateTime>2025-05-22T18:23:41Z</createdDateTime>
    <period.timeInterval>
        <start>2025-05-20T22:00Z</start>
        <end>2025-05-21T22:00Z</end>
    </period.timeInterval>
    <TimeSeries>
        <mRID>1</mRID>
        <auction.type>A01</auction.type>
        <businessType>A62</businessType>
        <in_Domain.mRID codingScheme="A01">10YBE----------2</in_Domain.mRID>
        <out_Domain.mRID codingScheme="A01">10YBE----------2</out_Domain.mRID>
        <contract_MarketAgreement.type>A01</contract_MarketAgreement.type>
        <currency_Unit.name>EUR</currency_Unit.name>
        <price_Measure_Unit.name>MWH</price_Measure_Unit.name>
        <curveType>A03</curveType>
        <Period>
            <timeInterval>
                <start>2025-05-20T22:00Z</start>
                <end>2025-05-21T22:00Z</end>
            </timeInterval>
            <resolution>PT60M</resolution>
            <Point>
                <position>1</position>
                <price.amount>110</price.amount>
            </Point>
            <Point>
                <position>2</position>
                <price.amount>102.33</price.amount>
            </Point>
            <Point>
                <position>4</position>
                <price.amount>94.9</price.amount>
            </Point>
            <!-- Missing point 3, should be filled -->
        </Period>
    </TimeSeries>
</Publication_MarketDocument>
"""

# Response: "No matching data found"
XML_NO_DATA_FOUND = b"""<?xml version="1.0" encoding="UTF-8"?>
<Publication_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0">
    <Reason>
        <code>999</code>
        <text>No matching data found.</text>
    </Reason>
</Publication_MarketDocument>
"""

# Response: Other API Error
XML_API_ERROR = b"""<?xml version="1.0" encoding="UTF-8"?>
<Publication_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0">
    <Reason>
        <code>A01</code> <!-- Example error code -->
        <text>Invalid security token or request.</text>
    </Reason>
</Publication_MarketDocument>
"""

# Malformed XML
XML_MALFORMED = b"""<?xml version="1.0" encoding="UTF-8"?>
<Publication_MarketDocument> <!-- Missing xmlns -->
    <TimeSeries>
        <Period>
            <Point>1</pos><price>50</price></Point> <!-- Invalid structure -->
        </Period>
    </TimeSeries>
</Publication_MarketDocument>
"""

# XML with no TimeSeries element
XML_NO_TIMESERIES = b"""<?xml version="1.0" encoding="UTF-8"?>
<Publication_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0">
    <mRID>12345</mRID>
    <!-- No TimeSeries here -->
</Publication_MarketDocument>
"""


class TestApiEntsoe(unittest.TestCase):

    def setUp(self):
        self.mock_app_config = {
            "entsoe": {
                "api_base_url": "https://web-api.tp.entsoe.eu/api",
                "document_type": "A44",
                "domain": "10YBE----------2",  # Belgium
                "auction_opening_hour": 13  # CET/CEST
            }
        }
        # Dummy API key
        os.environ["ENTSOE_API_KEY"] = "DUMMY_TEST_KEY"
        self.local_tz = datetime.now().astimezone().tzinfo

    def tearDown(self):
        if "ENTSOE_API_KEY_TEMP" in os.environ:
            os.environ["ENTSOE_API_KEY"] = os.environ.pop("ENTSOE_API_KEY_TEMP")
        elif os.getenv("ENTSOE_API_KEY") == "DUMMY_TEST_KEY":
            del os.environ["ENTSOE_API_KEY"]

    def test_parse_resolution_to_minutes(self):
        self.assertEqual(api_entsoe._parse_resolution_to_minutes("PT60M"), 60)
        self.assertEqual(api_entsoe._parse_resolution_to_minutes("PT30M"), 30)
        self.assertEqual(api_entsoe._parse_resolution_to_minutes("PT15M"), 15)
        self.assertEqual(api_entsoe._parse_resolution_to_minutes("PT_UNKNOWN"), 60, "Should default for unknown")

    def test_parse_xml_success_pt60m(self):
        result = api_entsoe._parse_entsoe_price_xml(XML_SUCCESS_PT60M_4POINTS)
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 24)
        self.assertIsInstance(result[0], PricePoint)
        self.assertEqual(result[0].timestamp_utc, datetime(2025, 5, 20, 22, 0, tzinfo=timezone.utc))
        self.assertEqual(result[0].price_eur_per_mwh, 110.00)
        self.assertEqual(result[0].resolution_minutes, 60)
        self.assertEqual(result[0].position, 1)
        self.assertEqual(result[1].timestamp_utc, datetime(2025, 5, 20, 23, 0, tzinfo=timezone.utc))
        self.assertEqual(result[1].price_eur_per_mwh, 102.33)
        self.assertEqual(result[1].resolution_minutes, 60)
        self.assertEqual(result[1].position, 2)
        self.assertEqual(result[2].timestamp_utc, datetime(2025, 5, 21, 0, 0, tzinfo=timezone.utc))
        self.assertEqual(result[2].price_eur_per_mwh, 97.9)
        self.assertEqual(result[2].resolution_minutes, 60)
        self.assertEqual(result[2].position, 3)
        self.assertEqual(result[3].timestamp_utc, datetime(2025, 5, 21, 1, 0, tzinfo=timezone.utc))
        self.assertEqual(result[3].price_eur_per_mwh, 94.9)
        self.assertEqual(result[3].resolution_minutes, 60)
        self.assertEqual(result[3].position, 4)

    def test_parse_xml_success_pt15m_gap_filling(self):
        result = api_entsoe._parse_entsoe_price_xml(XML_SUCCESS_PT15M_GAP_FILLING)
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 24, "Should fill a gap for a 1-hour period with 60min resolution")

        # Check first point
        self.assertEqual(result[0].price_eur_per_mwh, 110.00)
        self.assertEqual(result[0].timestamp_utc, datetime(2025, 5, 20, 22, 0, tzinfo=timezone.utc))
        self.assertEqual(result[0].resolution_minutes, 60)

        # Check filled points (should have same price as previous known)
        self.assertEqual(result[1].price_eur_per_mwh, 102.33)
        self.assertEqual(result[1].timestamp_utc, datetime(2025, 5, 20, 23, 0, tzinfo=timezone.utc))
        self.assertEqual(result[2].price_eur_per_mwh, 102.33)
        self.assertEqual(result[2].timestamp_utc, datetime(2025, 5, 21, 0, 0, tzinfo=timezone.utc))
        self.assertEqual(result[3].price_eur_per_mwh, 94.9)
        self.assertEqual(result[3].timestamp_utc, datetime(2025, 5, 21, 1, 0, tzinfo=timezone.utc))

    def test_parse_xml_no_data_found_reason(self):
        result = api_entsoe._parse_entsoe_price_xml(XML_NO_DATA_FOUND)
        self.assertIsNotNone(result)  # Function returns [] for "no data yet"
        self.assertEqual(len(result), 0)

    def test_parse_xml_api_error_reason(self):
        result = api_entsoe._parse_entsoe_price_xml(XML_API_ERROR)
        self.assertIsNone(result)  # Function returns None for critical API errors

    def test_parse_xml_malformed(self):
        result = api_entsoe._parse_entsoe_price_xml(XML_MALFORMED)
        self.assertIsNone(result)

    def test_parse_xml_no_timeseries(self):
        result = api_entsoe._parse_entsoe_price_xml(XML_NO_TIMESERIES)
        self.assertIsNotNone(result)  # Returns [] if no TimeSeries but no error Reason
        self.assertEqual(len(result), 0)

    @patch('hec.data_sources.api_entsoe.requests.get')  # Patch requests.get within the module being tested
    def test_fetch_entsoe_prices_success(self, mock_get):
        # Configure the mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = XML_SUCCESS_PT60M_4POINTS
        mock_response.url = "mock://url"  # For logging in case of error
        mock_get.return_value = mock_response

        # Target day well after auction time
        target_day = (datetime.now(self.local_tz) - timedelta(days=5)).replace(hour=0, minute=0, second=0,
                                                                               microsecond=0)

        result = api_entsoe.fetch_entsoe_prices(target_day, self.mock_app_config)

        mock_get.assert_called_once()  # Check that requests.get was called
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 24)
        self.assertEqual(result[0].price_eur_per_mwh, 110)

    @patch('hec.data_sources.api_entsoe.requests.get')
    def test_fetch_entsoe_prices_http_error(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Server Error")
        mock_response.url = "mock://url"
        mock_get.return_value = mock_response

        target_day = (datetime.now(self.local_tz) - timedelta(days=5))
        result = api_entsoe.fetch_entsoe_prices(target_day, self.mock_app_config)

        self.assertIsNone(result)

    @patch('hec.data_sources.api_entsoe.requests.get')
    def test_fetch_entsoe_prices_timeout(self, mock_get):
        mock_get.side_effect = requests.exceptions.Timeout("Request timed out")

        target_day = (datetime.now(self.local_tz) - timedelta(days=5))
        result = api_entsoe.fetch_entsoe_prices(target_day, self.mock_app_config)

        self.assertIsNone(result)

    def test_fetch_entsoe_prices_no_api_key(self):
        # Temporarily remove API key from environment for this test
        original_key = os.environ.pop("ENTSOE_API_KEY", None)

        target_day = (datetime.now(self.local_tz) - timedelta(days=5))
        result = api_entsoe.fetch_entsoe_prices(target_day, self.mock_app_config)
        self.assertIsNone(result)

        if original_key:  # Restore it if it was there
            os.environ["ENTSOE_API_KEY"] = original_key

    def test_fetch_entsoe_prices_before_auction_time(self):
        # Target tomorrow, but assume current time is before auction_opening_hour
        # Simpler: Test with a target_day that is D+2, assuming today is before auction for D+1
        now_local = datetime.now(self.local_tz)

        # Scenario 1: Target tomorrow, before auction time
        auction_hour = self.mock_app_config["entsoe"]["auction_opening_hour"]
        if now_local.hour < auction_hour:
            target_day_tomorrow = (now_local + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            with patch('hec.data_sources.api_entsoe.datetime') as mock_dt:  # Mock datetime inside the target module
                mock_dt.now.return_value.astimezone.return_value = now_local  # Ensure mocked now() is used
                mock_dt.combine = datetime.combine  # Allow combine to work
                mock_dt.strptime = datetime.strptime  # Allow strptime
                mock_dt.fromisoformat = datetime.fromisoformat

                result = api_entsoe.fetch_entsoe_prices(target_day_tomorrow, self.mock_app_config)
                self.assertIsNotNone(result)
                self.assertEqual(len(result), 0,
                                 "Should return empty list if fetching for tomorrow before auction time.")
        else:
            self.skipTest(
                f"Skipping 'before_auction_time' test as current time ({now_local.hour}) is not before auction hour ({auction_hour}).")

        # Scenario 2: Target D+2 (should attempt fetch, assuming API key is set)
        target_day_d_plus_2 = (now_local + timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
        with patch('hec.data_sources.api_entsoe.requests.get') as mock_get_dplus2:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.content = XML_NO_DATA_FOUND  # Assume D+2 data isn't there yet
            mock_get_dplus2.return_value = mock_response

            result_dplus2 = api_entsoe.fetch_entsoe_prices(target_day_d_plus_2, self.mock_app_config)
            mock_get_dplus2.assert_called()  # Should have attempted the call
            self.assertIsNotNone(result_dplus2)
            self.assertEqual(len(result_dplus2), 0)  # Because XML_NO_DATA_FOUND was returned


if __name__ == '__main__':
    unittest.main()
