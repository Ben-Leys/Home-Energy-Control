# data_sources/day_ahead_price_api.py
import logging
import os
import requests
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from xml.etree import ElementTree as ElTree

logger = logging.getLogger(__name__)

# ENTSO-E API Configuration
ENTSOE_API_BASE_URL = "https://web-api.tp.entsoe.eu/api"
DOCUMENT_TYPE_A44 = "A44"  # Price Document
BELGIUM_DOMAIN = "10YBE----------2"


class PricePoint:
    def __init__(self, timestamp_utc: datetime, price_eur_per_mwh: float, position: int, resolution_minutes: int):
        self.timestamp_utc = timestamp_utc  # Start of the interval (UTC)
        self.price_eur_per_mwh = price_eur_per_mwh
        self.position = position  # Original position from API (1-based)
        self.resolution_minutes = resolution_minutes

    def __repr__(self):
        return f"PricePoint(ts='{self.timestamp_utc.isoformat()}', price={self.price_eur_per_mwh}, pos={self.position}, res={self.resolution_minutes}min)"


def fetch_entsoe_prices(target_day_local: datetime, entsoe_api_key: str) -> Optional[List[PricePoint]]:
    """
    Fetches day-ahead electricity prices from ENTSO-E for a given target day.
    The target_day_local is expected to be a datetime object representing the start of the day in the local timezone.
    Prices are  for the day after the auction closes (usually auction for D+1 happens on D).

    Returns:
        A list of PricePoint objects if successful, None otherwise.
    """
    if not entsoe_api_key:
        logger.error("ENTSO-E API key is not configured.")
        return None

    # ENTSO-E API expects periodStart and periodEnd in UTC
    # If target_day_local is for tomorrow, the period starts at 00:00 tomorrow local time
    # and ends at 00:00 the day after tomorrow local time.
    # We need to convert these local times to UTC for the API query.

    # Assume target_day_local is already the day for which prices are needed.
    # E.g., if today is Monday, and we want Tuesday's prices, target_day_local is Tuesday 00:00.

    # To correctly handle DST transitions for the period, we need the local timezone.
    # For simplicity, let's assume the system's local timezone.
    local_tz = datetime.now().astimezone().tzinfo

    period_start_local = target_day_local.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=local_tz)
    period_end_local = (target_day_local + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0,
                                                                      tzinfo=local_tz)

    period_start_utc_str = period_start_local.astimezone(timezone.utc).strftime('%Y%m%d%H%M')
    period_end_utc_str = period_end_local.astimezone(timezone.utc).strftime('%Y%m%d%H%M')

    params = {
        "documentType": DOCUMENT_TYPE_A44,
        "in_Domain": BELGIUM_DOMAIN,
        "out_Domain": BELGIUM_DOMAIN,
        "periodStart": period_start_utc_str,
        "periodEnd": period_end_utc_str,
        "securityToken": entsoe_api_key,
    }

    logger.info(f"Requesting ENTSO-E prices for period: {period_start_utc_str} UTC to {period_end_utc_str} UTC")

    response = ''
    try:
        response = requests.get(ENTSOE_API_BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        logger.debug(f"ENTSO-E API response status: {response.status_code}")
    except requests.exceptions.RequestException as e:
        logger.error(f"ENTSO-E API request failed: {e}")
        logger.debug(f"Request URL: {response.url if 'response' in locals() else ENTSOE_API_BASE_URL}")
        logger.debug(f"Response content: {response.content if 'response' in locals() and response else 'No response'}")
        return None

    try:
        return _parse_entsoe_price_xml(response.content, period_start_local)
    except Exception as e:
        logger.error(f"Failed to parse ENTSO-E XML response: {e}", exc_info=True)
        logger.debug(f"Problematic XML content: {response.content.decode('utf-8', errors='ignore')}")
        return None


def _parse_entsoe_price_xml(xml_content: bytes, period_start_local: datetime) -> Optional[List[PricePoint]]:
    """Helper function to parse the XML and handle DST/gaps."""
    try:
        root = ElTree.fromstring(xml_content)
    except ElTree.ParseError as e:
        logger.error(f"XML ParseError: {e}")
        return None

    # Namespace handling
    namespace = ''
    if '}' in root.tag:
        namespace = root.tag.split('}')[0][1:]  # e.g. urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0

    ns_map = {'ns': namespace} if namespace else {}

    # Find the 'Reason' element for error checking
    reason_element = root.find('.//ns:Reason/ns:text', ns_map) if namespace else root.find('.//Reason/text')
    if reason_element is not None and reason_element.text:
        # Common reason text when no data is available yet: No matching data found.
        if "No matching data found" in reason_element.text or \
                "No data available for the requested period" in reason_element.text:
            logger.warning(f"ENTSO-E: Data not yet available. Reason: {reason_element.text}")
            return []  # Return empty list to signify "not yet available" rather than a hard error
        else:
            logger.error(f"ENTSO-E API returned an error/reason: {reason_element.text}")
            return None  # Signifies an actual error condition

    time_series_elements = root.findall('.//ns:TimeSeries', ns_map) if namespace else root.findall('.//TimeSeries')
    if not time_series_elements:
        logger.warning("No TimeSeries found in ENTSO-E response.")
        # This could also mean "No matching data found" if the Reason element was missing
        # but the structure is otherwise valid but empty.
        return []

    all_price_points = []

    for ts_element in time_series_elements:
        period_element = ts_element.find('.//ns:Period', ns_map) if namespace else ts_element.find('.//Period')
        if period_element is None:
            continue

        resolution_str = period_element.findtext('.//ns:resolution', default='PT60M',
                                                 namespaces=ns_map if namespace else None)
        resolution_minutes = _parse_resolution_to_minutes(resolution_str)

        # The Period.timeInterval.start is crucial for anchoring the positions to actual UTC times
        # It should match the period_start_utc requested, but good to use what the API confirms.
        period_time_interval_start_str = period_element.findtext('.//ns:timeInterval/ns:start',
                                                                 namespaces=ns_map if namespace else None)
        if not period_time_interval_start_str:
            logger.error("Could not find Period.timeInterval.start in ENTSO-E response.")
            return None

        # Parse with timezone awareness
        # ENTSO-E timestamps are UTC time
        try:
            current_interval_start_utc = datetime.strptime(period_time_interval_start_str, '%Y-%m-%dT%H:%MZ').replace(
                tzinfo=timezone.utc)
        except ValueError:
            logger.error(f"Could not parse period start time: {period_time_interval_start_str}")
            return None

        logger.debug(
            f"Parsing Period starting at {current_interval_start_utc.isoformat()} with resolution {resolution_minutes} min")

        point_elements = period_element.findall('.//ns:Point', ns_map) if namespace else period_element.findall(
            './/Point')

        # Sort points by position in case they are out of order
        sorted_points_data = []
        for point_el in point_elements:
            pos_text = point_el.findtext('ns:position', namespaces=ns_map if namespace else None)
            price_text = point_el.findtext('ns:price.amount', namespaces=ns_map if namespace else None)
            if pos_text is not None and price_text is not None:
                try:
                    sorted_points_data.append({
                        "position": int(pos_text),
                        "price": float(price_text)
                    })
                except ValueError:
                    logger.warning(f"Could not parse position/price for point: pos='{pos_text}', price='{price_text}'")

        sorted_points_data.sort(key=lambda dp: dp["position"])

        for point_data in sorted_points_data:
            position = point_data["position"]
            price = point_data["price"]

            # Position is 1-based. Calculate timestamp for this point.
            # (position - 1) because first interval starts at current_interval_start_utc
            point_timestamp_utc = current_interval_start_utc + timedelta(minutes=(position - 1) * resolution_minutes)

            all_price_points.append(PricePoint(
                timestamp_utc=point_timestamp_utc,
                price_eur_per_mwh=price,
                position=position,
                resolution_minutes=resolution_minutes
            ))
            logger.debug(f"Parsed: Pos {position}, Price {price:.2f} @ {point_timestamp_utc.isoformat()}")

    if not all_price_points:
        logger.warning("No valid price points extracted from ENTSO-E response.")
        return []

    logger.info(f"Successfully fetched and parsed {len(all_price_points)} price points from ENTSO-E.")
    return all_price_points


def _parse_resolution_to_minutes(resolution_str: str) -> int:
    if resolution_str == 'PT60M':
        return 60
    elif resolution_str == 'PT30M':
        return 30
    elif resolution_str == 'PT15M':
        return 15
    else:
        logger.warning(f"Unknown resolution string: {resolution_str}. Defaulting to 60 minutes.")
        return 60


# --- Example Usage (for testing this module directly) ---
if __name__ == '__main__':
    # Load .env for API key if running directly
    from dotenv import load_dotenv

    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)

    entsoe_key = os.getenv("ENTSOE_API_KEY")
    if not entsoe_key:
        print("Please set ENTSOE_API_KEY in your .env file to test.")
    else:
        # Test for today's prices
        # test_target_day = (datetime.now()).replace(hour=0, minute=0, second=0, microsecond=0)
        # Test for tomorrow
        test_target_day = (datetime.now()).replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        # test_target_day = datetime(2023, 10, 29).replace(hour=0, minute=0, second=0, microsecond=0) # Fall DST
        # test_target_day = datetime(2024, 3, 31).replace(hour=0, minute=0, second=0, microsecond=0) # Spring DST

        print(f"Attempting to fetch prices for local day: {test_target_day.strftime('%Y-%m-%d')}")

        # Configure basic logging for standalone test
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        prices = fetch_entsoe_prices(test_target_day, entsoe_key)

        if prices is None:
            print("API call failed critically or bad API key.")
        elif not prices:  # Empty list
            print("No prices available yet for the target day, or no data found.")
        else:
            print(f"\nRetrieved {len(prices)} price points:")
            for p in prices:
                print(p)
