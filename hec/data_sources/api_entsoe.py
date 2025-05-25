# data_sources/api_entsoe.py
import logging
import os
from datetime import datetime, timedelta, timezone, time
from typing import List, Optional, Dict
from xml.etree import ElementTree as ElTree

import requests

from hec.core.models import PricePoint

logger = logging.getLogger(__name__)


def fetch_entsoe_prices(t_day_local: datetime, app_config: dict) -> Optional[List[PricePoint]]:
    """
    Fetches day-ahead electricity prices from ENTSO-E for a given target day.
    The t_day_local is expected to be a datetime object representing the start of the day in the local timezone.
    Prices are for the day after the auction closes (usually auction for D+1 happens on D).
    If entsoe_api_key is not provided, it attempts to load it from the environment.

    Returns:
        A list of PricePoint objects if successful, otherwise None.
    """

    entsoe_config = app_config['entsoe']
    auction_opening_hour = entsoe_config.get('auction_opening_hour')

    # Target day checks
    now_local = datetime.now().astimezone()
    tomorrow_local = now_local + timedelta(days=1)
    if (t_day_local.date() > tomorrow_local.date() or
            (t_day_local.date() == tomorrow_local.date() and now_local.hour < auction_opening_hour)):
        logger.info(
            f"Attempting to fetch prices for ({t_day_local.strftime('%Y-%m-%d')}) before auction opening time "
            f"({auction_opening_hour}:00 local). Data will not be available. Returning empty list.")
        return []

    entsoe_api_key = os.getenv("ENTSOE_API_KEY")
    if not entsoe_api_key:
        logger.warning("ENTSO-E API key not found in environment variable ENTSOE_API_KEY.")
        return None

    # ENTSO-E API expects periodStart and periodEnd in UTC
    # If t_day_local is for tomorrow, the period starts at 00:00 tomorrow local time
    # and ends at 00:00 the day after tomorrow local time. These can be two different time zones (DST)
    # and need to be converted to UTC. To correctly handle we need the local timezone.

    period_start_tz = datetime.combine(t_day_local, time.min).astimezone().tzinfo
    period_end_tz = datetime.combine(t_day_local, time.max).astimezone().tzinfo

    period_start_local = datetime.combine(t_day_local, time.min, tzinfo=period_start_tz)
    period_end_local = datetime.combine(t_day_local + timedelta(days=1), time.min, tzinfo=period_end_tz)

    period_start_utc_str = period_start_local.astimezone(timezone.utc).strftime('%Y%m%d%H%M')
    period_end_utc_str = period_end_local.astimezone(timezone.utc).strftime('%Y%m%d%H%M')

    params = {
        "documentType": entsoe_config.get('document_type'),
        "in_Domain": entsoe_config.get('domain'),
        "out_Domain": entsoe_config.get('domain'),
        "periodStart": period_start_utc_str,
        "periodEnd": period_end_utc_str,
        "securityToken": entsoe_api_key,
    }

    logger.info(f"Requesting ENTSO-E prices for period: {period_start_utc_str} UTC to {period_end_utc_str} UTC")

    response = ''
    try:
        response = requests.get(entsoe_config.get('api_base_url'), params=params, timeout=30)
        response.raise_for_status()
        logger.debug(f"ENTSO-E API response status: {response.status_code}")
    except requests.exceptions.Timeout:
        logger.info(f"ENTSO-E API request timed out: {entsoe_config.get('api_base_url')}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"ENTSO-E API request failed: {e}")
        logger.debug(f"Request URL: {response.url if response else entsoe_config.get('api_base_url')}")
        logger.debug(f"Response content: {response.content if 'response' in locals() and response else 'No response'}")
        return None

    try:
        return _parse_entsoe_price_xml(response.content)
    except Exception as e:
        logger.error(f"Failed to parse ENTSO-E XML response: {e}", exc_info=True)
        logger.debug(f"Problematic XML content: {response.content.decode('utf-8', errors='ignore')}")
        return None


def _parse_entsoe_price_xml(xml_content: bytes) -> Optional[List[PricePoint]]:
    """
    Helper function to parse the XML and handle DST/gaps.
    Returns a list of PricePoint objects if successful, an empty list if no data was received and
    None in case of failure.
    """
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
            logger.info(f"ENTSO-E: Data not yet available. Reason: {reason_element.text}")
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

        # The Period.timeInterval.start is crucial for anchoring the positions to actual UTC times.
        # It should match the period_start_utc requested, but better to use what the API confirms.
        period_time_interval_start_str = period_element.findtext('.//ns:timeInterval/ns:start',
                                                                 namespaces=ns_map if namespace else None)
        period_time_interval_end_str = period_element.findtext('.//ns:timeInterval/ns:end',
                                                               namespaces=ns_map if namespace else None)
        if not period_time_interval_start_str or not period_time_interval_end_str:
            logger.error("Could not find Period.timeInterval.start or Period.timeInterval.end in ENTSO-E response. "
                         "Skipping period.")
            return None

        # ENTSO-E timestamps are UTC time
        try:
            current_interval_start_utc = datetime.strptime(period_time_interval_start_str, '%Y-%m-%dT%H:%MZ').replace(
                tzinfo=timezone.utc)
            period_interval_end_utc = datetime.strptime(period_time_interval_end_str, '%Y-%m-%dT%H:%MZ').replace(
                tzinfo=timezone.utc)
        except ValueError:
            logger.error(f"Could not parse period start/end time: start='{period_time_interval_start_str}', "
                         f"end='{period_time_interval_end_str}'. Skipping period.")
            return None

        # ENTSO_E doesn't give a price point if the price did not change. We want to copy the previous for consistency.
        duration_seconds = (period_interval_end_utc - current_interval_start_utc).total_seconds()
        if duration_seconds < 0:
            logger.error(f"Period end time {period_interval_end_utc} before start time {current_interval_start_utc}.")
            continue
        expected_total_positions = int(duration_seconds / (resolution_minutes * 60))
        logger.debug(
            f"Parsing Period from {current_interval_start_utc.isoformat()} to {period_interval_end_utc.isoformat()} "
            f"with resolution {resolution_minutes} min. Expecting {expected_total_positions} points.")

        point_elements = period_element.findall('.//ns:Point', ns_map) if namespace else period_element.findall(
            './/Point')

        # Sort points by position in case they are out of order
        parsed_points_data: Dict[int, float] = {}
        for point_el in point_elements:
            pos_text = point_el.findtext('ns:position', namespaces=ns_map if namespace else None)
            price_text = point_el.findtext('ns:price.amount', namespaces=ns_map if namespace else None)
            if pos_text is not None and price_text is not None:
                try:
                    position = int(pos_text)
                    price = float(price_text)
                    parsed_points_data[position] = price
                except ValueError:
                    logger.warning(f"Could not parse position/price for point: pos='{pos_text}', price='{price_text}'")

        current_period_price_points: List[PricePoint] = []
        last_known_price: Optional[float] = None

        for current_pos in range(1, expected_total_positions + 1):
            point_timestamp_utc = current_interval_start_utc + timedelta(minutes=(current_pos - 1) * resolution_minutes)
            price_to_use: Optional[float] = None

            if current_pos in parsed_points_data:
                price_to_use = parsed_points_data[current_pos]
                last_known_price = price_to_use
                logger.debug(f"Using pos {current_pos} price {price_to_use:.2f} @ {point_timestamp_utc.isoformat()}")
            elif last_known_price is not None:  # Gap, but we have a previous price
                price_to_use = last_known_price
                logger.debug(f"FILLING GAP: Pos {current_pos}, Price {price_to_use:.2f} "
                             f"(carried from pos {current_pos - 1}) @ {point_timestamp_utc.isoformat()}")
            else:
                # Could first price of the D be equal to last price of D-1? Will it be empty?
                logger.error(
                    f"Cannot fill gap at position {current_pos} for period {current_interval_start_utc.isoformat()}: "
                    f"No preceding price found. Aborting parse for this TimeSeries.")
                current_period_price_points.clear()
                break

            if price_to_use is not None:
                current_period_price_points.append(PricePoint(
                    timestamp_utc=point_timestamp_utc,
                    price_eur_per_mwh=price_to_use,
                    position=current_pos,
                    resolution_minutes=resolution_minutes
                ))

        if len(current_period_price_points) == expected_total_positions:
            all_price_points.extend(current_period_price_points)
        elif current_period_price_points:
            logger.warning(
                f"Failed to construct a complete set of points for period {current_interval_start_utc.isoformat()}. "
                f"Expected {expected_total_positions}, got {len(current_period_price_points)}. Discarding.")

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

# --- Example for testing ---
# if __name__ == '__main__':
#     from hec.core.app_initializer import load_app_config
#     logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#     logger_main = logging.getLogger(__name__)
#     APP_CONFIG = load_app_config()
#     test_day = (datetime.now()).replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
#     test_day_winter = datetime(2025, 1, 1).replace(hour=0, minute=0, second=0, microsecond=0)
#     fall_dst_day = datetime(2024, 10, 27).replace(hour=0, minute=0, second=0, microsecond=0)
#     spring_dst_day = datetime(2025, 3, 30).replace(hour=0, minute=0, second=0, microsecond=0)
#     test_target_day = fall_dst_day
#     print(f"Attempting to fetch prices for local day: {test_target_day.strftime('%Y-%m-%d')}")
#
#     # Configure basic logging for standalone test
#
#     prices = fetch_entsoe_prices(test_target_day, APP_CONFIG)
#
#     if prices is None:
#         print("API call failed critically or bad API key.")
#     elif not prices:  # Empty list
#         print("No prices available yet for the target day, or no data found.")
#     else:
#         print(f"\nRetrieved {len(prices)} price points:")
#         for p in prices:
#             print(p)
