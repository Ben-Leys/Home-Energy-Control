# hec/logic_engine/utils.py
import logging
import os
import smtplib
from datetime import date, datetime, timedelta
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Optional, Tuple

from astral import LocationInfo
from astral.sun import sun

from hec.core.app_state import GLOBAL_APP_STATE
from hec.core.models import NetElectricityPriceInterval
from hec.database_ops.db_handler import DatabaseHandler
from hec.logic_engine.cost_calculator import calculate_net_intervals_for_day

logger = logging.getLogger(__name__)


def get_interval_from_list(target_local: datetime, intervals: List[NetElectricityPriceInterval]) \
        -> Optional[NetElectricityPriceInterval]:
    """
    Finds the active NetElectricityPriceInterval for the given 'target_local' datetime.

    Args:
        target_local: The datetime for which the active interval is being sought.
        intervals: A list of NetElectricityPriceInterval objects.

    Returns:
        The active NetElectricityPriceInterval if found, otherwise None.
    """
    for interval in intervals:
        try:
            interval_start = interval.interval_start_local

            # Ensure timezone alignment between target_local and interval_start
            if target_local.tzinfo and interval_start.tzinfo is None:
                interval_start = interval_start.replace(tzinfo=target_local.tzinfo)

            interval_end = interval_start + timedelta(minutes=interval.resolution_minutes)

            if interval_start <= target_local < interval_end:
                return interval
        except Exception as e:
            logger.error(f"Error processing interval: {interval}. Error: {e}", exc_info=True)

    logger.warning(f"No active interval found for {target_local.isoformat()} in the provided list.")
    return None


def process_price_points_to_app_state(price_points: list, target_day: datetime,
                                      app_state_key: str, app_config, db_handler: DatabaseHandler = None):
    """
    Processes price points by storing them in the database in raw format and updating the AppState with NEPI's.

    Args:
        price_points (list): List of price points retrieved from the API.
        target_day (datetime): The target day for the price points (timezone-aware).
        app_state_key (str): The key under which to store the processed price points in the AppState.
        app_config: Dict with application configuration data.
        db_handler (DatabaseHandler): Database handler for storing the price points if necessary.

    Returns:
        True in case of success, False in case of failure.
    """
    if price_points is None:
        # No error logging. Caught by calling functions
        GLOBAL_APP_STATE.set(app_state_key, [])
        return False
    elif not price_points:
        logger.debug(f"No price points available for {target_day.date()} (API data not yet published).")
        GLOBAL_APP_STATE.set(app_state_key, [])
        return False

    logger.info(f"Processing {len(price_points)} price points for {target_day.date()}.")

    # Store raw price points in the database
    if db_handler:
        db_handler.store_da_prices(price_points)
        logger.debug(f"Stored {len(price_points)} price points in the database.")

    # Convert and process price points for the AppState
    nepis = calculate_net_intervals_for_day(db_handler, app_config, target_day, price_points)

    # Update AppState
    GLOBAL_APP_STATE.set(app_state_key, nepis)
    logger.info(f"Updated AppState with {len(nepis)} price points for '{app_state_key}'.")

    return True


def is_daylight(app_config: dict) -> bool:
    """Checks if it's currently daylight hours based on configured location."""
    location_config = app_config.get('inverter').get('location')
    if not location_config or not all(k in location_config for k in ['latitude', 'longitude', 'timezone']):
        logger.warning("Location for sunrise/sunset calculation not fully configured. Assuming daylight.")
        return True

    now_dt_aware = datetime.now().astimezone()
    city = LocationInfo(location_config['city'],
                        location_config['region_name_for_astral_optional'],
                        location_config['timezone'],
                        location_config['latitude'],
                        location_config['longitude'])
    s = sun(city.observer, date=now_dt_aware.date(), tzinfo=city.timezone)

    sunrise_local = s["sunrise"]
    sunset_local = s["sunset"]

    is_light = sunrise_local <= now_dt_aware <= sunset_local
    logger.debug(f"Daylight check: Now={now_dt_aware.strftime('%H:%M')}, Sunrise={sunrise_local.strftime('%H:%M')}, "
                 f"Sunset={sunset_local.strftime('%H:%M')} -> Is Daylight: {is_light}")
    return is_light


def send_email_with_attachments(
        smtp_config: dict,
        sender_email: str,
        recipients: List[str],
        subject: str,
        html_body: str,
        images: Optional[List[Tuple[bytes, str, str]]] = None
) -> bool:
    """
    Sends an email with HTML body and optional image attachments.

    Args:
        smtp_config (dict): SMTP server details {'host', 'port', 'user'}
        sender_email (str): The sender's email address.
        recipients (List[str]): List of recipient email addresses.
        subject (str): Email subject.
        html_body (str): HTML content for the email body.
        images (Optional[List[Tuple[bytes, str, str]]]):
            List of image attachments. Each tuple: (image_bytes, filename.png, content_id_for_cid)
    """
    if not smtp_config.get('host') or not sender_email or not recipients:
        logger.error("Email sending failed: SMTP config, sender, or recipients missing.")
        return False

    msg = MIMEMultipart('related')
    msg['From'] = sender_email
    msg['To'] = ", ".join(recipients)
    msg['Subject'] = subject

    msg_alternative = MIMEMultipart('alternative')
    msg.attach(msg_alternative)

    # HTML body
    msg_text_html = MIMEText(html_body, 'html', 'utf-8')
    msg_alternative.attach(msg_text_html)

    # Images
    if images:
        for img_bytes, filename, img_cid in images:
            try:
                img = MIMEImage(img_bytes, name=filename)
                img.add_header('Content-ID', f'<{img_cid}>')  # For cid: in HTML
                img.add_header('Content-Disposition', 'inline', filename=filename)
                msg.attach(img)
            except Exception as e:
                logger.error(f"Failed to attach image {filename}: {e}")

    try:
        port = smtp_config.get('port', 465)
        server = smtplib.SMTP_SSL(smtp_config['host'], port, timeout=120)

        if smtp_config.get('user') and os.getenv('GMAIL_SMTP_PASSWORD'):
            server.login(smtp_config['user'], os.getenv('GMAIL_SMTP_PASSWORD'))
            server.sendmail(sender_email, recipients, msg.as_string())
            server.quit()
            logger.info(f"Email '{subject}' sent successfully to {recipients}.")
            return True
        else:
            logger.warning(f"Email sending failed: SMTP config, user, or password missing.")

    except Exception as e:
        logger.error(f"Failed to send email '{subject}': {e}", exc_info=True)
        return False


def is_a_holiday(t_date: [date, datetime]):
    if isinstance(t_date, datetime):
        t_date = t_date.date()
    elif not isinstance(t_date, date):
        raise TypeError("t_date must be of type date or datetime.")

    holidays = {
        date(t_date.year, 1, 1),  # New Year's Day
        date(t_date.year, 5, 1),  # Labor Day
        date(t_date.year, 7, 21),  # National Day
        date(t_date.year, 8, 15),  # Assumption Day
        date(t_date.year, 11, 1),  # All Saints' Day
        date(t_date.year, 11, 11),  # Armistice Day
        date(t_date.year, 12, 25),  # Christmas Day
    }

    holidays.add(calculate_easter(t_date.year) + timedelta(days=1))  # Easter Monday
    holidays.add(calculate_easter(t_date.year) + timedelta(days=39))  # Ascension Day
    holidays.add(calculate_easter(t_date.year) + timedelta(days=50))  # Pentecost Monday

    return t_date in holidays


def calculate_easter(year: int):
    """Computes the date of Easter Sunday for the given year."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = (19 * a + b - b // 4 - ((b - (b + 8) // 25 + 1) // 3) + 15) % 30
    e = (32 + 2 * (b % 4) + 2 * (c // 4) - d - (c % 4)) % 7
    f = d + e - 7 * ((a + 11 * d + 22 * e) // 451) + 114
    month = f // 31
    day = f % 31 + 1
    return date(year, month, day)


def convert_power(current_a: Optional[float] = None, power_kw: Optional[float] = None) -> float:
    """
    Convert between power (kW) and current (A) for a single-phase system.
    Voltage: 230 V. Power Factor: 1.0

    Args:
        current_a (Optional[float]): Current in amperes.
        power_kw (Optional[float]): Power in kilowatts.

    Returns:
        float: The converted value (kW if current_a is given, A if power_kw is given).
    """
    voltage = 230
    power_factor = 1.0

    if current_a is not None and power_kw is None:
        # Convert current (A) to power (kW)
        return current_a * voltage * power_factor / 1000  # Convert W to kW
    elif power_kw is not None and current_a is None:
        # Convert power (kW) to current (A)
        return power_kw * 1000 / (voltage * power_factor)  # Convert kW to W and calculate A
    else:
        logger.error("Provide exactly one parameter: either 'current_a' or 'power_kw'.")


# if __name__ == '__main__':
#     import os
#     from dotenv import load_dotenv
#     from pathlib import Path
#
#     BASE_DIR = Path(__file__).resolve().parent.parent
#     env_path = BASE_DIR / ".env"
#     load_dotenv(dotenv_path=env_path)
#
#     logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#     test_config = {'inverter': {'location': {'latitude': 51.05483, 'longitude': 4.62877,
#                                              'timezone': 'Europe/Brussels',
#                                              'region_name_for_astral_optional': 'Belgium'}}}
#     print(is_daylight(test_config))
#     test_config = {"host": "smtp.gmail.com", "port": 465,
#                    "user": "***REMOVED***", "sender_email": "***REMOVED***",
#                    "default_recipients": ["***REMOVED***", "***REMOVED***"]}
#     print(send_email_with_attachments(test_config, test_config['sender_email'], test_config['default_recipients'],
#                                       'Test Email', html_body='<html><body></body></html>'))
