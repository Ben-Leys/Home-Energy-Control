import logging

from hec.core.app_state import GLOBAL_APP_STATE
from datetime import datetime, timedelta
from typing import Optional, List


logger = logging.getLogger(__name__)

class MarketContext:
    def __init__(self):
        self.buy_price: float = 0.0
        self.sell_price: float = 0.0
        self.next_update_at: Optional[datetime] = None
        self.is_fixed_contract: bool = False

    def refresh_if_needed(self) -> bool:
        now = datetime.now().astimezone()

        if self.next_update_at and now < self.next_update_at:
            return True

        intervals = GLOBAL_APP_STATE.get('electricity_prices_today', [])
        current = self._find_active_interval(now, intervals)

        if not current:
            return False

        contract = current.active_contract_type
        if contract == 'fixed':
            self.is_fixed_contract = True
            return False

        prices = current.net_prices_eur_per_kwh.get(contract, {})
        self.buy_price = prices.get('buy', 0.0)
        self.sell_price = prices.get('sell', 0.0)

        # Calculate expiry: Start + Resolution
        self.next_update_at = (current.interval_start_local.astimezone() +
                               timedelta(minutes=current.resolution_minutes)).replace(second=0)
        return True

    @staticmethod
    def _find_active_interval(now, intervals):
        for inv in intervals:
            start = inv.interval_start_local.astimezone()
            if start <= now < (start + timedelta(minutes=inv.resolution_minutes)):
                return inv
        return None

if __name__ == '__main__':
    import pytz
    from hec.core.app_initializer import load_app_config
    from hec.database_ops.db_handler import DatabaseHandler
    from hec.utils.utils import process_price_points_to_app_state
    from zoneinfo import ZoneInfo

    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger_main = logging.getLogger(__name__)

    config = load_app_config()
    local_tz = ZoneInfo(config['scheduler']['timezone'])
    db_handler = DatabaseHandler(config['database'])
    db_handler.initialize_database()

    # Fill app_state with NEPIs from PricePoints in database
    first_day_start = datetime(2026, 3, 21, 23, 00, 0, tzinfo=pytz.UTC)
    first_day_end = datetime(2026, 3, 22, 22, 45, 0, tzinfo=pytz.UTC)
    price_points = db_handler.get_da_prices(first_day_start.astimezone(local_tz))
    process_price_points_to_app_state(price_points, first_day_start, "electricity_prices_today", config, db_handler)

    market_context = MarketContext()
    print(market_context.refresh_if_needed())
    print(market_context.buy_price)
    print(market_context.sell_price)
    print(market_context.next_update_at)
    print(market_context.is_fixed_contract)
