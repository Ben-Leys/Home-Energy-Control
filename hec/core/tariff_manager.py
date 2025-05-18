# hec/core/tariff_manager.py
import yaml
import logging
from datetime import date, datetime
from typing import List, Dict, Optional, Any
from pathlib import Path

logger = logging.getLogger(__name__)

# Default path to the tariffs file, relative to project root
DEFAULT_TARIFFS_FILE_NAME = "tariffs.yaml"
PROJECT_ROOT = Path(__file__).resolve().parent.parent


class TariffManager:
    def __init__(self, app_config: Dict):
        self.tariffs_file_path = PROJECT_ROOT / app_config.get('application').get('tariffs_file_name',
                                                                                  DEFAULT_TARIFFS_FILE_NAME)
        self.all_tariffs: Dict[str, Dict[str, Any | Dict[str, Any]]] = {}
        self._load_tariffs()

    def _load_tariffs(self):
        """Loads tariffs from YAML and processes"""

        if not self.tariffs_file_path.exists():
            logger.error(f"Tariff file '{self.tariffs_file_path}' not found.")
            return

        try:
            with open(self.tariffs_file_path, 'r') as f:
                raw_tariffs = yaml.safe_load(f)

            if not raw_tariffs:
                logger.warning(f"Tariff file '{self.tariffs_file_path}' is empty.")
                return

            # Process energy supplier section
            energy_supplier_tariffs = {}
            for contract_type, tariffs in raw_tariffs.get("energy_supplier", {}).items():
                energy_supplier_tariffs[contract_type] = self.process_section(tariffs)

            # Process other sections
            self.all_tariffs = {
                "contract_types": raw_tariffs.get("contract_types", []),
                "active_contract": raw_tariffs.get("active_contract", {}),
                "energy_supplier": energy_supplier_tariffs,
                "grid_operator": self.process_section(raw_tariffs.get("grid_operator", {})),
                "government": self.process_section(raw_tariffs.get("government", {})),
            }
            logger.info(f"Tariffs successfully loaded from '{self.tariffs_file_path}'.")

        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML tariff file '{self.tariffs_file_path}': {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Unexpected error loading tariffs: {e}", exc_info=True)

    @staticmethod
    def process_section(section: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        """Process and sort a section by start_date for each key."""
        processed_section = {}
        for key, value_list in section.items():
            processed_section[key] = sorted(value_list, key=lambda x: x["start_date"], reverse=True)
        return processed_section

    @staticmethod
    def _find_active(target_date: date, section: List[Dict[str, Any]]) -> Optional[Any]:
        """Find the active item for a given date."""
        for item in section:
            if date.fromisoformat(item["start_date"]) <= target_date:
                return item["value"]
        logger.warning(f"No active period found for date {target_date} in provided list.")
        return None

    def get_active_contract_type(self, target_date: date) -> Optional[str]:
        """Get the active contract type for a given date."""
        contract = self._find_active(target_date, self.all_tariffs.get("active_contract", []))
        if contract:
            return contract
        logger.warning(f"No active contract type found for date {target_date}.")
        return None

    def get_all_tariffs(self, target_date: date) -> Dict[str, Any]:
        """Get all active tariffs, including all energy_supplier contracts and the active one."""
        contract_type = self.get_active_contract_type(target_date)
        if not contract_type:
            logger.warning(f"No active contract type found for {target_date}.")
            return {}

        active_tariffs = {"active_contract": contract_type,
                          "energy_supplier": {contract_type: {} for contract_type in
                                              self.all_tariffs["contract_types"]},
                          "grid_operator": {}, "government": {}}

        # Process energy supplier tariffs
        for contract_type in self.all_tariffs["contract_types"]:
            contract_tariffs = self.all_tariffs["energy_supplier"].get(contract_type, {})
            for key, key_data in contract_tariffs.items():
                active_value = self._find_active(target_date, key_data)
                if active_value is not None:
                    active_tariffs["energy_supplier"][contract_type][key] = active_value

        # Process grid operator tariffs
        for key, key_data in self.all_tariffs["grid_operator"].items():
            active_value = self._find_active(target_date, key_data)
            if active_value is not None:
                active_tariffs["grid_operator"][key] = active_value

        # Process government tariffs
        for key, key_data in self.all_tariffs["government"].items():
            active_value = self._find_active(target_date, key_data)
            if active_value is not None:
                active_tariffs["government"][key] = active_value

        return active_tariffs


def initialize_tariff_manager(app_config: Dict) -> TariffManager:
    return TariffManager(app_config)


# Only for testing
# if __name__ == "__main__":
    # from hec.core.app_initializer import load_app_config
    # config = {}
    # config = load_app_config()
    # tm = initialize_tariff_manager(config)
    # print("Tariffs for 01/01/2024")
    # print(tm.get_all_tariffs(date(2024, 1, 1)))
    # print("\nTariffs for 01/01/2025")
    # print(tm.get_all_tariffs(date(2025, 1, 1)))
