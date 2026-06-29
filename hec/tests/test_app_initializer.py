import unittest
from pathlib import Path
from unittest.mock import patch

from hec.core import app_initializer


class TestAppInitializerConfig(unittest.TestCase):
    def write_config_fixture(self, file_name, content):
        scratch_root = Path.cwd() / "_scratch"
        scratch_root.mkdir(exist_ok=True)
        config_path = scratch_root / file_name
        config_path.write_text(content, encoding="utf-8")
        self.addCleanup(config_path.unlink, missing_ok=True)
        return scratch_root

    def test_load_app_config_rejects_empty_config_file(self):
        config_file_name = "app-initializer-empty.yaml"
        config_dir = self.write_config_fixture(config_file_name, "")

        with (
            patch.object(app_initializer, "BASE_DIR", config_dir),
            patch.object(app_initializer, "CONFIG_FILE_NAME", config_file_name),
        ):
            with self.assertRaisesRegex(ValueError, "is empty"):
                app_initializer.load_app_config()

    def test_load_app_config_rejects_non_mapping_yaml(self):
        config_file_name = "app-initializer-list.yaml"
        config_dir = self.write_config_fixture(config_file_name, "- application\n- database\n")

        with (
            patch.object(app_initializer, "BASE_DIR", config_dir),
            patch.object(app_initializer, "CONFIG_FILE_NAME", config_file_name),
        ):
            with self.assertRaisesRegex(ValueError, "must contain a YAML mapping"):
                app_initializer.load_app_config()


if __name__ == "__main__":
    unittest.main()
