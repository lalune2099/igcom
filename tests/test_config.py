import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


class ConfigTests(unittest.TestCase):
    def test_reads_selected_ig_account(self):
        import config

        env = {
            "IG_ACCOUNT2_USERNAME": "user2",
            "IG_ACCOUNT2_PASSWORD": "pass2",
            "IG_ACCOUNT2_API_KEY": "key2",
            "IG_ACCOUNT2_ACC_TYPE": "DEMO",
        }

        with patch.dict(os.environ, env, clear=True), patch.object(config, "_load_dotenv_files", lambda: None):
            account = config.get_ig_account("ACCOUNT2")

        self.assertEqual(account.username, "user2")
        self.assertEqual(account.password, "pass2")
        self.assertEqual(account.api_key, "key2")
        self.assertEqual(account.acc_type, "DEMO")

    def test_ig_profile_env_overrides_default(self):
        import config

        env = {
            "IG_PROFILE": "ACCOUNT4",
            "IG_ACCOUNT4_USERNAME": "user4",
            "IG_ACCOUNT4_PASSWORD": "pass4",
            "IG_ACCOUNT4_API_KEY": "key4",
        }

        with patch.dict(os.environ, env, clear=True), patch.object(config, "_load_dotenv_files", lambda: None):
            account = config.get_ig_account("ACCOUNT1")

        self.assertEqual(account.username, "user4")
        self.assertEqual(account.acc_type, "LIVE")

    def test_missing_required_ig_value_raises_clear_error(self):
        import config

        with patch.dict(os.environ, {"IG_ACCOUNT1_USERNAME": "user1"}, clear=True), patch.object(
            config, "_load_dotenv_files", lambda: None
        ):
            with self.assertRaisesRegex(RuntimeError, "IG_ACCOUNT1_PASSWORD"):
                config.get_ig_account("ACCOUNT1")

    def test_reads_gmail_config(self):
        import config

        env = {
            "GMAIL_USER": "sender",
            "GMAIL_APP_PASSWORD": "app-password",
            "GMAIL_RECIPIENTS": "first; second",
        }

        with patch.dict(os.environ, env, clear=True), patch.object(config, "_load_dotenv_files", lambda: None):
            gmail = config.get_gmail_config()

        self.assertEqual(gmail.send_usr, "sender")
        self.assertEqual(gmail.send_pwd, "app-password")
        self.assertEqual(gmail.receive_usr_list, ["first", "second"])
        self.assertEqual(gmail.email_server, "smtp.gmail.com")
        self.assertEqual(gmail.email_port, 587)


if __name__ == "__main__":
    unittest.main()
