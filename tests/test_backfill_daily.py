import types
import unittest
from datetime import datetime, timedelta, timezone


class BackfillDailyTests(unittest.TestCase):
    def test_configure_job_module_uses_simulated_beijing_date_and_disables_email(self):
        from backfill_daily import configure_job_module

        job = types.SimpleNamespace(
            datetime=datetime,
            RUN_DATE="20260512",
            UPDATED_TEMPLATE_FILE_NAME="old-template.xlsx",
            FILLED_OUTPUT_FILE_NAME="old-filled.xlsx",
            SEND_EMAIL=True,
            TZ_LONDON=timezone(timedelta(hours=1)),
        )
        shanghai = timezone(timedelta(hours=8))
        london = timezone(timedelta(hours=1))
        simulated = datetime(2026, 5, 9, 4, 5, 0, tzinfo=shanghai)

        run_date = configure_job_module(job, simulated)

        self.assertEqual("20260509", run_date)
        self.assertEqual("20260509", job.RUN_DATE)
        self.assertEqual("IG变化率_模版更新_20260509.xlsx", job.UPDATED_TEMPLATE_FILE_NAME)
        self.assertEqual("IG变化率_20260509.xlsx", job.FILLED_OUTPUT_FILE_NAME)
        self.assertFalse(job.SEND_EMAIL)
        self.assertEqual(simulated, job.datetime.now(shanghai))
        self.assertEqual(
            simulated.astimezone(london),
            job.datetime.now(london),
        )
        self.assertIsInstance(datetime(2026, 5, 8, 0, 0, 0), job.datetime)

    def test_parse_beijing_time_rejects_wrong_format(self):
        from backfill_daily import parse_beijing_time

        with self.assertRaises(ValueError):
            parse_beijing_time("2026/05/09 04:05:00")


if __name__ == "__main__":
    unittest.main()
