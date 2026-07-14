import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


class CombinedMonthlyReportsTests(unittest.TestCase):
    @patch("combined_monthly_reports.build_detailed_monthly_report")
    @patch("combined_monthly_reports.build_monthly_formula_report")
    def test_builds_both_reports_without_loading_email_config(
        self,
        build_formula,
        build_detailed,
    ):
        from combined_monthly_reports import build_and_send_combined_monthly_reports

        build_formula.return_value = "/reports/formula.xlsx"
        build_detailed.return_value = "/reports/detailed.xlsx"
        formula_output = str(Path("/reports") / "IG变化率_202605_公式版.xlsx")
        detailed_output = str(Path("/reports") / "IG变化率_202605_详细版.xlsx")

        with patch("config.get_gmail_config") as get_gmail_config:
            result = build_and_send_combined_monthly_reports(
                output_root_dir="/outputs",
                report_dir="/reports",
                report_month="202605",
                send_email=False,
                template_file="/template.xlsx",
            )

        self.assertEqual(("/reports/formula.xlsx", "/reports/detailed.xlsx"), result)
        get_gmail_config.assert_not_called()
        build_formula.assert_called_once_with(
            output_root_dir="/outputs",
            output_file=formula_output,
            report_month="202605",
        )
        build_detailed.assert_called_once_with(
            output_root_dir="/outputs",
            report_dir="/reports",
            output_file=detailed_output,
            report_month="202605",
            template_file="/template.xlsx",
        )

    @patch("combined_monthly_reports.send_gmail_with_attachments")
    @patch("combined_monthly_reports.build_detailed_monthly_report")
    @patch("combined_monthly_reports.build_monthly_formula_report")
    def test_sends_both_reports_in_one_email(
        self,
        build_formula,
        build_detailed,
        send_email,
    ):
        from combined_monthly_reports import build_and_send_combined_monthly_reports

        build_formula.return_value = "/reports/formula.xlsx"
        build_detailed.return_value = "/reports/detailed.xlsx"
        send_email.return_value = True
        gmail_config = SimpleNamespace(
            send_usr="sender@example.com",
            send_pwd="password",
            receive_usr_list=["to@example.com"],
            email_server="smtp.example.com",
            email_port=587,
        )

        with patch("config.get_gmail_config", return_value=gmail_config):
            build_and_send_combined_monthly_reports(
                report_dir="/reports",
                report_month="202605",
                send_email=True,
            )

        send_email.assert_called_once()
        kwargs = send_email.call_args.kwargs
        self.assertEqual(
            ["/reports/formula.xlsx", "/reports/detailed.xlsx"],
            kwargs["attachment_paths"],
        )
        self.assertEqual("IG变化率月报（公式版+详细版） - 202605", kwargs["email_title"])
        self.assertIn("月度累计公式版", kwargs["content"])
        self.assertIn("月度详细版", kwargs["content"])

    @patch("combined_monthly_reports.send_gmail_with_attachments")
    @patch("combined_monthly_reports.build_detailed_monthly_report")
    @patch("combined_monthly_reports.build_monthly_formula_report")
    def test_reports_single_email_failure(
        self,
        build_formula,
        build_detailed,
        send_email,
    ):
        from combined_monthly_reports import build_and_send_combined_monthly_reports

        build_formula.return_value = "/reports/formula.xlsx"
        build_detailed.return_value = "/reports/detailed.xlsx"
        send_email.return_value = False
        gmail_config = SimpleNamespace(
            send_usr="sender@example.com",
            send_pwd="password",
            receive_usr_list=["to@example.com"],
            email_server="smtp.example.com",
            email_port=587,
        )

        with patch("config.get_gmail_config", return_value=gmail_config):
            with self.assertRaisesRegex(RuntimeError, "email sending failed"):
                build_and_send_combined_monthly_reports(
                    report_dir="/reports",
                    report_month="202605",
                    send_email=True,
                )

        send_email.assert_called_once()


if __name__ == "__main__":
    unittest.main()
