# -*- coding: utf-8 -*-
"""Generate and optionally email both monthly report variants in one run."""

import argparse
import os
from pathlib import Path
from typing import Optional, Tuple

from detailed_monthly_report import (
    DEFAULT_TEMPLATE_FILE,
    build_detailed_monthly_report,
)
from monthly_report import (
    MONTHLY_REPORT_DIR,
    OUTPUT_ROOT_DIR,
    _parse_report_month,
    build_monthly_formula_report,
    send_gmail_with_attachments,
)


def build_and_send_combined_monthly_reports(
    output_root_dir: str = OUTPUT_ROOT_DIR,
    report_dir: str = MONTHLY_REPORT_DIR,
    report_month: Optional[str] = None,
    send_email: bool = True,
    template_file: str = DEFAULT_TEMPLATE_FILE,
) -> Tuple[str, str]:
    """Build both monthly reports, then send them in one email."""
    month = _parse_report_month(report_month)
    report_path = Path(report_dir)

    formula_file = build_monthly_formula_report(
        output_root_dir=output_root_dir,
        output_file=str(report_path / f"IG变化率_{month}_公式版.xlsx"),
        report_month=month,
    )
    detailed_file = build_detailed_monthly_report(
        output_root_dir=output_root_dir,
        report_dir=report_dir,
        output_file=str(report_path / f"IG变化率_{month}_详细版.xlsx"),
        report_month=month,
        template_file=template_file,
    )

    if send_email:
        from config import get_gmail_config

        gmail_config = get_gmail_config()
        sent = send_gmail_with_attachments(
            send_usr=gmail_config.send_usr,
            send_pwd=gmail_config.send_pwd,
            receive_usr_list=gmail_config.receive_usr_list,
            attachment_paths=[formula_file, detailed_file],
            email_title=f"IG变化率月报（公式版+详细版） - {month}",
            content=(
                "这是当天生成的IG变化率月报，邮件包含月度累计公式版和月度详细版"
                "两个附件，请查收。"
            ),
            email_server=gmail_config.email_server,
            email_port=gmail_config.email_port,
        )
        if not sent:
            raise RuntimeError("Both reports were generated, but email sending failed")

    return formula_file, detailed_file


def _send_email_default() -> bool:
    value = os.getenv(
        "COMBINED_MONTHLY_REPORT_SEND_EMAIL",
        os.getenv("MONTHLY_REPORT_SEND_EMAIL", "true"),
    )
    return value.strip().lower() not in {"0", "false", "no", "off"}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build and send the formula and detailed monthly reports."
    )
    parser.add_argument("--report-month", help="Report month in YYYYMM format.")
    parser.add_argument(
        "--output-root-dir",
        default=OUTPUT_ROOT_DIR,
        help="Root directory containing historical_data_* folders.",
    )
    parser.add_argument(
        "--report-dir",
        default=MONTHLY_REPORT_DIR,
        help="Directory where monthly reports are saved.",
    )
    parser.add_argument(
        "--template-file",
        default=DEFAULT_TEMPLATE_FILE,
        help="Workbook used for detailed-report headers.",
    )
    email_group = parser.add_mutually_exclusive_group()
    email_group.add_argument(
        "--send-email",
        action="store_true",
        help="Send one email with both report attachments.",
    )
    email_group.add_argument("--no-email", action="store_true", help="Only generate files.")
    args = parser.parse_args()

    send_email = _send_email_default()
    if args.send_email:
        send_email = True
    elif args.no_email:
        send_email = False

    formula_file, detailed_file = build_and_send_combined_monthly_reports(
        output_root_dir=args.output_root_dir,
        report_dir=args.report_dir,
        report_month=args.report_month,
        send_email=send_email,
        template_file=args.template_file,
    )
    print(f"Formula monthly report ready: {formula_file}")
    print(f"Detailed monthly report ready: {detailed_file}")


if __name__ == "__main__":
    main()
