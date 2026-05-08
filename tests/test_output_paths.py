import ast
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAIN_SCRIPTS = ["all.py", "all2.py", "all3.py", "allMon.py"]


def _assignment_names(path):
    module = ast.parse(path.read_text(encoding="utf-8"))
    names = set()
    for node in module.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names.add(target.id)
    return names


class OutputPathTests(unittest.TestCase):
    def test_main_scripts_keep_template_at_igcom_root_and_write_outputs_under_outputs_dir(self):
        for script in MAIN_SCRIPTS:
            with self.subTest(script=script):
                path = ROOT / script
                text = path.read_text(encoding="utf-8")
                names = _assignment_names(path)

                self.assertIn("OUTPUT_ROOT_DIR", names)
                self.assertIn("RUN_DATE", names)
                self.assertIn("IG变化率表格(英区).xlsx", text)
                self.assertIn("IG变化率_模版更新_", text)
                self.assertIn('f"IG变化率_{RUN_DATE}.xlsx"', text)
                self.assertIn('os.path.join(OUTPUT_ROOT_DIR, f"historical_data_', text)
                self.assertIn("os.makedirs(OUTPUT_ROOT_DIR, exist_ok=True)", text)


if __name__ == "__main__":
    unittest.main()
