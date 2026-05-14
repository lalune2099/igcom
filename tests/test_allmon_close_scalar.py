import ast
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAIN_SCRIPTS = ["all.py", "all2.py", "all3.py", "allMon.py"]


def _load_get_scalar_close(path):
    module = ast.parse(path.read_text(encoding="utf-8"))
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name == "get_scalar_close":
            scope = {}
            ast.fix_missing_locations(node)
            exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), scope)
            return scope["get_scalar_close"]
    raise AssertionError("get_scalar_close is missing")


class AllMonCloseScalarTests(unittest.TestCase):
    def test_get_scalar_close_handles_duplicate_timestamp_series(self):
        get_scalar_close = _load_get_scalar_close(ROOT / "allMon.py")

        class FakeSeries:
            iloc = [1.352305, 1.352305]

        class FakeLoc:
            def __getitem__(self, key):
                return FakeSeries()

        class FakeDataFrame:
            loc = FakeLoc()

        self.assertEqual(1.352305, get_scalar_close(FakeDataFrame(), "timestamp"))

    def test_main_scripts_use_scalar_helper_for_close_lookups(self):
        for script in MAIN_SCRIPTS:
            with self.subTest(script=script):
                path = ROOT / script
                text = path.read_text(encoding="utf-8")
                module = ast.parse(text)
                fill_func = next(
                    node
                    for node in module.body
                    if isinstance(node, ast.FunctionDef) and node.name == "fill_template_with_close_data"
                )
                fill_source = ast.get_source_segment(text, fill_func)

                _load_get_scalar_close(path)
                self.assertEqual(3, text.count("close_value = get_scalar_close(df,"))
                self.assertNotIn('close_value = df.loc[ts, "Close"]', fill_source)
                self.assertNotIn('close_value = df.loc[ts_1800, "Close"]', fill_source)
                self.assertNotIn('close_value = df.loc[ts_1830, "Close"]', fill_source)


if __name__ == "__main__":
    unittest.main()
