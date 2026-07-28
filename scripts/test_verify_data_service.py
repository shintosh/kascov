import importlib.util
import pathlib
import types
import unittest


SCRIPT = pathlib.Path(__file__).with_name("verify_data_service.py")
SPEC = importlib.util.spec_from_file_location("verify_data_service", SCRIPT)
verify_data_service = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(verify_data_service)


class VerifyDataServiceTest(unittest.TestCase):
    def test_commands_cover_every_local_contract_owner(self):
        commands = verify_data_service.command_specs()
        rendered = [" ".join(command) for command in commands]
        self.assertEqual(5, len(commands))
        self.assertIn("cargo test -p kascov-core --lib --tests", rendered)
        self.assertIn("cargo test -p kascov-argent", rendered)
        self.assertIn("cargo test -p kascov --bin kascov --tests", rendered)
        self.assertTrue(any("scripts/test_verify_data_service.py" in command for command in rendered))
        self.assertTrue(any("clients/py/test_kascov.py" in command for command in rendered))
        self.assertTrue(any("clients/js/kascov.test.mjs" in command for command in rendered))
        self.assertTrue(any("web/pending.test.mjs" in command for command in rendered))

    def test_runner_stops_at_first_failure_and_reports_it(self):
        calls = []

        def runner(command, **_kwargs):
            calls.append(command)
            return types.SimpleNamespace(returncode=7 if len(calls) == 2 else 0)

        report = verify_data_service.run_commands(
            [["first"], ["second"], ["never"]],
            runner=runner,
            cwd=pathlib.Path("."),
        )
        self.assertEqual("failed", report["status"])
        self.assertEqual([["first"], ["second"]], calls)
        self.assertEqual(7, report["commands"][-1]["returncode"])

    def test_runner_reports_complete_success(self):
        environments = []

        def runner(_command, **kwargs):
            environments.append(kwargs["env"])
            return types.SimpleNamespace(returncode=0)

        report = verify_data_service.run_commands(
            [["first"], ["python3", "-m", "unittest", "client_test.py"]],
            runner=runner,
            cwd=pathlib.Path("."),
        )
        self.assertEqual("passed", report["status"])
        self.assertEqual(2, len(report["commands"]))
        self.assertIsNone(environments[0])
        self.assertIn("clients/py", environments[1]["PYTHONPATH"])


if __name__ == "__main__":
    unittest.main()
