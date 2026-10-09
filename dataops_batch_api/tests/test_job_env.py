import unittest
from pathlib import Path
from unittest.mock import patch

from cli.jobs._env import load_project_env


class TestLoadProjectEnv(unittest.TestCase):
    @patch("cli.jobs._env.load_dotenv")
    def test_loads_repository_root_env_file_without_overriding(self, mock_load_dotenv):
        load_project_env()

        project_root = Path(__file__).resolve().parents[2]
        mock_load_dotenv.assert_called_once_with(project_root / ".env", override=False)


if __name__ == "__main__":
    unittest.main()
