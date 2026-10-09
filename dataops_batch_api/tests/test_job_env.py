import os
import unittest
from pathlib import Path
from unittest.mock import patch

from cli.jobs._env import load_project_env


class TestLoadProjectEnv(unittest.TestCase):
    @patch("cli.jobs._env.load_dotenv")
    def test_loads_repository_env_and_uses_host_minio_endpoint(self, mock_load_dotenv):
        with patch.dict(os.environ, {}, clear=True):
            load_project_env()

            project_root = Path(__file__).resolve().parents[2]
            mock_load_dotenv.assert_called_once_with(project_root / ".env", override=False)
            self.assertEqual(os.environ["AWS_ENDPOINT_URL"], "http://localhost:9010")

    @patch("cli.jobs._env.load_dotenv")
    def test_preserves_explicit_endpoint(self, mock_load_dotenv):
        with patch.dict(os.environ, {"AWS_ENDPOINT_URL": "http://minio:9000"}, clear=True):
            load_project_env()

            self.assertEqual(os.environ["AWS_ENDPOINT_URL"], "http://minio:9000")

    @patch("cli.jobs._env.load_dotenv")
    def test_uses_configured_host_endpoint(self, mock_load_dotenv):
        with patch.dict(os.environ, {"MINIO_HOST_ENDPOINT": "http://127.0.0.1:19010"}, clear=True):
            load_project_env()

            self.assertEqual(os.environ["AWS_ENDPOINT_URL"], "http://127.0.0.1:19010")


if __name__ == "__main__":
    unittest.main()
