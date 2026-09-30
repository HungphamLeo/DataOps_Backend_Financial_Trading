"""
logger_setup.py — Head Logger Manager cho toàn bộ dataops_batch_api.

Đây là ENTRY POINT DUY NHẤT để khởi tạo logging cho cả pipeline.
Tất cả modules chỉ cần gọi `logging.getLogger(__name__)` sau khi
`setup_pipeline_logging()` được gọi một lần từ entrypoint.

Hierarchy logger trong pipeline:

    Root (WARNING)
    ├── dataops_batch_api.cli.flows.serving.bronze_flow     → bronze_flow  (INFO+)
    ├── dataops_batch_api.cli.flows.serving.silver_flow     → silver_flow  (INFO+)
    ├── dataops_batch_api.cli.flows.serving.backfill_flow   → backfill_flow (INFO+)
    ├── dataops_batch_api.cli.flows.serving.daily_pipeline  → daily_pipeline (INFO+)
    ├── dataops_batch_api.cli.ingestion.sources.okx_client  → okx_client   (DEBUG+)
    ├── dataops_batch_api.cli.ingestion.sources.vps_client  → vps_client   (DEBUG+)
    ├── dataops_batch_api.cli.ingestion.indicators.*        → indicator.*  (WARNING+)
    ├── dataops_batch_api.platforms.processing.*            → platform.*   (INFO+)
    ├── delta_utils                                         → delta_utils  (INFO+)
    ├── prefect_main                                        → prefect_main (INFO+)
    └── dataops_batch_api.config                            → config       (INFO+)

Usage (entrypoint — gọi 1 lần duy nhất):
    from log.logger_setup import setup_pipeline_logging
    setup_pipeline_logging()

Usage (mọi module khác — chỉ getLogger, KHÔNG gọi setup):
    import logging
    logger = logging.getLogger(__name__)
    logger.info("msg — key=%s", value)

Rules:
    - Dùng %s formatting, KHÔNG dùng f-string trong logger calls.
    - Module-level `logger = logging.getLogger(__name__)`, không import inline.
    - Không gọi logging.basicConfig() ở bất kỳ file nào ngoài logger_setup.py.
"""
from __future__ import annotations

import logging
import logging.config
import sys
from pathlib import Path


# ─── Root của dataops_batch_api package ───────────────────────────────────────
_BASE_DIR = Path(__file__).parent.parent.resolve()
_LOG_STORAGE = _BASE_DIR / "log" / "log_storage"
_LOGGER_CONFIG_YAML = _BASE_DIR / "log" / "config" / "logger_config.yaml"

# Module-level logger — chỉ dùng trong logger_setup.py này
_log = logging.getLogger(__name__)


def setup_pipeline_logging(
    config_path: Path | None = None,
    log_storage: Path | None = None,
) -> None:
    """
    Khởi tạo logging từ logger_config.yaml.

    Gọi đúng 1 lần tại entrypoint (daily_pipeline.py, prefect_main.py, backfill_flow.py
    khi chạy __main__, hoặc unit test conftest.py).

    Sau khi gọi hàm này, mọi `logging.getLogger(name)` trên toàn bộ pipeline
    đều được route đúng handler theo logger_config.yaml.

    Args:
        config_path: Override đường dẫn đến logger_config.yaml.
                     Mặc định: dataops_batch_api/log/config/logger_config.yaml
        log_storage: Override thư mục lưu log files.
                     Mặc định: dataops_batch_api/log/log_storage/

    Raises:
        Không raise exception — fallback sang basicConfig nếu YAML có lỗi.
        Lý do: logging không được phép làm crash entrypoint của pipeline.
    """
    resolved_config   = config_path or _LOGGER_CONFIG_YAML
    resolved_storage  = log_storage or _LOG_STORAGE

    # Tạo thư mục log_storage trước khi RotatingFileHandler cố mở file
    try:
        resolved_storage.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        # Không thể tạo thư mục — fallback ngay, không crash
        _bootstrap_fallback()
        _log.warning(
            "Cannot create log_storage dir %s: %s — using console-only logging",
            resolved_storage, exc,
        )
        return

    # Load dictConfig từ YAML
    if not resolved_config.exists():
        _bootstrap_fallback()
        _log.warning(
            "logger_config.yaml not found at %s — using console-only logging",
            resolved_config,
        )
        return

    try:
        import yaml
        with open(resolved_config, "r", encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh) or {}

        # Patch filename trong RotatingFileHandler sang đường dẫn tuyệt đối
        # (tránh phụ thuộc vào working directory khi chạy pipeline)
        _patch_file_handler_paths(cfg, resolved_storage)

        logging.config.dictConfig(cfg)
        _log.debug(
            "Pipeline logging configured from %s — log_storage=%s",
            resolved_config, resolved_storage,
        )

    except Exception as exc:  # pylint: disable=broad-except
        _bootstrap_fallback()
        _log.warning(
            "Failed to load logger_config.yaml (%s): %s — using console-only logging",
            resolved_config, exc,
        )


def get_logger(name: str) -> logging.Logger:
    """
    Convenience wrapper: trả về logger theo name.

    Dùng thay cho `logging.getLogger(name)` khi muốn gọi rõ ràng
    rằng đây là logger của pipeline (không bắt buộc, chỉ là alias).

    Args:
        name: Tên logger — thường là __name__ của module gọi vào.

    Returns:
        logging.Logger instance đã được cấu hình bởi setup_pipeline_logging().
    """
    return logging.getLogger(name)


# ─── Internal helpers ──────────────────────────────────────────────────────────

def _bootstrap_fallback() -> None:
    """
    Cấu hình logging tối thiểu khi logger_config.yaml không khả dụng.

    Chỉ ghi ra stdout, level INFO — đảm bảo pipeline vẫn chạy và
    có đủ output để debug mà không crash vì lỗi logging setup.
    """
    if logging.root.handlers:
        # Đã có handler (gọi nhiều lần hoặc đã basicConfig) — không override
        return
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )


def _patch_file_handler_paths(cfg: dict, log_storage: Path) -> None:
    """
    Thay thế filename trong RotatingFileHandler config thành đường dẫn tuyệt đối.

    Khi logger_config.yaml khai báo:
        filename: "log/log_storage/pipeline.log"
    thì sẽ được patch thành:
        filename: "/absolute/path/to/dataops_batch_api/log/log_storage/pipeline.log"

    Điều này quan trọng vì working directory khi chạy pipeline có thể khác nhau
    (chạy từ root repo vs chạy từ dataops_batch_api/).

    Args:
        cfg: dict đọc từ logger_config.yaml (sẽ bị modify in-place)
        log_storage: đường dẫn tuyệt đối đến thư mục log_storage
    """
    handlers = cfg.get("handlers", {})
    for handler_name, handler_cfg in handlers.items():
        if "filename" in handler_cfg:
            original = handler_cfg["filename"]
            # Chỉ patch nếu là relative path
            original_path = Path(original)
            if not original_path.is_absolute():
                # Lấy tên file cuối cùng (e.g. "pipeline.log") và ghép với log_storage
                new_path = log_storage / original_path.name
                handler_cfg["filename"] = str(new_path)
                _log.debug(
                    "Patched handler '%s' filename: %s → %s",
                    handler_name, original, new_path,
                )
