"""
batch_helpers.py — TCBS Batch Helper methods.

Phạm trù: Batch Helpers / Pagination Utilities
    - get_ticker_commons_batch()        : Chia danh sách symbols lớn thành nhiều request nhỏ (batching).
    - get_intraday_history_all_pages()  : Tự động paginate qua toàn bộ tick history.

Design:
    - Các method này không gọi API trực tiếp — chúng orchestrate các method domain
      (get_ticker_commons, get_intraday_history) với logic retry / skip per-batch.
    - Lỗi per-batch được log WARNING và skip (không dừng toàn bộ run).
    - Lỗi per-page được log ERROR và dừng sớm (avoid infinite loop).

TCBS API constraints:
    - ticker_commons: không document giới hạn symbols/request → dùng batch_size=50 an toàn.
    - intraday_history: max 100 records/page (max_size từ tcbs_constants.yaml).
"""
from __future__ import annotations

from typing import Any, List

from log.logger_setup import logger_manager
from cli.ingestion.sources.tcbs_constants_loader import TCBSConstants

logger = logger_manager.get_logger(__name__)


class TCBSBatchHelpersMixin:
    """
    Phạm trù Batch Helpers — pagination và batching utilities.

    Kế thừa bởi TCBSRestClient. Gọi các method domain từ các mixin khác.
    """

    def get_ticker_commons_batch(
        self,
        tickers: List[str],
        batch_size: int = 50,
    ) -> List[Any]:
        """
        Lấy ticker commons cho danh sách lớn bằng cách chia nhỏ thành batches.

        TCBS API không document giới hạn symbols/request → dùng batch_size=50 an toàn.
        Mỗi batch lỗi được log và skip, không dừng toàn bộ run.

        Args:
            tickers:    Danh sách mã, e.g. ["VCB", "VHM", "TCB", ...]
            batch_size: Số symbols tối đa mỗi request (default 50).

        Returns:
            Danh sách tất cả ticker objects đã flatten từ tất cả batch responses.
        """
        all_data: List[Any] = []
        total_batches = (len(tickers) + batch_size - 1) // batch_size

        logger.info(
            "get_ticker_commons_batch — %d tickers, %d batches (batch_size=%d)",
            len(tickers), total_batches, batch_size,
        )

        for idx, i in enumerate(range(0, len(tickers), batch_size), start=1):
            chunk = tickers[i: i + batch_size]
            try:
                result = self.get_ticker_commons(tickers=chunk)  # type: ignore[attr-defined]
                data = result.get("data", [])
                if isinstance(data, list):
                    all_data.extend(data)
                logger.debug(
                    "get_ticker_commons_batch — batch %d/%d: %d records",
                    idx, total_batches, len(data) if isinstance(data, list) else 0,
                )
            except Exception as exc:
                logger.warning(
                    "get_ticker_commons_batch — batch %d/%d failed "
                    "(symbols %d-%d): %s — skipping",
                    idx, total_batches, i, i + len(chunk), exc,
                )

        logger.info(
            "get_ticker_commons_batch — complete: %d total records", len(all_data)
        )
        return all_data

    def get_intraday_history_all_pages(
        self,
        ticker: str,
        max_pages: int = 10,
    ) -> List[Any]:
        """
        Lấy toàn bộ lịch sử khớp lệnh intraday bằng cách paginate qua tất cả pages.

        Dừng khi: (1) hết data, (2) đạt max_pages, (3) lỗi liên tiếp.

        Args:
            ticker:    Mã cổ phiếu.
            max_pages: Số trang tối đa để tránh vô hạn loop (default 10 = 1000 ticks).

        Returns:
            Danh sách tất cả tick records đã flatten.
        """
        all_ticks: List[Any] = []
        hist_cfg  = TCBSConstants.intraday_history_config()
        page_size = hist_cfg.get("default_size", 100)
        head_index: int = hist_cfg.get("default_head_index", -1)

        logger.info(
            "get_intraday_history_all_pages — ticker=%s, max_pages=%d, size=%d",
            ticker, max_pages, page_size,
        )

        for page in range(max_pages):
            try:
                result = self.get_intraday_history(  # type: ignore[attr-defined]
                    ticker=ticker,
                    page=page,
                    size=page_size,
                    head_index=head_index,
                )
                data = result.get("data", [])
                if not data:
                    logger.debug(
                        "get_intraday_history_all_pages — ticker=%s: "
                        "empty page %d — stopping", ticker, page,
                    )
                    break
                all_ticks.extend(data)

                # Cập nhật headIndex từ response cho reverse paging
                head_index = result.get("headIndex", head_index)
                num_items  = result.get("numberOfItems", 0)

                logger.debug(
                    "get_intraday_history_all_pages — ticker=%s page=%d: %d items",
                    ticker, page, num_items,
                )

                # Dừng nếu page này ít hơn page_size (đã hết data)
                if len(data) < page_size:
                    break

            except Exception as exc:
                logger.error(
                    "get_intraday_history_all_pages — ticker=%s page=%d: %s — stopping",
                    ticker, page, exc,
                )
                break

        logger.info(
            "get_intraday_history_all_pages — ticker=%s: %d total ticks",
            ticker, len(all_ticks),
        )
        return all_ticks
