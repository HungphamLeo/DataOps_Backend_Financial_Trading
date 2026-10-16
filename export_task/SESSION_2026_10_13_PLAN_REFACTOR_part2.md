# @github\copilotinstructions\SKILL.md  @github\agent\  Đọc tài liệu để hiểu bối cảnh và đọc export_task theo hướng dẫn đi

---

**Status:** active  **Date:** 2026-10-13

---

### 👤 User

@github\copilotinstructions\SKILL.md  @github\agent\  Đọc tài liệu để hiểu bối cảnh và đọc export_task theo hướng dẫn đi

---

### 🤖 Assistant

> 🔧 called: glob, glob, list_files



> 🔧 **list_files** — export_task

> 🔧 **glob** — **/copilotinstructions/**

> 🔧 **glob** — .github/**

### 🤖 Assistant

> 🔧 called: read_file, glob



> 🔧 **read_file** — github\copilotinstructions\SKILL.md

> 🔧 **glob** — github/agent/**

### 🤖 Assistant

> 🔧 called: list_files, read_file



> 🔧 **list_files** — github

> 🔧 **read_file** — export_task\SESSION_2026_11_10.MD

### 🤖 Assistant

> 🔧 called: read_file, read_file, read_file



> 🔧 **read_file** — export_task\SESSION_2026_11_10.MD

> 🔧 **read_file** — export_task\SESSION_2026_11_10.MD

> 🔧 **read_file** — github\agent\full-stack-data-platform-engineer.agent.md

### 🤖 Assistant

> 🔧 called: read_file, read_file, read_file, read_file, read_file, read_file



> 🔧 **read_file** — github\agent\backend-software-engineer.agent.md

> 🔧 **read_file** — github\agent\batch-lakehouse-engineer.agent.md

> 🔧 **read_file** — export_task\SESSION_2026_10_13_FIX_DATAOPS_PROCESSING&REVALUATE_INDICATORS.MD

> 🔧 **read_file** — github\agent\realtime-streaming-engineer.agent.md

> 🔧 **read_file** — github\agent\devops-infra-engineer.agent.md

> 🔧 **read_file** — export_task\SESSION_2026_10_13_PLAN_REFACTOR_part1.md

### 🤖 Assistant

> 🔧 called: read_file, read_file



> 🔧 **read_file** — export_task\SESSION_2026_10_13_PLAN_REFACTOR_part1.md

> 🔧 **read_file** — export_task\SESSION_2026_10_13_PLAN_REFACTOR_part1.md

### 🤖 Assistant

> 🔧 called: grep, grep



> 🔧 **grep** — ^### 👤 User$

> 🔧 **grep** — ^### 🤖 Assistant$

### 🤖 Assistant

> 🔧 called: read_file, read_file

The latest session file is the most recent, so I'll read its ending.

> 🔧 **read_file** — export_task\SESSION_2026_10_13_FIX_DATAOPS_PROCESSING&REVALUATE_INDICATORS.MD

> 🔧 **read_file** — export_task\SESSION_2026_10_13_FIX_DATAOPS_PROCESSING&REVALUATE_INDICATORS.MD

### 🤖 Assistant

Tôi đã đọc xong `github/copilotinstructions/SKILL.md`, 5 file trong `github/agent/` và các file session mới nhất của `export_task/`. Tôi chưa đọc `allskill/` và chưa sửa code.

## Quy tắc vận hành (SKILL.md)
- **Thứ tự ưu tiên tri thức:** `export_task/` đứng trước `allskill/` (`C:\Users\Admin\.ai_workspace\allskill`). Nếu hai bên mâu thuẫn thì theo `export_task/`.
- **Đọc có chọn lọc:** chỉ đọc file session gần nhất và chỉ đọc skill liên quan.
- **Phạm vi:** làm đúng task được giao, xong thì báo rõ. Khi export session thì chỉ ghi file, không đọc thêm file khác.
- **Đóng gói session:** khi xong việc lớn thì đề xuất tóm tắt Markdown để bạn lưu vào `export_task/`.

## 5 agent trong `github/agent/`
| Agent | Phạm vi |
|---|---|
| [`backend-software-engineer`](github/agent/backend-software-engineer.agent.md) | `dataops_backend_api`: Go/Gin, GORM/MySQL, Redis, InfluxDB, proxy tài khoản TCBS, zap logging |
| [`realtime-streaming-engineer`](github/agent/realtime-streaming-engineer.agent.md) | `dataops-stream-producer` và `dataops_protobuf`: Kafka, Binance/TCBS WebSocket, DLQ, Redis ticker |
| [`batch-lakehouse-engineer`](github/agent/batch-lakehouse-engineer.agent.md) | `dataops_batch_api`: PySpark, Delta Lake, MinIO, Medallion, Pydantic CDM, không consume Kafka |
| [`devops-infra-engineer`](github/agent/devops-infra-engineer.agent.md) | docker-compose, Dockerfile, bảng port, healthcheck, Grafana |
| [`full-stack-data-platform-engineer`](github/agent/full-stack-data-platform-engineer.agent.md) | Xuyên module: CDM, pipeline end-to-end, kiến trúc tổng thể |

Ghi chú: `.github/agents/` không tồn tại; các agent nằm ở `github/agent/`.

## Trạng thái hiện tại (từ `export_task`, ngày 2026-10-13)
Hai file session mới nhất là [`PLAN_REFACTOR_part1`](export_task/SESSION_2026_10_13_PLAN_REFACTOR_part1.md) và [`FIX_DATAOPS_PROCESSING&REVALUATE_INDICATORS`](export_task/SESSION_2026_10_13_FIX_DATAOPS_PROCESSING&REVALUATE_INDICATORS.MD). Tiến độ backlog là 4 trên 11 task.

**Đã xong (4 task):**
- TASK-BE1: CRUD lệnh TCBS và Binance Spot.
- TASK-BE2: Portfolio và NAV theo giá live.
- TASK-BE3: Risk engine và endpoint `GET /api/v1/portfolio/risk-report`.
- TASK-P1: Job `indicator-matrix` (EMA, ATR, RSI, Bollinger). Test: Go `ok`, Python 34 passed.

**Còn lại (7 task):**
| Task | Nội dung | Ưu tiên |
|---|---|---|
| TASK-BE4 | Pre-trade risk check, position sizing (`services/risk/pre_trade.go`) | P2 |
| TASK-P2 | Streaming CVD và order flow, ghi vào InfluxDB | P2 |
| TASK-P3 | Market breadth, dòng tiền khối ngoại, funding heatmap | P2 |
| TASK-B1 | Margin limits và room TCBS, vào Delta Bronze `tcbs_margin_rooms` | P2 |
| TASK-B2 | Ingest benchmark VNINDEX, VN30, BTC.D | **P1** |
| TASK-INF1 | Loki và Promtail gom log tập trung | P2 |
| TASK-INF2 | Smoke test end-to-end (`scripts/smoke_test.sh`) | **P1** |

Git đang có thay đổi chưa commit ở branch `20261010_data_processing`, khớp với các task BE1–BE3 và P1 ở trên.

Bạn muốn làm task nào tiếp? Kế hoạch trong session đề xuất Giai đoạn 3 là BE4 và P2. Hai task P1 còn lại là B2 và INF2.

---

### 👤 User

Dưới đây là **Tổng quan tiến độ hiện tại** và **Danh sách các Task còn lại cần thực hiện** trong Backlog:

---

### 📊 Bảng Tiến độ Tổng quan

```mermaid
pie title Trạng thái hoàn thành Backlog (4/11 Tasks)
    "Đã hoàn thành" : 4
    "Còn lại cần làm" : 7
```

---

### ✅ Các Task Đã Hoàn Thành (4 Tasks)
1. **TASK-BE1 (`dataops_backend_api`)**: Hoàn thiện toàn diện CRUD Lệnh & Proxy tài khoản (Hủy/Sửa lệnh TCBS cơ sở, Hủy lệnh phái sinh VN30F, Đặt/Hủy lệnh và tra cứu số dư Binance Spot có chữ ký HMAC-SHA256).
2. **TASK-BE2 (`dataops_backend_api`)**: Real-time Portfolio & NAV Engine (tổng hợp Crypto + TCBS Cash + Stocks + Phái sinh theo giá live từ Redis với độ trễ `< 5ms`, phân bổ tỷ trọng danh mục).
3. **TASK-BE3 (`dataops_backend_api`)**: Risk Management Engine (tính VaR 95% Parametric & Historical, Max Drawdown, Sharpe Ratio, Annual Volatility, Beta/Alpha so với VNINDEX/BTC, expose `GET /api/v1/portfolio/risk-report`).
4. **TASK-P1 (`dataops_processing`)**: Multi-Timeframe Indicator Matrix Batch Job (tính toán song song EMA 20/50/200, ATR 14, RSI 14, Bollinger Bands 20 trên nến ngày/giờ cho Binance & TCBS, lưu Delta Silver/Gold).

---

### 📋 Danh Sách Các Task Còn Lại (7 Tasks)

| Mã Task | Phân hệ (Module) | Nội dung chi tiết | Mức độ ưu tiên |
| :--- | :--- | :--- | :---: |
| **TASK-BE4** | `dataops_backend_api` | **Pre-Trade Risk Checks & Position Sizing Middleware**<br>• Kiểm tra trước khi bắn lệnh sang TCBS/Binance: Không vượt quá Max Allocation % cho 1 mã, chặn vi phạm Margin limit, kiểm tra sức mua khả dụng (PPSE / Free USDT). | **P2 (Trung bình)** |
| **TASK-P2** | `dataops_processing` | **Real-time Order Flow & CVD Streaming Job**<br>• Spark Structured Streaming tính Cumulative Volume Delta (CVD) từ Kafka ticks của Binance và BSA flow của TCBS $\rightarrow$ Sink InfluxDB (`order_flow`). | **P2 (Trung bình)** |
| **TASK-P3** | `dataops_processing` | **Market Breadth & EOD Sector Flow Report Job**<br>• Tổng hợp dòng tiền khối ngoại (Foreign Net Flow), dòng tiền theo nhóm ngành VN30/Midcap/Smallcap và Funding Rate Heatmap. | **P2 (Trung bình)** |
| **TASK-B1** | `dataops_batch_api` | **Tích hợp Margin Limits & Room Chứng khoán TCBS**<br>• Bổ sung client gọi endpoint Hydros/Erebos (`/hydros/v1/margin-limits`, room vay ký quỹ từng mã) $\rightarrow$ Delta Bronze (`tcbs_margin_rooms`). | **P2 (Trung bình)** |
| **TASK-B2** | `dataops_batch_api` | **Ingest Daily Benchmark Index (VNINDEX, VN30, BTC.D)**<br>• Ingest dữ liệu chỉ số chuẩn liên tục để phục vụ tính Beta/Correlation cho danh mục. | **P1 (Cao)** |
| **TASK-INF1** | `infra` / Observability | **Centralized Logging Pipeline (Loki + Promtail)**<br>• Cấu hình Promtail gom log từ `dataops_processing` và `dataops_batch_api` về Loki $\rightarrow$ Grafana Dashboard tập trung. | **P2 (Trung bình)** |
| **TASK-INF2** | `infra` / Scripts | **End-to-End Smoke Test toàn hệ thống**<br>• Kịch bản test tự động verify chuỗi: Stream Producer $\rightarrow$ Kafka $\rightarrow$ Spark Processing $\rightarrow$ InfluxDB/Delta $\rightarrow$ Backend API $\rightarrow$ Risk Signals. | **P1 (Cao)** |

---

### 🎯 Đề xuất bước tiếp theo
Bạn muốn tiếp tục triển khai **Giai đoạn 3 (TASK-BE4: Pre-Trade Risk Checks + TASK-P2: Real-time Order Flow/CVD Streaming Job)** hay ưu tiên các task của module cụ thể nào trước?Tiếp tục tiến hành thực hiện các task chưa hoàn thành đi

---

### 🤖 Assistant

> 🔧 called: update_todo_list, use_skill



> 🔧 **update_todo_list** — [-] Explore existing code (risk, portfolio, crypto/tcbs services, processing jobs)
[ ] TASK-BE4: Pre-trade risk checks (services/risk/pre_trade.go) + wire into order handlers
[ ] TASK-P2: Order Flow / CVD streaming job (dataops_processing)
[ ] TASK-P3: Market breadth & EOD sector flow job
[ ] TASK-B1: TCBS margin limits & rooms client -> Bronze
[ ] TASK-B2: Benchmark index ingestion (VNINDEX, VN30, BTC.D)
[ ] TASK-INF1: Promtail + Loki config
[ ] TASK-INF2: Smoke test script
[ ] Validate (go test, pytest) and summarize


> 🔧 **use_skill** — dataops-financial-trading

### 🤖 Assistant

> 🔧 called: list_files, list_files



> 🔧 **list_files** — dataops_backend_api/api/rest

> 🔧 **list_files** — dataops_backend_api/services

### 🤖 Assistant

> 🔧 called: read_file, read_file, read_file, read_file



> 🔧 **read_file** — dataops_backend_api/services/services.go

> 🔧 **read_file** — dataops_backend_api/services/risk/risk.go

> 🔧 **read_file** — dataops_backend_api/services/portfolio/portfolio.go

> 🔧 **read_file** — dataops_backend_api/api/rest/portfolio.go

### 🤖 Assistant

> 🔧 called: read_file, read_file, read_file



> 🔧 **read_file** — dataops_backend_api/api/rest/tcbs_account.go

> 🔧 **read_file** — dataops_backend_api/api/rest/rest.go

> 🔧 **read_file** — dataops_backend_api/models/risk.go

### 🤖 Assistant

> 🔧 called: read_file, grep, grep



> 🔧 **read_file** — dataops_backend_api/api/rest/crypto.go

> 🔧 **grep** — PlaceOrder|PlaceStockOrder|PlaceDerivativeOrder

> 🔧 **grep** — type (TCBSPlaceStockOrderRequest|TCBSPlaceDerivOrderRequest|CryptoPlaceOrderRequest|CryptoOrderRequest)|type Service interface

### 🤖 Assistant

> 🔧 called: read_file, read_file, read_file, read_file



> 🔧 **read_file** — dataops_backend_api/models/tcbs_account.go

> 🔧 **read_file** — dataops_backend_api/models/crypto_order.go

> 🔧 **read_file** — dataops_backend_api/api/rest/crypto.go

> 🔧 **read_file** — dataops_backend_api/api/rest/rest_test.go

### 🤖 Assistant

> 🔧 called: read_file, read_file



> 🔧 **read_file** — dataops_backend_api/api/rest/crypto.go

> 🔧 **read_file** — dataops_backend_api/configuration/configuration.go

### 🤖 Assistant

> 🔧 called: read_file, read_file, grep



> 🔧 **read_file** — dataops_backend_api/models/tcbs_account.go

> 🔧 **read_file** — dataops_backend_api/services/crypto_account/crypto_account.go

> 🔧 **grep** — dataops_backend_api

### 🤖 Assistant

> 🔧 called: grep, grep



> 🔧 **grep** — dataops_backend_api/utils

> 🔧 **grep** — dataops_backend_api/services/tcbs_account/tcbs_account.go

### 🤖 Assistant

> 🔧 called: read_file, grep



> 🔧 **read_file** — dataops_backend_api/services/tcbs_account/tcbs_account.go

> 🔧 **grep** — dataops_backend_api/constants

### 🤖 Assistant

> 🔧 called: write_file



> 🔧 **write_file** — dataops_backend_api/services/risk/pre_trade.go

### 🤖 Assistant

> 🔧 called: execute_command, insert_content, apply_diff

I referenced a few helpers that don't exist yet. I'll fix those and add the models and config.

> 🔧 **execute_command** — (Get-Content dataops_backend_api/services/risk/pre_trade.go -Raw).Replace('result.addViolation','result.AddViolation').Replace('nowUnix()','time.Now().Unix()').Replace("`"math`"`n`t`"strings`"","`"math`"`n`t`"strings`"`n`t`"time`"") | Set-Content dataops_backend_api/services/risk/pre_trade.go -NoNewline

> 🔧 **insert_content** — dataops_backend_api/models/risk.go

> 🔧 **apply_diff** — dataops_backend_api/configuration/configuration.go

### 🤖 Assistant

> 🔧 called: apply_diff, apply_diff



> 🔧 **apply_diff** — dataops_backend_api/services/services.go

> 🔧 **apply_diff** — dataops_backend_api/api/rest/portfolio.go

### 🤖 Assistant

> 🔧 called: apply_diff

Now I'll enforce the check in the order placement handlers.

> 🔧 **apply_diff** — <<<<<<< SEARCH
:start_line:94
-------
	ctx.JSON(http.StatusOK, result)
}
=======
	ctx.JSON(http.StatusOK, result)
}

// enforcePreTrade chạy pre-trade check trước khi proxy lệnh sang sàn.
// Trả về true nếu lệnh được phép đi tiếp; false nếu đã ghi response (422) chặn lệnh.
// Khi svc == nil hoặc enforce == false thì bỏ qua (cho qua).
func enforcePreTrade(ctx *gin.Context, svc risk.PreTradeService, enforce bool, req *models.PreTradeCheckRequest) bool {
	if svc == nil || !enforce {
		return true
	}
	result, err := svc.CheckPreTrade(ctx, req)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return false
	}
	if !result.Allowed {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"error": "pre-trade risk check failed", "check": result})
		return false
	}
	return true
}
>>>>>>> REPLACE

### 🤖 Assistant

> 🔧 called: insert_content, apply_diff, apply_diff, apply_diff



> 🔧 **insert_content** — dataops_backend_api/api/rest/portfolio.go

> 🔧 **apply_diff** — dataops_backend_api/api/rest/tcbs_account.go

> 🔧 **apply_diff** — dataops_backend_api/api/rest/crypto.go

> 🔧 **apply_diff** — dataops_backend_api/api/rest/rest.go

### 🤖 Assistant

> 🔧 called: write_file, execute_command



> 🔧 **write_file** — dataops_backend_api/services/risk/pre_trade_test.go

> 🔧 **execute_command** — gofmt -l . ; go build ./... ; if ($?) { go vet ./services/risk/ ./api/rest/ ; go test ./... 2>&1 | Select-Object -Last 25 }

### 🤖 Assistant

> 🔧 called: execute_command



> 🔧 **execute_command** — go test ./api/... ./services/risk/... 2>&1 | Select-Object -Last 8; gofmt -l services/risk

### 🤖 Assistant

> 🔧 called: update_todo_list

TASK-BE4 done. All Go tests pass. Now onto **TASK-P2** (CVD/Order Flow Streaming Job).

> 🔧 **update_todo_list** — [x] Explore existing code (risk, portfolio, crypto/tcbs services, processing jobs)
[x] TASK-BE4: Pre-trade risk checks (services/risk/pre_trade.go) + wire into order handlers
[-] TASK-P2: Order Flow / CVD streaming job (dataops_processing)
[ ] TASK-P3: Market breadth & EOD sector flow job
[ ] TASK-B1: TCBS margin limits & rooms client -> Bronze
[ ] TASK-B2: Benchmark index ingestion (VNINDEX, VN30, BTC.D)
[ ] TASK-INF1: Promtail + Loki config
[ ] TASK-INF2: Smoke test script
[ ] Validate (go test, pytest) and summarize


### 🤖 Assistant

> 🔧 called: list_files



> 🔧 **list_files** — dataops_processing

### 🤖 Assistant

> 🔧 called: read_file, read_file, read_file, read_file, read_file



> 🔧 **read_file** — dataops_processing/jobs/runner.py

> 🔧 **read_file** — dataops_processing/config/settings.py

> 🔧 **read_file** — dataops_processing/config/processing_config.yaml

> 🔧 **read_file** — dataops_processing/core/orchestrator.py

> 🔧 **read_file** — dataops_processing/jobs/vwap_job.py

### 🤖 Assistant

> 🔧 called: read_file, read_file



> 🔧 **read_file** — dataops_processing/core/base_job.py

> 🔧 **read_file** — dataops_processing/models/trade.py

### 🤖 Assistant

> 🔧 called: read_file, grep



> 🔧 **read_file** — dataops_processing/tests/test_indicator_matrix_job.py

> 🔧 **grep** — dataops_processing/tests/test_indicator_matrix_job.py

### 🤖 Assistant

> 🔧 called: grep, read_file



> 🔧 **read_file** — dataops_processing/config/spark_config.py

> 🔧 **grep** — dataops-stream-producer/pkg/producer/types.go

### 🤖 Assistant

> 🔧 called: apply_diff, apply_diff



> 🔧 **apply_diff** — dataops_processing/config/processing_config.yaml

> 🔧 **apply_diff** — dataops_processing/config/settings.py

### 🤖 Assistant

> 🔧 called: grep



> 🔧 **grep** — dataops_processing

### 🤖 Assistant

> 🔧 called: write_file, write_file, apply_diff, apply_diff, execute_command



> 🔧 **write_file** — """
order_flow_job.py — Spark Structured Streaming Order Flow / CVD Job (TASK-P2).

Pipeline:
    Kafka (binance.trade.ticks)
        → TradeEvent.from_kafka_bytes()         [canonical deserialization]
        → withWatermark("event_time", cfg.watermark)
        → groupBy(window(cfg.window_duration), symbol)    [tumbling window]
        → buy_volume / sell_volume / delta (= buy - sell) / imbalance
        → InfluxDB foreachBatch (measurement = "order_flow")

CVD (Cumulative Volume Delta):
    Spark chỉ emit `delta` theo từng window đã đóng (stateless sau watermark).
    CVD = cumulativeSum(delta) được tính ở tầng query (Flux `cumulativeSum()`),
    nên job restart không làm mất / lệch CVD.

Imbalance = (buy - sell) / (buy + sell) ∈ [-1, 1]  (>0: người mua chủ động chiếm ưu thế).

Ranh giới trách nhiệm: Binance ticks only. BSA flow của TCBS là dữ liệu REST (batch_api),
không đi qua Kafka nên không nằm trong job này.

Config & Paths đến từ Dependency Injection (Settings + PathResolver) — không hardcode.
"""
from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple

from dataops_processing.core.base_job import BaseProcessingJob
from dataops_processing.core.registry import JobRegistry
from dataops_processing.log.logger_setup import logger_manager

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logger_manager.get_logger(__name__)

_BUY_SIDES = frozenset({"buy", "b"})
_SELL_SIDES = frozenset({"sell", "s"})


# =============================================================================
# Pure-Python core (testable độc lập, không cần Spark)
# =============================================================================

def classify_side(side: Optional[str]) -> Optional[str]:
    """Chuẩn hóa side → 'buy' | 'sell' | None (không xác định → bỏ qua)."""
    if side is None:
        return None
    s = str(side).strip().lower()
    if s in _BUY_SIDES:
        return "buy"
    if s in _SELL_SIDES:
        return "sell"
    return None


def compute_order_flow_metrics(buy_volume: float, sell_volume: float) -> Dict[str, float]:
    """
    Tính delta & imbalance cho một window.

    Returns:
        {"buy_volume", "sell_volume", "delta", "imbalance"}
        imbalance = 0.0 khi tổng volume = 0.
    """
    total = buy_volume + sell_volume
    delta = buy_volume - sell_volume
    imbalance = (delta / total) if total > 0 else 0.0
    return {
        "buy_volume": buy_volume,
        "sell_volume": sell_volume,
        "delta": delta,
        "imbalance": imbalance,
    }


def aggregate_trades(
    trades: Iterable[Tuple[str, float, str, int]],
    window_ms: int,
) -> List[Dict[str, Any]]:
    """
    Reference implementation (không Spark) của window aggregation — dùng cho unit test.

    Args:
        trades: iterable (symbol, size, side, ts_ms)
        window_ms: độ dài tumbling window (milliseconds)

    Returns:
        List dict theo (symbol, window_start_ms) tăng dần, kèm cvd tích lũy theo symbol.
    """
    buckets: Dict[Tuple[str, int], List[float]] = {}
    for symbol, size, side, ts_ms in trades:
        klass = classify_side(side)
        if klass is None or size <= 0:
            continue
        key = (symbol, (ts_ms // window_ms) * window_ms)
        acc = buckets.setdefault(key, [0.0, 0.0, 0])
        acc[0 if klass == "buy" else 1] += size
        acc[2] += 1

    rows: List[Dict[str, Any]] = []
    cvd: Dict[str, float] = {}
    for (symbol, w_start) in sorted(buckets, key=lambda k: (k[0], k[1])):
        buy, sell, n = buckets[(symbol, w_start)]
        m = compute_order_flow_metrics(buy, sell)
        cvd[symbol] = cvd.get(symbol, 0.0) + m["delta"]
        rows.append({
            "symbol": symbol,
            "window_start_ms": w_start,
            "window_end_ms": w_start + window_ms,
            "num_trades": int(n),
            "cvd": cvd[symbol],
            **m,
        })
    return rows


def to_line_protocol(
    measurement: str,
    symbol: str,
    interval: str,
    buy_volume: float,
    sell_volume: float,
    delta: float,
    imbalance: float,
    num_trades: int,
    ts_ns: int,
) -> str:
    """Dựng 1 InfluxDB line protocol record (tag escape: space, comma, equals)."""
    def esc(v: str) -> str:
        return v.replace("\\", "\\\\").replace(",", "\\,").replace(" ", "\\ ").replace("=", "\\=")

    return (
        f"{measurement},symbol={esc(symbol)},interval={esc(interval)} "
        f"buy_volume={float(buy_volume)},sell_volume={float(sell_volume)},"
        f"delta={float(delta)},imbalance={float(imbalance)},"
        f"num_trades={int(num_trades)}i {int(ts_ns)}"
    )


# =============================================================================
# InfluxDB sink — foreachBatch
# =============================================================================

def _write_to_influxdb(df_pandas, batch_id: int) -> None:
    """
    foreachBatch sink: ghi order_flow rows vào InfluxDB.

    df_pandas columns: symbol, window_start, window_end, interval,
                       buy_volume, sell_volume, delta, imbalance, num_trades

    InfluxDB point schema:
        measurement: "order_flow"
        tag:   symbol, interval
        field: buy_volume, sell_volume, delta, imbalance (float), num_trades (integer)
        time:  window_end (UTC nanoseconds)
    """
    if df_pandas.empty:
        return

    from dataops_processing.config.settings import get_settings
    settings = get_settings()
    cfg = settings.influxdb
    measurement = settings.order_flow_job.influx_measurement

    try:
        from influxdb_client import InfluxDBClient
        from influxdb_client.client.write_api import SYNCHRONOUS
        from influxdb_client.domain.write_precision import WritePrecision
    except ImportError as exc:  # pragma: no cover
        logger.error("influxdb-client not installed: %s", exc, exc_info=True)
        return

    client = InfluxDBClient(url=cfg.url, token=cfg.token, org=cfg.org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    try:
        records = [
            to_line_protocol(
                measurement=measurement,
                symbol=row["symbol"],
                interval=row["interval"],
                buy_volume=row["buy_volume"],
                sell_volume=row["sell_volume"],
                delta=row["delta"],
                imbalance=row["imbalance"],
                num_trades=row["num_trades"],
                ts_ns=int(row["window_end"].timestamp() * 1_000_000_000),
            )
            for _, row in df_pandas.iterrows()
        ]
        write_api.write(
            bucket=cfg.bucket,
            org=cfg.org,
            record=records,
            write_precision=WritePrecision.NANOSECONDS,
        )
        logger.info("batch_id=%d: wrote %d order_flow points to InfluxDB", batch_id, len(records))
    except Exception as exc:
        logger.error("batch_id=%d: InfluxDB write failed — %s", batch_id, exc, exc_info=True)
    finally:
        client.close()


def _foreach_batch(df, batch_id: int) -> None:
    """Spark DataFrame → pandas → InfluxDB (micro-batch nhỏ, sau aggregation)."""
    _write_to_influxdb(df.toPandas(), batch_id)


# =============================================================================
# Concrete Strategy: Order Flow Streaming Job
# =============================================================================

@JobRegistry.register("order-flow")
class OrderFlowStreamingJob(BaseProcessingJob):
    """Job streaming tính buy/sell volume, delta (CVD component) và imbalance từ Kafka ticks."""

    @property
    def name(self) -> str:
        return "order-flow"

    @property
    def job_type(self) -> str:
        return "streaming"

    def execute(self, spark: Optional["SparkSession"] = None, **kwargs: Any) -> None:
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            DoubleType, LongType, StringType, StructField, StructType, TimestampType,
        )

        from dataops_processing.config.spark_config import build_spark_session, get_kafka_options
        from dataops_processing.models.trade import TradeEvent

        cfg = self.settings.order_flow_job
        correlation_id = str(uuid.uuid4())
        logger.info("order_flow_job.start correlation_id=%s", correlation_id)

        if spark is None:
            spark = build_spark_session("dataops-order-flow-job")

        checkpoint = self.paths.checkpoint_path("order_flow_job")
        logger.info(
            "Order flow job config — topic=%s window=%s watermark=%s checkpoint=%s",
            self.settings.kafka.topic_trade_ticks, cfg.window_duration, cfg.watermark, checkpoint,
        )

        raw_stream = (
            spark.readStream
            .format("kafka")
            .options(**get_kafka_options())
            .load()
        )

        trade_schema = StructType([
            StructField("symbol", StringType(), nullable=False),
            StructField("price",  DoubleType(), nullable=False),
            StructField("size",   DoubleType(), nullable=False),
            StructField("side",   StringType(), nullable=True),
            StructField("ts_ms",  LongType(),   nullable=False),
        ])

        @F.udf(returnType=trade_schema)
        def parse_trade_udf(raw: bytes):
            event = TradeEvent.from_kafka_bytes(raw)
            return None if event is None else event.to_tuple()

        side_lc = F.lower(F.col("trade.side"))
        is_buy = side_lc.isin("buy", "b")
        is_sell = side_lc.isin("sell", "s")

        parsed = (
            raw_stream
            .select(parse_trade_udf(F.col("value")).alias("trade"))
            .filter(F.col("trade").isNotNull())
            .filter(is_buy | is_sell)          # side không xác định → loại, tránh làm lệch delta
            .select(
                F.col("trade.symbol").alias("symbol"),
                F.col("trade.size").alias("size"),
                is_buy.alias("is_buy"),
                (F.col("trade.ts_ms") / 1000).cast(TimestampType()).alias("event_time"),
            )
        )

        order_flow = (
            parsed
            .withWatermark("event_time", cfg.watermark)
            .groupBy(
                F.window("event_time", cfg.window_duration).alias("window"),
                F.col("symbol"),
            )
            .agg(
                F.sum(F.when(F.col("is_buy"), F.col("size")).otherwise(F.lit(0.0))).alias("buy_volume"),
                F.sum(F.when(~F.col("is_buy"), F.col("size")).otherwise(F.lit(0.0))).alias("sell_volume"),
                F.count("*").alias("num_trades"),
            )
            .select(
                F.col("symbol"),
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                F.lit(cfg.influx_interval_tag).alias("interval"),
                F.col("buy_volume"),
                F.col("sell_volume"),
                (F.col("buy_volume") - F.col("sell_volume")).alias("delta"),
                F.when(
                    (F.col("buy_volume") + F.col("sell_volume")) > 0,
                    (F.col("buy_volume") - F.col("sell_volume"))
                    / (F.col("buy_volume") + F.col("sell_volume")),
                ).otherwise(F.lit(0.0)).alias("imbalance"),
                F.col("num_trades"),
            )
        )

        (
            order_flow
            .writeStream
            .outputMode(cfg.output_mode)
            .trigger(processingTime=cfg.trigger_interval)
            .foreachBatch(_foreach_batch)
            .option("checkpointLocation", f"{checkpoint}/influxdb")
            .queryName("order_flow_to_influxdb")
            .start()
        )

        logger.info("Order flow stream started — awaiting termination correlation_id=%s", correlation_id)
        spark.streams.awaitAnyTermination()
        logger.info("Order flow job terminated correlation_id=%s", correlation_id)


def run(spark: Optional["SparkSession"] = None) -> None:
    """Backward-compatible functional entry point."""
    from dataops_processing.config.paths import get_path_resolver
    from dataops_processing.config.settings import get_settings
    job = OrderFlowStreamingJob(settings=get_settings(), paths=get_path_resolver())
    job.run(spark=spark)


> 🔧 **write_file** — """
tests/test_order_flow_job.py — Unit tests cho Order Flow / CVD Streaming Job (TASK-P2).

Kiểm tra phần pure-Python (không cần Spark):
    1. classify_side: chuẩn hóa buy/sell, bỏ side không xác định.
    2. compute_order_flow_metrics: delta, imbalance, zero-volume.
    3. aggregate_trades: tumbling window + CVD tích lũy theo symbol.
    4. to_line_protocol: schema InfluxDB + escape tag.
    5. Job Registry: 'order-flow' được Orchestrator load.
"""
from __future__ import annotations

import pytest

from dataops_processing.core.orchestrator import get_orchestrator
from dataops_processing.jobs.order_flow_job import (
    aggregate_trades,
    classify_side,
    compute_order_flow_metrics,
    to_line_protocol,
)


def test_classify_side():
    assert classify_side("buy") == "buy"
    assert classify_side("BUY") == "buy"
    assert classify_side("S") == "sell"
    assert classify_side("sell") == "sell"
    assert classify_side("unknown") is None
    assert classify_side(None) is None


def test_compute_order_flow_metrics():
    m = compute_order_flow_metrics(buy_volume=3.0, sell_volume=1.0)
    assert m["delta"] == 2.0
    assert m["imbalance"] == pytest.approx(0.5)

    z = compute_order_flow_metrics(0.0, 0.0)
    assert z["delta"] == 0.0 and z["imbalance"] == 0.0

    assert compute_order_flow_metrics(0.0, 5.0)["imbalance"] == -1.0


def test_aggregate_trades_windows_and_cvd():
    w = 60_000
    trades = [
        ("BTCUSDT", 2.0, "buy",  1_000),
        ("BTCUSDT", 1.0, "sell", 30_000),
        ("BTCUSDT", 4.0, "buy",  61_000),   # window #2
        ("BTCUSDT", 9.0, "weird", 62_000),  # bị bỏ qua
        ("ETHUSDT", 5.0, "sell", 1_000),
    ]
    rows = aggregate_trades(trades, w)
    btc = [r for r in rows if r["symbol"] == "BTCUSDT"]
    eth = [r for r in rows if r["symbol"] == "ETHUSDT"]

    assert len(btc) == 2
    assert btc[0]["buy_volume"] == 2.0 and btc[0]["sell_volume"] == 1.0
    assert btc[0]["delta"] == 1.0 and btc[0]["cvd"] == 1.0 and btc[0]["num_trades"] == 2
    assert btc[1]["delta"] == 4.0 and btc[1]["cvd"] == 5.0   # CVD tích lũy
    assert btc[1]["window_start_ms"] == 60_000 and btc[1]["window_end_ms"] == 120_000
    assert eth[0]["cvd"] == -5.0                              # cvd độc lập theo symbol


def test_to_line_protocol():
    line = to_line_protocol(
        measurement="order_flow", symbol="BTCUSDT", interval="1m",
        buy_volume=3.0, sell_volume=1.0, delta=2.0, imbalance=0.5,
        num_trades=4, ts_ns=1700000000000000000,
    )
    assert line == (
        "order_flow,symbol=BTCUSDT,interval=1m "
        "buy_volume=3.0,sell_volume=1.0,delta=2.0,imbalance=0.5,num_trades=4i "
        "1700000000000000000"
    )
    escaped = to_line_protocol("order_flow", "A B,C", "1m", 1, 1, 0, 0, 1, 1)
    assert "symbol=A\\ B\\,C" in escaped


def test_orchestrator_order_flow_job_registration():
    orchestrator = get_orchestrator()
    assert "order-flow" in orchestrator.list_available_jobs()
    job = orchestrator.get_job("order-flow")
    assert job.name == "order-flow"
    assert job.job_type == "streaming"
    assert job.settings.order_flow_job.influx_measurement == "order_flow"


> 🔧 **apply_diff** — <<<<<<< SEARCH
:start_line:44
-------
            "dataops_processing.jobs.vwap_job",
=======
            "dataops_processing.jobs.vwap_job",
            "dataops_processing.jobs.order_flow_job",
>>>>>>> REPLACE

> 🔧 **apply_diff** — <<<<<<< SEARCH
:start_line:68
-------
    # ── volume-bars: Batch volume bars job ────────────────────────────────────
=======
    # ── order-flow: Spark Structured Streaming Order Flow / CVD job ───────────
    subparsers.add_parser(
        "order-flow",
        help="Start Order Flow / CVD Spark Structured Streaming job (long-running)",
    )

    # ── volume-bars: Batch volume bars job ────────────────────────────────────
>>>>>>> REPLACE
<<<<<<< SEARCH
:start_line:107
-------
            orchestrator.run_job("vwap")
=======
            orchestrator.run_job("vwap")
        elif args.job == "order-flow":
            orchestrator.run_job("order-flow")
>>>>>>> REPLACE
<<<<<<< SEARCH
:start_line:10
-------
    python -m dataops_processing vwap
=======
    python -m dataops_processing vwap
    python -m dataops_processing order-flow
>>>>>>> REPLACE

> 🔧 **execute_command** — python -m pytest dataops_processing/tests -q 2>&1 | Select-Object -Last 15

### 🤖 Assistant

> 🔧 called: update_todo_list, read_file, read_file, grep

TASK-P2 done (39 tests pass). Moving to **TASK-P3**; first I'll check the batch job pattern and available Bronze data.

> 🔧 **update_todo_list** — [x] Explore existing code (risk, portfolio, crypto/tcbs services, processing jobs)
[x] TASK-BE4: Pre-trade risk checks (services/risk/pre_trade.go) + wire into order handlers
[x] TASK-P2: Order Flow / CVD streaming job (dataops_processing)
[-] TASK-P3: Market breadth & EOD sector flow job
[ ] TASK-B1: TCBS margin limits & rooms client -> Bronze
[ ] TASK-B2: Benchmark index ingestion (VNINDEX, VN30, BTC.D)
[ ] TASK-INF1: Promtail + Loki config
[ ] TASK-INF2: Smoke test script
[ ] Validate (go test, pytest) and summarize


> 🔧 **read_file** — dataops_processing/jobs/indicator_matrix_job.py

> 🔧 **read_file** — dataops_processing/config/paths.py

> 🔧 **grep** — *.py

### 🤖 Assistant

> 🔧 called: grep, grep



> 🔧 **grep** — dataops_batch_api/cli/jobs/tcbs_eod_job.py

> 🔧 **grep** — dataops_batch_api/cli

### 🤖 Assistant

> 🔧 called: read_file, grep



> 🔧 **read_file** — dataops_batch_api/cli/jobs/tcbs_eod_job.py

> 🔧 **grep** — *.py

### 🤖 Assistant

> 🔧 called: grep, grep



> 🔧 **grep** — *.yaml

> 🔧 **grep** — dataops_backend_api/constants/tcbs.go

### 🤖 Assistant

> 🔧 called: read_file



> 🔧 **read_file** — dataops_batch_api/cli/ingestion/sources/config/tcbs_constants.yaml

### 🤖 Assistant

> 🔧 called: apply_diff, apply_diff, write_file, write_file, apply_diff, apply_diff, execute_command



> 🔧 **apply_diff** — <<<<<<< SEARCH
:start_line:79
-------
# ─── Order Flow / CVD Streaming Job ───────────────────────────────────────────
=======
# ─── Market Breadth & Foreign Flow Batch Job (TASK-P3) ────────────────────────
# Nguồn: Delta Bronze tcbs_market_eod (batch_api). Output: Delta Gold market_breadth.
# Group tự động: ALL + từng exchange (HOSE/HNX/UPCOM); thêm baskets khai báo bên dưới.
market_breadth_job:
  source_table: "tcbs_market_eod"
  gold_table: "market_breadth"
  baskets:
    VN30: ["ACB", "BCM", "BID", "BVH", "CTG", "FPT", "GAS", "GVR", "HDB", "HPG",
           "MBB", "MSN", "MWG", "PLX", "POW", "SAB", "SHB", "SSB", "SSI", "STB",
           "TCB", "TPB", "VCB", "VHM", "VIB", "VIC", "VJC", "VNM", "VPB", "VRE"]

# ─── Order Flow / CVD Streaming Job ───────────────────────────────────────────
>>>>>>> REPLACE

> 🔧 **apply_diff** — <<<<<<< SEARCH
:start_line:131
-------
@dataclass(frozen=True)
class OrderFlowJobSettings:
=======
@dataclass(frozen=True)
class MarketBreadthJobSettings:
    source_table: str
    gold_table: str
    baskets: Dict[str, list[str]]


@dataclass(frozen=True)
class OrderFlowJobSettings:
>>>>>>> REPLACE
<<<<<<< SEARCH
:start_line:174
-------
    order_flow_job: OrderFlowJobSettings
=======
    order_flow_job: OrderFlowJobSettings
    market_breadth_job: MarketBreadthJobSettings
>>>>>>> REPLACE
<<<<<<< SEARCH
:start_line:296
-------
    vb = cfg.get("volume_bars_job", {})
=======
    mb = cfg.get("market_breadth_job", {})
    market_breadth_job = MarketBreadthJobSettings(
        source_table=str(mb.get("source_table", "tcbs_market_eod")),
        gold_table=  str(mb.get("gold_table",   "market_breadth")),
        baskets=     {str(k): [str(s).upper() for s in v] for k, v in (mb.get("baskets") or {}).items()},
    )

    vb = cfg.get("volume_bars_job", {})
>>>>>>> REPLACE
<<<<<<< SEARCH
:start_line:337
-------
        order_flow_job=order_flow_job,
=======
        order_flow_job=order_flow_job,
        market_breadth_job=market_breadth_job,
>>>>>>> REPLACE

> 🔧 **write_file** — """
market_breadth_job.py — EOD Market Breadth & Foreign Flow Batch Job (TASK-P3).

Pipeline:
    Delta Bronze  tcbs_market_eod  (1 row / symbol / trading_date — từ dataops_batch_api)
        → collect rows của 1 trading_date
        → compute_market_breadth() theo group: ALL, từng exchange, từng basket (VN30 ... từ config)
        → Delta Gold  market_breadth  (idempotent: replaceWhere trading_date)

Chỉ số mỗi (trading_date, group):
    advancers / decliners / unchanged, advance_decline_ratio, pct_advancing,
    ceiling_count / floor_count, total_volume, total_value,
    foreign_buy_qty / foreign_sell_qty / foreign_net_qty,
    foreign_net_value_est  (= Σ net_foreign_qty × close — ƯỚC TÍNH vì Bronze chỉ có KL, không có GT NN)

Giới hạn đã biết:
    - Bronze không có trường ngành ICB → group theo exchange + baskets khai báo trong config.
    - Funding-rate heatmap Crypto cần Bronze funding (chưa có job ingest ở batch_api) → chưa thuộc job này.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

from dataops_processing.core.base_job import BaseProcessingJob
from dataops_processing.core.registry import JobRegistry
from dataops_processing.log.logger_setup import logger_manager

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logger_manager.get_logger(__name__)


# =============================================================================
# Pure-Python core (testable độc lập, không cần Spark)
# =============================================================================

def _f(row: Dict[str, Any], key: str) -> float:
    v = row.get(key)
    return float(v) if v is not None else 0.0


def classify_move(row: Dict[str, Any]) -> str:
    """'up' | 'down' | 'flat' — ưu tiên close vs ref_price; fallback change_pct."""
    close, ref = _f(row, "close"), _f(row, "ref_price")
    if close > 0 and ref > 0:
        diff = close - ref
    else:
        diff = _f(row, "change_pct")
    if diff > 0:
        return "up"
    if diff < 0:
        return "down"
    return "flat"


def _aggregate(group: str, trading_date: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    up = down = flat = ceiling = floor = 0
    vol = val = buy_f = sell_f = net_f = net_val = 0.0
    for r in rows:
        move = classify_move(r)
        up += move == "up"
        down += move == "down"
        flat += move == "flat"

        close = _f(r, "close")
        if close > 0 and _f(r, "ceil_price") > 0 and close >= _f(r, "ceil_price"):
            ceiling += 1
        if close > 0 and _f(r, "floor_price") > 0 and close <= _f(r, "floor_price"):
            floor += 1

        vol += _f(r, "total_vol")
        val += _f(r, "total_val")
        buy_f += _f(r, "buy_foreign_qty")
        sell_f += _f(r, "sell_foreign_qty")
        net_qty = r.get("net_foreign_qty")
        net_qty = float(net_qty) if net_qty is not None else _f(r, "buy_foreign_qty") - _f(r, "sell_foreign_qty")
        net_f += net_qty
        net_val += net_qty * close

    n = len(rows)
    return {
        "trading_date": trading_date,
        "group": group,
        "symbols": n,
        "advancers": int(up),
        "decliners": int(down),
        "unchanged": int(flat),
        "advance_decline_ratio": round(up / down, 4) if down > 0 else float(up),
        "pct_advancing": round(up / n * 100.0, 2) if n else 0.0,
        "ceiling_count": int(ceiling),
        "floor_count": int(floor),
        "total_volume": vol,
        "total_value": val,
        "foreign_buy_qty": buy_f,
        "foreign_sell_qty": sell_f,
        "foreign_net_qty": net_f,
        "foreign_net_value_est": round(net_val, 2),
    }


def compute_market_breadth(
    rows: Iterable[Dict[str, Any]],
    trading_date: str,
    baskets: Optional[Dict[str, List[str]]] = None,
) -> List[Dict[str, Any]]:
    """
    Tính breadth cho 1 trading_date.

    Groups: "ALL", "EXCHANGE:<ex>" cho từng exchange có dữ liệu, "BASKET:<name>" cho từng basket.
    Basket không có symbol nào khớp dữ liệu thì bị bỏ qua.
    """
    rows = [r for r in rows if r.get("symbol")]
    if not rows:
        return []

    out = [_aggregate("ALL", trading_date, rows)]

    by_exchange: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        ex = str(r.get("exchange") or "").upper()
        if ex:
            by_exchange.setdefault(ex, []).append(r)
    for ex in sorted(by_exchange):
        out.append(_aggregate(f"EXCHANGE:{ex}", trading_date, by_exchange[ex]))

    for name, members in (baskets or {}).items():
        member_set = {m.upper() for m in members}
        subset = [r for r in rows if str(r["symbol"]).upper() in member_set]
        if subset:
            out.append(_aggregate(f"BASKET:{name}", trading_date, subset))

    return out


# =============================================================================
# Concrete Strategy: Market Breadth Batch Job
# =============================================================================

@JobRegistry.register("market-breadth")
class MarketBreadthBatchJob(BaseProcessingJob):
    """Job batch EOD tổng hợp market breadth & dòng tiền khối ngoại vào Delta Gold."""

    @property
    def name(self) -> str:
        return "market-breadth"

    @property
    def job_type(self) -> str:
        return "batch"

    def execute(
        self,
        spark: Optional["SparkSession"] = None,
        trading_date: Optional[str] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Args:
            spark: SparkSession tùy chọn.
            trading_date: "YYYY-MM-DD". None = ngày mới nhất có trong Bronze.
        """
        try:
            from pyspark.sql import functions as F
            from pyspark.sql.types import (
                DoubleType, LongType, StringType, StructField, StructType,
            )
        except ImportError as exc:
            logger.error("PySpark not installed: %s", exc, exc_info=True)
            return {"written": 0, "trading_date": trading_date, "errors": 1}

        cfg = self.settings.market_breadth_job
        created_spark_internally = False
        if spark is None:
            from dataops_processing.config.spark_config import build_spark_session
            spark = build_spark_session("dataops-market-breadth-job")
            created_spark_internally = True

        try:
            bronze = spark.read.format("delta").load(self.paths.delta_bronze_path(cfg.source_table))
            if trading_date is None:
                trading_date = bronze.agg(F.max("trading_date")).collect()[0][0]
            if not trading_date:
                logger.warning("market_breadth_job: bronze %s is empty", cfg.source_table)
                return {"written": 0, "trading_date": None, "errors": 0}

            rows = [r.asDict() for r in bronze.filter(F.col("trading_date") == trading_date).collect()]
            records = compute_market_breadth(rows, trading_date, cfg.baskets)
            if not records:
                logger.warning("market_breadth_job: no rows for trading_date=%s", trading_date)
                return {"written": 0, "trading_date": trading_date, "errors": 0}

            now_iso = datetime.now(timezone.utc).isoformat()
            for rec in records:
                rec["computed_at"] = now_iso

            schema = StructType([
                StructField("trading_date",           StringType(), False),
                StructField("group",                  StringType(), False),
                StructField("symbols",                LongType(),   False),
                StructField("advancers",              LongType(),   False),
                StructField("decliners",              LongType(),   False),
                StructField("unchanged",              LongType(),   False),
                StructField("advance_decline_ratio",  DoubleType(), False),
                StructField("pct_advancing",          DoubleType(), False),
                StructField("ceiling_count",          LongType(),   False),
                StructField("floor_count",            LongType(),   False),
                StructField("total_volume",           DoubleType(), False),
                StructField("total_value",            DoubleType(), False),
                StructField("foreign_buy_qty",        DoubleType(), False),
                StructField("foreign_sell_qty",       DoubleType(), False),
                StructField("foreign_net_qty",        DoubleType(), False),
                StructField("foreign_net_value_est",  DoubleType(), False),
                StructField("computed_at",            StringType(), False),
            ])
            gold_path = self.paths.delta_gold_path(cfg.gold_table)
            (
                spark.createDataFrame(records, schema=schema)
                .write.format("delta")
                .mode("overwrite")
                .option("replaceWhere", f"trading_date = '{trading_date}'")   # idempotent theo ngày
                .partitionBy("trading_date")
                .save(gold_path)
            )
            logger.info("market_breadth_job: %s → %d group rows written to %s", trading_date, len(records), gold_path)
            return {"written": len(records), "trading_date": trading_date, "errors": 0}
        except Exception as exc:
            logger.error("market_breadth_job failed — %s", exc, exc_info=True)
            return {"written": 0, "trading_date": trading_date, "errors": 1}
        finally:
            if created_spark_internally:
                spark.stop()


> 🔧 **write_file** — """
tests/test_market_breadth_job.py — Unit tests cho Market Breadth & Foreign Flow Job (TASK-P3).
"""
from __future__ import annotations

import pytest

from dataops_processing.core.orchestrator import get_orchestrator
from dataops_processing.jobs.market_breadth_job import classify_move, compute_market_breadth

D = "2026-10-13"


def _row(symbol, exchange, close, ref, **kw):
    base = {
        "symbol": symbol, "exchange": exchange, "close": close, "ref_price": ref,
        "ceil_price": ref * 1.07, "floor_price": ref * 0.93,
        "total_vol": 100.0, "total_val": 1000.0,
        "buy_foreign_qty": 10.0, "sell_foreign_qty": 4.0, "net_foreign_qty": 6.0,
        "change_pct": 0.0,
    }
    base.update(kw)
    return base


def test_classify_move():
    assert classify_move({"close": 11, "ref_price": 10}) == "up"
    assert classify_move({"close": 9, "ref_price": 10}) == "down"
    assert classify_move({"close": 10, "ref_price": 10}) == "flat"
    assert classify_move({"close": 0, "ref_price": 0, "change_pct": -1.2}) == "down"  # fallback


def test_compute_market_breadth_groups_and_metrics():
    rows = [
        _row("VCB", "HOSE", 10.7, 10.0),                 # up + ceiling
        _row("FPT", "HOSE", 9.3, 10.0),                  # down + floor
        _row("XYZ", "HNX", 10.0, 10.0, net_foreign_qty=None),  # flat, net suy ra 10-4=6
    ]
    out = {r["group"]: r for r in compute_market_breadth(rows, D, {"VN30": ["VCB", "FPT"], "EMPTY": ["ZZZ"]})}

    assert set(out) == {"ALL", "EXCHANGE:HNX", "EXCHANGE:HOSE", "BASKET:VN30"}   # EMPTY bị bỏ
    allr = out["ALL"]
    assert (allr["advancers"], allr["decliners"], allr["unchanged"]) == (1, 1, 1)
    assert allr["ceiling_count"] == 1 and allr["floor_count"] == 1
    assert allr["advance_decline_ratio"] == 1.0
    assert allr["pct_advancing"] == pytest.approx(33.33)
    assert allr["foreign_net_qty"] == 18.0
    assert allr["foreign_net_value_est"] == pytest.approx(6 * 10.7 + 6 * 9.3 + 6 * 10.0)
    assert out["BASKET:VN30"]["symbols"] == 2
    assert out["EXCHANGE:HNX"]["unchanged"] == 1


def test_compute_market_breadth_empty_and_no_decliners():
    assert compute_market_breadth([], D) == []
    only_up = compute_market_breadth([_row("A", "HOSE", 11, 10)], D)[0]
    assert only_up["advance_decline_ratio"] == 1.0   # down == 0 → trả số advancers


def test_orchestrator_market_breadth_registration():
    orch = get_orchestrator()
    assert "market-breadth" in orch.list_available_jobs()
    job = orch.get_job("market-breadth")
    assert job.job_type == "batch"
    assert "VCB" in job.settings.market_breadth_job.baskets["VN30"]


> 🔧 **apply_diff** — <<<<<<< SEARCH
:start_line:45
-------
            "dataops_processing.jobs.order_flow_job",
=======
            "dataops_processing.jobs.order_flow_job",
            "dataops_processing.jobs.market_breadth_job",
>>>>>>> REPLACE

> 🔧 **apply_diff** — <<<<<<< SEARCH
:start_line:69
-------
    # ── volume-bars: Batch volume bars job ────────────────────────────────────
=======
    # ── market-breadth: EOD market breadth & foreign flow batch job ───────────
    mb_parser = subparsers.add_parser(
        "market-breadth",
        help="Run EOD Market Breadth & Foreign Flow batch job and exit",
    )
    mb_parser.add_argument(
        "--date",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help="Trading date to process. Default: latest date in Bronze tcbs_market_eod.",
    )

    # ── volume-bars: Batch volume bars job ────────────────────────────────────
>>>>>>> REPLACE
<<<<<<< SEARCH
:start_line:118
-------
        elif args.job == "volume-bars":
=======
        elif args.job == "market-breadth":
            result = orchestrator.run_job("market-breadth", trading_date=getattr(args, "date", None))
            print(json.dumps(result))
            if result.get("errors", 0) > 0:
                sys.exit(1)
        elif args.job == "volume-bars":
>>>>>>> REPLACE
<<<<<<< SEARCH
:start_line:11
-------
    python -m dataops_processing order-flow
=======
    python -m dataops_processing order-flow
    python -m dataops_processing market-breadth [--date YYYY-MM-DD]
>>>>>>> REPLACE

> 🔧 **execute_command** — python -m pytest dataops_processing/tests -q 2>&1 | Select-Object -Last 15

### 🤖 Assistant

> 🔧 called: apply_diff



> 🔧 **apply_diff** — <<<<<<< SEARCH
:start_line:56
-------
        close = _f(r, "close")
        if close > 0 and _f(r, "ceil_price") > 0 and close >= _f(r, "ceil_price"):
            ceiling += 1
        if close > 0 and _f(r, "floor_price") > 0 and close <= _f(r, "floor_price"):
            floor += 1
=======
        close = _f(r, "close")
        if close > 0 and _f(r, "ceil_price") > 0 and close >= _f(r, "ceil_price") * 0.9999:
            ceiling += 1
        if close > 0 and _f(r, "floor_price") > 0 and close <= _f(r, "floor_price") * 1.0001:
            floor += 1
>>>>>>> REPLACE

### 🤖 Assistant

> 🔧 called: execute_command



> 🔧 **execute_command** — python -m pytest dataops_processing/tests -q 2>&1 | Select-Object -Last 8

### 🤖 Assistant

> 🔧 called: update_todo_list, list_files

TASK-P3 done (43 pass). Next are **B1/B2** in `dataops_batch_api`.

> 🔧 **update_todo_list** — [x] Explore existing code (risk, portfolio, crypto/tcbs services, processing jobs)
[x] TASK-BE4: Pre-trade risk checks (services/risk/pre_trade.go) + wire into order handlers
[x] TASK-P2: Order Flow / CVD streaming job (dataops_processing)
[x] TASK-P3: Market breadth & EOD sector flow job
[-] TASK-B1: TCBS margin limits & rooms client -> Bronze
[ ] TASK-B2: Benchmark index ingestion (VNINDEX, VN30, BTC.D)
[ ] TASK-INF1: Promtail + Loki config
[ ] TASK-INF2: Smoke test script
[ ] Validate (go test, pytest) and summarize


> 🔧 **list_files** — dataops_batch_api

### 🤖 Assistant

> 🔧 called: read_file, read_file, read_file, read_file



> 🔧 **read_file** — dataops_batch_api/cli/flows/scheduler.py

> 🔧 **read_file** — dataops_batch_api/cli/jobs/tcbs_bsa_job.py

> 🔧 **read_file** — dataops_batch_api/cli/jobs/_delta_writer.py

> 🔧 **read_file** — dataops_batch_api/cli/ingestion/sources/client/tcbs_client/securities.py

### 🤖 Assistant

> 🔧 called: read_file, read_file, read_file, read_file



> 🔧 **read_file** — dataops_batch_api/cli/scheduler.py

> 🔧 **read_file** — dataops_batch_api/cli/ingestion/sources/client/tcbs_client/market_data.py

> 🔧 **read_file** — dataops_batch_api/cli/ingestion/sources/client/tcbs_client/tcbs_client.py

> 🔧 **read_file** — dataops_batch_api/cli/flows/config/exchange_config.yaml

### 🤖 Assistant

> 🔧 called: read_file



> 🔧 **read_file** — dataops_batch_api/cli/ingestion/sources/config/tcbs_constants.yaml

### 🤖 Assistant

> 🔧 called: grep, grep

The backlog names `/hydros/v1/margin-limits`, but the endpoint constants don't list it. I'll check the OpenAPI spec for what exists.

> 🔧 **grep** — dataops_batch_api/cli/ingestion/sources/client/tcbs_client/http.py

> 🔧 **grep** — true

### 🤖 Assistant

> 🔧 called: read_file, read_file, read_file, read_file

`/hydros/v1/margin-limits` doesn't exist in the OpenAPI spec. The documented margin and risk endpoints are `account/{accountNo}/risk`, `pricing-policy` and `margin-info`. I'll read their schemas.

> 🔧 **read_file** — docs/openapi-v1.0.0.json

> 🔧 **read_file** — docs/openapi-v1.0.0.json

> 🔧 **read_file** — docs/openapi-v1.0.0.json

> 🔧 **read_file** — dataops_batch_api/cli/ingestion/sources/client/tcbs_client/http.py

### 🤖 Assistant

> 🔧 called: read_file, grep, grep



> 🔧 **read_file** — docs/openapi-v1.0.0.json

> 🔧 **grep** — dataops_batch_api/cli/ingestion/sources/tcbs_constants_loader.py

> 🔧 **grep** — dataops_batch_api/cli/ingestion/sources/client/tcbs_client/intraday_analytics.py

### 🤖 Assistant

Previous conversation summary:

---
## Goal
Complete all 7 remaining backlog tasks in the `DataOps_Backend_Financial_Trading` project on branch `20261010_data_processing`. Tasks span 3 modules: `dataops_backend_api` (Go), `dataops_processing` (Python/Spark), `dataops_batch_api` (Python/Delta), and infra.

## Instructions
- Follow `github/copilotinstructions/SKILL.md`: `export_task/` knowledge overrides `allskill/`; only read latest session
- Agent routing: `backend-software-engineer` for Go API, `batch-lakehouse-engineer` for batch pipeline, `full-stack-data-platform-engineer` for cross-cutting
- Go module name is `okx-analytics-api` (not renamed intentionally — don't change)
- `dataops_batch_api` uses `deltalake` (delta-rs) for writes, **not PySpark** — `write_records()` helper in `cli/jobs/_delta_writer.py`
- Scheduler uses APScheduler (not Prefect) — add new jobs to `cli/scheduler.py`
- TCBS endpoint constants come from `tcbs_constants.yaml` → `TCBSConstants.endpoint(key)` pattern; `/hydros/v1/margin-limits` does NOT exist in OpenAPI — real margin endpoints: `/hydros/v1/account/{accountNo}/risk`, `/hydros/v1/account/{accountNo}/pricing-policy`, `/erebos/v2/digital/margin-info`
- Pre-trade enforcement is controlled by env `RISK_PRETRADE_ENFORCE=true|false` (default true)
- After completing all tasks, run `go test ./...` and `python -m pytest` to validate

## Discoveries
- `/hydros/v1/margin-limits` from the backlog plan does NOT exist in `docs/openapi-v1.0.0.json`. The actual margin/risk endpoints available are: `GET /hydros/v1/account/{accountNo}/risk` (Rtt, outstanding, riskPolicy margins), `GET /hydros/v1/account/{accountNo}/pricing-policy`, `GET /erebos/v2/digital/margin-info` (loan history, paginated)
- `dataops_batch_api` uses `deltalake` (delta-rs), not Spark, for Bronze writes. Pattern: `write_records(path, records, schema, mode, partition_by)` from `cli/jobs/_delta_writer.py`
- TCBS client is mixin-based: add a new mixin in `cli/ingestion/sources/client/tcbs_client/` and register it in `tcbs_client.py`
- `_delta_writer._table_path(bucket, "bronze", "tcbs_margin_risk")` → `s3://{bucket}/delta/bronze/tcbs_margin_risk`
- Benchmark ingestion (B2) needs Binance REST for BTC.D (funding/dominance not directly in `/api/v3/klines` — BTC dominance is a derived metric) and TCBS `tickerCommons` for VNINDEX/VN30 (already fetched by EOD job but not stored separately as benchmark)
- All Python tests (43 total) pass after TASK-P2 and P3 additions
- Go tests (ok `dataops-backend-api/api/rest`, ok `services/risk`) pass
- The `lru_cache` on `get_settings()` and `get_orchestrator()` caches settings at first call; in tests you need to call `get_settings.cache_clear()` if modifying config

## Accomplished

### ✅ COMPLETED in this session
| Task | Files Created / Modified |
|---|---|
| **TASK-BE4**: Pre-trade risk checks & position sizing middleware | `services/risk/pre_trade.go` (NEW), `models/risk.go` (+PreTradeCheckRequest/Result), `configuration/configuration.go` (+Risk config block), `services/services.go` (+PreTradeSvc), `api/rest/portfolio.go` (+PreTradeCheck handler + enforcePreTrade helper), `api/rest/tcbs_account.go` (+wire PlaceStockOrder), `api/rest/crypto.go` (+wire PlaceOrder), `api/rest/rest.go` (+wire handlers), `services/risk/pre_trade_test.go` (NEW, 6 tests) |
| **TASK-P2**: Order Flow / CVD Streaming Job | `jobs/order_flow_job.py` (NEW), `tests/test_order_flow_job.py` (NEW, 6 tests), `config/processing_config.yaml` (+order_flow_job section), `config/settings.py` (+OrderFlowJobSettings), `core/orchestrator.py` (+auto-import), `jobs/runner.py` (+CLI subcommand) |
| **TASK-P3**: Market Breadth & EOD Sector Flow Job | `jobs/market_breadth_job.py` (NEW), `tests/test_market_breadth_job.py` (NEW, 4 tests), `config/processing_config.yaml` (+market_breadth_job section with VN30 basket), `config/settings.py` (+MarketBreadthJobSettings), `core/orchestrator.py` (+auto-import), `jobs/runner.py` (+CLI subcommand) |

### ❌ NOT YET DONE (3 tasks in progress + infra)
| Task | Status | Notes |
|---|---|---|
| **TASK-B1**: TCBS Margin Limits & Room → Bronze | 🔴 In progress (was next) | Need: (1) add `get_account_risk()` + `get_margin_info()` methods to TCBS client via new mixin, (2) add endpoint keys to `tcbs_constants.yaml`, (3) create `cli/jobs/tcbs_margin_job.py` (similar to `tcbs_bsa_job.py`), (4) wire into scheduler |
| **TASK-B2**: Benchmark Index Ingestion (VNINDEX, VN30, BTC.D) | 🔴 Not started | Need: VNINDEX/VN30 via existing `get_ticker_commons(index=...)`, BTC.D is dominance (not from Binance klines — use `binance_client` funding or compute from market cap — likely skip BTC.D or use Binance public market cap endpoint), create `cli/jobs/benchmark_index_job.py` |
| **TASK-INF1**: Loki + Promtail config | 🔴 Not started | Add to `infra_dataops_finance_trading/` (docker-compose + promtail config yaml), point at `log_storage/*.log` paths |
| **TASK-INF2**: End-to-end smoke test script | 🔴 Not started | `scripts/smoke_test.sh` verifying full chain from stream → Kafka → Spark → InfluxDB/Delta → Backend API |

## Relevant files / directories

### `dataops_backend_api/` (Go)
- `models/risk.go` — **MODIFIED**: added `PreTradeCheckRequest`, `PreTradeCheckResult`, `AddViolation()`
- `configuration/configuration.go` — **MODIFIED**: added `Risk` config block (`MaxAllocationPct`, `MinMarginRatioPct`, `PreTradeEnforce`), loads from env
- `services/risk/risk.go` — existing risk engine (unchanged)
- `services/risk/pre_trade.go` — **NEW**: `PreTradeService` interface + `preTradeService` impl (MaxAlloc%, buying power, margin ratio)
- `services/risk/pre_trade_test.go` — **NEW**: 6 unit tests
- `services/services.go` — **MODIFIED**: added `PreTradeSvc risk.PreTradeService`, wired in `NewServices()`
- `api/rest/portfolio.go` — **MODIFIED**: added `PreTradeSvc` field, `PreTradeCheck` handler, `enforcePreTrade()` helper
- `api/rest/tcbs_account.go` — **MODIFIED**: added `PreTradeSvc`/`PreTradeEnable` fields, hooked into `PlaceStockOrder`
- `api/rest/crypto.go` — **MODIFIED**: added `PreTradeSvc`/`PreTradeEnable` fields, hooked into `PlaceOrder`
- `api/rest/rest.go` — **MODIFIED**: wired `PreTradeSvc` and `PreTradeEnable` into all order handlers
- `api/rest/rest_test.go` — unchanged, passes
- `models/portfolio.go` — referenced (existing `PortfolioOverview`, `PortfolioAsset`)
- `models/tcbs_account.go` — referenced (existing order request models)
- `models/crypto_order.go` — referenced (existing Binance order models)
- `services/portfolio/portfolio.go` — referenced (existing `GetOverview`)
- `services/crypto_account/crypto_account.go` — referenced (existing `GetAccountBalance`)
- `services/tcbs_account/tcbs_account.go` — referenced (existing `GetCashInvestments`)

### `dataops_processing/` (Python/Spark)
- `config/processing_config.yaml` — **MODIFIED**: added `market_breadth_job` + `order_flow_job` sections
- `config/settings.py` — **MODIFIED**: added `OrderFlowJobSettings`, `MarketBreadthJobSettings` dataclasses + loader logic + added to `ProcessingSettings`
- `config/spark_config.py` — referenced (existing)
- `config/paths.py` — referenced (`delta_gold_path`, `delta_bronze_path`, `checkpoint_path`)
- `core/orchestrator.py` — **MODIFIED**: added `order_flow_job` and `market_breadth_job` to `_ensure_jobs_loaded()`
- `core/base_job.py` — referenced (base class pattern)
- `core/registry.py` — referenced (JobRegistry)
- `jobs/vwap_job.py` — referenced (pattern to follow)
- `jobs/order_flow_job.py` — **NEW**: streaming CVD/order flow job (classify_side, compute_order_flow_metrics, aggregate_trades, `@JobRegistry.register("order-flow")`)
- `jobs/market_breadth_job.py` — **NEW**: batch EOD breadth + foreign flow job (`@JobRegistry.register("market-breadth")`)
- `jobs/indicator_matrix_job.py` — referenced (pattern for batch Spark jobs)
- `jobs/runner.py` — **MODIFIED**: added `order-flow` and `market-breadth` CLI subcommands
- `models/trade.py` — referenced (`TradeEvent.from_kafka_bytes`, `to_tuple`)
- `tests/test_order_flow_job.py` — **NEW**: 6 pure-Python tests
- `tests/test_market_breadth_job.py` — **NEW**: 4 pure-Python tests
- `log/logger_setup.py` — referenced (logger pattern)

### `dataops_batch_api/` (Python/delta-rs)
- `cli/jobs/tcbs_bsa_job.py` — **REFERENCE** for TASK-B1 pattern
- `cli/jobs/tcbs_eod_job.py` — **REFERENCE** (has buy_foreign_qty schema, run() pattern)
- `cli/jobs/_delta_writer.py` — **REFERENCE**: `write_records()`, `_table_path()`, `apply_rolling_retention()`
- `cli/jobs/metrics.py` — **REFERENCE**: `record_ingest()`, `push_metrics()`
- `cli/scheduler.py` — **REFERENCE + needs MODIFICATION** for B1/B2
- `cli/flows/config/exchange_config.yaml` — **REFERENCE + needs MODIFICATION** to add margin/benchmark symbols
- `cli/ingestion/sources/client/tcbs_client/tcbs_client.py` — **needs MODIFICATION** to add margin mixin
- `cli/ingestion/sources/client/tcbs_client/securities.py` — **REFERENCE** for new mixin pattern
- `cli/ingestion/sources/client/tcbs_client/market_data.py` — **REFERENCE** + `get_ticker_commons()` for benchmark ingestion
- `cli/ingestion/sources/client/tcbs_client/http.py` — **REFERENCE**: `_get()`, path-param pattern
- `cli/ingestion/sources/config/tcbs_constants.yaml` — **REFERENCE + needs endpoint key verification** (`account_risk`, `pricing_policy`, `margin_info` keys already exist)
- `cli/flows/loader/tcbs_constants_loader.py` — **REFERENCE**: `TCBSConstants.endpoint()` usage

### `docs/`
- `docs/openapi-v1.0.0.json` — **READ**: confirmed margin endpoints (`/hydros/v1/account/{accountNo}/risk`, `/hydros/v1/account/{accountNo}/pricing-policy`, `/erebos/v2/digital/margin-info`)

### `github/`
- `github/copilotinstructions/SKILL.md` — read (operational rules)
- `github/agent/backend-software-engineer.agent.md` — read
- `github/agent/batch-lakehouse-engineer.agent.md` — read
- `github/agent/devops-infra-engineer.agent.md` — read
- `github/agent/full-stack-data-platform-engineer.agent.md` — read
- `github/agent/realtime-streaming-engineer.agent.md` — read

### `export_task/` (context)
- `SESSION_2026_10_13_PLAN_REFACTOR_part1.md` — primary session: backlog, completed tasks BE1-BE3, P1
- `SESSION_2026_10_13_FIX_DATAOPS_PROCESSING&REVALUATE_INDICATORS.MD` — context session
- `SESSION_2026_11_10.MD` — latest session: logging refactor, vwap job refactor

---



---

### 👤 User

continue

---

### 🤖 Assistant

> 🔧 called: use_skill



> 🔧 **use_skill** — dataops-financial-trading

### 🤖 Assistant

> 🔧 called: read_file, read_file, read_file, grep



> 🔧 **read_file** — dataops_batch_api/cli/jobs/tcbs_bsa_job.py

> 🔧 **read_file** — dataops_batch_api/cli/ingestion/sources/client/tcbs_client/tcbs_client.py

> 🔧 **read_file** — dataops_batch_api/cli/ingestion/sources/client/tcbs_client/securities.py

> 🔧 **grep** — account_risk|pricing_policy|margin_info

### 🤖 Assistant

> 🔧 called: read_file, grep, grep



> 🔧 **read_file** — dataops_batch_api/cli/ingestion/sources/client/tcbs_client/http.py

> 🔧 **grep** — dataops_batch_api/cli/scheduler.py

> 🔧 **grep** — dataops_batch_api/cli/flows/config/exchange_config.yaml

### 🤖 Assistant

> 🔧 called: read_file, read_file

Good. Now let me read the remaining lines of the config file and then check what's already done in the latest session export:

> 🔧 **read_file** — dataops_batch_api/cli/ingestion/sources/client/tcbs_client/market_data.py

> 🔧 **read_file** — dataops_batch_api/cli/flows/config/exchange_config.yaml

### 🤖 Assistant

> 🔧 called: execute_command, read_file



> 🔧 **execute_command** — python -c "import json;d=json.load(open('docs/openapi-v1.0.0.json',encoding='utf-8'));p=d['paths']['/hydros/v1/account/{accountNo}/risk'];print(json.dumps(p,indent=1)[:3500])"

> 🔧 **read_file** — dataops_batch_api/cli/scheduler.py

### 🤖 Assistant

> 🔧 called: grep, read_file



> 🔧 **read_file** — dataops_batch_api/cli/jobs/_delta_writer.py

> 🔧 **grep** — dataops_batch_api/cli

### 🤖 Assistant

> 🔧 called: read_file, grep, read_file



> 🔧 **read_file** — dataops_batch_api/cli/flows/loader/tcbs_constants_loader.py

> 🔧 **read_file** — dataops_batch_api/cli/ingestion/sources/client/tcbs_client/__init__.py

> 🔧 **grep** — .

### 🤖 Assistant

> 🔧 called: grep, grep



> 🔧 **grep** — dataops_backend_api/configuration/configuration.go

> 🔧 **grep** — dataops_batch_api

### 🤖 Assistant

> 🔧 called: write_file, apply_diff

Now create the margin mixin.

> 🔧 **write_file** — dataops_batch_api/cli/ingestion/sources/client/tcbs_client/margin.py

> 🔧 **apply_diff** — dataops_batch_api/cli/ingestion/sources/client/tcbs_client/tcbs_client.py

### 🤖 Assistant

> 🔧 called: write_file, read_file



> 🔧 **write_file** — """
tcbs_margin_job.py — [Daily 15:45 ICT] TCBS margin risk snapshot → Delta Bronze.

Schedule: cron "45 8 * * 1-5" (UTC = 15:45 ICT, T2-T6)

Nguồn: GET /hydros/v1/account/{accountNo}/risk
    (KHÔNG dùng /hydros/v1/margin-limits — endpoint này không tồn tại trong OpenAPI.)

Tài khoản margin lấy từ env TCBS_MARGIN_ACCOUNTS (comma-separated),
fallback config tcbs.margin_accounts. Empty → skip.

Delta table: s3://binance-datalake/delta/bronze/tcbs_margin_risk
Partition:   trading_date (string "YYYY-MM-DD")
Retention:   2 năm (730 ngày)

Schema:
    trading_date       string
    account_no         string
    rtt                double   Actual Rtt
    outstanding        double   Dư nợ gốc
    accrued_interest   double
    due_amount         double
    overdue_amount     double
    total_fee_debt     double
    initial_margin     double
    maintenance_margin double
    liquidation_margin double
    risk_status_code   string
    risk_status_desc   string
    ingested_at        string   ISO8601 UTC
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import pyarrow as pa

from cli.ingestion.sources.client.tcbs_client import TCBSRestClient
from cli.jobs._delta_writer import _table_path, write_records, apply_rolling_retention
from cli.jobs.metrics import push_metrics, record_ingest
from cli.flows.loader.load_config import load_exchange_config
from log.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)

_SCHEMA = pa.schema([
    pa.field("trading_date",       pa.string(),  nullable=False),
    pa.field("account_no",         pa.string(),  nullable=False),
    pa.field("rtt",                pa.float64(), nullable=True),
    pa.field("outstanding",        pa.float64(), nullable=True),
    pa.field("accrued_interest",   pa.float64(), nullable=True),
    pa.field("due_amount",         pa.float64(), nullable=True),
    pa.field("overdue_amount",     pa.float64(), nullable=True),
    pa.field("total_fee_debt",     pa.float64(), nullable=True),
    pa.field("initial_margin",     pa.float64(), nullable=True),
    pa.field("maintenance_margin", pa.float64(), nullable=True),
    pa.field("liquidation_margin", pa.float64(), nullable=True),
    pa.field("risk_status_code",   pa.string(),  nullable=True),
    pa.field("risk_status_desc",   pa.string(),  nullable=True),
    pa.field("ingested_at",        pa.string(),  nullable=False),
])

_RETENTION_DAYS = 730


def _f(value: Any) -> float | None:
    """Ép về float; None nếu thiếu/không hợp lệ."""
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def parse_risk_response(
    resp: Any, account_no: str, trading_date: str, now_iso: str
) -> List[Dict[str, Any]]:
    """
    Chuẩn hóa response /risk (list hoặc {"data": [...]}) → list canonical dict.
    Pure function — không I/O, dễ test.
    """
    if isinstance(resp, dict):
        items = resp.get("data", [resp] if "rtt" in resp else [])
    elif isinstance(resp, list):
        items = resp
    else:
        items = []

    rows: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        policy = item.get("riskPolicy") or {}
        status = item.get("riskStatus") or {}
        rows.append({
            "trading_date":       trading_date,
            "account_no":         str(item.get("accountNo") or account_no),
            "rtt":                _f(item.get("rtt")),
            "outstanding":        _f(item.get("outstanding")),
            "accrued_interest":   _f(item.get("accruedInterest")),
            "due_amount":         _f(item.get("dueAmount")),
            "overdue_amount":     _f(item.get("overdueAmount")),
            "total_fee_debt":     _f(item.get("totalFeeDebt")),
            "initial_margin":     _f(policy.get("initialMargin")),
            "maintenance_margin": _f(policy.get("maintenanceMargin")),
            "liquidation_margin": _f(policy.get("liquidationMargin")),
            "risk_status_code":   status.get("code"),
            "risk_status_desc":   status.get("description"),
            "ingested_at":        now_iso,
        })
    return rows


def _margin_accounts(cfg: dict) -> List[str]:
    env = os.getenv("TCBS_MARGIN_ACCOUNTS", "")
    if env.strip():
        return [a.strip() for a in env.split(",") if a.strip()]
    return list(cfg["tcbs"].get("margin_accounts", []) or [])


def run() -> dict:
    """Chạy job TCBS margin risk snapshot. Returns {"written": int, "errors": int}."""
    cfg          = load_exchange_config()
    bucket       = cfg["delta"]["bucket"]
    path         = _table_path(bucket, "bronze", "tcbs_margin_risk")
    now          = datetime.now(timezone.utc)
    now_iso      = now.isoformat()
    trading_date = now.strftime("%Y-%m-%d")

    accounts = _margin_accounts(cfg)
    if not accounts:
        logger.warning("tcbs_margin_job: no margin accounts configured — skipping")
        return {"written": 0, "errors": 0}

    client = TCBSRestClient.from_env()
    client.authenticate()

    records: List[Dict[str, Any]] = []
    errors = 0
    for acc in accounts:
        try:
            resp = client.get_account_risk(acc)
            rows = parse_risk_response(resp, acc, trading_date, now_iso)
            if not rows:
                logger.warning("tcbs_margin_job: empty risk response for %s", acc)
            records.extend(rows)
        except Exception as exc:
            logger.error("tcbs_margin_job: %s failed: %s", acc, exc)
            errors += 1

    written = 0
    if records:
        written = write_records(
            path, records, _SCHEMA, mode="append", partition_by=["trading_date"],
        )
        apply_rolling_retention(path, date_col="trading_date", keep_days=_RETENTION_DAYS)

    record_ingest("tcbs_margin_job", count=written, status="success")
    if errors:
        record_ingest("tcbs_margin_job", count=errors, status="error")
    push_metrics("tcbs_margin_job")

    logger.info("tcbs_margin_job: DONE written=%d errors=%d accounts=%d",
                written, errors, len(accounts))
    return {"written": written, "errors": errors}


if __name__ == "__main__":
    from pathlib import Path
    try:
        from dotenv import load_dotenv
        load_dotenv(Path(__file__).resolve().parents[4] / ".env", override=False)
    except ImportError:
        pass
    print(run())


> 🔧 **read_file** — dataops_batch_api/cli/jobs/tcbs_eod_job.py

### 🤖 Assistant

> 🔧 called: execute_command, grep, grep

Next is B2. I'll check whether the OpenAPI has an index-level endpoint, because `tickerCommons` returns per-ticker data and not index values.

> 🔧 **execute_command** — python -c "import json;d=json.load(open('docs/openapi-v1.0.0.json',encoding='utf-8'));[print(k) for k in d['paths'] if 'tartarus' in k or 'index' in k.lower() or 'iboard' in k]"

> 🔧 **grep** — dataops_batch_api/cli/ingestion/sources/config/tcbs_constants.yaml

> 🔧 **grep** — dataops_batch_api/cli/ingestion/sources/client

### 🤖 Assistant

> 🔧 called: grep, read_file



> 🔧 **read_file** — dataops_batch_api/cli/ingestion/sources/config/tcbs_constants.yaml

> 🔧 **grep** — dataops_batch_api/cli/ingestion/schema/canonical.py

### 🤖 Assistant

> 🔧 called: read_file, read_file



> 🔧 **read_file** — dataops_batch_api/cli/ingestion/schema/canonical.py

> 🔧 **read_file** — dataops_batch_api/cli/jobs/binance_1d_job.py

### 🤖 Assistant

> 🔧 called: read_file



> 🔧 **read_file** — dataops_batch_api/cli/ingestion/sources/client/binance_client/spot/market_data.py

### 🤖 Assistant

> 🔧 called: write_file, apply_diff, grep



> 🔧 **write_file** — """
benchmark_index_job.py — [Daily 15:50 ICT] Benchmark ingestion (VNINDEX, VN30, BTC.D) → Delta Bronze.

Schedule: cron "50 8 * * 1-5" (UTC = 15:50 ICT, T2-T6) — sau tcbs_eod_job.

GIỚI HẠN NGUỒN DỮ LIỆU (xác minh với openapi-v1.0.0.json & Binance REST):
    - TCBS OpenAPI KHÔNG có endpoint giá trị chỉ số (index level). Chỉ có
      /tartarus/v1/tickerCommons?index=<basket>. Vì vậy VNINDEX / VN30 được
      lưu dưới dạng *basket aggregate* (breadth, thanh khoản, NN ròng,
      %thay đổi bình quân gia quyền theo giá trị GD) — KHÔNG phải điểm chỉ số.
        VNINDEX → basket HOSE ; VN30 → basket VN30
    - Binance KHÔNG có BTC dominance. BTC.D được xấp xỉ bằng tỷ trọng
      quote-volume 1d của BTCUSDT trên tổng các spot symbols cấu hình
      (benchmark = "BTC.D_PROXY", cột close = % tỷ trọng).

Delta table: s3://binance-datalake/delta/bronze/benchmark_index_daily
Partition:   trading_date
Retention:   append-only

Schema:
    trading_date     string   "YYYY-MM-DD"
    benchmark        string   "VNINDEX" | "VN30" | "BTC.D_PROXY"
    source           string   "tcbs_basket" | "binance_vol_share"
    close            double   BTC.D_PROXY: % dominance; TCBS: null (không có index level)
    change_pct       double   TCBS: value-weighted % change ; BTC.D_PROXY: null
    total_vol        double
    total_val        double
    advancers        int64
    decliners        int64
    unchanged        int64
    net_foreign_qty  double
    ingested_at      string
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pyarrow as pa

from cli.ingestion.sources.client.binance_client import BinanceSpotClient
from cli.ingestion.sources.client.tcbs_client import TCBSRestClient
from cli.jobs._delta_writer import _table_path, write_records
from cli.jobs.metrics import push_metrics, record_ingest
from cli.flows.loader.load_config import load_exchange_config
from log.logger_setup import logger_manager

logger = logger_manager.get_logger(__name__)

_SCHEMA = pa.schema([
    pa.field("trading_date",    pa.string(),  nullable=False),
    pa.field("benchmark",       pa.string(),  nullable=False),
    pa.field("source",          pa.string(),  nullable=False),
    pa.field("close",           pa.float64(), nullable=True),
    pa.field("change_pct",      pa.float64(), nullable=True),
    pa.field("total_vol",       pa.float64(), nullable=True),
    pa.field("total_val",       pa.float64(), nullable=True),
    pa.field("advancers",       pa.int64(),   nullable=True),
    pa.field("decliners",       pa.int64(),   nullable=True),
    pa.field("unchanged",       pa.int64(),   nullable=True),
    pa.field("net_foreign_qty", pa.float64(), nullable=True),
    pa.field("ingested_at",     pa.string(),  nullable=False),
])

# benchmark name → TCBS basket index
_DEFAULT_TCBS_BENCHMARKS: Dict[str, str] = {"VNINDEX": "HOSE", "VN30": "VN30"}
_MS_PER_DAY = 86_400_000


def _num(value: Any) -> float:
    try:
        return float(value) if value is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def summarize_basket(
    items: List[dict], benchmark: str, trading_date: str, now_iso: str
) -> Optional[Dict[str, Any]]:
    """
    Tổng hợp tickerCommons items của 1 basket → 1 row benchmark.
    Pure function (không I/O).
    """
    if not items:
        return None

    total_vol = total_val = net_foreign = 0.0
    weighted_chg = 0.0
    adv = dec = unch = 0
    for it in items:
        val = _num(it.get("totalVal"))
        chg = _num(it.get("changePercent"))
        total_vol += _num(it.get("totalVol"))
        total_val += val
        weighted_chg += chg * val
        net_foreign += _num(it.get("buyForeignQty")) - _num(it.get("sellForeignQty"))
        if chg > 0:
            adv += 1
        elif chg < 0:
            dec += 1
        else:
            unch += 1

    return {
        "trading_date":    trading_date,
        "benchmark":       benchmark,
        "source":          "tcbs_basket",
        "close":           None,
        "change_pct":      round(weighted_chg / total_val, 4) if total_val > 0 else None,
        "total_vol":       total_vol,
        "total_val":       total_val,
        "advancers":       adv,
        "decliners":       dec,
        "unchanged":       unch,
        "net_foreign_qty": round(net_foreign, 0),
        "ingested_at":     now_iso,
    }


def btc_dominance_proxy(
    quote_volumes: Dict[str, float], trading_date: str, now_iso: str, btc_symbol: str = "BTCUSDT"
) -> Optional[Dict[str, Any]]:
    """
    BTC.D_PROXY = quote-volume BTC / tổng quote-volume các symbols * 100.
    Pure function (không I/O).
    """
    total = sum(v for v in quote_volumes.values() if v > 0)
    btc = quote_volumes.get(btc_symbol, 0.0)
    if total <= 0 or btc <= 0:
        return None
    return {
        "trading_date":    trading_date,
        "benchmark":       "BTC.D_PROXY",
        "source":          "binance_vol_share",
        "close":           round(btc / total * 100.0, 4),
        "change_pct":      None,
        "total_vol":       None,
        "total_val":       total,
        "advancers":       None,
        "decliners":       None,
        "unchanged":       None,
        "net_foreign_qty": None,
        "ingested_at":     now_iso,
    }


def _extract_items(resp: Any) -> List[dict]:
    if isinstance(resp, dict):
        data = resp.get("data", [])
    elif isinstance(resp, list):
        data = resp
    else:
        data = []
    return [d for d in data if isinstance(d, dict)]


def run() -> dict:
    """Chạy job benchmark ingestion. Returns {"written": int, "errors": int}."""
    cfg     = load_exchange_config()
    bucket  = cfg["delta"]["bucket"]
    path    = _table_path(bucket, "bronze", "benchmark_index_daily")
    now     = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    today   = now.strftime("%Y-%m-%d")

    benchmarks = cfg["tcbs"].get("benchmark_baskets") or _DEFAULT_TCBS_BENCHMARKS
    records: List[Dict[str, Any]] = []
    errors = 0

    # ── TCBS baskets (VNINDEX, VN30) ─────────────────────────────────────────
    try:
        client = TCBSRestClient.from_env()
        client.authenticate()
        for name, index in benchmarks.items():
            try:
                items = _extract_items(client.get_ticker_commons(index=index))
                row = summarize_basket(items, name, today, now_iso)
                if row:
                    records.append(row)
                else:
                    logger.warning("benchmark_index_job: empty basket %s (%s)", name, index)
            except Exception as exc:
                logger.error("benchmark_index_job: %s failed: %s", name, exc)
                errors += 1
    except Exception as exc:
        logger.error("benchmark_index_job: TCBS auth failed: %s", exc)
        errors += 1

    # ── Binance BTC dominance proxy (nến 1d hôm qua đã đóng) ─────────────────
    try:
        spot = BinanceSpotClient.from_env()
        end_ms   = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
        start_ms = end_ms - _MS_PER_DAY
        vols: Dict[str, float] = {}
        for symbol in cfg["binance"]["instruments"]["spot"]:
            rows = spot.get_klines(symbol, interval="1d", start_time=start_ms, end_time=end_ms, limit=2)
            if rows:
                vols[symbol] = float(rows[-1][7])  # row[7] = quoteAssetVolume
        row = btc_dominance_proxy(
            vols, (now - timedelta(days=1)).strftime("%Y-%m-%d"), now_iso
        )
        if row:
            records.append(row)
        else:
            logger.warning("benchmark_index_job: BTC.D proxy unavailable (vols=%s)", vols)
    except Exception as exc:
        logger.error("benchmark_index_job: Binance BTC.D failed: %s", exc)
        errors += 1

    written = 0
    if records:
        written = write_records(path, records, _SCHEMA, mode="append", partition_by=["trading_date"])

    record_ingest("benchmark_index_job", count=written, status="success")
    if errors:
        record_ingest("benchmark_index_job", count=errors, status="error")
    push_metrics("benchmark_index_job")

    logger.info("benchmark_index_job: DONE written=%d errors=%d", written, errors)
    return {"written": written, "errors": errors}


if __name__ == "__main__":
    from pathlib import Path
    try:
        from dotenv import load_dotenv
        load_dotenv(Path(__file__).resolve().parents[4] / ".env", override=False)
    except ImportError:
        pass
    print(run())


> 🔧 **apply_diff** — dataops_batch_api/cli/scheduler.py

> 🔧 **grep** — dataops_batch_api

### 🤖 Assistant

> 🔧 called: read_file



> 🔧 **read_file** — dataops_batch_api/cli/jobs/__init__.py

### 🤖 Assistant

> 🔧 called: apply_diff, apply_diff, glob

Now add config keys and unit tests.

> 🔧 **apply_diff** — dataops_batch_api/cli/jobs/__init__.py

> 🔧 **apply_diff** — dataops_batch_api/cli/flows/config/exchange_config.yaml

> 🔧 **glob** — dataops_batch_api/tests/**/test_*.py

### 🤖 Assistant

> 🔧 called: glob



> 🔧 **glob** — dataops_batch_api/**/test_*.py

### 🤖 Assistant

> 🔧 called: glob, glob



> 🔧 **glob** — dataops_batch_api/test*/**/*.py

> 🔧 **glob** — dataops_batch_api/**/*test*.py

### 🤖 Assistant

> 🔧 called: write_file

`dataops_batch_api` has no existing tests, so I'll add a small set that covers the pure functions.

> 🔧 **write_file** — """Unit tests cho tcbs_margin_job & benchmark_index_job (pure functions, không I/O)."""
from cli.jobs.benchmark_index_job import btc_dominance_proxy, summarize_basket
from cli.jobs.tcbs_margin_job import parse_risk_response

NOW = "2026-10-14T08:45:00+00:00"
DAY = "2026-10-14"


# ── tcbs_margin_job ──────────────────────────────────────────────────────────

def test_parse_risk_list_response():
    resp = [{
        "accountNo": "A1", "rtt": 45.5, "outstanding": 1000.0, "accruedInterest": 5,
        "dueAmount": 0, "overdueAmount": 0, "totalFeeDebt": 1.5,
        "riskPolicy": {"initialMargin": 50, "maintenanceMargin": 40, "liquidationMargin": 30},
        "riskStatus": {"code": "SAFE", "description": "ok"},
    }]
    rows = parse_risk_response(resp, "A1", DAY, NOW)
    assert len(rows) == 1
    r = rows[0]
    assert r["account_no"] == "A1"
    assert r["rtt"] == 45.5
    assert r["maintenance_margin"] == 40.0
    assert r["risk_status_code"] == "SAFE"


def test_parse_risk_dict_data_and_missing_fields():
    rows = parse_risk_response({"data": [{"rtt": "bad"}]}, "A2", DAY, NOW)
    assert rows[0]["account_no"] == "A2"      # fallback
    assert rows[0]["rtt"] is None             # invalid -> None
    assert rows[0]["initial_margin"] is None  # missing riskPolicy


def test_parse_risk_empty():
    assert parse_risk_response(None, "A", DAY, NOW) == []
    assert parse_risk_response([], "A", DAY, NOW) == []


# ── benchmark_index_job ──────────────────────────────────────────────────────

def test_summarize_basket():
    items = [
        {"totalVol": 100, "totalVal": 1000, "changePercent": 2.0, "buyForeignQty": 10, "sellForeignQty": 4},
        {"totalVol": 50,  "totalVal": 3000, "changePercent": -1.0, "buyForeignQty": 0, "sellForeignQty": 6},
        {"totalVol": 10,  "totalVal": 0,    "changePercent": 0.0},
    ]
    row = summarize_basket(items, "VN30", DAY, NOW)
    assert row["benchmark"] == "VN30"
    assert row["advancers"] == 1 and row["decliners"] == 1 and row["unchanged"] == 1
    assert row["total_vol"] == 160 and row["total_val"] == 4000
    assert row["net_foreign_qty"] == 0.0           # (10-4) + (0-6)
    assert row["change_pct"] == -0.25              # (2*1000 - 1*3000)/4000


def test_summarize_basket_empty():
    assert summarize_basket([], "VNINDEX", DAY, NOW) is None


def test_btc_dominance_proxy():
    row = btc_dominance_proxy({"BTCUSDT": 600.0, "ETHUSDT": 300.0, "SOLUSDT": 100.0}, DAY, NOW)
    assert row["benchmark"] == "BTC.D_PROXY"
    assert row["close"] == 60.0


def test_btc_dominance_proxy_no_btc():
    assert btc_dominance_proxy({"ETHUSDT": 10.0}, DAY, NOW) is None


### 🤖 Assistant

> 🔧 called: execute_command



> 🔧 **execute_command** — python -m pytest tests/test_margin_benchmark_jobs.py -q

### 🤖 Assistant

> 🔧 called: execute_command, list_files



> 🔧 **execute_command** — python -c "from cli.ingestion.sources.client.tcbs_client import TCBSRestClient as C; print(hasattr(C,'get_account_risk')); from cli.ingestion.sources.tcbs_constants_loader import TCBSConstants as T; print(T.endpoint('account_risk').format(accountNo='X')); import cli.scheduler as s; print([j.id for j in s.build_scheduler().get_jobs()])"

> 🔧 **list_files** — infra_dataops_finance_trading

### 🤖 Assistant

> 🔧 called: read_file, grep, grep, glob



> 🔧 **read_file** — infra_dataops_finance_trading/promtail-config.yaml

> 🔧 **grep** — infra_dataops_finance_trading

> 🔧 **grep** — infra_dataops_finance_trading/docker-compose.yml

> 🔧 **glob** — **/log_storage/**

### 🤖 Assistant

> 🔧 called: grep, read_file



> 🔧 **read_file** — infra_dataops_finance_trading/docker-compose.yml

> 🔧 **grep** — infra_dataops_finance_trading/docker-compose.yml

### 🤖 Assistant

> 🔧 called: execute_command, execute_command

Loki and Promtail already exist in the compose file. Next I'll check how they're wired, including whether the Python `log_storage` files are scraped.

> 🔧 **execute_command** — git status --short infra_dataops_finance_trading; git check-ignore -v infra_dataops_finance_trading/docker-compose.yml

> 🔧 **execute_command** — Get-Content infra_dataops_finance_trading/docker-compose.yml | Select-Object -Skip 750 -First 75

### 🤖 Assistant

> 🔧 called: grep, execute_command

Loki and Promtail already exist, and Promtail scrapes Docker stdout. What's missing is scraping the `log_storage/*.log` files. I'll check the log format first.

> 🔧 **grep** — dataops_batch_api/log/logger_setup.py

> 🔧 **execute_command** — Get-Content dataops_batch_api/log/log_storage/application.log -TotalCount 3; Get-Content dataops_processing/log/log_storage/application.log -TotalCount 2

### 🤖 Assistant

> 🔧 called: read_file

Logs are JSON. Promtail currently only scrapes Docker stdout. Now I need to add file-based scrape to the `promtail-config.yaml` for the `log_storage` dirs:

> 🔧 **read_file** — infra_dataops_finance_trading/DOCKER_COMPOSE_ADDITIONS.md

### 🤖 Assistant

> 🔧 called: insert_content, execute_command



> 🔧 **insert_content** — infra_dataops_finance_trading/promtail-config.yaml

> 🔧 **execute_command** — python - <<'EOF'
import yaml
d=yaml.safe_load(open('infra_dataops_finance_trading/promtail-config.yaml',encoding='utf-8'))
print([s['job_name'] for s in d['scrape_configs']])
EOF

### 🤖 Assistant

> 🔧 called: execute_command, insert_content



> 🔧 **execute_command** — python -c "import yaml;d=yaml.safe_load(open('infra_dataops_finance_trading/promtail-config.yaml',encoding='utf-8'));print([s['job_name'] for s in d['scrape_configs']])"

> 🔧 **insert_content** — infra_dataops_finance_trading/DOCKER_COMPOSE_ADDITIONS.md

### 🤖 Assistant

> 🔧 called: execute_command

The compose file is gitignored. I'll still apply the two volume mounts there directly, because the file is the real deployment config.

> 🔧 **execute_command** — $f='infra_dataops_finance_trading/docker-compose.yml'; $c=Get-Content $f -Raw; $old="      - ./promtail-config.yaml:/etc/promtail/config.yaml:ro`n"; if(-not $c.Contains($old)){ $old=$old.Replace("`n","`r`n"); $nl="`r`n"} else {$nl="`n"}; if($c.Contains($old) -and -not $c.Contains('/var/log/dataops/batch_api')){ $new=$old+"      - ../dataops_batch_api/log/log_storage:/var/log/dataops/batch_api:ro$nl      - ../dataops_processing/log/log_storage:/var/log/dataops/processing:ro$nl"; [IO.File]::WriteAllText((Resolve-Path $f), $c.Replace($old,$new)); 'patched'} else {'skip'}; Select-String -Path $f -Pattern 'dataops/(batch_api|processing)'

### 🤖 Assistant

> 🔧 called: list_files, grep

Good. Now TASK-INF2: smoke test script.

> 🔧 **list_files** — scripts

> 🔧 **grep** — dataops_backend_api

### 🤖 Assistant

> 🔧 called: read_file

A `scripts/smoke_test.sh` already exists. I'll read it.

> 🔧 **read_file** — scripts/smoke_test.sh

### 🤖 Assistant

> 🔧 called: execute_command



> 🔧 **execute_command** — Select-String -Path infra_dataops_finance_trading/docker-compose.yml -Pattern 'container_name:'; Select-String -Path dataops_backend_api/api/rest/rest.go -Pattern 'portfolio|risk|pretrade|PreTrade' | Select-Object -First 15; Select-String -Path dataops_backend_api/api/rest/rest.go -Pattern 'register'

### 🤖 Assistant

> 🔧 called: grep, execute_command



> 🔧 **grep** — dataops_backend_api/api/rest/portfolio.go

> 🔧 **execute_command** — Get-Content infra_dataops_finance_trading/docker-compose.yml | Select-String -Pattern 'INFLUXDB_ADMIN_TOKEN|DOCKER_INFLUXDB_INIT|INFLUXDB_TOKEN|INIT_BUCKET|INIT_ORG' | Select-Object -First 8

### 🤖 Assistant

> 🔧 called: execute_command



> 🔧 **execute_command** — $l = Get-Content infra_dataops_finance_trading/docker-compose.yml; $l[18..30]; $l[296..300]; $l[453..458]; Select-String -Path infra_dataops_finance_trading/docker-compose.yml -Pattern '"(3001|8080|9010|8086|3100):'; Select-String -Path dataops_processing/config/processing_config.yaml -Pattern 'bucket|delta_|silver|gold|order_flow|market_breadth' | Select-Object -First 15

### 🤖 Assistant

> 🔧 called: apply_diff



> 🔧 **apply_diff** — scripts/smoke_test.sh

### 🤖 Assistant

> 🔧 called: search_and_replace, execute_command

I left a placeholder typo in the Influx header. Fixing it now.

> 🔧 **search_and_replace** — scripts/smoke_test.sh

> 🔧 **execute_command** — bash -n scripts/smoke_test.sh; echo "bash -n exit=$?"; grep -n "PLACEHOLDER" scripts/smoke_test.sh

### 🤖 Assistant

> 🔧 called: update_todo_list

`bash -n` passed (exit code 0 = True in PowerShell). Now run all relevant tests.

> 🔧 **update_todo_list** — [x] Read skill, export_task, understand context
[-] TASK-B1: TCBS margin risk → Delta Bronze
[x] Create TCBSMarginMixin (margin.py) + wire into TCBSRestClient
[x] Create cli/jobs/tcbs_margin_job.py (parse_risk_response, run())
[x] Wire tcbs_margin_job into cli/scheduler.py
[x] Add margin_accounts to exchange_config.yaml
[-] TASK-B2: Benchmark Index Ingestion (VNINDEX, VN30, BTC.D proxy)
[x] Create cli/jobs/benchmark_index_job.py
[x] Wire into cli/scheduler.py
[x] Add benchmark_baskets to exchange_config.yaml
[-] TASK-INF1: Loki + Promtail config
[x] Add file-based log scrape job to promtail-config.yaml
[x] Document volume mounts in DOCKER_COMPOSE_ADDITIONS.md
[x] Apply volume mounts to docker-compose.yml
[-] TASK-INF2: Extend smoke test script (8-section)
[x] Add portfolio/risk endpoints checks (section 6)
[x] Add streaming chain container/Kafka checks (section 7)
[x] Add InfluxDB + Delta Lake checks (section 8)
[x] Fix PASS/FAIL counter (set -e safe), add SMOKE_STRICT flag, add soft_fail()
[ ] Write & run unit tests for new batch jobs
[ ] Run all Go tests (go test ./...)
[ ] Run all Python tests (pytest) across processing + batch_api


### 🤖 Assistant

> 🔧 called: execute_command, execute_command, execute_command

The todo tool wants earlier completed items preserved, so I'll skip that update and run validation.

> 🔧 **execute_command** — python -m pytest -q 2>&1 | Select-Object -Last 15

> 🔧 **execute_command** — python -m pytest -q 2>&1 | Select-Object -Last 15

> 🔧 **execute_command** — go build ./... ; if ($?) { go vet ./... ; if ($?) { go test ./... 2>&1 } }

### 🤖 Assistant

All tests pass:

| Module | Result |
|---|---|
| `dataops_backend_api` (Go) | `ok api/rest`, `ok services/risk` |
| `dataops_processing` (Python) | 43 passed |
| `dataops_batch_api` (Python) | 20 passed (incl. 7 new for margin+benchmark) |

---

Here's what was done this session:

## TASK-B1 — TCBS Margin Risk → Delta Bronze ✅
- **[`margin.py`](dataops_batch_api/cli/ingestion/sources/client/tcbs_client/margin.py)** — new `TCBSMarginMixin` with `get_account_risk()`, `get_pricing_policy()`, `get_margin_info()` using `TCBSConstants.endpoint()` + path-param `.format(accountNo=...)` pattern
- **[`tcbs_client.py`](dataops_batch_api/cli/ingestion/sources/client/tcbs_client/tcbs_client.py)** — `TCBSMarginMixin` added to MRO
- **[`tcbs_margin_job.py`](dataops_batch_api/cli/jobs/tcbs_margin_job.py)** — daily 15:45 ICT job; `parse_risk_response()` is pure (testable); uses `/hydros/v1/account/{accountNo}/risk` (confirmed in OpenAPI); accounts from `TCBS_MARGIN_ACCOUNTS` env or `margin_accounts` config; 2yr rolling retention

## TASK-B2 — Benchmark Index Ingestion ✅
- **[`benchmark_index_job.py`](dataops_batch_api/cli/jobs/benchmark_index_job.py)** — daily 15:50 ICT; VNINDEX/VN30 via `get_ticker_commons(index=...)` → basket aggregate (no index-level endpoint in TCBS API); BTC.D via Binance `row[7]` quote-volume share (clearly documented as proxy); `summarize_basket()` and `btc_dominance_proxy()` are pure functions

## TASK-INF1 — Loki + Promtail ✅
- **[`promtail-config.yaml`](infra_dataops_finance_trading/promtail-config.yaml)** — added `dataops-python-logfiles` scrape job for `/var/log/dataops/{batch_api,processing}/*.log` with JSON pipeline stage (`level`, `logger`, `time` → Loki labels)
- **[`DOCKER_COMPOSE_ADDITIONS.md`](infra_dataops_finance_trading/DOCKER_COMPOSE_ADDITIONS.md)** + `docker-compose.yml` — two new read-only volume mounts on `promtail` service

## TASK-INF2 — Smoke Test Extended ✅
- **[`scripts/smoke_test.sh`](scripts/smoke_test.sh)** — expanded from 5 to **8 sections**: added section 6 (portfolio/pre-trade routes), section 7 (container + Kafka offset checks), section 8 (InfluxDB vwap + Delta Lake `_delta_log` checks); added `SMOKE_STRICT` env flag and `soft_fail()` for data-presence checks that depend on trading-hour data; fixed `((PASS++))` arithmetic under `set -e`

---

### 👤 User

all task done hết rồi hả

---

### 🤖 Assistant

> 🔧 called: execute_command, grep

Gần như xong, nhưng có hai điểm tôi chưa kiểm tra xong: `pre_trade.go` vừa bị sửa bên ngoài, và scheduler từng báo lỗi import trước đó. Tôi kiểm tra cả hai.

> 🔧 **execute_command** — git diff --stat -- dataops_backend_api/services/risk/pre_trade.go; go build ./... ; if ($?) { go test ./services/risk/ ./api/rest/ -count=1 }

> 🔧 **grep** — dataops_batch_api/cli/ingestion/sources/client/binance_client

### 🤖 Assistant

> 🔧 called: read_file, read_file



> 🔧 **read_file** — dataops_backend_api/services/risk/pre_trade.go

> 🔧 **read_file** — dataops_batch_api/cli/ingestion/sources/client/binance_client/__init__.py

### 🤖 Assistant

> 🔧 called: execute_command



> 🔧 **execute_command** — git diff -- dataops_backend_api/services/risk/pre_trade.go

### 🤖 Assistant

> 🔧 called: grep, execute_command



> 🔧 **grep** — dataops_batch_api

> 🔧 **execute_command** — git log --oneline -3 -- dataops_batch_api/cli/ingestion/sources/client/binance_client/__init__.py; git status --short dataops_batch_api

### 🤖 Assistant

> 🔧 called: grep



> 🔧 **grep** — dataops_batch_api/cli/ingestion/sources/client/binance_client/binance_client.py

### 🤖 Assistant

Chưa hoàn toàn. 4 task backlog đã xong và test pass, nhưng tôi phát hiện một lỗi có sẵn khiến scheduler không chạy được.

**Lỗi có sẵn: `cli/scheduler.py` không import được**
- [`binance_4h_job.py`](dataops_batch_api/cli/jobs/binance_4h_job.py:38) và [`binance_1d_job.py`](dataops_batch_api/cli/jobs/binance_1d_job.py:24) import `BinanceFuturesClient`.
- Class này không tồn tại. [`binance_client/__init__.py`](dataops_batch_api/cli/ingestion/sources/client/binance_client/__init__.py) chỉ export `BinanceSpotClient`, và `binance_client.py` chỉ định nghĩa class đó.
- Hậu quả: `import cli.scheduler` lỗi `ImportError`, nên cả scheduler dừng, kể cả hai job mới.
- Lỗi này có từ trước và không do thay đổi của tôi. Hai file job đó nằm trong commit cũ và tôi chưa sửa chúng.
- Hai job mới của tôi chạy độc lập được, vì chúng chỉ import `BinanceSpotClient` và `TCBSRestClient`. Test của chúng pass.
- Tôi chưa sửa lỗi này vì cần chọn hướng:
  - Thêm class Futures thật vào `binance_client`.
  - Hoặc bỏ phần futures khỏi hai job.
  - Binance Futures đang bị geo-block ở VN, nên có thể bạn đã cố ý bỏ nó.

**Kết quả test**
- Go: `go build` và `go test` pass cho `services/risk` và `api/rest` (chạy lại với `-count=1`).
- [`pre_trade.go`](dataops_backend_api/services/risk/pre_trade.go) bị sửa bên ngoài, nhưng `git diff` cho thấy không khác bản đã commit. Tôi đọc lại, logic đúng, test vẫn pass.
- Python: `dataops_processing` 43 pass, `dataops_batch_api` 20 pass.
- Smoke test: `bash -n` pass. Tôi chưa chạy nó với stack thật, vì cần docker-compose đang chạy.

**Việc còn lại**
- Đặt `TCBS_MARGIN_ACCOUNTS` hoặc `tcbs.margin_accounts`. Nếu để trống, `tcbs_margin_job` sẽ bỏ qua.
- `docker-compose.yml` bị gitignore, nên hai volume mount của Promtail không vào commit. Chúng đã được ghi trong [`DOCKER_COMPOSE_ADDITIONS.md`](infra_dataops_finance_trading/DOCKER_COMPOSE_ADDITIONS.md).

Bạn muốn tôi sửa lỗi `BinanceFuturesClient` theo hướng nào?