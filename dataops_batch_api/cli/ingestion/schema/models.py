"""
models.py — Pydantic Schemas Validation cho Delta Lake Bronze/Silver.

Validate dữ liệu tại runtime TRƯỚC khi ghi vào Delta Lake.
Nếu dữ liệu vi phạm business rule → raise ValidationError (không ghi record xấu).

Design principle: "Bad data in = bad data out"
  → Validate ở tầng ingestion, không phải tầng analytics.

Schemas:
    BinanceCandleRecord  — OHLCV candle từ Binance REST API
    TCBSMarketRecord     — Full ticker snapshot từ TCBS tickerCommons API
    TCBSTickerSnapRecord — Foreign room + lightweight snapshot từ TCBS tickerSnaps API
    TCBSBSARecord        — Buy/Sell Analysis (cung/cầu) từ TCBS Nyx service
    TCBSIntradayTickRecord — Tick-by-tick khớp lệnh từ TCBS intraday history API
    OHLCVRecord          — Generic normalized OHLCV
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class BinanceCandleRecord(BaseModel):
    """
    Validated OHLCV record từ Binance REST API.

    Fields:
        inst_id:   Instrument ID, e.g. "BTCUSDT", "ETHUSDT"
        inst_type: Loại instrument — SPOT | FUTURES | PERP
        ts_ms:     Unix timestamp milliseconds (close time của candle)
        open/high/low/close: OHLC prices — phải > 0
        vol:       Volume tính theo base currency
        vol_ccy:   Volume tính theo quote currency (USDT)
        confirm:   1 = candle đã đóng hoàn toàn, 0 = đang trong progress
    """
    inst_id:   str
    inst_type: str           # SPOT | SWAP | FUTURES
    ts_ms:     int
    open:      float = Field(gt=0)
    high:      float = Field(gt=0)
    low:       float = Field(gt=0)
    close:     float = Field(gt=0)
    vol:       float = Field(ge=0)
    vol_ccy:   float = Field(ge=0, default=0.0)
    confirm:   int   = Field(default=1, ge=0, le=1)

    @model_validator(mode="after")
    def high_gte_low(self) -> BinanceCandleRecord:
        """Business rule: high price phải >= low price."""
        if self.high < self.low:
            raise ValueError(f"high {self.high} < low {self.low}")
        return self


# ─── TCBS Schemas ─────────────────────────────────────────────────────────────

class TCBSMarketRecord(BaseModel):
    """
    Validated full market ticker snapshot từ TCBS /tartarus/v1/tickerCommons.

    Chứa orderbook 3 bước giá (bid/offer × 3), OHLC, foreign room, average.
    Chứa đầy đủ orderbook 3 cấp, OHLC, foreign room.

    Fields:
        symbol:          Mã cổ phiếu, e.g. "VCB", "VHM"
        exchange:        Tên sàn — "HOSE" | "HNX" | "UPCOM"
        ref_price:       Giá tham chiếu
        ceil_price:      Giá trần
        floor_price:     Giá sàn
        match_price:     Giá khớp lệnh hiện tại
        match_qty:       Khối lượng khớp lệnh
        bid_price_01..03 / bid_qty_01..03: Bảng chào mua 3 cấp
        offer_price_01..03 / offer_qty_01..03: Bảng chào bán 3 cấp
        open/high/low/avg: OHLC + avg price trong phiên
        total_vol:       Tổng khối lượng giao dịch
        total_val:       Tổng giá trị giao dịch
        buy_foreign_qty: Khối lượng nước ngoài mua
        sell_foreign_qty: Khối lượng nước ngoài bán
        room:            Room nước ngoài còn lại
        change:          Thay đổi giá tuyệt đối so với tham chiếu
        change_pct:      Thay đổi giá phần trăm
        trading_date:    Ngày giao dịch (string từ API, e.g. "dd/MM/yyyy")
        ts_ms:           Unix timestamp milliseconds lúc snapshot
    """
    symbol:           str
    exchange:         str = "HOSE"

    ref_price:        float = Field(ge=0, default=0.0)
    ceil_price:       float = Field(ge=0, default=0.0)
    floor_price:      float = Field(ge=0, default=0.0)
    match_price:      float = Field(ge=0, default=0.0)
    match_qty:        float = Field(ge=0, default=0.0)

    # Orderbook 3 bước giá — ge=0 vì có thể rỗng (giá = 0 khi không có lệnh)
    bid_price_01:     float = Field(ge=0, default=0.0)
    bid_price_02:     float = Field(ge=0, default=0.0)
    bid_price_03:     float = Field(ge=0, default=0.0)
    bid_qty_01:       float = Field(ge=0, default=0.0)
    bid_qty_02:       float = Field(ge=0, default=0.0)
    bid_qty_03:       float = Field(ge=0, default=0.0)

    offer_price_01:   float = Field(ge=0, default=0.0)
    offer_price_02:   float = Field(ge=0, default=0.0)
    offer_price_03:   float = Field(ge=0, default=0.0)
    offer_qty_01:     float = Field(ge=0, default=0.0)
    offer_qty_02:     float = Field(ge=0, default=0.0)
    offer_qty_03:     float = Field(ge=0, default=0.0)

    open:             float = Field(ge=0, default=0.0)
    high:             float = Field(ge=0, default=0.0)
    low:              float = Field(ge=0, default=0.0)
    avg:              float = Field(ge=0, default=0.0)

    total_vol:        float = Field(ge=0, default=0.0)
    total_val:        float = Field(ge=0, default=0.0)
    buy_foreign_qty:  float = Field(ge=0, default=0.0)
    sell_foreign_qty: float = Field(ge=0, default=0.0)
    room:             str   = ""

    change:           float = 0.0
    change_pct:       float = 0.0
    index_number:     Optional[float] = None

    trading_date:     str   = ""
    ts_ms:            int

    @field_validator("match_price")
    @classmethod
    def match_price_within_bands(cls, v: float, info) -> float:
        """VN market rule: giá khớp không vượt trần/sàn (0.1% tolerance cho rounding)."""
        data = info.data
        ceil  = data.get("ceil_price", 0.0)
        floor = data.get("floor_price", 0.0)
        if v > 0 and ceil > 0 and v > ceil * 1.001:
            raise ValueError(f"match_price {v} exceeds ceil_price {ceil}")
        if v > 0 and floor > 0 and v < floor * 0.999:
            raise ValueError(f"match_price {v} below floor_price {floor}")
        return v


class TCBSTickerSnapRecord(BaseModel):
    """
    Lightweight market snapshot từ TCBS /tartarus/v1/tickerSnaps.

    Tập trung vào foreign room data + price change.
    Dùng cho foreign flow tracking pipeline.

    Fields:
        symbol:          Mã cổ phiếu
        ref_price:       Giá tham chiếu
        ceil_price/floor_price: Trần/sàn
        match_price/match_qty:  Giá và KL khớp
        change/change_pct:      Thay đổi giá
        total_volume/total_value: Tổng khối lượng và giá trị
        buy_foreign_qty/sell_foreign_qty: KL nước ngoài mua/bán
        room:            Room nước ngoài còn lại
        trading_date:    Ngày giao dịch
        ts_ms:           Unix timestamp milliseconds
    """
    symbol:           str
    ref_price:        float = Field(ge=0, default=0.0)
    ceil_price:       float = Field(ge=0, default=0.0)
    floor_price:      float = Field(ge=0, default=0.0)
    match_price:      float = Field(ge=0, default=0.0)
    match_qty:        float = Field(ge=0, default=0.0)
    change:           float = 0.0
    change_pct:       float = 0.0
    total_volume:     float = Field(ge=0, default=0.0)
    total_value:      float = Field(ge=0, default=0.0)
    buy_foreign_qty:  float = Field(ge=0, default=0.0)
    sell_foreign_qty: float = Field(ge=0, default=0.0)
    room:             str   = ""
    trading_date:     str   = ""
    ts_ms:            int


class TCBSBSARecord(BaseModel):
    """
    Buy/Sell Analysis record từ TCBS /nyx/v1/intraday/{ticker}/bsa.

    Cung cấp thông tin về cán cân cung/cầu: tỷ lệ mua chủ động (bup),
    bán chủ động (sdp), và tỷ lệ mua/bán (bsr) theo thời gian trong ngày.

    Fields:
        ticker:        Mã cổ phiếu
        investor_type: Loại nhà đầu tư — sheep | wolf | shark | all
        bup:           Buy Up Percent — % giao dịch mua chủ động
        sdp:           Sell Down Percent — % giao dịch bán chủ động
        bsr:           Buy/Sell Ratio — tỷ lệ mua/bán
        event_time:    Thời gian giao dịch từ API (string, e.g. "09:15:00")
        ts_ms:         Unix timestamp milliseconds lúc snapshot
    """
    ticker:        str
    investor_type: str

    bup:           float = Field(ge=0.0, le=100.0, default=0.0)
    sdp:           float = Field(ge=0.0, le=100.0, default=0.0)
    bsr:           float = Field(ge=0.0, default=0.0)
    event_time:    str   = ""
    ts_ms:         int

    @field_validator("investor_type")
    @classmethod
    def validate_investor_type(cls, v: str) -> str:
        """Validate investor_type theo constants YAML."""
        valid = {"sheep", "wolf", "shark", "all"}
        if v not in valid:
            raise ValueError(f"investor_type '{v}' not in {valid}")
        return v


class TCBSIntradayTickRecord(BaseModel):
    """
    Tick-by-tick khớp lệnh intraday từ TCBS /nyx/v1/intraday/{ticker}/his/paging.

    Lưu từng lệnh khớp thực tế trong phiên giao dịch.

    Fields:
        ticker:       Mã cổ phiếu
        price:        Giá khớp lệnh (p)
        volume:       Khối lượng khớp (v)
        price_change: Thay đổi giá so với tham chiếu (cp)
        action:       "Buy Up" hoặc "Sell Down" (a)
        buy_acc:      Lũy kế mua chủ động (ba)
        sell_acc:     Lũy kế bán chủ động (sa)
        highlight:    True nếu giá trị > 200 triệu (hl)
        event_time:   Thời gian khớp lệnh từ sàn (t)
        ts_ms:        Unix timestamp milliseconds lúc snapshot
    """
    ticker:       str
    price:        float = Field(ge=0, default=0.0)
    volume:       float = Field(ge=0, default=0.0)
    price_change: float = 0.0
    action:       str   = ""
    buy_acc:      float = Field(ge=0, default=0.0)
    sell_acc:     float = Field(ge=0, default=0.0)
    highlight:    bool  = False
    event_time:   str   = ""
    ts_ms:        int


# ─── Generic ──────────────────────────────────────────────────────────────────

class OHLCVRecord(BaseModel):
    """
    Generic OHLCV record (normalized form) dùng cho DeltaBronzeWriter.
    Phù hợp cho cả Binance và VPS khi đã qua normalize step.
    """
    symbol:    str
    exchange:  str
    interval:  str
    open:      float = Field(gt=0)
    high:      float = Field(gt=0)
    low:       float = Field(gt=0)
    close:     float = Field(gt=0)
    volume:    float = Field(ge=0)
    timestamp: datetime

    @field_validator("high")
    @classmethod
    def validate_high(cls, v: float, info) -> float:
        if "low" in info.data and v < info.data["low"]:
            raise ValueError("High price cannot be lower than Low price")
        return v


# ─── TCBS Trading / Account Contracts ─────────────────────────────────────────
# Dùng cho dataops_backend_api TCBS proxy layer — validate response trước khi cache.
# Reference: openapi-v1.0.0.json — TCBS iFlash Open API v1.0.0

class TCBSDerivativeRecord(BaseModel):
    """
    Derivative contract info từ TCBS /tartarus/v1/derivatives.

    Cung cấp thông tin giá + open interest cho phái sinh VN30F.
    Dùng cho Bronze ingestion vào Delta Lake (batch, REST poll EOD).

    Fields:
        symbol:       Contract symbol, e.g. "VN30F2411"
        ceil_price:   Giá trần
        floor_price:  Giá sàn
        ref_price:    Giá tham chiếu
        bid_price_01 / bid_qty_01: Bid top 1
        offer_price_01 / offer_qty_01: Ask top 1
        match_price:  Giá khớp lệnh hiện tại
        match_qty:    Khối lượng khớp
        change:       Thay đổi giá tuyệt đối
        change_pct:   Thay đổi phần trăm
        total_vol:    Tổng khối lượng trong phiên
        open_vol:     Open interest (số hợp đồng đang mở)
        expiry_date:  Ngày đáo hạn (string từ API)
        ts_ms:        Unix timestamp milliseconds lúc snapshot
    """
    symbol:         str
    ceil_price:     float = Field(ge=0, default=0.0)
    floor_price:    float = Field(ge=0, default=0.0)
    ref_price:      float = Field(ge=0, default=0.0)
    bid_price_01:   float = Field(ge=0, default=0.0)
    bid_qty_01:     float = Field(ge=0, default=0.0)
    offer_price_01: float = Field(ge=0, default=0.0)
    offer_qty_01:   float = Field(ge=0, default=0.0)
    match_price:    float = Field(ge=0, default=0.0)
    match_qty:      float = Field(ge=0, default=0.0)
    change:         float = 0.0
    change_pct:     float = 0.0
    total_vol:      float = Field(ge=0, default=0.0)
    open_vol:       float = Field(ge=0, default=0.0)
    expiry_date:    str   = ""
    ts_ms:          int


class TCBSAccountCashRecord(BaseModel):
    """
    Cash balance record từ TCBS /aion/v1/accounts/{accountNo}/cashInvestments.

    Dùng để validate response trước khi cache vào Redis và trả về client.
    Không persist vào Delta Lake (account data là transactional, không phải market data).

    Fields:
        account_no:          Số tài khoản
        bod_balance:         Số dư đầu ngày tại TCBS
        cash_balance:        Số dư tiền mặt
        pp0_for_bf:          Sức mua cơ bản cho sản phẩm BOND/FUND
        bank_avl_balance_bf: Tổng số dư IA khả dụng cho BOND/FUND
        total_count:         Tổng số records
        ts_ms:               Unix timestamp milliseconds lúc query
    """
    account_no:          str
    bod_balance:         float = Field(ge=0, default=0.0)
    cash_balance:        float = Field(ge=0, default=0.0)
    pp0_for_bf:          float = Field(ge=0, default=0.0)
    bank_avl_balance_bf: float = Field(ge=0, default=0.0)
    total_count:         int   = 0
    ts_ms:               int


class TCBSStockAssetRecord(BaseModel):
    """
    Stock holding record từ TCBS /aion/v1/accounts/{accountNo}/se.

    Một record = một mã cổ phiếu đang nắm giữ trong tài khoản.
    Validate market value consistency: market_value = quantity * avg_price (approx).

    Fields:
        account_no:   Số tài khoản sub-account
        symbol:       Mã cổ phiếu, e.g. "VCB"
        quantity:     Số lượng cổ phiếu đang nắm giữ
        avg_price:    Giá vốn bình quân
        market_value: Giá trị thị trường hiện tại
        ts_ms:        Unix timestamp milliseconds lúc query
    """
    account_no:   str
    symbol:       str
    quantity:     int   = Field(ge=0)
    avg_price:    float = Field(ge=0, default=0.0)
    market_value: float = Field(ge=0, default=0.0)
    ts_ms:        int


class TCBSOrderRecord(BaseModel):
    """
    Stock order record từ TCBS /aion/v1/accounts/{accountNo}/orders.

    Validate order fields theo TCBS business rules.
    Dùng để map response từ TCBS sang internal format cho logging/audit (MySQL).

    Fields:
        account_no:  Số tài khoản
        order_id:    Order ID (orderId field)
        symbol:      Mã cổ phiếu
        exec_type:   Direction: "NB" = Normal Buy, "NS" = Normal Sell
        price:       Giá đặt lệnh
        quantity:    Số lượng đặt
        status:      Trạng thái lệnh (string từ TCBS)
        ts_ms:       Unix timestamp milliseconds
    """
    account_no: str
    order_id:   str
    symbol:     str
    exec_type:  str    # "NB" | "NS"
    price:      int    = Field(ge=0)   # Binance-style: VND price as int
    quantity:   int    = Field(ge=0)
    status:     str    = ""
    ts_ms:      int

    @field_validator("exec_type")
    @classmethod
    def validate_exec_type(cls, v: str) -> str:
        """Validate exec_type theo TCBS order direction enum."""
        valid = {"NB", "NS"}
        if v not in valid:
            raise ValueError(f"exec_type '{v}' must be one of {valid}")
        return v


class TCBSOpenPositionRecord(BaseModel):
    """
    Open derivative position từ TCBS /khronos/v1/account/portfolio/status.

    Validate position fields và P&L consistency.
    Dùng cho account portfolio dashboard và risk management display.

    Fields:
        account_id:     Custody account number
        sub_account_id: Derivative sub-account
        symbol:         Contract symbol, e.g. "VN30F2411"
        im:             Margin ratio (string từ API)
        deliver:        Số vị thế SHORT đang mở
        receive:        Số vị thế LONG đang mở
        net:            Net position (>0 = LONG, <0 = SHORT)
        side:           "L" = Long, "S" = Short
        last_price:     Giá thị trường hiện tại
        im_value:       Initial margin value
        vm_value:       Unrealized P&L
        mr_value:       Required margin value
        duedate:        Ngày đáo hạn
        pc_remain:      Open position P&L %
        ts_ms:          Unix timestamp milliseconds lúc query
    """
    account_id:     str
    sub_account_id: str
    symbol:         str
    im:             str   = ""
    deliver:        int   = Field(ge=0, default=0)
    receive:        int   = Field(ge=0, default=0)
    net:            int   = 0
    side:           str   = ""     # "L" | "S"
    last_price:     float = Field(ge=0, default=0.0)
    im_value:       float = Field(ge=0, default=0.0)
    vm_value:       float = 0.0    # can be negative (P&L)
    mr_value:       float = Field(ge=0, default=0.0)
    duedate:        str   = ""
    pc_remain:      str   = ""
    ts_ms:          int

    @field_validator("side")
    @classmethod
    def validate_side(cls, v: str) -> str:
        """Validate position side theo TCBS khronos response."""
        if v and v not in {"L", "S"}:
            raise ValueError(f"side '{v}' must be 'L' (Long) or 'S' (Short)")
        return v

