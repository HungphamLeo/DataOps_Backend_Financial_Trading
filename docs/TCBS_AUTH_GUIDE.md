
# TCBS iFlash Open API — Hướng dẫn Xác thực & Lấy Token

> **Reference:** TCBS iFlash Open API v1.0.0 — `https://openapi.tcbs.com.vn`
> **Contact:** openapi@tcbs.com.vn

## 1. Tổng quan Auth Flow

---
```
┌──────────┐        ┌───────────┐        ┌─────────────────┐
│ Khách hàng│        │ TCInvest  │        │ iFlash Open API │
└────┬─────┘        └─────┬─────┘        └────────┬────────┘
     │  ① Đăng ký         │                        │
     │──────────────────► │                        │
     │  ② Nhận API Key    │                        │
     │◄────────────────── │                        │
     │                    │                        │
     │  ③ POST /token (apiKey + Smart OTP)         │
     │────────────────────────────────────────────►│
     │  ④ Nhận Access Token (JWT, TTL = 8 giờ)    │
     │◄────────────────────────────────────────────│
     │                    │                        │
     │  ⑤ Gọi API với Bearer Token                │
     │────────────────────────────────────────────►│
     │  ⑥ Phản hồi API                            │
     │◄────────────────────────────────────────────│
     │                    │                        │
     │   [Token hết hạn sau 8 giờ — lấy token mới]│
     │                    │                        │
     │  ⑦ Huỷ API Key (tuỳ chọn)                 │
     │──────────────────► │                        │
     │  ⑧ API Key đã huỷ │                        │
     │◄────────────────── │                        │
└────┴─────┘        └─────┴─────┘        └────────┴────────┘
```
---
**Điểm quan trọng:**
- TCBS chỉ dùng **1 API Key duy nhất** — không có `secret_key`, không có HMAC signing.
- Token lấy qua `apiKey + Smart OTP` (iOTP từ TCInvest app).
- Token có TTL **8 giờ** — hết hạn cần lấy lại bằng OTP mới.
- Smart OTP **chỉ dùng 1 lần** và valid trong **~30 giây**.

## 2. Chuẩn bị

### 2.1 Yêu cầu
- Đã đăng ký tài khoản TCBS (TCInvest) và có **TCBS_API_KEY**
- Đã cài app **TCInvest** trên điện thoại và kích hoạt **Smart OTP (iOTP)**
- `curl` + `python3` hoặc PowerShell (Windows)

### 2.2 Set API Key vào `.env`

Mở file `.env` ở root repo và điền:

---
```bash
TCBS_API_KEY=<api_key_nhận_từ_TCInvest>
TCBS_JWT_TOKEN=                           # để trống, sẽ điền sau khi lấy token
```
---
## 3. Lấy JWT Token — Step by step

### Bước 1 — Mở Smart OTP trên TCInvest app

1. Mở app **TCInvest** trên điện thoại
2. Vào mục **Smart OTP** (iOTP)
3. Ghi nhớ **6 chữ số** đang hiển thị (valid ~30 giây, đếm ngược)

> ⚠️ Thực hiện **Bước 2 ngay lập tức** sau khi thấy mã — không để quá 30 giây.

### Bước 2 — Gọi Auth Endpoint

#### Từ WSL / Linux Terminal

---
```bash
# Thay YOUR_API_KEY và YOUR_OTP_6DIGITS
curl -s -X POST https://openapi.tcbs.com.vn/gaia/v1/oauth2/openapi/token \
  -H "Content-Type: application/json" \
  -d '{"apiKey":"YOUR_API_KEY","otp":"YOUR_OTP_6DIGITS"}' \
  | python3 -m json.tool
```
---
#### Từ PowerShell (Windows)

---
```powershell
$apiKey = "YOUR_API_KEY"
$otp    = "YOUR_OTP_6DIGITS"

$body = @{ apiKey = $apiKey; otp = $otp } | ConvertTo-Json
$resp = Invoke-RestMethod `
  -Method POST `
  -Uri "https://openapi.tcbs.com.vn/gaia/v1/oauth2/openapi/token" `
  -Body $body `
  -ContentType "application/json"

Write-Host "Token: $($resp.token)"
```
---
#### Kết quả mong đợi (thành công)

---
```json
{
    "token": "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY..."
}
```
---
#### Các lỗi thường gặp

| HTTP Status | Lỗi | Nguyên nhân | Cách xử lý |
|---|---|---|---|
| `401` | `OTP expired` | OTP đã hết hạn (>30s) | Lấy OTP mới từ app, gọi lại ngay |
| `401` | `OTP used` | OTP đã dùng 1 lần rồi | Lấy OTP mới từ app |
| `401` | `Invalid apiKey` | API Key sai | Kiểm tra lại `TCBS_API_KEY` trong `.env` |
| `429` | `Too many requests` | Gọi quá nhiều lần | Chờ 1 phút rồi thử lại |
| Network error | `Connection refused` | Không có internet / TCBS down | Kiểm tra kết nối mạng |

### Bước 3 — Lưu token vào `.env`

Sau khi có token, copy toàn bộ giá trị `token` (chuỗi `eyJ...`) và điền vào `.env`:

---
```bash
TCBS_API_KEY=<api_key_của_bạn>
TCBS_JWT_TOKEN=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0...
```
---
> Lưu ý: paste **toàn bộ chuỗi JWT** — thường dài 500-1000 ký tự, không bị ngắt dòng.

### Bước 4 — Restart services

---
```bash
cd ~/DataOps_Backend_Financial_Trading/infra_dataops_finance_trading

docker compose --env-file ../.env up -d --force-recreate \
  dataops-stream-producer \
  dataops-batch-api
```
---
### Bước 5 — Verify token đã nhận

---
```bash
# stream-producer: không còn WARNING về missing token
docker logs dataops-stream-producer --tail=20
```
---
**Log bình thường khi token hợp lệ:**
---
---
---
---
<<<<<<< HEAD
=======
---
---
---
---
---
>>>>>>> 20261010_data_processing
```
[TCBSPoller] Starting — indexes=[HOSE VN30 HNX]
[TCBSPoller] Cron jobs started
# KHÔNG thấy dòng: WARNING: cả TCBS_JWT_TOKEN và TCBS_API_KEY đều rỗng
```
---
---
```bash
# batch-api: TCBS steps không còn error
docker logs dataops-batch-api --tail=30
```
---
**Log bình thường:**
---
---
---
---
<<<<<<< HEAD
=======
---
---
---
---
---
>>>>>>> 20261010_data_processing
```
[daily_pipeline] Step 3/5 — Bronze TCBS Market (all configured indexes)
[bronze_flow:tcbs] Fetching HOSE market snapshot...
[bronze_flow:tcbs] Wrote N TCBS market records
```
---
## 4. Refresh Token (sau mỗi ~7 giờ)

Token hết hạn sau **8 giờ**. Quy trình refresh:

---
```bash
# Script refresh token nhanh (chạy trong WSL)
echo "Bước 1: Mở TCInvest app → lấy Smart OTP"
echo "Bước 2: Nhập OTP vào lệnh sau:"

read -p "API Key: " TCBS_API_KEY
read -p "Smart OTP (6 digits): " OTP

NEW_TOKEN=$(curl -s -X POST https://openapi.tcbs.com.vn/gaia/v1/oauth2/openapi/token \
  -H "Content-Type: application/json" \
  -d "{\"apiKey\":\"$TCBS_API_KEY\",\"otp\":\"$OTP\"}" \
  | python3 -c "import sys,json; print(json.load(sys.stdin).get('token','ERROR'))")

if [ "$NEW_TOKEN" = "ERROR" ]; then
  echo "❌ Lấy token thất bại — kiểm tra OTP hoặc API Key"
else
  echo "✅ Token mới: ${NEW_TOKEN:0:40}..."
  echo ""
  echo "Dán dòng sau vào .env:"
  echo "TCBS_JWT_TOKEN=$NEW_TOKEN"
fi
```
---
Sau khi cập nhật `.env`:

---
```bash
cd ~/DataOps_Backend_Financial_Trading/infra_dataops_finance_trading
docker compose --env-file ../.env up -d --force-recreate \
  dataops-stream-producer \
  dataops-batch-api
```
---
## 5. Kiểm tra token còn hiệu lực không

---
```bash
# Decode phần payload của JWT (không cần verify signature)
echo "YOUR_JWT_TOKEN" | python3 -c "
import sys, base64, json
token = sys.stdin.read().strip()
parts = token.split('.')
if len(parts) < 2:
    print('Invalid JWT format')
    sys.exit(1)
# Thêm padding nếu thiếu
payload = parts[1] + '=' * (4 - len(parts[1]) % 4)
data = json.loads(base64.urlsafe_b64decode(payload))
import datetime
exp = data.get('exp')
if exp:
    exp_dt = datetime.datetime.fromtimestamp(exp)
    remaining = exp_dt - datetime.datetime.now()
    print(f'Expires at: {exp_dt}')
    print(f'Remaining:  {remaining}')
else:
    print('No exp field in token:', json.dumps(data, indent=2))
"
```
---
## 6. Tóm tắt env vars liên quan TCBS

| Env Var | Bắt buộc | Mô tả |
|---|---|---|
| `TCBS_API_KEY` | ✅ | API Key từ TCInvest — không đổi |
| `TCBS_JWT_TOKEN` | ✅ | Access Token lấy qua Smart OTP — refresh mỗi 8h |
| `TCBS_BASE_URL` | ➖ | Default: `https://openapi.tcbs.com.vn` |
| `TCBS_TIMEOUT` | ➖ | Request timeout seconds. Default: `15` |
| `TCBS_RETRY` | ➖ | Số lần retry khi API lỗi. Default: `3` |
| `TCBS_TOKEN_THRESHOLD_SEC` | ➖ | Warn khi token còn < N giây. Default: `7200` (2h) |
| `TCBS_SYMBOLS` | ➖ | Danh sách mã cổ phiếu, CSV. Default: `HPG,FPT,SSI,...` |
| `TCBS_INDEXES` | ➖ | Basket indexes poll. Default: `HOSE,VN30,HNX` |
