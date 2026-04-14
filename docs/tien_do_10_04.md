# Báo cáo tiến độ — 10/04/2026
**Sinh viên:** Đỗ Vĩnh Khang — 20224865

---

## Đã làm được

### Script thu thập dữ liệu real-time và đẩy vào Kafka

Đã viết script Python (`etl.py`) tự động thu thập dữ liệu cờ vua từ Lichess API và đẩy vào Confluent Kafka.

**Luồng hoạt động:**

```
Lichess API
    │
    ├─► Lấy top 50 players mỗi loại (bullet / blitz / rapid) → ~137 unique players
    ├─► Kiểm tra ai đang online, ai đang trong game
    ├─► Stream các game đang diễn ra lúc khởi động (thread riêng mỗi game)
    └─► Mở stream liên tục theo danh sách players — tự nhận event khi có game mới
                                │
                                ▼
                    Confluent Kafka (cloud)
                    ├── lichess.game_start
                    ├── lichess.moves
                    └── lichess.game_end
```

Tại lớp stream, script dùng thư viện `python-chess` để theo dõi bàn cờ theo từng nước đi và tính thêm các trường:
- `move_number` — nước thứ mấy của ván
- `game_phase` — opening / middlegame / endgame
- `time_spent_s` — số giây người chơi suy nghĩ cho nước này
- `time_pressure` — còn dưới 10 giây không
- `is_check` — nước này có chiếu tướng không

Mỗi game được xử lý độc lập — board state lưu riêng theo `game_id`, không ảnh hưởng lẫn nhau.

**Kết quả chạy thực tế (05/04/2026):**
- 137 players được track
- Thu được đầy đủ 3 loại event: `game_start`, `move` (54 nước đi), `game_end`
- Game kết thúc: Black thắng vì White hết giờ (`outoftime`)
- Toàn bộ events vào Kafka thành công

---

### Viết script consumer đọc Kafka và ghi vào PostgreSQL

```
Kafka
├── lichess.game_start  ──►
├── lichess.moves       ──►  consumer.py  ──►  PostgreSQL
└── lichess.game_end    ──►
```

Script `consumer.py` đọc liên tục từ 3 Kafka topics, ghép và xử lý dữ liệu, ghi vào các bảng trong PostgreSQL.

---

### Thiết kế schema

#### Bảng `players`
Lưu thông tin từng người chơi. Upsert mỗi khi xuất hiện trong `game_start`.

| Cột | Kiểu | Ý nghĩa |
|-----|------|---------|
| `player_id` | VARCHAR PK | ID cố định của player (chữ thường, không đổi dù đổi username) |
| `title` | VARCHAR | Danh hiệu: GM, IM, FM... Null nếu không có |
| `last_seen_at` | TIMESTAMP | Lần cuối xuất hiện trong game được track |
| `created_at` | TIMESTAMP | Lần đầu tiên được thêm vào hệ thống |

---

#### Bảng `games`
Lưu thông tin từng ván cờ. Tạo khi nhận `game_start`, cập nhật khi nhận `game_end`.

| Cột | Kiểu | Ý nghĩa |
|-----|------|---------|
| `game_id` | VARCHAR PK | ID ván cờ do Lichess sinh ra |
| `white_id` | VARCHAR FK → players | Player cầm quân trắng |
| `black_id` | VARCHAR FK → players | Player cầm quân đen |
| `white_rating` | INTEGER | Rating của White tại thời điểm chơi ván này |
| `black_rating` | INTEGER | Rating của Black tại thời điểm chơi ván này |
| `white_title` | VARCHAR | Danh hiệu của White lúc chơi |
| `black_title` | VARCHAR | Danh hiệu của Black lúc chơi |
| `speed` | VARCHAR | Loại ván: `bullet`, `blitz`, `rapid`, `classical` |
| `rated` | BOOLEAN | Có tính rating không |
| `variant` | VARCHAR | Luật chơi: `standard`, `chess960`... |
| `source` | VARCHAR | Cách ván được tạo: `pool`, `friend`, `tournament` |
| `tournament_id` | VARCHAR | ID giải đấu nếu có, null nếu không |
| `opening_eco` | VARCHAR | Mã khai cuộc ECO (A00–E99) — điền sau khi game kết thúc |
| `opening_name` | VARCHAR | Tên khai cuộc đầy đủ — điền sau khi game kết thúc |
| `winner` | VARCHAR | `white` / `black` / null (hòa) |
| `status` | VARCHAR | Lý do kết thúc: `mate`, `resign`, `outoftime`, `draw`... |
| `started_at` | TIMESTAMP | Thời điểm ván bắt đầu |
| `ended_at` | TIMESTAMP | Thời điểm ván kết thúc |
| `total_moves` | INTEGER | Tổng số nước đi của ván |

---

#### Bảng `moves`
Lưu từng nước đi. Đây là bảng lớn nhất — mỗi ván sinh ra ~30–80 rows.

| Cột | Kiểu | Ý nghĩa |
|-----|------|---------|
| `id` | SERIAL PK | Auto increment |
| `game_id` | VARCHAR FK → games | Thuộc ván nào |
| `move_number` | INTEGER | Thứ tự nước đi trong ván |
| `player_id` | VARCHAR FK → players | Ai đi nước này |
| `move_uci` | VARCHAR | Nước đi dạng UCI: `"e2e4"`, `"g1f3"`, `"e7e8q"` |
| `fen_after` | TEXT | Trạng thái bàn cờ sau nước này (FEN) |
| `white_clock` | INTEGER | Đồng hồ của White còn bao nhiêu giây |
| `black_clock` | INTEGER | Đồng hồ của Black còn bao nhiêu giây |
| `time_spent_s` | INTEGER | Giây suy nghĩ cho nước này. Null ở nước đầu |
| `time_pressure` | BOOLEAN | Đồng hồ còn dưới 10 giây lúc đi nước này |
| `game_phase` | VARCHAR | `opening` / `middlegame` / `endgame` |
| `is_check` | BOOLEAN | Nước này có chiếu tướng không |
| `is_capture` | BOOLEAN | Nước này có ăn quân không |
| `played_at` | TIMESTAMP | Thời điểm nước đi xảy ra |

---

### Quan hệ giữa các bảng

```
players ──── games (white_id, black_id)
               │
               └──── moves (game_id, player_id)
```

Một player chơi nhiều games. Mỗi game có nhiều moves. Mỗi move biết thuộc game nào và do player nào đi.

---

## Dự định tiếp theo

Bước tiếp theo là lớp Serving:

- **Analytics** — win rate, khai cuộc phổ biến, time pressure theo player và rating band
- **Player profiling** — phong cách chơi, điểm mạnh/yếu, khai cuộc ưa dùng của từng player
- **AI Chess Coach** — người dùng hỏi về ván cờ của mình, AI tra cứu data từ PostgreSQL để trả lời có số liệu thực tế
