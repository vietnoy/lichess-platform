# Thu thập dữ liệu từ Lichess

---

## Nguồn 1 — Real-time Stream

### Bước 1: Xây dựng danh sách player

Lichess cung cấp API trả về top players của từng channel. Hệ thống gọi API này để lấy danh sách ban đầu:

```
/api/player/top/50/bullet   → 50 top bullet players
/api/player/top/50/blitz    → 50 top blitz players
/api/player/top/50/rapid    → 50 top rapid players
...
```

Gộp lại và loại trùng → khoảng 150-200 unique players. Mỗi player được lưu bằng **id** (không phải username) vì id là cố định, không thay đổi dù player có đổi tên.

```
username: "DrNykterstein"  ← có thể đổi bất cứ lúc nào
id:       "drnykterstein"  ← cố định mãi mãi
```

Danh sách tự động mở rộng theo thời gian: sau mỗi ván kết thúc, opponent của player đang được track (người chưa có trong danh sách) sẽ được thêm vào. Vì stream chỉ bắt games của players đã có trong danh sách nên trong mỗi game_end event, chắc chắn có 1 người đã có trong list và 1 người là opponent mới.

```
Danh sách có: "hikaru"

game_end event:
  white = "hikaru"         ← đã có trong list
  black = "drnykterstein"  ← opponent mới → thêm vào danh sách
```

### Bước 2: Check ai đang online (mỗi 30 giây)

```
/api/users/status?ids=id1,id2,...&withGameIds=true
→ Lichess trả về: ai đang online, ai đang trong game, game ID của họ
```

### Bước 3: Mở stream

```
POST /api/stream/games-by-users
body: id1\nid2\nid3\n... (tối đa 300 per connection)
→ 1 connection persistent, Lichess tự push event khi có nước đi mới
→ Nếu có hơn 300 users thì mở thêm connection
   (600 users = 2 connections, 900 users = 3 connections,...)
```

**Lưu ý:** `games-by-users` chỉ push events cho games bắt đầu **sau khi** subscribe. Để không bỏ sót games đang diễn ra lúc hệ thống khởi động, hệ thống dùng thêm `/api/stream/game/{id}` cho từng game đang mid-game, chạy song song cho đến khi game đó kết thúc.

### Volume và Speed thực đo được

| Chỉ số | Giá trị |
|--------|---------|
| Players tracked ban đầu | 142 players (top 50 bullet + blitz + rapid) |
| Online tại 1 thời điểm | ~6 players (đo lúc 10:41AM giờ Việt Nam — top players ở Châu Âu đang ngủ, giờ cao điểm sẽ cao hơn nhiều) |
| Đang trong game | ~4 players → 2 games active cùng lúc |
| Events thu được | 63 events trong 30 giây từ 2 games concurrent |
| Tốc độ real-time | ~2 events/giây (tăng tuyến tính khi có nhiều games hơn) |

---

## Events và Features nhận được từ Stream

Mỗi game sinh ra 3 loại event theo thứ tự:

### Event 1 — game_start
Bắn **1 lần** khi ván bắt đầu, chứa thông tin về 2 người chơi và ván cờ.

| Feature | Ý nghĩa |
|---------|---------|
| `game_id` | ID định danh ván cờ |
| `timestamp` | Thời điểm ván bắt đầu |
| `speed` | Loại ván theo thời gian: **bullet** (dưới 3 phút/người), **blitz** (3-8 phút), **rapid** (8-25 phút), **classical** (trên 25 phút) |
| `rated` | Có tính rating không — rated nghĩa là thắng/thua ván này sẽ làm rating tăng/giảm |
| `variant` | Luật chơi: **standard** (cờ vua tiêu chuẩn), **chess960** (vị trí quân ngẫu nhiên), **atomic** (ăn quân gây nổ)... |
| `white_id` | ID người chơi cầm quân trắng |
| `white_rating` | Rating của White tại thời điểm chơi ván đó — rating là chỉ số đánh giá trình độ, càng cao càng giỏi |
| `white_title` | Danh hiệu chuyên nghiệp của White: **GM** (Grandmaster — cao nhất), **IM**, **FM**... Không phải ai cũng có |
| `black_id` | ID người chơi cầm quân đen |
| `black_rating` | Rating của Black tại thời điểm chơi |
| `black_title` | Danh hiệu của Black (nếu có) |
| `source` | Ván này được tạo ra như thế nào: **pool** (hệ thống tự ghép đối), **friend** (rủ bạn chơi), **tournament** (trong giải đấu) |
| `tournament_id` | ID giải đấu nếu là ván thi đấu (nếu có) |

### Event 2 — move
Bắn **mỗi khi có nước đi mới**, chứa thông tin về nước vừa đi và trạng thái ván cờ.

| Feature | Kiểu | Nguồn | Ý nghĩa |
|---------|------|-------|---------|
| `game_id` | string | raw | Thuộc ván nào |
| `timestamp` | datetime | raw | Thời điểm nước đi xảy ra |
| `move` | string | raw | Nước đi dạng UCI — ví dụ "e2e4" nghĩa là di chuyển quân từ ô e2 đến ô e4 |
| `fen` | string | raw | Mô tả toàn bộ trạng thái bàn cờ tại thời điểm đó — mỗi ký tự đại diện cho 1 ô, dùng để vẽ lại bàn cờ |
| `white_clock` | integer | raw | Đồng hồ của White còn bao nhiêu giây |
| `black_clock` | integer | raw | Đồng hồ của Black còn bao nhiêu giây |
| `move_number` | integer | derived | Đây là nước thứ mấy của ván |
| `game_phase` | string | derived | Ván đang ở giai đoạn nào: **opening** (khai cuộc — những nước đầu triển khai quân), **middlegame** (trung cuộc — giai đoạn tấn công chính), **endgame** (tàn cuộc — ít quân còn lại, cần kỹ thuật kết thúc ván) |
| `time_spent_s` | integer | derived | Người chơi nghĩ bao nhiêu giây cho nước này — tính bằng cách lấy đồng hồ nước trước trừ đồng hồ nước này |
| `time_pressure` | boolean | derived | Đồng hồ còn dưới 10 giây — lúc này người chơi phải đi rất nhanh, dễ mắc lỗi |
| `is_check` | boolean | derived | Nước này có đang **chiếu tướng** không — tức là tấn công thẳng vào quân Vua của đối phương |
| `is_capture` | boolean | derived | Nước này có **ăn quân** của đối phương không |

> **Raw**: nhận thẳng từ Lichess API
> **Derived**: tự tính bằng thư viện `python-chess` tại lớp stream processing

### Event 3 — game_end
Bắn **1 lần** khi ván kết thúc.

| Feature | Ý nghĩa |
|---------|---------|
| `game_id` | Thuộc ván nào |
| `timestamp` | Thời điểm ván kết thúc |
| `winner` | Ai thắng: **white** / **black** / **null** (nếu hòa) |
| `status` | Lý do kết thúc: **mate** (chiếu hết — Vua bị chiếu và không có nước thoát), **resign** (bỏ cuộc), **outoftime** (hết giờ), **draw** (hòa), **stalemate** (hòa vì không còn nước hợp lệ dù Vua chưa bị chiếu) |

---

## Nguồn 2 — Historical Backfill

Chạy 1 lần khi mới thêm một player vào danh sách để có đủ dữ liệu lịch sử ngay, không cần chờ real-time stream tích lũy.

### 2a. Per-user export

```
GET /api/games/user/{id}?moves=true&clocks=true&opening=true
```

Kéo toàn bộ lịch sử ván đã chơi của player đó. Mỗi ván lịch sử có đầy đủ tất cả fields của game_start + game_end, cộng thêm:

| Feature | Ý nghĩa |
|---------|---------|
| `moves` | Toàn bộ nước đi của ván |
| `clocks` | Đồng hồ tại từng nước (centiseconds) |
| `opening_eco` | Mã khai cuộc theo chuẩn quốc tế ECO — ví dụ **B90** là Sicilian Najdorf, **C65** là Ruy Lopez. Có 500 mã từ A00 đến E99, mỗi mã đại diện cho một kiểu khai cuộc khác nhau |
| `opening_name` | Tên đầy đủ của khai cuộc — ví dụ "Sicilian Defense: Najdorf Variation" |
| `opening_ply` | Khai cuộc kết thúc ở nước thứ mấy — sau nước này 2 bên bắt đầu tự sáng tạo, không còn đi theo lý thuyết nữa |
| `rating_diff` | Rating thay đổi bao nhiêu sau ván — ví dụ +8 nghĩa là thắng được 8 điểm rating, -5 nghĩa là thua mất 5 điểm |

**Speed thực đo được:** ~1,300 moves/giây khi backfill một player.

### 2b. Bulk PGN dumps

```
database.lichess.org → 158 file từ 2013 đến nay, ~30GB compressed/tháng
```

Per-user export chỉ lấy được lịch sử của top players trong danh sách. Nhưng để trả lời câu hỏi như *"Người chơi rating 1400-1600 hay dùng khai cuộc gì?"* thì cần data của hàng triệu players ở mọi trình độ — đó là thứ file dump cung cấp.

| | Per-user export | Bulk PGN dump |
|---|---|---|
| Phạm vi | Top players trong danh sách | Toàn bộ community Lichess |
| Dùng cho | Analytics cá nhân từng player | Thống kê cộng đồng theo rating band |
| Ví dụ | "Hikaru hay chơi khai cuộc gì?" | "Player rating 1600 hay chơi khai cuộc gì?" |

---

## Nguồn 3 — Vector Database (Qdrant)

Vector database không lưu data dạng bảng như PostgreSQL mà lưu dạng **embedding** — tức là mỗi đoạn thông tin được chuyển thành một dãy số đại diện cho "ý nghĩa" của nó. Khi tìm kiếm, hệ thống tìm những embedding có ý nghĩa gần giống nhau thay vì tìm kiếm từ khóa chính xác.

Vector database phục vụ cho **AI Chat Coach** — cho phép AI tra cứu kiến thức khi trả lời câu hỏi của người dùng.

Có 3 loại thông tin được lưu trong vector database:

### Loại 1 — Lý thuyết khai cuộc
**Nguồn:** sách lý thuyết cờ vua, Wikipedia, các bài phân tích của GM

**Nội dung:** các đoạn text mô tả kế hoạch, ý tưởng của từng khai cuộc

**Dùng để:** AI trả lời các câu hỏi như *"Tại sao khai cuộc Sicilian lại phổ biến?"*, *"Kế hoạch chính của Ruy Lopez là gì?"*

### Loại 2 — Thống kê thực tế từ platform
**Nguồn:** kết quả aggregate từ PostgreSQL, được refresh định kỳ

**Nội dung:** các đoạn text đóng gói số liệu thực của platform

```
Ví dụ một chunk:
"Sicilian Defense tại blitz rating 1600-1800:
 chơi 2.3 triệu lần, White thắng 48%, hòa 4%, Black thắng 48%.
 Nước tiếp theo phổ biến nhất: Nf3 (44%), Nc3 (15%), f4 (12%)"
```

**Dùng để:** AI trả lời với số liệu thực của platform thay vì chỉ dựa vào lý thuyết chung chung — *"Ở trình độ của bạn, khai cuộc này có win rate bao nhiêu?"*

### Loại 3 — Position embeddings
**Nguồn:** FEN positions từ các ván cờ nổi tiếng

**Nội dung:** vector đại diện cho trạng thái bàn cờ tại một position cụ thể

**Dùng để:** người dùng paste vào 1 position → tìm các ván lịch sử có position tương tự → *"Magnus đã xử lý position này như thế nào?"*

---

## Phần 4 — Khai cuộc là gì và xác định như thế nào?

### Khai cuộc là gì?

Trong cờ vua, **khai cuộc** (opening) là giai đoạn đầu của ván cờ — thường là 10-15 nước đi đầu tiên. Đây là giai đoạn 2 bên triển khai quân ra khỏi vị trí ban đầu, kiểm soát trung tâm bàn cờ và bảo vệ Vua.

Qua hàng trăm năm, các kỳ thủ đã nghiên cứu và đặt tên cho hàng trăm kiểu khai cuộc khác nhau. Mỗi kiểu có chiến lược riêng, ưu nhược điểm riêng.

```
Ví dụ:
  White đi e4 → Black đi c5
  → Đây là "Sicilian Defense" — khai cuộc phổ biến nhất thế giới
  → Black không chiếm trung tâm ngay mà tấn công từ cạnh

  White đi e4 → Black đi e5 → White đi Nf3 → Black đi Nc6 → White đi Bb5
  → Đây là "Ruy Lopez" — khai cuộc có từ thế kỷ 16
  → White gây áp lực lên quân bảo vệ trung tâm của Black
```

### Hệ thống ECO — chuẩn phân loại quốc tế

Để thống nhất tên gọi, toàn bộ khai cuộc cờ vua được phân loại theo hệ thống **ECO (Encyclopaedia of Chess Openings)** — gồm 500 mã từ **A00 đến E99**, chia thành 5 nhóm lớn:

| Nhóm | Nước mở đầu | Ví dụ |
|------|-------------|-------|
| **A** | 1.d4 hoặc các nước khác | A00 - A99 |
| **B** | 1.e4, Black không đi e5 | B00 - B99 (Sicilian, Caro-Kann...) |
| **C** | 1.e4 e5 | C00 - C99 (Ruy Lopez, Italian...) |
| **D** | 1.d4 d5 | D00 - D99 (Queen's Gambit...) |
| **E** | 1.d4 Nf6 | E00 - E99 (King's Indian, Nimzo-Indian...) |

Càng đi nhiều nước thì mã ECO càng cụ thể hơn:

```
Sau nước 1: e4, c5
  → B20: Sicilian Defense (tên chung)

Sau nước 6: e4, c5, Nf3, d6, d4, cxd4, Nxd4, Nf6, Nc3, a6
  → B90: Sicilian Defense: Najdorf Variation (biến thể cụ thể)
```

### Cách hệ thống xác định khai cuộc

Lichess có sẵn một **opening database** — bảng map từ chuỗi nước đi → mã ECO + tên khai cuộc. Hệ thống tra bảng này để xác định khai cuộc.

**Với historical data (per-user export + bulk dump):**
Lichess đã tự gắn sẵn ECO code vào mỗi game, hệ thống chỉ cần đọc ra:
```
opening.eco  = "B90"
opening.name = "Sicilian Defense: Najdorf Variation"
opening.ply  = 12  ← khai cuộc được chốt tại nước 12
```

**Với real-time stream:**
Không có sẵn ECO code, hệ thống tự tra bảng bằng cách so chuỗi nước đi với opening database. Lichess publish database này dưới dạng file mở, hệ thống import vào là dùng được.

```
Nhận được các nước đi: e4, c5, Nf3, d6, d4, cxd4, Nxd4, Nf6, Nc3, a6
→ Tra bảng → B90: Sicilian Najdorf
→ Lưu vào game record
```

Khai cuộc được "chốt" khi chuỗi nước đi không còn khớp với bất kỳ mã ECO nào nữa — lúc đó 2 bên đã bắt đầu tự sáng tạo, thoát khỏi lý thuyết.

---

## Tất cả data đều đi qua Kafka

```
Real-time stream  ──►
Per-user export   ──►  Kafka  ──►  Processing  ──►  PostgreSQL + Vector DB
Bulk PGN dumps    ──►
```

Ba nguồn được chuẩn hóa về cùng format trước khi đẩy vào Kafka. Processing layer phía sau không cần biết data đến từ nguồn nào.
