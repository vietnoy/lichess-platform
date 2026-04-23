# Kịch bản huấn luyện & thiết kế UI tương tác

---

## Tổng quan triết lý thiết kế

Thay vì chỉ *hiển thị thông tin*, hệ thống được thiết kế theo nguyên tắc **"Coach, không phải Oracle"** — nghĩa là UI không đưa ra đáp án ngay, mà dẫn dắt người chơi tự khám phá thông qua câu hỏi, gợi ý từng bước, và phản hồi tức thì. Mọi nội dung huấn luyện đều được trích xuất từ chính dữ liệu ván đấu thực tế của người chơi — không phải bài tập generic từ sách giáo khoa.

---

## Kịch bản 1: Game Explorer — "Khám nghiệm tử thi" một ván thua

### Luồng trải nghiệm

Người chơi nhập Game ID vào ô tìm kiếm. Thay vì tải thẳng bàn cờ, UI hiển thị một **"màn hình tóm tắt ván đấu"** trong 2 giây:

```
♟ Ván đấu #RPJr6MMX
─────────────────────────────────────
  Bạn (Đen, 1487) vs. opponent (Trắng, 1512)
  Blitz 5+0  ·  Sicilian Defense  ·  Thua (resign)
  
  ⚠️  Phát hiện 1 blunder nghiêm trọng ở nước 26
      Thế cờ lật ngược hoàn toàn chỉ sau 1 nước đi
─────────────────────────────────────
  [ Xem toàn bộ ván ]   [ Nhảy đến điểm mấu chốt ]
```

Nếu chọn **"Nhảy đến điểm mấu chốt"**, bàn cờ mở ra ở đúng nước trước blunder. UI không tô đỏ nước sai ngay — thay vào đó hiển thị:

> *"Đây là thế cờ ở nước 25. Bạn đang chơi Đen, đồng hồ còn **8 giây**. Nhìn vào bàn cờ — bạn thấy gì đáng chú ý không?"*

Người chơi có thể tự suy nghĩ hoặc chọn gợi ý theo từng cấp:

- **Gợi ý 1:** *"Quét nhanh các quân Trắng — quân nào đang không có quân bảo vệ?"*
- **Gợi ý 2:** *"Mã Đen đang ở e4. Nó có thể với tới ô nào?"*
- **Gợi ý 3:** Hiển thị mũi tên xanh lên nước đúng

Sau khi người chơi đã hiểu (hoặc xem đáp án), UI chuyển sang phân tích eval:

```
Biểu đồ đánh giá thế cờ (Evaluation Chart)
+3.0 ┤                              ╭──────
+1.0 ┤          ╭──────────────────╯
 0.0 ┼──────────╯
-1.0 ┤                         ╲
-3.0 ┤                          ╲______ ← nước 26: -2.8 (blunder)
     └────────────────────────────────── nước đi
```

Phía dưới biểu đồ, nút **"Phân tích với AI"** gọi Gemini để diễn giải toàn bộ ván bằng ngôn ngữ tự nhiên — xác định chuỗi sai lầm dẫn đến blunder quyết định, không chỉ nêu nước sai.

---

## Kịch bản 2: Pressure Drill — Luyện tập dưới áp lực thời gian thực tế

### Ý tưởng cốt lõi

Dữ liệu `clock_remaining` trong hệ thống cho biết chính xác người chơi còn bao nhiêu giây tại từng nước. Kịch bản này tái lập đúng áp lực đồng hồ đó — không phải luyện với thời gian vô hạn, mà luyện đúng ở điều kiện khắc nghiệt đã xảy ra trong thực tế.

### Luồng trải nghiệm

Từ Player Dashboard, sau khi phân tích xong, xuất hiện panel:

```
┌─────────────────────────────────────────────────┐
│  🎯 Bài tập cá nhân hóa từ ván của bạn          │
│                                                  │
│  Phát hiện 7 vị trí bạn đã có nước đi tốt hơn  │
│  nhưng đã bỏ lỡ do áp lực thời gian.            │
│                                                  │
│  Thử lại với đúng số giây bạn có lúc đó?        │
│                                                  │
│          [ Bắt đầu luyện tập ]                  │
└─────────────────────────────────────────────────┘
```

Khi bắt đầu, mỗi bài tập hiển thị:

```
Bài 3 / 7 — Trích từ ván ngày 20/04/2026, nước 31
─────────────────────────────────────────────────
[ Bàn cờ ]          ⏱️  12 giây

                    Đen đi. Tìm nước tốt nhất.
                    
                    [ Gợi ý ] [ Bỏ qua ]
```

Đồng hồ đếm ngược thật sự. Nếu hết giờ mà chưa đi:

> *"Hết giờ — đúng như ván thật. Khi bị áp lực, não bạn chọn nước an toàn thay vì nước tốt nhất. Nước đúng là Nxd2 — bắt Xe không được bảo vệ."*

Nếu đi đúng trong thời gian:

> *"✅ Chính xác! Bạn vừa làm được điều mà chính bạn không làm được trong ván thật. Eval tăng từ -0.3 lên +1.2."*

Sau 7 bài, hệ thống tổng kết:

```
Kết quả phiên luyện
─────────────────────────────────────
  Đúng trong giờ:   4 / 7  (57%)
  Đúng hết giờ:     2 / 7
  Bỏ qua:           1 / 7

  Cải thiện so với ván thật: +4 nước đúng

  💡 Nhận xét: Bạn làm tốt hơn hẳn khi có
     trên 15 giây. Dưới 10 giây, độ chính xác
     giảm mạnh — đây là điểm cần luyện tiếp.
─────────────────────────────────────
  [ Luyện lại ]   [ Xem phân tích chi tiết ]
```

### Tại sao kịch bản này có giá trị kỹ thuật cao

Hầu hết hệ thống luyện cờ cho người chơi thời gian thoải mái. Kịch bản này khai thác dữ liệu `clock_remaining` — một trường dữ liệu mà chỉ hệ thống có pipeline thu thập đầy đủ mới có — để tái lập điều kiện thực tế. Đây là ứng dụng trực tiếp của kỹ thuật dữ liệu vào trải nghiệm người dùng.

---

## Kịch bản 3: Weakness Spotlight — Hệ thống chỉ thẳng điểm yếu

### Luồng trải nghiệm

Sau khi Player Dashboard load xong, bên cạnh các biểu đồ thống kê, có một panel riêng:

```
┌──────────────────────────────────────────────────┐
│  🔍 AI Coach phát hiện 3 điểm yếu rõ ràng        │
│                                                   │
│  1. Sicilian Defense (chơi Đen)                  │
│     8 ván · Thắng 1 · Thua 7 · Win rate: 12%    │
│     → Tất cả 7 ván thua đều mất lợi thế         │
│       trước nước 20                              │
│                                                   │
│  2. Endgame khi đồng hồ < 15 giây               │
│     Win rate: 21% (so với 58% khi > 30 giây)    │
│                                                   │
│  3. Khi chơi với đối thủ cao rating hơn 100+    │
│     Win rate: 19% — thấp hơn kỳ vọng thống kê  │
│                                                   │
│  [ Luyện điểm yếu #1 ]  [ Hỏi AI Coach ]        │
└──────────────────────────────────────────────────┘
```

Khi click **"Luyện điểm yếu #1"**, hệ thống truy vấn các ván Sicilian thua của người chơi, trích xuất các vị trí then chốt trước nước 20, và tạo thành chuỗi bài tập tương tự Kịch bản 2 — nhưng lần này tập trung vào đúng opening đang yếu.

UI hướng dẫn bằng câu hỏi dẫn dắt:

> *"Trong thế trận Sicilian, Trắng thường tấn công cánh Vua. Trước khi đi, hãy tự hỏi: đối thủ đang chuẩn bị gì ở cánh Vua của tôi?"*

---

## Thiết kế UI tổng thể — Nguyên tắc hướng dẫn

### Không bao giờ đưa đáp án trực tiếp ngay lập tức

Mọi câu trả lời đều đi qua ít nhất một câu hỏi dẫn dắt trước. Người chơi phải *nghĩ* trước khi *xem*. Điều này không phải làm khó — mà là cơ chế học tập hiệu quả hơn so với học vẹt.

### Phản hồi luôn kèm ngữ cảnh dữ liệu

Thay vì: *"Nước đó sai."*

Hệ thống nói: *"Nước đó làm eval giảm 2.1 điểm — từ thế ngang bằng sang thua rõ. Và đây là lần thứ 3 trong tháng bạn mắc lỗi dạng này ở cùng một khai cuộc."*

### Ba cấp độ gợi ý, không bao giờ ép buộc

Người chơi luôn có quyền chọn:
- Tự làm hoàn toàn
- Xem gợi ý từng bước (3 cấp độ)
- Xem đáp án ngay

Hệ thống không phán xét — chỉ ghi nhận và phản ánh lại qua thống kê.

### Kết nối liên tục giữa luyện tập và dữ liệu thực

Mọi bài tập đều trích dẫn ván thật: *"Bài tập này đến từ ván ngày 18/04, nước 31."* Người chơi luôn cảm nhận được rằng họ đang học từ chính sai lầm của mình, không phải từ tình huống xa lạ.

---

*→ Tiếp theo: đặt tên đề tài và viết phần động lực*
