# CHƯƠNG 1: GIỚI THIỆU

---

## 1.1 Bối cảnh và động lực nghiên cứu

Trong lịch sử phát triển của trí tuệ nhân tạo, cờ vua luôn là một trong những thử thách biểu tượng nhất — từ Deep Blue đánh bại Garry Kasparov năm 1997, đến AlphaZero tự học từ đầu và vượt qua Stockfish chỉ sau 4 giờ huấn luyện năm 2017. Tuy nhiên, nghịch lý là dù AI đã chinh phục cờ vua ở tầm thế giới, hàng triệu người chơi bình thường vẫn không có công cụ phù hợp để cải thiện trình độ của chính mình.

Lichess.org — nền tảng cờ trực tuyến mã nguồn mở lớn nhất thế giới — xử lý trung bình hơn **500.000 ván đấu mỗi ngày**, tạo ra một khối lượng dữ liệu khổng lồ chứa đựng toàn bộ hành vi, sai lầm, và pattern tư duy của người chơi ở mọi cấp độ. Dữ liệu này hoàn toàn công khai — nhưng gần như chưa được khai thác có hệ thống để phục vụ mục tiêu huấn luyện cá nhân hóa.

Đề tài này xuất phát từ một quan sát đơn giản: **khoảng cách giữa dữ liệu tồn tại và insight có thể hành động được là rất lớn** — và đó chính xác là vấn đề mà kỹ thuật dữ liệu và AI có thể giải quyết.

---

## 1.2 Đặt vấn đề

### 1.2.1 Vấn đề của người chơi nghiệp dư

Một người chơi cờ ở mức rating 1200–1600 trên Lichess thường trải qua một vòng lặp không tiến bộ: chơi nhiều, thua nhiều, không biết mình sai ở đâu. Họ biết kết quả — thua — nhưng không có cơ chế phản hồi có cấu trúc để hiểu *nguyên nhân sâu xa*:

- Lỗi sai xảy ra nhất quán ở giai đoạn nào của ván đấu?
- Họ có xu hướng mắc sai lầm khi nào gần hết thời gian không?
- Khai cuộc nào họ đang hiểu sai về mặt chiến lược, không chỉ lý thuyết?
- Pattern tư duy nào đang lặp đi lặp lại xuyên suốt hàng trăm ván?

Công cụ phân tích hiện tại — kể cả công cụ tích hợp của Lichess — chỉ phân tích **từng ván riêng lẻ**. Người chơi biết họ mắc blunder ở nước 23 của ván hôm nay, nhưng không ai tổng hợp và nói với họ rằng: *trong 3 tháng qua, 71% blunder của bạn xảy ra ở middlegame khi đồng hồ xuống dưới 20 giây.*

### 1.2.2 Khoảng trống kỹ thuật

Từ góc độ hệ thống, bài toán chia thành ba lớp:

**Lớp thu thập:** Lichess cung cấp API streaming theo thời gian thực, nhưng không có hệ thống nào thu thập liên tục, xử lý và lưu trữ dữ liệu theo cấu trúc phù hợp cho phân tích sâu — đặc biệt là dữ liệu đồng hồ (clock), khai cuộc (opening), và kết quả ở cấp độ từng nước đi.

**Lớp xử lý:** Dữ liệu thô từ Lichess là chuỗi ký hiệu SAN (Standard Algebraic Notation) — cần được giải mã thành từng trạng thái bàn cờ, gán nhãn giai đoạn ván đấu, và liên kết với metadata người chơi trước khi có thể phân tích.

**Lớp trí tuệ:** Ngay cả khi có dữ liệu sạch và có cấu trúc, việc chuyển đổi con số thống kê thành lời khuyên cụ thể, có ngữ cảnh, bằng ngôn ngữ tự nhiên — đòi hỏi sự kết hợp giữa truy vấn dữ liệu thực tế và mô hình ngôn ngữ lớn (LLM).

### 1.2.3 Câu hỏi nghiên cứu

Từ phân tích trên, đề tài đặt ra câu hỏi nghiên cứu trung tâm:

> **Làm thế nào để thiết kế và triển khai một hệ thống kỹ thuật dữ liệu end-to-end — từ thu thập streaming thời gian thực đến phân tích AI — có khả năng biến dữ liệu ván cờ thô thành insight cá nhân hóa, giúp người chơi hiểu và cải thiện trình độ một cách có căn cứ?**

---

## 1.3 Mục tiêu đề tài

### Mục tiêu 1 — Pipeline thu thập dữ liệu thời gian thực
Xây dựng hệ thống streaming kết nối trực tiếp với Lichess API, thu thập liên tục sự kiện ván đấu (game start, game end, nước đi, đồng hồ), đưa vào message queue (Kafka), và lưu trữ dạng columnar trên object storage (MinIO/Iceberg).

### Mục tiêu 2 — Kho dữ liệu phân tích (Data Warehouse)
Xử lý dữ liệu thô qua Apache Spark, tạo bảng phân tích phẳng (flat table) ở cấp độ từng nước đi — mỗi bản ghi chứa đầy đủ context: trạng thái bàn cờ (FEN), thời gian còn lại, thông tin người chơi, khai cuộc, giai đoạn ván, kết quả. Lưu trữ định dạng Iceberg trên Apache Polaris (REST catalog).

### Mục tiêu 3 — Dashboard phân tích hiệu suất người chơi
Xây dựng giao diện trực quan hóa các chỉ số xuyên nhiều ván: tỷ lệ thắng theo màu quân, khai cuộc, loại đối thủ, áp lực thời gian theo giai đoạn, và lịch sử gần đây.

### Mục tiêu 4 — Phân tích ván đấu từng nước với Stockfish
Tích hợp Stockfish engine để đánh giá thế cờ tại mỗi nước đi, xây dựng biểu đồ eval xuyên suốt ván, và sử dụng LLM để xác định khoảnh khắc then chốt quyết định kết quả.

### Mục tiêu 5 — AI Coach tương tác
Xây dựng tác nhân AI (agent) kết hợp LLM (Gemini 2.5 Flash qua Vertex AI) với function calling để truy vấn dữ liệu thực tế, trả lời câu hỏi tự nhiên của người chơi bằng nhận định có căn cứ từ dữ liệu — không phải lời khuyên chung chung.

---

## 1.4 Phạm vi và giới hạn

Đề tài tập trung vào:
- Người chơi trên nền tảng **Lichess.org** (dữ liệu công khai, API mở)
- Các loại ván **có đồng hồ** (Bullet, Blitz, Rapid) — vì dữ liệu clock là yếu tố phân tích quan trọng
- Phân tích **hành vi và pattern** của người chơi, không phải lý thuyết khai cuộc hay tàn cuộc thuần túy

Đề tài **không** bao gồm:
- Xây dựng engine cờ vua mới
- Phân tích ván đấu của Grandmaster hay cờ thế giới
- Chức năng chơi cờ trực tiếp với AI

---

## 1.5 Các kịch bản sử dụng điển hình

### Kịch bản 1: Hiểu một ván thua cụ thể

Người chơi nhập Game ID vừa thua vào Game Explorer. Hệ thống tải toàn bộ ván đấu, hiển thị bàn cờ tương tác có thể điều hướng từng nước. Stockfish đánh giá từng vị trí theo thời gian thực; biểu đồ eval cho thấy thế cờ "rơi" ở đâu. Nút "Analyze with AI" gọi Gemini tổng hợp toàn bộ dữ liệu eval và xác định: nước nào là blunder quyết định, sai lầm chiến lược nào đã dẫn đến nó, và đối thủ đã khai thác như thế nào.

Thay vì chỉ biết "tôi thua", người chơi hiểu "tôi thua vì nước 26 — tôi bỏ lỡ taktik Hanging Piece trong tình huống áp lực thời gian, và đây là lần thứ tư trong tháng tôi mắc lỗi tương tự."

### Kịch bản 2: Nhìn tổng thể nhiều tháng chơi

Người chơi nhập username vào Player Dashboard. Hệ thống truy vấn toàn bộ lịch sử ván đấu đã thu thập và hiển thị:

- Tỷ lệ thắng theo từng loại thời gian và màu quân
- Top khai cuộc theo tần suất và win rate — khai cuộc nào đang "rò rỉ" điểm
- Phân phối thời gian đồng hồ theo giai đoạn: người chơi có đang cạn thời gian ở tàn cuộc không?
- Win rate khi gặp đối thủ cao hơn, ngang bằng, thấp hơn rating

Đây là thông tin không thể có nếu chỉ nhìn từng ván một.

### Kịch bản 3: Hỏi AI Coach theo ngôn ngữ tự nhiên

Người chơi vào tab AI Coach và hỏi bằng tiếng Việt hoặc tiếng Anh:

> *"Tôi hay thua nhất khi nào?"*

Agent không trả lời bằng kiến thức chung. Nó gọi các tool truy vấn StarRocks để lấy dữ liệu thực tế của người chơi đó — win rate theo giai đoạn, phân phối blunder theo thời gian đồng hồ, opening performance — rồi tổng hợp thành nhận định cụ thể:

> *"Dựa trên 847 ván của bạn, tỷ lệ thua tăng gần gấp đôi khi đồng hồ xuống dưới 15 giây (từ 38% lên 71%). Điểm yếu tập trung ở endgame khi chơi quân Đen trong các ván Blitz — bạn có muốn xem chi tiết 10 ván gần nhất không?"*

---

## 1.6 Đóng góp của đề tài

Đề tài có ba đóng góp chính:

**Về kỹ thuật:** Thiết kế và triển khai một pipeline dữ liệu end-to-end hoàn chỉnh — từ streaming real-time (Kafka), object storage (MinIO), batch processing (Spark + Iceberg/Polaris), serving layer (StarRocks), đến AI agent (Vertex AI) — trên môi trường Kubernetes. Đây là kiến trúc hiện đại tương đương các hệ thống data platform ở quy mô production.

**Về ứng dụng AI:** Xây dựng AI agent có khả năng kết hợp function calling với truy vấn dữ liệu thực tế, tạo ra câu trả lời dựa trên bằng chứng thay vì kiến thức chung của LLM — một pattern ngày càng quan trọng trong các hệ thống AI doanh nghiệp.

**Về giá trị người dùng:** Cung cấp cho người chơi nghiệp dư loại insight mà trước đây chỉ có thể có được từ huấn luyện viên cá nhân — với chi phí bằng không và có thể mở rộng cho bất kỳ người chơi nào trên Lichess.

---

## 1.7 Cấu trúc luận văn

Phần còn lại của luận văn được tổ chức như sau:

- **Chương 2** trình bày tổng quan các công nghệ và công trình liên quan — kiến trúc Lambda/Kappa, Iceberg table format, LLM agent với function calling, và các hệ thống phân tích cờ hiện có.
- **Chương 3** mô tả kiến trúc tổng thể của hệ thống và thiết kế từng thành phần.
- **Chương 4** đi sâu vào cài đặt và triển khai — từ streaming pipeline đến batch processing và AI agent.
- **Chương 5** trình bày kết quả thực nghiệm và đánh giá hệ thống.
- **Chương 6** kết luận và đề xuất hướng phát triển tiếp theo.
