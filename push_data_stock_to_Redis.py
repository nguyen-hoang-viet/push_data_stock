import psycopg2
import psycopg2.extras
import redis
import json
from dotenv import load_dotenv
import os
import logging
from fastapi import FastAPI, HTTPException

# --- CẤU HÌNH LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- CÁC HÀM KẾT NỐI (Giữ nguyên như file cũ) ---
def get_db_connection():
    load_dotenv()
    conn = psycopg2.connect(
        user=os.getenv("user"),
        password=os.getenv("password"),
        host=os.getenv("host"),
        port=os.getenv("port"),
        dbname=os.getenv("dbname")
    )
    return conn

def get_redis_connection():
    load_dotenv()
    r = redis.Redis(
        host=os.getenv("REDIS_HOST"),
        port=os.getenv("REDIS_PORT"),
        password=os.getenv("REDIS_PASSWORD"),
        decode_responses=True
    )
    return r

# --- HÀM LOGIC MỚI ---

def fetch_stock_data(cursor, stock_ticker: str, time_condition: str):
    """
    Hàm phụ trợ để lấy và xử lý dữ liệu cho một cổ phiếu với một điều kiện thời gian cụ thể.
    """
    table_name = f'"{stock_ticker}_Stock"'
    logging.info(f"Bắt đầu lấy dữ liệu cho bảng: {table_name} với điều kiện: {time_condition or 'Tất cả'}...")
    
    # Xây dựng câu query động
    # Chỉ lấy 2 cột cần thiết và sắp xếp theo ngày để biểu đồ vẽ đúng
    query = f"""
        SELECT "date", "close_price" 
        FROM {table_name} 
        {time_condition}
        ORDER BY "date" ASC;
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    logging.info(f"Đã lấy được {len(rows)} dòng.")

    processed_rows = []
    for row in rows:
        # Kiểm tra nếu 'date' hoặc 'close_price' là None (NULL trong DB) thì bỏ qua dòng này
        if row.get('date') is None or row.get('close_price') is None:
            logging.warning(f"Bỏ qua dòng dữ liệu bị thiếu thông tin: date hoặc close_price là NULL. Dữ liệu: {row}")
            continue
        
        # Code xử lý chỉ chạy khi dữ liệu hợp lệ
        try:
            processed_rows.append({
                'date': row['date'].strftime('%Y-%m-%d'),
                'close_price': float(str(row['close_price']).replace(',', ''))
            })
        except (ValueError, TypeError) as e:
            # Bắt thêm lỗi nếu giá trị không phải là số sau khi đã xử lý
            logging.error(f"Không thể chuyển đổi giá trị close_price thành số: '{row['close_price']}'. Lỗi: {e}. Bỏ qua dòng này.")
            continue
        
    return processed_rows
def sync_stock_data_to_redis():
    """
    Hàm chính để đồng bộ dữ liệu giá cổ phiếu từ Postgres (Supabase) sang Redis.
    """
    logging.info("Bắt đầu quá trình đồng bộ dữ liệu CỔ PHIẾU...")
    pg_conn = None
    
    # Định nghĩa các cổ phiếu và các khung thời gian cần lấy
    STOCKS_TO_PROCESS = ["FPT", "GAS", "IMP", "VCB"]
    TIME_RANGES = {
        "all": "", # Lấy tất cả dữ liệu
        "1M": "WHERE \"date\" >= NOW() - INTERVAL '1 month'",
        "3M": "WHERE \"date\" >= NOW() - INTERVAL '3 months'",
        "1Y": "WHERE \"date\" >= NOW() - INTERVAL '1 year'",
        "5Y": "WHERE \"date\" >= NOW() - INTERVAL '5 years'",
    }

    try:
        pg_conn = get_db_connection()
        cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        redis_conn = get_redis_connection()

        with redis_conn.pipeline() as pipe:
            # Lặp qua từng cổ phiếu
            for ticker in STOCKS_TO_PROCESS:
                # Lặp qua từng khung thời gian
                for range_key, condition in TIME_RANGES.items():
                    # Gọi hàm phụ trợ để lấy dữ liệu
                    stock_data = fetch_stock_data(cursor, ticker, condition)
                    
                    if stock_data:
                        # Tạo key cho Redis, ví dụ: "stock:FPT:1Y"
                        redis_key = f"stock:{ticker}:{range_key}"
                        json_data = json.dumps(stock_data)
                        
                        # Thêm lệnh SET vào pipeline, tự động hết hạn sau 1 ngày (86400 giây)
                        pipe.set(redis_key, json_data, ex=86400)
                        logging.info(f"Đã chuẩn bị đẩy {len(stock_data)} bản ghi cho key '{redis_key}'.")

            # Thực thi tất cả các lệnh trong pipeline một lần duy nhất
            pipe.execute()
        
        logging.info("Đã đẩy thành công tất cả dữ liệu cổ phiếu lên Redis.")
        return {"status": "success", "message": "Stock data synced successfully."}

    except Exception as e:
        logging.error(f"Đã xảy ra lỗi trong quá trình đồng bộ dữ liệu cổ phiếu: {e}")
        raise e
    finally:
        if pg_conn:
            pg_conn.close()
            logging.info("Đã đóng kết nối PostgreSQL.")


# --- TẠO ỨNG DỤNG VÀ API ENDPOINT (Tùy chọn, nếu bạn muốn kích hoạt qua API) ---
app = FastAPI()

# --- ENDPOINT MỚI ĐƯỢC THÊM VÀO ---
@app.get("/")
async def health_check():
    """
    Endpoint này chỉ dùng để kiểm tra xem server có hoạt động không.
    Google Apps Script sẽ gọi đến đây để giữ cho server không bị "ngủ".
    """
    return {"status": "alive"}

@app.post("/push_stock_data")
async def trigger_stock_sync_endpoint():
    """
    Endpoint để kích hoạt quá trình đồng bộ dữ liệu cổ phiếu.
    """
    try:
        result = sync_stock_data_to_redis()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Để có thể chạy file này độc lập để kiểm tra:
# if __name__ == "__main__":
#     sync_stock_data_to_redis()