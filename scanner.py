import ccxt
import json
import time
import schedule
from datetime import datetime
import os
import sys

if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

CONFIG_FILE = "config.json"
FILTERED_PAIRS_FILE = "filtered_pairs.json"
MATCHES_FILE = "matches.json"

def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Lỗi khi đọc cấu hình: {e}")
    return {
        "max_price_usdt": 10.0,
        "timeframe": "15m",
        "limit_candles": 4,
        "interval_minutes": 15
    }

def get_binance_futures_exchange():
    return ccxt.binance({
        'options': {
            'defaultType': 'future',
        }
    })

def filter_and_save_pairs(exchange, max_price):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Đang lấy danh sách các cặp và lọc giá < {max_price} USDT...")
    try:
        exchange.load_markets()
        tickers = exchange.fetch_tickers()
        
        filtered_pairs = []
        for symbol, market in exchange.markets.items():
            if market.get('active', False) and market.get('quote') == 'USDT':
                if market.get('linear', False) or market.get('type') in ['swap', 'future']:
                    ticker = tickers.get(symbol)
                    if ticker and ticker.get('last') is not None:
                        last_price = ticker['last']
                        if last_price < max_price:
                            filtered_pairs.append(symbol)
                        
        with open(FILTERED_PAIRS_FILE, 'w') as f:
            json.dump(filtered_pairs, f, indent=4)
            
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Đã lưu {len(filtered_pairs)} cặp có giá < {max_price} vào {FILTERED_PAIRS_FILE}")
        return filtered_pairs
    except Exception as e:
        print(f"Lỗi khi lọc danh sách cặp: {e}")
        return []

def check_red_candles(exchange, symbol, timeframe, limit):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        if not ohlcv or len(ohlcv) < 3:
            return False
            
        is_red = lambda candle: candle[4] < candle[1]
        red_candles = [is_red(candle) for candle in ohlcv]
        
        count = 0
        for is_r in red_candles:
            if is_r:
                count += 1
                if count >= 3:
                    return True
            else:
                count = 0
                
        return False
        
    except Exception as e:
        print(f"Lỗi khi lấy dữ liệu nến cho {symbol}: {e}")
        return False

def job():
    config = load_config()
    max_price = config.get("max_price_usdt", 10.0)
    timeframe = config.get("timeframe", "15m")
    limit = config.get("limit_candles", 4)
    
    exchange = get_binance_futures_exchange()
    
    filtered_pairs = filter_and_save_pairs(exchange, max_price)
    
    if not filtered_pairs:
        print("Không có cặp nào thoả mãn hoặc có lỗi.")
        return []
        
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Bắt đầu quét nến ({timeframe}) cho {len(filtered_pairs)} cặp...")
    
    matched_pairs = []
    for symbol in filtered_pairs:
        if check_red_candles(exchange, symbol, timeframe, limit):
            matched_pairs.append({
                "symbol": symbol,
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            print(f"  --> PHÁT HIỆN: {symbol} có ít nhất 3 nến {timeframe} đỏ liền nhau trong {limit} nến gần nhất!")
            
    with open(MATCHES_FILE, "w") as f:
        json.dump(matched_pairs, f, indent=4)
        
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Hoàn thành chu kỳ quét! Tổng cộng tìm thấy {len(matched_pairs)} cặp thoả mãn.")
    print("-" * 50)
    return matched_pairs

def main():
    config = load_config()
    interval = config.get("interval_minutes", 15)
    
    print(f"Khởi động bot. Bot sẽ quét tự động mỗi {interval} phút.")
    print("Bạn có thể sửa file config.json để thay đổi cài đặt và khởi động lại bot.")
    print("-" * 50)
    
    job()
    
    schedule.every(interval).minutes.do(job)
    
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
