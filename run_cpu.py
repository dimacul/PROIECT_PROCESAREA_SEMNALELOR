import os
import csv
import json
from datetime import datetime
from multivariate_storage import MultiVariateSeries

# Configurare
CSV_PATH = os.path.join("timseries", "data_cpu.csv")
OUTPUT_PREFIX = os.path.join("compressed_output", "cpu_load")

def format_bytes(size):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024: return f"{size:.2f} {unit}"
        size /= 1024

def main():
    print("=== EXECUTIE SERIE UNIVARIATA (CPU) ===")
    if not os.path.exists(CSV_PATH):
        print(f"Eroare: Nu s-a gasit {CSV_PATH}")
        return

    # 1. Incarcare si Compresie
    series = MultiVariateSeries(["cpu_load"], block_duration_ms=7200000)
    
    with open(CSV_PATH, 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            dt = datetime.strptime(row[0].strip(), "%Y-%m-%d %H:%M:%S")
            ts_ms = int(dt.timestamp() * 1000)
            series.insert(ts_ms, {"cpu_load": float(row[1])})
    
    series.flush()

    # 2. Salvare Binara si Metadata
    if not os.path.exists("compressed_output"): os.makedirs("compressed_output")
    
    total_bytes = 0
    with open(f"{OUTPUT_PREFIX}.bin", 'wb') as f:
        for _, _, data in series._closed_blocks:
            f.write(data)
            total_bytes += len(data)

    with open(f"{OUTPUT_PREFIX}.meta.json", 'w') as f:
        json.dump({"points": series.total_points, "size": total_bytes}, f)

    # 3. Raport
    csv_size = os.path.getsize(CSV_PATH)
    print(f"Puncte: {series.total_points}")
    print(f"Dimensiune CSV: {format_bytes(csv_size)}")
    print(f"Dimensiune Gorilla: {format_bytes(total_bytes)}")
    print(f"Rata Compresie: {csv_size / total_bytes:.2f}x")

if __name__ == "__main__":
    main()