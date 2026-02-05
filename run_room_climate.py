import os
import json
from multivariate_storage import load_room_climate_csv

# Configurare
CSV_PATH = os.path.join("timseries", "room_climate_location_A.csv")
OUTPUT_PREFIX = os.path.join("compressed_output", "room_climate")

def format_bytes(size):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024: return f"{size:.2f} {unit}"
        size /= 1024

def main():
    print("=== EXECUTIE SERIE MULTIVARIATA (CLIMA) ===")
    if not os.path.exists(CSV_PATH):
        print(f"Eroare: Nu s-a gasit {CSV_PATH}")
        return

    # 1. Incarcare (folosind utilitarul dedicat din multivariate_storage)
    series = load_room_climate_csv(CSV_PATH)
    series.flush()

    # 2. Salvare Binara si Metadata
    if not os.path.exists("compressed_output"): os.makedirs("compressed_output")
    
    total_bytes = 0
    with open(f"{OUTPUT_PREFIX}.bin", 'wb') as f:
        for _, _, data in series._closed_blocks:
            f.write(data)
            total_bytes += len(data)

    with open(f"{OUTPUT_PREFIX}.meta.json", 'w') as f:
        json.dump({
            "variables": series.variable_names,
            "points": series.total_points,
            "size": total_bytes
        }, f, indent=2)

    # 3. Raport
    csv_size = os.path.getsize(CSV_PATH)
    stats = series.get_compression_stats()
    
    print(f"Puncte: {series.total_points}")
    print(f"Variabile: {len(series.variable_names)}")
    print(f"Dimensiune CSV: {format_bytes(csv_size)}")
    print(f"Dimensiune Gorilla: {format_bytes(total_bytes)}")
    print(f"Biti per punct: {stats['bits_per_point']:.2f}")

if __name__ == "__main__":
    main()