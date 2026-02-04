"""
Script specializat pentru procesarea datelor de radiatii (Chernobyl - 1986).
Format CSV: PAYS,Code,Location,Longitude,Latitude,Date,I_131,Cs_134,Cs_137
"""

import os
import time
import csv
import json
from datetime import datetime
from multivariate_storage import MultiVariateSeries

# --- CONFIGURARE CAI ---
# Presupunem ca fisierul se numeste 'radiation_data.csv' in folderul 'timseries'
BASE_DIR = os.path.dirname(__file__)
CSV_PATH = os.path.join(BASE_DIR, "timseries", "Chernobyl_ Chemical_Radiation.csv")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "compressed_output")

def format_bytes(size: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024: return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"

def load_radiation_data(filepath):
    """
    Incarca si comprima datele de radiatii.
    Extrage: I_131_(Bq/m3), Cs_134_(Bq/m3), Cs_137_(Bq/m3)
    """
    # Definim variabilele pentru seria multivariata
    variables = ["I_131", "Cs_134", "Cs_137"]
    
    # Folosim un bloc de 24 ore (86.400.000 ms) deoarece masuratorile sunt zilnice
    series = MultiVariateSeries(variables, block_duration_ms=86400000)

    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Nu s-a gasit fisierul la: {filepath}")

    with open(filepath, 'r', newline='', encoding='utf-8') as f:
        # Folosim DictReader pentru a accesa coloanele dupa nume
        reader = csv.DictReader(f)
        
        for row in reader:
            try:
                # Parsare data: "86/04/27" -> %y/%m/%d
                dt = datetime.strptime(row['Date'].strip(), "%y/%m/%d")
                ts_ms = int(dt.timestamp() * 1000)

                # Mapare date numerice (curatam numele coloanelor de paranteze daca e cazul)
                data_point = {
                    "I_131": float(row['I_131_(Bq/m3)']),
                    "Cs_134": float(row['Cs_134_(Bq/m3)']),
                    "Cs_137": float(row['Cs_137_(Bq/m3)'])
                }
                
                series.insert(ts_ms, data_point)
            except (ValueError, KeyError, TypeError) as e:
                # Ignoram randurile cu date lipsa sau formate gresite
                continue

    series.flush()
    return series

def save_output(series, name):
    """Salveaza rezultatele comprimate pe disc."""
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

    prefix = os.path.join(OUTPUT_FOLDER, name)
    bin_path = f"{prefix}.bin"
    
    total_bytes = 0
    with open(bin_path, 'wb') as f:
        for _, _, data in series._closed_blocks:
            f.write(data)
            total_bytes += len(data)
            
    # Salvare Metadata simplificat
    with open(f"{prefix}.meta.json", 'w') as f:
        json.dump({
            "name": name,
            "points": series.total_points,
            "variables": series.variable_names,
            "size_bytes": total_bytes
        }, f, indent=2)
        
    return bin_path, total_bytes

def main():
    print("="*60)
    print("PROCESARE DATE RADIATII (GORILLA COMPRESSION)")
    print("="*60)

    try:
        start_time = time.time()
        
        # 1. Incarcare si compresie
        series = load_radiation_data(CSV_PATH)
        
        # 2. Salvare
        bin_file, compressed_size = save_output(series, "chernobyl_radiation")
        
        duration = time.time() - start_time

        # 3. Afisare Rezultate
        stats = series.get_compression_stats()
        csv_size = os.path.getsize(CSV_PATH)

        print(f"\n[STATISTICI]")
        print(f"-> Puncte procesate:   {series.total_points}")
        print(f"-> Timp executie:      {duration*1000:.2f} ms")
        print(f"-> Dimensiune CSV:     {format_bytes(csv_size)}")
        print(f"-> Dimensiune Gorilla: {format_bytes(compressed_size)}")
        print(f"-> Rata vs CSV:        {csv_size / compressed_size:.2f}x")
        print(f"-> Economie spatiu:    {stats['savings_percent']:.1f}%")

        # 4. Verificare date (Query)
        print(f"\n[DEMO QUERY - Primele 5 inregistrari]")
        results = series.query_all()
        for ts, vals in results[:5]:
            d = datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d')
            print(f"   {d} | I-131: {vals['I_131']:<8.4f} | Cs-137: {vals['Cs_137']:.4f}")

    except Exception as e:
        print(f"\n[EROARE] {e}")

if __name__ == "__main__":
    main()