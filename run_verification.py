import time
import csv
import os
import json
import struct
from datetime import datetime
from multivariate_storage import MultiVariateSeries, MultiVariateDecoder

def load_points_from_csv(file_path):
    """Citește datele din CSV și le pregătește pentru procesare."""
    points = []
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Fișierul {file_path} nu a fost găsit!")

    with open(file_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                dt = datetime.strptime(row['timestamp'].strip(), "%Y-%m-%d %H:%M:%S")
                ts = int(dt.timestamp() * 1000)
                val = float(row['value'].strip())
                points.append((ts, {"value": val}))
            except (ValueError, KeyError):
                continue
    return points

def save_compressed_data(series, filename_prefix):
    """Salvează datele binare (cu header de bloc) și metadatele JSON."""
    bin_filename = f"{filename_prefix}.bin"
    total_bytes = 0
    
    # 1. Salvarea datelor binare
    with open(bin_filename, 'wb') as f:
        # Blocuri închise
        for _, count, data in series._closed_blocks:
            # Header obligatoriu pentru ca query_from_files să știe cum să fragmenteze
            f.write(struct.pack("II", count, len(data)))
            f.write(data)
            total_bytes += len(data) + 8
        
        # Bloc deschis
        if series._open_block and series._open_block.count > 0:
            data = series._open_block.get_compressed_data()
            f.write(struct.pack("II", series._open_block.count, len(data)))
            f.write(data)
            total_bytes += len(data) + 8
            
    # 2. Salvarea METADATELOR (Aceasta lipsea din scriptul tău!)
    meta_filename = f"{filename_prefix}_meta.json"
    metadata = {
        "series_name": filename_prefix,
        "variable_names": series.variable_names,
        "total_points": series.total_points,
        "compressed_bytes": total_bytes,
        "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    with open(meta_filename, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=4)
        
    return bin_filename, total_bytes

def run_test_and_save(points, method_name, file_prefix):
    """Execută compresia și salvează rezultatul."""
    series = MultiVariateSeries(["value"])
    
    start_time = time.perf_counter()
    for ts, vals in points:
        if method_name == "add_value":
            series.insert(ts, vals)
        else:
            if series._open_block is None:
                series._create_new_block(ts)
            series._open_block.add_verification(ts, vals)
    series.flush()
    end_time = time.perf_counter()
    
    bin_file, bytes_size = save_compressed_data(series, file_prefix)
    execution_time = (end_time - start_time) * 1000
    
    return execution_time, series.get_compression_stats(), bin_file

def query_from_files(filename_prefix, start_date_str, end_date_str):
    """
    Interoghează datele salvate pe disc într-un interval de timp.
    """
    meta_filename = f"{filename_prefix}_meta.json"
    bin_filename = f"{filename_prefix}.bin"

    if not os.path.exists(meta_filename) or not os.path.exists(bin_filename):
        return "Eroare: Fișierele binare sau meta nu au fost găsite!"

    # --- PASUL 1: DEFINIREA LUI 'meta' ---
    # Citim metadatele din JSON pentru a ști numele variabilelor
    with open(meta_filename, 'r') as f:
        meta = json.load(f)
    
    # --- PASUL 2: DEFINIREA LUI 't_start' și 't_end' ---
    # Convertim string-urile de tip "2015-02-26 21:42:53" în milisecunde (int)
    t_start = int(datetime.strptime(start_date_str, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
    t_end = int(datetime.strptime(end_date_str, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)

    results = []

    # --- PASUL 3: CITIREA FIȘIERULUI BINAR ---
    with open(bin_filename, 'rb') as f:
        while True:
            # Citim header-ul de 8 octeți scris de save_compressed_data (II = 2x Unsigned Int)
            header = f.read(8)
            if not header or len(header) < 8: 
                break
            
            count, length = struct.unpack("II", header)
            block_data = f.read(length)
            
            # Recreăm decoderul pentru fiecare bloc (resetăm contextul XOR)
            decoder = MultiVariateDecoder(block_data, meta['variable_names'])
            
            try:
                for _ in range(count):
                    ts, values = decoder.read_point()
                    # Acum codul știe cine sunt t_start și t_end
                    if t_start <= ts <= t_end:
                        results.append((ts, values))
            except Exception as e:
                # Dacă un bloc e corupt sau s-a terminat brusc, trecem peste
                continue 
                
    return results

# --- FUNCTIE DE AFISARE ---
def print_results(results):
    if not results:
        print("Nu s-au găsit date în intervalul specificat.")
        return
    
    print(f"\n--- Rezultate Query ({len(results)} puncte) ---")
    for ts, val in results[:5]: # Afișăm primele 5
        dt = datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{dt}] -> {val}")
    if len(results) > 5:
        print("...")

csv_file = "timseries\Twitter_volume_UPS.csv"

if os.path.exists(csv_file):
    print(f"--- Începere procesare {csv_file} ---")
    
    # PASUL 1: Definim/Extragem punctele
    points = load_points_from_csv(csv_file)
    print(f"Puncte încarcate: {len(points)}")

    # PASUL 2: Rulăm testele
    t_std, s_std, f_std = run_test_and_save(points, "add_value", "rezultat_standard")
    t_ver, s_ver, f_ver = run_test_and_save(points, "add_verification", "rezultat_verificare")

    # PASUL 3: Afișăm rezultatele
    print("\n" + "="*60)
    print(f"{'Metoda':<25} | {'Timp (ms)':<12} | {'Marime Bin (Bytes)':<15}")
    print("-" * 60)
    print(f"{'Standard  ':<25} | {t_std:<12.4f} | {s_std['compressed_bytes']:<15}")
    print(f"{'Verificare':<25} | {t_ver:<12.4f} | {s_ver['compressed_bytes']:<15}")
    
    print(f"Fisier binar: {f_std}")
    print(f"Economie spatiu: {s_std['savings_percent']:.2f}%")
    print("="*60)
else:
    print(f"Eroare: Nu am gasit fisierul {csv_file}")

# --- CONFIGURARE RULARE ---
prefix = "rezultat_standard"  # Numele fișierelor (fără extensie)

# Intervalul pe care vrei să îl vezi (exemplu pentru Twitter_volume_UPS.csv)
inceput = "2015-02-27 10:32:53"
sfarsit = "2015-02-27 10:32:53"

print(f"Interogare fișier: {prefix}.bin")
print(f"Interval: {inceput} -> {sfarsit}\n")

rezultate = query_from_files(prefix, inceput, sfarsit)

if isinstance(rezultate, list):
    print(f"S-au găsit {len(rezultate)} puncte în acest interval.")
    print("-" * 50)
    
    # Afișăm primele 5 rezultate pentru verificare
    for ts, values in rezultate[:5]:
        dt_obj = datetime.fromtimestamp(ts / 1000)
        print(f"[{dt_obj}] Data: {values}")
    
    if len(rezultate) > 5:
        print("...")
else:
    print(rezultate) # Afișează eroarea

