from datetime import datetime
import csv
import os
from storage_minimal import GorillaStore

def proceseaza_si_salveaza_gorilla(input_path: str, bin_output_path: str, csv_output_path: str):
    store = GorillaStore()
    nume_senzor = "cpu_monitor"
    
    print(f"--- Pas 1: Compresie date din {input_path} ---")
    
    with open(input_path, mode='r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)  # Sare peste rândul cu "datetime","cpu"
        print(f"Am detectat coloanele: {header}")

        count = 0
        for row in reader:
            if not row or len(row) < 2:
                continue
            
            try:
                # 1. Convertim textul "2017-01-27 18:42:00" în obiect datetime
                dt = datetime.strptime(row[0].strip(), "%Y-%m-%d %H:%M:%S")
                
                # 2. Convertim în Unix Timestamp (număr întreg de secunde)
                ts = int(dt.timestamp())
                
                # 3. Luăm valoarea CPU
                val = float(row[1].strip())
                
                # 4. Inserăm în Gorilla
                store.insert(nume_senzor, ts, val)
                count += 1
            except ValueError as e:
                print(f"Eroare la conversia rândului {row}: {e}")
                continue

    print(f"Compresie finalizată pentru {count} puncte.")

    # --- Pasul 2: Salvare binară ---
    serie = store.series_map[nume_senzor]
    with open(bin_output_path, "wb") as f_bin:
        for block in serie.closed_blocks:
            f_bin.write(block.compressed_data)
        if serie.open_block:
            f_bin.write(serie.open_block.writer.to_bytes())

    # --- Pasul 3: Salvare CSV pentru verificare ---
    # Notă: Aici Gorilla îți va returna timestamp-ul ca număr (ex: 1485535320)
    rezultate = store.query(nume_senzor, 0, 2147483647)
    with open(csv_output_path, mode='w', newline='') as f_csv:
        writer = csv.writer(f_csv)
        writer.writerow(["timestamp_unix", "cpu_value"])
        writer.writerows(rezultate)

    # Statistici
    dim_in = os.path.getsize(input_path)
    dim_bin = os.path.getsize(bin_output_path)
    print(f"\n--- Statistici ---")
    print(f"Original: {dim_in} bytes")
    print(f"Binar (Gorilla): {dim_bin} bytes")
    print(f"Eficiență: {100 - (dim_bin/dim_in*100):.2f}% reducere spațiu.")

if __name__ == "__main__":
    # Asigură-te că fișierul data_cpu.csv este în același folder cu scriptul
    proceseaza_si_salveaza_gorilla("data_cpu.csv", "cpu_compressed.bin", "cpu_decompressed.csv")