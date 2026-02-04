"""
Script principal pentru demonstrarea compresiei Gorilla.

Proceseaza:
1. Serie UNIVARIATA: data_cpu.csv (datetime, cpu_load)
2. Serie MULTIVARIATA: room_climate_location_A.csv (8 variabile)

Afiseaza metrici de performanta si efectueaza query-uri pe datele comprimate.
Salveaza fisierele binare comprimate in folderul 'compressed_output'.
"""

import os
import time
import csv
import json
from datetime import datetime
from multivariate_storage import (
    MultiVariateSeries,
    load_room_climate_csv
)

# Cai catre fisierele CSV
TIMESERIES_FOLDER = os.path.join(os.path.dirname(__file__), "timseries")
CPU_CSV = os.path.join(TIMESERIES_FOLDER, "data_cpu.csv")
ROOM_CLIMATE_CSV = os.path.join(TIMESERIES_FOLDER, "room_climate_location_A.csv")

# Folder output pentru fisierele comprimate
COMPRESSED_OUTPUT_FOLDER = os.path.join(os.path.dirname(__file__), "compressed_output")


def format_bytes(size: int) -> str:
    """Formateaza dimensiunea in bytes."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"


def save_compressed_series(series: MultiVariateSeries, output_prefix: str, series_name: str):
    """
    Salveaza seria comprimata in fisiere binare.

    Genereaza:
    - {output_prefix}.bin - datele comprimate (toate blocurile concatenate)
    - {output_prefix}.meta.json - metadata (variabile, numar puncte, blocuri)
    """
    # Creare folder daca nu exista
    output_dir = os.path.dirname(output_prefix)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Salvare date binare
    bin_path = f"{output_prefix}.bin"
    total_bytes = 0

    with open(bin_path, 'wb') as f:
        # Scriem fiecare bloc
        for _, _, data in series._closed_blocks:
            f.write(data)
            total_bytes += len(data)

        # Blocul deschis (daca exista)
        if series._open_block and series._open_block.count > 0:
            data = series._open_block.get_compressed_data()
            f.write(data)
            total_bytes += len(data)

    # Salvare metadata JSON
    meta_path = f"{output_prefix}.meta.json"
    metadata = {
        "series_name": series_name,
        "variable_names": series.variable_names,
        "total_points": series.total_points,
        "num_blocks": series.num_blocks,
        "block_duration_ms": series._block_duration,
        "compressed_bytes": total_bytes,
        "blocks": [
            {"start_timestamp": start, "count": count, "size_bytes": len(data)}
            for start, count, data in series._closed_blocks
        ]
    }

    with open(meta_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2)

    return bin_path, meta_path, total_bytes


def load_cpu_csv(filepath: str) -> MultiVariateSeries:
    """
    Incarca fisierul CPU CSV intr-o serie univariata.

    Format: datetime, cpu_load
    """
    series = MultiVariateSeries(["cpu_load"], block_duration_ms=7200000)  # blocuri de 2 ore

    with open(filepath, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)  # Skip header

        for row in reader:
            if not row or len(row) < 2:
                continue
            try:
                # Convertim datetime string in timestamp milisecunde
                dt = datetime.strptime(row[0].strip(), "%Y-%m-%d %H:%M:%S")
                ts_ms = int(dt.timestamp() * 1000)

                cpu_val = float(row[1].strip())
                series.insert(ts_ms, {"cpu_load": cpu_val})
            except (ValueError, IndexError):
                continue

    return series


def demo_univariate():
    """Demonstreaza compresia pentru seria univariata (CPU)."""
    print("=" * 70)
    print("SERIA UNIVARIATA: CPU Load Average")
    print("=" * 70)

    if not os.path.exists(CPU_CSV):
        print(f"[EROARE] Fisierul nu exista: {CPU_CSV}")
        return None

    # 1. Incarcare si compresie
    print("\n[1] Incarcare si compresie...")
    t_start = time.time()
    series = load_cpu_csv(CPU_CSV)
    series.flush()
    t_load = time.time() - t_start

    print(f"    Timp incarcare: {t_load*1000:.2f} ms")
    print(f"    Puncte: {series.total_points}")
    print(f"    Blocuri: {series.num_blocks}")

    # 2. Salvare fisiere comprimate
    print("\n[2] Salvare fisiere comprimate:")
    output_prefix = os.path.join(COMPRESSED_OUTPUT_FOLDER, "cpu_load")
    bin_path, meta_path, total_bytes = save_compressed_series(series, output_prefix, "CPU Load Average")
    print(f"    Binar: {bin_path}")
    print(f"    Metadata: {meta_path}")

    # 3. Statistici compresie
    print("\n[3] Statistici compresie:")
    stats = series.get_compression_stats()

    original_csv_size = os.path.getsize(CPU_CSV)

    print(f"    Dimensiune CSV original: {format_bytes(original_csv_size)}")
    print(f"    Dimensiune binara naiva: {format_bytes(stats['original_bytes'])}")
    print(f"    Dimensiune Gorilla:      {format_bytes(stats['compressed_bytes'])}")
    print(f"    Rata compresie vs binar: {stats['compression_ratio']:.2f}x")
    print(f"    Rata compresie vs CSV:   {original_csv_size / stats['compressed_bytes']:.2f}x")
    print(f"    Economie spatiu:         {stats['savings_percent']:.1f}%")

    # 4. Query demo
    print("\n[4] Demo Query:")
    all_data = series.query_all()

    if all_data:
        first_ts = all_data[0][0]

        # Query pe primele 10 minute
        query_start = first_ts
        query_end = first_ts + 600000  # 10 minute in ms

        t_query_start = time.time()
        results = series.query(query_start, query_end)
        t_query = time.time() - t_query_start

        print(f"    Query interval: primele 10 minute")
        print(f"    Timp query: {t_query*1000:.3f} ms")
        print(f"    Rezultate: {len(results)} puncte")

        if results:
            print(f"\n    Primele 5 puncte:")
            for i, (ts, vals) in enumerate(results[:5]):
                dt = datetime.fromtimestamp(ts / 1000)
                print(f"      [{i+1}] {dt} -> CPU: {vals['cpu_load']:.2f}")

    return series


def demo_multivariate():
    """Demonstreaza compresia pentru seria multivariata (Room Climate)."""
    print("\n" + "=" * 70)
    print("SERIA MULTIVARIATA: Room Climate (8 variabile)")
    print("=" * 70)

    if not os.path.exists(ROOM_CLIMATE_CSV):
        print(f"[EROARE] Fisierul nu exista: {ROOM_CLIMATE_CSV}")
        return None

    # 1. Incarcare si compresie
    print("\n[1] Incarcare si compresie...")
    t_start = time.time()
    series = load_room_climate_csv(ROOM_CLIMATE_CSV)
    series.flush()
    t_load = time.time() - t_start

    print(f"    Timp incarcare: {t_load*1000:.2f} ms")
    print(f"    Puncte: {series.total_points}")
    print(f"    Variabile: {series.variable_names}")
    print(f"    Blocuri: {series.num_blocks}")

    # 2. Salvare fisiere comprimate
    print("\n[2] Salvare fisiere comprimate:")
    output_prefix = os.path.join(COMPRESSED_OUTPUT_FOLDER, "room_climate")
    bin_path, meta_path, _ = save_compressed_series(series, output_prefix, "Room Climate")
    print(f"    Binar: {bin_path}")
    print(f"    Metadata: {meta_path}")

    # 3. Statistici compresie
    print("\n[3] Statistici compresie:")
    stats = series.get_compression_stats()

    original_csv_size = os.path.getsize(ROOM_CLIMATE_CSV)

    print(f"    Dimensiune CSV original: {format_bytes(original_csv_size)}")
    print(f"    Dimensiune binara naiva: {format_bytes(stats['original_bytes'])}")
    print(f"    Dimensiune Gorilla:      {format_bytes(stats['compressed_bytes'])}")
    print(f"    Rata compresie vs binar: {stats['compression_ratio']:.2f}x")
    print(f"    Rata compresie vs CSV:   {original_csv_size / stats['compressed_bytes']:.2f}x")
    print(f"    Economie spatiu:         {stats['savings_percent']:.1f}%")
    print(f"    Biti per punct:          {stats['bits_per_point']:.2f}")

    # 4. Query demo
    print("\n[4] Demo Query:")
    all_data = series.query_all()

    if all_data:
        first_ts = all_data[0][0]
        last_ts = all_data[-1][0]
        duration_hours = (last_ts - first_ts) / 1000 / 3600

        print(f"    Durata totala date: {duration_hours:.1f} ore")

        # Query pe prima ora
        query_start = first_ts
        query_end = first_ts + 3600000  # 1 ora in ms

        t_query_start = time.time()
        results = series.query(query_start, query_end)
        t_query = time.time() - t_query_start

        print(f"    Query interval: prima ora")
        print(f"    Timp query: {t_query*1000:.3f} ms")
        print(f"    Rezultate: {len(results)} puncte")

        if results:
            print(f"\n    Primele 3 puncte (toate variabilele):")
            for i, (ts, vals) in enumerate(results[:3]):
                dt = datetime.fromtimestamp(ts / 1000)
                print(f"      [{i+1}] {dt}")
                print(f"          Temp: {vals['temp']:.2f}C, Humidity: {vals['humidity']:.2f}%")
                print(f"          Light1: {vals['light1']:.1f}, Light2: {vals['light2']:.1f}")
                print(f"          Occupancy: {int(vals['occupancy'])}, Activity: {int(vals['activity'])}")

    return series


def ensure_output_folder():
    """Creeaza folderul pentru output daca nu exista."""
    if not os.path.exists(COMPRESSED_OUTPUT_FOLDER):
        os.makedirs(COMPRESSED_OUTPUT_FOLDER)
        print(f"Folder creat: {COMPRESSED_OUTPUT_FOLDER}")


def comparatie_finala(cpu_stats, room_stats):
    """Afiseaza comparatia finala intre cele doua serii."""
    print("\n" + "=" * 70)
    print("COMPARATIE FINALA")
    print("=" * 70)

    print(f"\n{'Metrica':<30} {'CPU (univar.)':<20} {'Room Climate (multivar.)':<25}")
    print("-" * 75)

    if cpu_stats:
        cpu = cpu_stats
    else:
        cpu = {"total_points": "N/A", "compression_ratio": "N/A", "savings_percent": "N/A"}

    if room_stats:
        room = room_stats
    else:
        room = {"total_points": "N/A", "compression_ratio": "N/A", "savings_percent": "N/A"}

    print(f"{'Puncte':<30} {cpu.get('total_points', 'N/A'):<20} {room.get('total_points', 'N/A'):<25}")
    print(f"{'Variabile/punct':<30} {1:<20} {room.get('num_variables', 'N/A'):<25}")
    print(f"{'Rata compresie':<30} {cpu.get('compression_ratio', 0):<20.2f}x {room.get('compression_ratio', 0):<25.2f}x")
    print(f"{'Economie spatiu':<30} {cpu.get('savings_percent', 0):<20.1f}% {room.get('savings_percent', 0):<25.1f}%")


def main():
    print("\n" + "#" * 70)
    print("#" + " " * 20 + "GORILLA COMPRESSION DEMO" + " " * 24 + "#")
    print("#" * 70)

    # Creare folder output
    ensure_output_folder()

    # Demo univariate
    cpu_series = demo_univariate()
    cpu_stats = cpu_series.get_compression_stats() if cpu_series else None

    # Demo multivariate
    room_series = demo_multivariate()
    room_stats = room_series.get_compression_stats() if room_series else None

    # Comparatie
    comparatie_finala(cpu_stats, room_stats)

    print("\n" + "=" * 70)
    print("Demo finalizat cu succes!")
    print("=" * 70)


if __name__ == "__main__":
    main()