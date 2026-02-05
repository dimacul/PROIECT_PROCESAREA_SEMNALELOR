# Comparatie intre Gorilla standard si varianta optimizata
# Testeaza pe toate cele 3 seturi de date:
# 1. CPU Load (univariat)
# 2. Room Climate (multivariat - 8 variabile)
# 3. Twitter Volume (univariat)

import os
import time
import csv
from datetime import datetime
from multivariate_storage import MultiVariateSeries, load_room_climate_csv


TIMESERIES_FOLDER = os.path.join(os.path.dirname(__file__), "timseries")
CPU_CSV = os.path.join(TIMESERIES_FOLDER, "data_cpu.csv")
ROOM_CLIMATE_CSV = os.path.join(TIMESERIES_FOLDER, "room_climate_location_A.csv")
TWITTER_CSV = os.path.join(TIMESERIES_FOLDER, "Twitter_volume_UPS.csv")


def format_bytes(size: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"


# Incarca CPU CSV
def load_cpu_data(filepath: str):
    points = []
    with open(filepath, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        for row in reader:
            if not row or len(row) < 2:
                continue
            try:
                dt = datetime.strptime(row[0].strip(), "%Y-%m-%d %H:%M:%S")
                ts_ms = int(dt.timestamp() * 1000)
                cpu_val = float(row[1].strip())
                points.append((ts_ms, {"cpu_load": cpu_val}))
            except (ValueError, IndexError):
                continue
    return points


# Incarca Twitter CSV
def load_twitter_data(filepath: str):
    points = []
    with open(filepath, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                dt = datetime.strptime(row['timestamp'].strip(), "%Y-%m-%d %H:%M:%S")
                ts_ms = int(dt.timestamp() * 1000)
                val = float(row['value'].strip())
                points.append((ts_ms, {"value": val}))
            except (ValueError, KeyError):
                continue
    return points


# Incarca Room Climate si returneaza ca lista de puncte
def load_room_climate_data(filepath: str):
    variable_names = ["temp", "humidity", "light1", "light2",
                     "occupancy", "activity", "door", "window"]
    COL_TIMESTAMP = 1
    points = []

    with open(filepath, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader, None)  # Skip header
        for row in reader:
            if not row or len(row) < 12:
                continue
            try:
                timestamp = int(row[COL_TIMESTAMP].strip())
                values = {
                    variable_names[0]: float(row[4].strip()),
                    variable_names[1]: float(row[5].strip()),
                    variable_names[2]: float(row[6].strip()),
                    variable_names[3]: float(row[7].strip()),
                    variable_names[4]: float(row[8].strip()),
                    variable_names[5]: float(row[9].strip()),
                    variable_names[6]: float(row[10].strip()),
                    variable_names[7]: float(row[11].strip()),
                }
                points.append((timestamp, values))
            except (ValueError, IndexError):
                continue
    return points, variable_names


# Comprima cu metoda standard (add)
def compress_standard(points, variable_names, block_duration_ms=7200000):
    series = MultiVariateSeries(variable_names, block_duration_ms=block_duration_ms)

    t_start = time.time()
    for ts, vals in points:
        series.insert(ts, vals)
    series.flush()
    t_elapsed = time.time() - t_start

    return series, t_elapsed


# Comprima cu metoda optimizata (add_verification)
def compress_optimized(points, variable_names, block_duration_ms=7200000):
    series = MultiVariateSeries(variable_names, block_duration_ms=block_duration_ms)

    t_start = time.time()
    for ts, vals in points:
        # Folosim add_verification in loc de add
        if series._open_block is None:
            series._create_new_block(ts)

        block_start = series._open_block._start_timestamp
        if block_start is None:
            block_start = ts

        if ts >= block_start + series._block_duration:
            series._close_current_block()
            series._create_new_block(ts)

        series._open_block.add_verification(ts, vals)
    series.flush()
    t_elapsed = time.time() - t_start

    return series, t_elapsed


# Testeaza un singur dataset
def test_dataset(name, points, variable_names, csv_path):
    print(f"\n{'='*70}")
    print(f"DATASET: {name}")
    print(f"{'='*70}")

    if not points:
        print(f"[EROARE] Nu s-au gasit date!")
        return None

    print(f"Puncte: {len(points)}")
    print(f"Variabile: {len(variable_names)} ({', '.join(variable_names[:3])}{'...' if len(variable_names) > 3 else ''})")

    # Compresia standard
    print("\n[1] Compresia Gorilla Standard...")
    series_std, time_std = compress_standard(points, variable_names)
    stats_std = series_std.get_compression_stats()

    # Compresia optimizata
    print("[2] Compresia Gorilla Optimizata (verificare fereastra)...")
    series_opt, time_opt = compress_optimized(points, variable_names)
    stats_opt = series_opt.get_compression_stats()

    # Dimensiunea CSV
    csv_size = os.path.getsize(csv_path)

    # Calculeaza diferentele
    diff_bytes = stats_std['compressed_bytes'] - stats_opt['compressed_bytes']
    improvement_percent = (diff_bytes / stats_std['compressed_bytes']) * 100 if stats_std['compressed_bytes'] > 0 else 0

    # Afisare rezultate
    print(f"\n{'METRICA':<35} {'STANDARD':<20} {'OPTIMIZAT':<20}")
    print("-" * 75)
    print(f"{'Dimensiune CSV original':<35} {format_bytes(csv_size):<20}")
    print(f"{'Dimensiune binara naiva':<35} {format_bytes(stats_std['original_bytes']):<20}")
    print(f"{'Dimensiune comprimata':<35} {format_bytes(stats_std['compressed_bytes']):<20} {format_bytes(stats_opt['compressed_bytes']):<20}")
    print(f"{'Rata compresie (vs binar)':<35} {stats_std['compression_ratio']:.2f}x{'':<17} {stats_opt['compression_ratio']:.2f}x")
    print(f"{'Economie spatiu':<35} {stats_std['savings_percent']:.2f}%{'':<16} {stats_opt['savings_percent']:.2f}%")
    print(f"{'Biti per punct':<35} {stats_std['bits_per_point']:.2f}{'':<17} {stats_opt['bits_per_point']:.2f}")
    print(f"{'Timp compresie':<35} {time_std*1000:.2f} ms{'':<14} {time_opt*1000:.2f} ms")

    print("-" * 75)
    if diff_bytes > 0:
        print(f"IMBUNATATIRE: Varianta optimizata economiseste {diff_bytes} bytes ({improvement_percent:.2f}%)")
    elif diff_bytes < 0:
        print(f"NOTA: Varianta standard e mai buna cu {abs(diff_bytes)} bytes (date atipice)")
    else:
        print(f"REZULTAT: Ambele variante produc aceeasi dimensiune")

    return {
        'name': name,
        'points': len(points),
        'variables': len(variable_names),
        'csv_bytes': csv_size,
        'original_bytes': stats_std['original_bytes'],
        'standard_bytes': stats_std['compressed_bytes'],
        'standard_ratio': stats_std['compression_ratio'],
        'standard_savings': stats_std['savings_percent'],
        'standard_bits_per_point': stats_std['bits_per_point'],
        'optimized_bytes': stats_opt['compressed_bytes'],
        'optimized_ratio': stats_opt['compression_ratio'],
        'optimized_savings': stats_opt['savings_percent'],
        'optimized_bits_per_point': stats_opt['bits_per_point'],
        'improvement_bytes': diff_bytes,
        'improvement_percent': improvement_percent,
    }


def print_summary_table(results):
    print("\n" + "#" * 80)
    print("#" + " " * 25 + "TABEL COMPARATIV FINAL" + " " * 31 + "#")
    print("#" * 80)

    # Header
    print(f"\n{'Dataset':<20} {'Puncte':<10} {'Var':<5} {'Standard':<12} {'Optimizat':<12} {'Diferenta':<12} {'Imbunat.':<10}")
    print("-" * 81)

    for r in results:
        if r is None:
            continue
        print(f"{r['name']:<20} {r['points']:<10} {r['variables']:<5} "
              f"{format_bytes(r['standard_bytes']):<12} "
              f"{format_bytes(r['optimized_bytes']):<12} "
              f"{r['improvement_bytes']:<12} "
              f"{r['improvement_percent']:.2f}%")

    # Totaluri
    print("-" * 81)
    total_std = sum(r['standard_bytes'] for r in results if r)
    total_opt = sum(r['optimized_bytes'] for r in results if r)
    total_diff = total_std - total_opt
    total_improvement = (total_diff / total_std) * 100 if total_std > 0 else 0

    print(f"{'TOTAL':<20} {'':<10} {'':<5} "
          f"{format_bytes(total_std):<12} "
          f"{format_bytes(total_opt):<12} "
          f"{total_diff:<12} "
          f"{total_improvement:.2f}%")

    # Tabel pentru rata de compresie
    print(f"\n{'Dataset':<20} {'Rata Std':<12} {'Rata Opt':<12} {'Economie Std':<15} {'Economie Opt':<15}")
    print("-" * 74)

    for r in results:
        if r is None:
            continue
        print(f"{r['name']:<20} {r['standard_ratio']:.2f}x{'':<8} {r['optimized_ratio']:.2f}x{'':<8} "
              f"{r['standard_savings']:.2f}%{'':<10} {r['optimized_savings']:.2f}%")


def main():
    print("\n" + "#" * 80)
    print("#" + " " * 15 + "COMPARATIE GORILLA: STANDARD vs OPTIMIZAT" + " " * 22 + "#")
    print("#" * 80)
    print("\nVarianta optimizata foloseste conditia: M_anterior - M_curent > 11")
    print("pentru a decide cand sa creeze o fereastra noua in loc sa o refoloseasca.\n")

    results = []

    # 1. CPU Load (univariat)
    if os.path.exists(CPU_CSV):
        points = load_cpu_data(CPU_CSV)
        r = test_dataset("CPU Load", points, ["cpu_load"], CPU_CSV)
        results.append(r)
    else:
        print(f"[SKIP] CPU CSV nu exista: {CPU_CSV}")

    # 2. Twitter Volume (univariat)
    if os.path.exists(TWITTER_CSV):
        points = load_twitter_data(TWITTER_CSV)
        r = test_dataset("Twitter Volume", points, ["value"], TWITTER_CSV)
        results.append(r)
    else:
        print(f"[SKIP] Twitter CSV nu exista: {TWITTER_CSV}")

    # 3. Room Climate (multivariat)
    if os.path.exists(ROOM_CLIMATE_CSV):
        variable_names = ["temp", "humidity", "light1", "light2",
                         "occupancy", "activity", "door", "window"]
        points, var_names = load_room_climate_data(ROOM_CLIMATE_CSV)
        r = test_dataset("Room Climate", points, var_names, ROOM_CLIMATE_CSV)
        results.append(r)
    else:
        print(f"[SKIP] Room Climate CSV nu exista: {ROOM_CLIMATE_CSV}")

    # Sumar final
    print_summary_table(results)

    print("\n" + "=" * 80)
    print("Comparatie finalizata!")
    print("=" * 80)


if __name__ == "__main__":
    main()