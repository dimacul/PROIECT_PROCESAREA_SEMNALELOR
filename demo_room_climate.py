"""
Demo: Incarcare si compresie Room Climate Dataset cu Gorilla.

Acest script demonstreaza:
1. Incarcarea datelor dintr-un CSV Room Climate
2. Compresia eficienta cu algoritmul Gorilla multivariate
3. Statistici despre compresie
4. Query pe interval de timp
"""

import os
import time
from multivariate_storage import load_room_climate_csv, MultiVariateSeries

# Calea catre fisierul CSV
CSV_PATH = os.path.join(os.path.dirname(__file__), "room_climate_location_A.csv")


def format_bytes(size: int) -> str:
    """Formateaza dimensiunea in bytes intr-un format usor de citit."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"


def main():
    print("=" * 70)
    print("DEMO: Compresie Room Climate Dataset cu Gorilla Multivariate")
    print("=" * 70)

    # Verificam ca fisierul exista
    if not os.path.exists(CSV_PATH):
        print(f"\n[EROARE] Fisierul nu a fost gasit: {CSV_PATH}")
        print("Descarca mai intai fisierul room_climate_location_A.csv")
        return

    # 1. Incarcam datele
    print(f"\n[1] Incarcare date din: {os.path.basename(CSV_PATH)}")
    print("-" * 50)

    t_start = time.time()
    series = load_room_climate_csv(CSV_PATH)
    series.flush()  # Inchidem toate blocurile
    t_load = time.time() - t_start

    print(f"    Timp incarcare: {t_load*1000:.2f} ms")
    print(f"    Puncte incarcate: {series.total_points}")
    print(f"    Variabile: {series.variable_names}")
    print(f"    Blocuri create: {series.num_blocks}")

    # 2. Statistici compresie
    print(f"\n[2] Statistici compresie")
    print("-" * 50)

    stats = series.get_compression_stats()

    print(f"    Total puncte:     {stats['total_points']:,}")
    print(f"    Variabile/punct:  {stats['num_variables']}")
    print(f"    Blocuri:          {stats['num_blocks']}")
    print()
    print(f"    Dimensiune originala:   {format_bytes(stats['original_bytes']):>12}")
    print(f"    Dimensiune comprimata:  {format_bytes(stats['compressed_bytes']):>12}")
    print()
    print(f"    Rata de compresie:      {stats['compression_ratio']:.2f}x")
    print(f"    Economie spatiu:        {stats['savings_percent']:.1f}%")
    print(f"    Biti per punct:         {stats['bits_per_point']:.2f}")

    # 3. Query demo
    print(f"\n[3] Demo Query")
    print("-" * 50)

    # Luam toate datele pentru a vedea intervalul
    all_data = series.query_all()

    if all_data:
        first_ts = all_data[0][0]
        last_ts = all_data[-1][0]
        duration_sec = (last_ts - first_ts) / 1000

        print(f"    Interval total: {first_ts} -> {last_ts}")
        print(f"    Durata: {duration_sec:.1f} secunde ({duration_sec/60:.1f} minute)")

        # Query pe primele 10 secunde
        query_start = first_ts
        query_end = first_ts + 10000  # 10 secunde

        t_query_start = time.time()
        results = series.query(query_start, query_end)
        t_query = time.time() - t_query_start

        print(f"\n    Query interval: [{query_start}, {query_end}]")
        print(f"    Timp query: {t_query*1000:.3f} ms")
        print(f"    Rezultate: {len(results)} puncte")

        # Afisam primele 5 puncte
        print(f"\n    Primele 5 puncte din query:")
        for i, (ts, values) in enumerate(results[:5]):
            temp = values['temp']
            hum = values['humidity']
            print(f"      [{i+1}] ts={ts}, temp={temp:.2f}C, humidity={hum:.2f}%")

    # 4. Comparatie cu stocarea naiva
    print(f"\n[4] Comparatie cu alte metode de stocare")
    print("-" * 50)

    num_points = stats['total_points']
    num_vars = stats['num_variables']

    # Stocare naiva: timestamp (8B) + valori (8B * num_vars)
    naive_size = num_points * (8 + 8 * num_vars)

    # Gorilla: dimensiunea comprimata
    gorilla_size = stats['compressed_bytes']

    # CSV text (aproximativ): ~15 chars per valoare
    csv_approx = num_points * (15 + 15 * num_vars)

    print(f"    CSV text (aprox):      {format_bytes(csv_approx):>12}")
    print(f"    Binar naiv:            {format_bytes(naive_size):>12}")
    print(f"    Gorilla multivariate:  {format_bytes(gorilla_size):>12}")
    print()
    print(f"    Gorilla vs CSV:    {csv_approx / gorilla_size:.1f}x mai mic")
    print(f"    Gorilla vs Binar:  {naive_size / gorilla_size:.1f}x mai mic")

    print("\n" + "=" * 70)
    print("Demo finalizat cu succes!")
    print("=" * 70)


if __name__ == "__main__":
    main()