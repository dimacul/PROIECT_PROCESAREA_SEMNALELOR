"""
Script pentru vizualizarea seriilor temporale.

Genereaza grafice PDF pentru:
1. Serie univariata: CPU Load Average (data_cpu.csv)
2. Serie multivariata: Room Climate (room_climate_location_A.csv) - toate variabilele
"""

import os
import csv
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.backends.backend_pdf import PdfPages

# Cai catre fisiere
TIMESERIES_FOLDER = os.path.join(os.path.dirname(__file__), "timseries")
CPU_CSV = os.path.join(TIMESERIES_FOLDER, "data_cpu.csv")
ROOM_CLIMATE_CSV = os.path.join(TIMESERIES_FOLDER, "room_climate_location_A.csv")

# Folder output pentru PDF-uri
OUTPUT_FOLDER = os.path.join(os.path.dirname(__file__), "grafice_output")


def load_cpu_data(filepath: str):
    """Incarca datele CPU din CSV."""
    timestamps = []
    values = []

    with open(filepath, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header

        for row in reader:
            if len(row) >= 2:
                try:
                    dt = datetime.strptime(row[0].strip(), "%Y-%m-%d %H:%M:%S")
                    val = float(row[1].strip())
                    timestamps.append(dt)
                    values.append(val)
                except ValueError:
                    continue

    return timestamps, values


def load_room_climate_data(filepath: str):
    """Incarca datele Room Climate din CSV."""
    data = {
        'timestamps': [],
        'temp': [],
        'humidity': [],
        'light1': [],
        'light2': [],
        'occupancy': [],
        'activity': [],
        'door': [],
        'window': []
    }

    with open(filepath, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header

        for row in reader:
            if len(row) >= 12:
                try:
                    ts_ms = int(row[1].strip())
                    dt = datetime.fromtimestamp(ts_ms / 1000)

                    data['timestamps'].append(dt)
                    data['temp'].append(float(row[4].strip()))
                    data['humidity'].append(float(row[5].strip()))
                    data['light1'].append(float(row[6].strip()))
                    data['light2'].append(float(row[7].strip()))
                    data['occupancy'].append(int(float(row[8].strip())))
                    data['activity'].append(int(float(row[9].strip())))
                    data['door'].append(int(float(row[10].strip())))
                    data['window'].append(int(float(row[11].strip())))
                except (ValueError, IndexError):
                    continue

    return data


def plot_cpu_timeseries(timestamps, values, output_path):
    """Genereaza graficul pentru CPU Load Average."""
    print(f"Generare grafic CPU -> {output_path}")

    fig, ax = plt.subplots(figsize=(12, 6))

    ax.plot(timestamps, values, 'b-', linewidth=0.8, alpha=0.8)
    ax.fill_between(timestamps, values, alpha=0.3)

    ax.set_xlabel('Timp', fontsize=12)
    ax.set_ylabel('CPU Load Average', fontsize=12)
    ax.set_title('Serie Temporala: CPU Load Average\n(data_cpu.csv)', fontsize=14, fontweight='bold')

    # Formatare axa X
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))
    plt.xticks(rotation=45)

    # Grid
    ax.grid(True, alpha=0.3)
    ax.set_axisbelow(True)

    # Statistici
    avg_val = sum(values) / len(values)
    ax.axhline(y=avg_val, color='r', linestyle='--', alpha=0.7, label=f'Media: {avg_val:.2f}')
    ax.axhline(y=1.0, color='g', linestyle=':', alpha=0.7, label='Prag utilizare 100%')
    ax.legend(loc='upper right')

    # Info text
    info_text = f'Puncte: {len(values)}\nMin: {min(values):.2f}\nMax: {max(values):.2f}'
    ax.text(0.02, 0.98, info_text, transform=ax.transAxes, fontsize=9,
            verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

    plt.tight_layout()

    # Salvare PDF
    with PdfPages(output_path) as pdf:
        pdf.savefig(fig, dpi=150)

    plt.close(fig)
    print(f"    Salvat: {output_path}")


def plot_room_climate_timeseries(data, output_path):
    """Genereaza graficul pentru Room Climate (toate variabilele)."""
    print(f"Generare grafic Room Climate -> {output_path}")

    timestamps = data['timestamps']

    # Cream o figura cu 4 subploturi
    fig, axes = plt.subplots(4, 1, figsize=(14, 16), sharex=True)
    fig.suptitle('Serie Temporala Multivariata: Room Climate\n(room_climate_location_A.csv)',
                 fontsize=14, fontweight='bold')

    # Subplot 1: Temperatura si Umiditate
    ax1 = axes[0]
    color1 = 'tab:red'
    ax1.set_ylabel('Temperatura (C)', color=color1, fontsize=10)
    line1, = ax1.plot(timestamps, data['temp'], color=color1, linewidth=0.5, alpha=0.8, label='Temperatura')
    ax1.tick_params(axis='y', labelcolor=color1)
    ax1.set_ylim([min(data['temp']) - 1, max(data['temp']) + 1])

    ax1_twin = ax1.twinx()
    color2 = 'tab:blue'
    ax1_twin.set_ylabel('Umiditate (%)', color=color2, fontsize=10)
    line2, = ax1_twin.plot(timestamps, data['humidity'], color=color2, linewidth=0.5, alpha=0.8, label='Umiditate')
    ax1_twin.tick_params(axis='y', labelcolor=color2)

    ax1.legend([line1, line2], ['Temperatura', 'Umiditate'], loc='upper right')
    ax1.set_title('Temperatura si Umiditate', fontsize=11)
    ax1.grid(True, alpha=0.3)

    # Subplot 2: Senzori de lumina
    ax2 = axes[1]
    ax2.plot(timestamps, data['light1'], 'orange', linewidth=0.5, alpha=0.8, label='Light Sensor 1')
    ax2.plot(timestamps, data['light2'], 'gold', linewidth=0.5, alpha=0.8, label='Light Sensor 2')
    ax2.set_ylabel('Lumina (nm)', fontsize=10)
    ax2.set_title('Senzori de Lumina', fontsize=11)
    ax2.legend(loc='upper right')
    ax2.grid(True, alpha=0.3)

    # Subplot 3: Ocupare si Activitate
    ax3 = axes[2]
    ax3.step(timestamps, data['occupancy'], 'green', linewidth=1, alpha=0.8, where='post', label='Ocupare (persoane)')
    ax3.set_ylabel('Ocupare', color='green', fontsize=10)
    ax3.tick_params(axis='y', labelcolor='green')
    ax3.set_ylim([-0.5, max(data['occupancy']) + 0.5])

    ax3_twin = ax3.twinx()
    ax3_twin.step(timestamps, data['activity'], 'purple', linewidth=1, alpha=0.8, where='post', label='Activitate')
    ax3_twin.set_ylabel('Activitate', color='purple', fontsize=10)
    ax3_twin.tick_params(axis='y', labelcolor='purple')
    ax3_twin.set_ylim([-0.5, max(max(data['activity']), 1) + 0.5])

    ax3.set_title('Ocupare si Activitate (0=n/a, 1=read, 2=stand, 3=walk, 4=work)', fontsize=11)
    ax3.grid(True, alpha=0.3)

    # Subplot 4: Stare usa si fereastra
    ax4 = axes[3]
    ax4.fill_between(timestamps, data['door'], step='post', alpha=0.5, color='brown', label='Usa (1=deschis)')
    ax4.fill_between(timestamps, data['window'], step='post', alpha=0.5, color='cyan', label='Fereastra (1=deschis)')
    ax4.set_ylabel('Stare (0/1)', fontsize=10)
    ax4.set_xlabel('Timp', fontsize=10)
    ax4.set_title('Stare Usa si Fereastra', fontsize=11)
    ax4.legend(loc='upper right')
    ax4.set_ylim([-0.1, 1.5])
    ax4.grid(True, alpha=0.3)

    # Formatare axa X
    ax4.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    ax4.xaxis.set_major_locator(mdates.DayLocator(interval=2))
    plt.xticks(rotation=45)

    # Info text
    info_text = f'Puncte: {len(timestamps)}\nDurata: {(timestamps[-1] - timestamps[0]).days} zile'
    fig.text(0.02, 0.98, info_text, fontsize=9,
             verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

    plt.tight_layout()
    plt.subplots_adjust(top=0.93)

    # Salvare PDF
    with PdfPages(output_path) as pdf:
        pdf.savefig(fig, dpi=150)

    plt.close(fig)
    print(f"    Salvat: {output_path}")


def main():
    print("=" * 60)
    print("GENERARE GRAFICE PENTRU SERIILE TEMPORALE")
    print("=" * 60)

    # Creare folder output daca nu exista
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
        print(f"\nFolder creat: {OUTPUT_FOLDER}")

    # 1. Grafic CPU
    print("\n[1] Serie univariata: CPU Load Average")
    if os.path.exists(CPU_CSV):
        timestamps, values = load_cpu_data(CPU_CSV)
        print(f"    Incarcat: {len(values)} puncte")
        output_cpu = os.path.join(OUTPUT_FOLDER, "grafic_cpu_load.pdf")
        plot_cpu_timeseries(timestamps, values, output_cpu)
    else:
        print(f"    [EROARE] Fisier inexistent: {CPU_CSV}")

    # 2. Grafic Room Climate
    print("\n[2] Serie multivariata: Room Climate")
    if os.path.exists(ROOM_CLIMATE_CSV):
        data = load_room_climate_data(ROOM_CLIMATE_CSV)
        print(f"    Incarcat: {len(data['timestamps'])} puncte, 8 variabile")
        output_room = os.path.join(OUTPUT_FOLDER, "grafic_room_climate.pdf")
        plot_room_climate_timeseries(data, output_room)
    else:
        print(f"    [EROARE] Fisier inexistent: {ROOM_CLIMATE_CSV}")

    print("\n" + "=" * 60)
    print(f"Grafice salvate in: {OUTPUT_FOLDER}")
    print("=" * 60)


if __name__ == "__main__":
    main()