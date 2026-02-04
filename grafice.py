import os
import csv
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.backends.backend_pdf import PdfPages

# Aplicăm un stil vizual modern
plt.style.use('seaborn-v0_8-muted')

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

    # Linie principală și umplere subtilă
    ax.plot(timestamps, values, color='#1f77b4', linewidth=1.5, alpha=0.9, label='CPU Load')
    ax.fill_between(timestamps, values, color='#1f77b4', alpha=0.15)

    # Etichete și titlu
    ax.set_xlabel('Timp (HH:MM)', fontsize=11, labelpad=10)
    ax.set_ylabel('Load Average', fontsize=11, labelpad=10)
    ax.set_title('Serie Temporala: CPU Load Average\n(Sursa: data_cpu.csv)', 
                 fontsize=14, fontweight='bold', pad=15, color='#2c3e50')

    # Formatare axa X
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))
    plt.xticks(rotation=0) # Am pus 0 pentru că se citește mai bine pe orizontală

    # Grid elegant
    ax.grid(True, linestyle='--', alpha=0.4)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    # Statistici și linii de referință
    avg_val = sum(values) / len(values)
    ax.axhline(y=avg_val, color='#e74c3c', linestyle='--', linewidth=1, label=f'Media: {avg_val:.2f}')
    ax.axhline(y=1.0, color='#27ae60', linestyle=':', linewidth=1.2, label='Prag 100%')
    ax.legend(loc='upper right', frameon=True, shadow=False)

    # Info box
    info_text = f'Nr. Puncte: {len(values)}\nMin: {min(values):.2f}\nMax: {max(values):.2f}'
    ax.text(0.02, 0.95, info_text, transform=ax.transAxes, fontsize=10,
            verticalalignment='top', bbox=dict(boxstyle='round,pad=0.5', facecolor='#fcf3cf', alpha=0.5))

    plt.tight_layout()

    with PdfPages(output_path) as pdf:
        pdf.savefig(fig, dpi=150)

    plt.close(fig)
    print(f"    Salvat: {output_path}")


def plot_room_climate_timeseries(data, output_path):
    """Genereaza graficul pentru Room Climate (toate variabilele)."""
    print(f"Generare grafic Room Climate -> {output_path}")

    timestamps = data['timestamps']

    # Cream figura cu stil premium
    fig, axes = plt.subplots(4, 1, figsize=(14, 16), sharex=True)
    fig.suptitle('Analiza Multivariata: Climat si Prezenta Camera\n(room_climate_location_A.csv)',
                 fontsize=16, fontweight='bold', color='#34495e')

    # 1. Temperatura si Umiditate
    ax1 = axes[0]
    ax1.plot(timestamps, data['temp'], color='#e67e22', linewidth=1, label='Temp (°C)')
    ax1.set_ylabel('Temp. (°C)', color='#d35400', fontweight='bold')
    
    ax1_twin = ax1.twinx()
    ax1_twin.plot(timestamps, data['humidity'], color='#2980b9', linewidth=1, label='Umiditate (%)')
    ax1_twin.set_ylabel('Umid. (%)', color='#2980b9', fontweight='bold')
    ax1.set_title('Parametri Termici', fontsize=12, fontweight='bold', loc='left')
    ax1.grid(True, alpha=0.2)

    # 2. Senzori de lumina
    ax2 = axes[1]
    ax2.fill_between(timestamps, data['light1'], color='#f1c40f', alpha=0.2)
    ax2.plot(timestamps, data['light1'], color='#f39c12', linewidth=0.8, label='Senzor 1')
    ax2.plot(timestamps, data['light2'], color='#d4ac0d', linewidth=0.8, label='Senzor 2')
    ax2.set_ylabel('Lux (nm)', fontweight='bold')
    ax2.set_title('Nivel de Iluminare', fontsize=12, fontweight='bold', loc='left')
    ax2.legend(loc='upper right', frameon=False)
    ax2.grid(True, alpha=0.2)

    # 3. Ocupare si Activitate (Folosim 'step' pentru date discrete)
    ax3 = axes[2]
    ax3.step(timestamps, data['occupancy'], color='#27ae60', linewidth=1.5, where='post', label='Persoane')
    ax3.set_ylabel('Persoane', color='#27ae60', fontweight='bold')
    
    ax3_twin = ax3.twinx()
    ax3_twin.step(timestamps, data['activity'], color='#8e44ad', linewidth=1, alpha=0.5, where='post', label='Activitate')
    ax3_twin.set_ylabel('Nivel Act.', color='#8e44ad', fontweight='bold')
    ax3.set_title('Monitorizare Prezenta', fontsize=12, fontweight='bold', loc='left')
    ax3.grid(True, alpha=0.2)

    # 4. Stare usa si fereastra
    ax4 = axes[3]
    ax4.fill_between(timestamps, 0, data['door'], step='post', alpha=0.4, color='#c0392b', label='Usa')
    ax4.fill_between(timestamps, 0, data['window'], step='post', alpha=0.4, color='#16a085', label='Fereastra')
    ax4.set_ylabel('Status (0/1)', fontweight='bold')
    ax4.set_xlabel('Data / Ora', fontsize=11)
    ax4.set_ylim([-0.1, 1.3])
    ax4.set_yticks([0, 1])
    ax4.set_yticklabels(['Inchis', 'Deschis'])
    ax4.set_title('Stare Acces (Usa/Fereastra)', fontsize=12, fontweight='bold', loc='left')
    ax4.legend(loc='upper right', frameon=False)
    ax4.grid(True, alpha=0.1)

    # Formatare axa X
    ax4.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m %H:%M'))
    ax4.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.xticks(rotation=0)

    # Info text in coltul de sus
    info_text = f'Puncte: {len(timestamps)}\nAnaliza: {os.path.basename(output_path)}'
    fig.text(0.01, 0.01, info_text, fontsize=8, alpha=0.7)

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])

    with PdfPages(output_path) as pdf:
        pdf.savefig(fig, dpi=150)

    plt.close(fig)
    print(f"    Salvat: {output_path}")


def main():
    print("=" * 60)
    print("GENERARE GRAFICE PREMIUM")
    print("=" * 60)

    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
        print(f"\nFolder creat: {OUTPUT_FOLDER}")

    # 1. Grafic CPU
    if os.path.exists(CPU_CSV):
        timestamps, values = load_cpu_data(CPU_CSV)
        output_cpu = os.path.join(OUTPUT_FOLDER, "grafic_cpu_load.pdf")
        plot_cpu_timeseries(timestamps, values, output_cpu)
    else:
        print(f"[EROARE] Lipseste: {CPU_CSV}")

    # 2. Grafic Room Climate
    if os.path.exists(ROOM_CLIMATE_CSV):
        data = load_room_climate_data(ROOM_CLIMATE_CSV)
        output_room = os.path.join(OUTPUT_FOLDER, "grafic_room_climate.pdf")
        plot_room_climate_timeseries(data, output_room)
    else:
        print(f"[EROARE] Lipseste: {ROOM_CLIMATE_CSV}")

    print("\n" + "=" * 60)
    print(f"Finalizat. Rezultate in: {OUTPUT_FOLDER}")
    print("=" * 60)


if __name__ == "__main__":
    main()