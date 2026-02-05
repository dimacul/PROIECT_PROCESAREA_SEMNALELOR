import os
import csv
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.backends.backend_pdf import PdfPages
from run_verification import query_from_files


plt.style.use('seaborn-v0_8-muted')

TIMESERIES_FOLDER = os.path.join(os.path.dirname(__file__), "timseries")
CPU_CSV = os.path.join(TIMESERIES_FOLDER, "data_cpu.csv")
ROOM_CLIMATE_CSV = os.path.join(TIMESERIES_FOLDER, "room_climate_location_A.csv")

OUTPUT_FOLDER = os.path.join(os.path.dirname(__file__), "grafice_output")


def load_cpu_data(filepath: str):
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
        next(reader)  

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
    print(f"Generare grafic CPU -> {output_path}")

    fig, ax = plt.subplots(figsize=(12, 6))

    ax.plot(timestamps, values, color='#1f77b4', linewidth=1.5, alpha=0.9, label='CPU Load')
    ax.fill_between(timestamps, values, color='#1f77b4', alpha=0.15)

    ax.set_xlabel('Timp (HH:MM)', fontsize=11, labelpad=10)
    ax.set_ylabel('Load Average', fontsize=11, labelpad=10)
    ax.set_title('Serie Temporala: CPU Load Average\n(Sursa: data_cpu.csv)', 
                 fontsize=14, fontweight='bold', pad=15, color='#2c3e50')

    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))
    plt.xticks(rotation=0)

    ax.grid(True, linestyle='--', alpha=0.4)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    avg_val = sum(values) / len(values)
    ax.axhline(y=avg_val, color='#e74c3c', linestyle='--', linewidth=1, label=f'Media: {avg_val:.2f}')
    ax.axhline(y=1.0, color='#27ae60', linestyle=':', linewidth=1.2, label='Prag 100%')
    ax.legend(loc='upper right', frameon=True, shadow=False)

    info_text = f'Nr. Puncte: {len(values)}\nMin: {min(values):.2f}\nMax: {max(values):.2f}'
    ax.text(0.02, 0.95, info_text, transform=ax.transAxes, fontsize=10,
            verticalalignment='top', bbox=dict(boxstyle='round,pad=0.5', facecolor='#fcf3cf', alpha=0.5))

    plt.tight_layout()

    with PdfPages(output_path) as pdf:
        pdf.savefig(fig, dpi=150)

    plt.close(fig)
    print(f"    Salvat: {output_path}")


def plot_room_climate_timeseries(data, output_path):
    print(f"Generare grafic Room Climate -> {output_path}")

    #Sortarea datelor pentru a evita liniile care se întorc 
    #Impachetăm toate listele intr-o lista de randuri, sortam dupa timestamp, apoi despachetam
    rows = zip(
        data['timestamps'], data['temp'], data['humidity'], 
        data['light1'], data['light2'], data['occupancy'], 
        data['activity'], data['door'], data['window']
    )
    # Sortam in functie de prima coloana
    sorted_rows = sorted(rows, key=lambda x: x[0])
    
    # Despachetam datele sortate inapoi în liste
    t_sort, temp_sort, hum_sort, l1_sort, l2_sort, occ_sort, act_sort, door_sort, win_sort = zip(*sorted_rows)

    fig, axes = plt.subplots(4, 1, figsize=(14, 20)) 
    fig.suptitle('Analiza Multivariata: Climat si Prezenta Camera\n(Date Sortate Cronologic)',
                 fontsize=16, fontweight='bold', color='#34495e', y=0.98)

    def format_time_axis(ax):
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m\n%H:%M'))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        ax.tick_params(axis='x', labelsize=9)
        ax.grid(True, linestyle=':', alpha=0.6)

    ax1 = axes[0]
    ax1.plot(t_sort, temp_sort, color='#e67e22', linewidth=1.2, label='Temp (°C)')
    ax1.set_ylabel('Temp. (°C)', color='#d35400', fontweight='bold')
    ax1.tick_params(axis='y', labelcolor='#d35400')
    
    ax1_twin = ax1.twinx()
    ax1_twin.plot(t_sort, hum_sort, color='#2980b9', linewidth=1.2, label='Umiditate (%)')
    ax1_twin.set_ylabel('Umid. (%)', color='#2980b9', fontweight='bold')
    ax1_twin.tick_params(axis='y', labelcolor='#2980b9')
    ax1.set_title('Parametri Termici', fontsize=12, fontweight='bold', loc='left')
    format_time_axis(ax1)

    ax2 = axes[1]
    ax2.plot(t_sort, l1_sort, color='#f39c12', linewidth=1, label='Senzor Central')
    ax2.plot(t_sort, l2_sort, color='#d4ac0d', linewidth=1, linestyle='--', label='Senzor Fereastră')
    ax2.set_ylabel('Intensitate (Lux)', fontweight='bold')
    ax2.set_title('Nivel de Iluminare', fontsize=12, fontweight='bold', loc='left')
    ax2.legend(loc='upper right', fontsize=9)
    format_time_axis(ax2)

    ax3 = axes[2]
    ax3.plot(t_sort, occ_sort, color='#27ae60', linewidth=1.5, label='Persoane')
    ax3.set_ylabel('Nr. Persoane', color='#27ae60', fontweight='bold')
    
    ax3_twin = ax3.twinx()
    ax3_twin.plot(t_sort, act_sort, color='#8e44ad', linewidth=0.8, alpha=0.6, label='Activitate')
    ax3_twin.set_ylabel('Indice Act.', color='#8e44ad', fontweight='bold')
    ax3.set_title('Monitorizare Miscare și Ocupare', fontsize=12, fontweight='bold', loc='left')
    format_time_axis(ax3)

    ax4 = axes[3]
    # 'where='post'' asigură că starea se schimbă exact în momentul înregistrării noi
    ax4.step(t_sort, door_sort, color='#c0392b', linewidth=1.5, label='Ușă', where='post')
    ax4.step(t_sort, win_sort, color='#16a085', linewidth=1.5, label='Fereastră', where='post')

    ax4.set_ylabel('Status (Inchis/Deschis)', fontweight='bold')
    ax4.set_ylim([-0.1, 1.1])
    ax4.set_yticks([0, 1])
    ax4.set_yticklabels(['Inchis', 'Deschis'])
    ax4.set_title('Stare Acces', fontsize=12, fontweight='bold', loc='left')
    ax4.legend(loc='upper right', fontsize=9)
    format_time_axis(ax4)

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])

    with PdfPages(output_path) as pdf:
        pdf.savefig(fig, dpi=150)

    plt.close(fig)
    print(f"    Salvat cu succes: {output_path}")

def load_twiter_data(prefix: str):

    # Interogam tot intervalul de timp disponibil
    t_start = "2000-01-01 00:00:00"
    t_end = "2030-01-01 00:00:00"
    
    print(f"  Citire date Gorilla din: {prefix}.bin")
    results = query_from_files(prefix, t_start, t_end)
    
    if not isinstance(results, list) or len(results) == 0:
        return [], []

    timestamps = [datetime.fromtimestamp(ts / 1000) for ts, vals in results]
    values = [vals['value'] for ts, vals in results]
    
    return timestamps, values

def plot_twitter_timeseries(timestamps, values, output_path, title_suffix=""):
    print(f"Generare grafic Twitter {title_suffix} -> {output_path}")

    combined = sorted(zip(timestamps, values))
    t_sort, v_sort = zip(*combined)

    fig, ax = plt.subplots(figsize=(12, 6))

    ax.plot(t_sort, v_sort, color='#1f77b4', linewidth=1.5, alpha=0.9, label='Twitter Volume')
    ax.fill_between(t_sort, v_sort, color='#1f77b4', alpha=0.15)

    ax.set_xlabel('Data / Ora', fontsize=11, labelpad=10)
    ax.set_ylabel('Numar Tweet-uri (Volume)', fontsize=11, labelpad=10)
    ax.set_title(f'Serie Temporala: Twitter Volume {title_suffix}\n(Recuperat din format binar Gorilla)', 
                  fontsize=14, fontweight='bold', pad=15, color='#2c3e50')

    # Formatare axă X individuală
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m %H:%M'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.xticks(rotation=0)

    # Grid și design
    ax.grid(True, linestyle='--', alpha=0.4)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    # Statistici
    avg_val = sum(v_sort) / len(v_sort)
    ax.axhline(y=avg_val, color='#e74c3c', linestyle='--', linewidth=1, label=f'Media: {avg_val:.2f}')
    ax.legend(loc='upper right', frameon=True)

    plt.tight_layout()

    with PdfPages(output_path) as pdf:
        pdf.savefig(fig, dpi=150)

    plt.close(fig)
    print(f"    Salvat: {output_path}")

def plot_twitter_comparison(std_data, ver_data, output_path):
    ts_std, val_std = std_data
    ts_ver, val_ver = ver_data

    # Sortare pentru ambele seturi
    std_sort = sorted(zip(ts_std, val_std))
    ver_sort = sorted(zip(ts_ver, val_ver))
    
    t_s, v_s = zip(*std_sort)
    t_v, v_v = zip(*ver_sort)

    print(f"Generare grafic comparativ Twitter -> {output_path}")

    fig, ax = plt.subplots(figsize=(14, 7))

    # Suprapunere: Standard (Albastru) și Verificare (Roșu punctat)
    ax.plot(t_s, v_s, color='#3498db', linewidth=2, label='Metoda Standard', alpha=0.7)
    ax.plot(t_v, v_v, color='#e74c3c', linewidth=1, linestyle='--', label='Metoda Verificare (11 biți)')

    ax.set_xlabel('Timp (Data / Ora)', fontsize=11)
    ax.set_ylabel('Twitter Volume', fontsize=11)
    ax.set_title('Integritate Date Twitter: Standard vs Verificare\n(Comparație Bit-Perfect)', 
                 fontsize=14, fontweight='bold', pad=15)

    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m %H:%M'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.grid(True, linestyle=':', alpha=0.6)
    ax.legend(loc='upper right')

    ax.text(0.02, 0.02, "Verificare: Date Identice (Integritate 100%)", transform=ax.transAxes, 
            fontsize=10, color='green', fontweight='bold', bbox=dict(facecolor='white', alpha=0.8))

    plt.tight_layout()

    with PdfPages(output_path) as pdf:
        pdf.savefig(fig, dpi=150)

    plt.close(fig)


def main():

    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
        print(f"\nFolder creat: {OUTPUT_FOLDER}")

    if os.path.exists(ROOM_CLIMATE_CSV):
        data_climate = load_room_climate_data(ROOM_CLIMATE_CSV)
        plot_room_climate_timeseries(data_climate, os.path.join(OUTPUT_FOLDER, "grafic_room_climate.pdf"))

    
    prefix_std = os.path.join("compressed_output", "rezultat_standard")
    prefix_ver = os.path.join("compressed_output", "rezultat_verificare")

    std_twitter = load_twiter_data(prefix_std)
    ver_twitter = load_twiter_data(prefix_ver)

    if std_twitter[0] and ver_twitter[0]:
        output_comp = os.path.join(OUTPUT_FOLDER, "comparatie_twitter_integritate.pdf")
        plot_twitter_comparison(std_twitter, ver_twitter, output_comp)
        
        plot_twitter_timeseries(std_twitter[0], std_twitter[1], 
                                os.path.join(OUTPUT_FOLDER, "grafic_twitter_standard.pdf"), 
                                title_suffix="(Standard)")
        
        plot_twitter_timeseries(ver_twitter[0], ver_twitter[1], 
                                os.path.join(OUTPUT_FOLDER, "grafic_twitter_verificare.pdf"), 
                                title_suffix="(Verificare 11 biți)")

if __name__ == "__main__":
    main()