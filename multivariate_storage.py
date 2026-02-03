"""
Stocare eficientă pentru serii temporale multivariate folosind compresia Gorilla.

CONCEPTUL:
----------
O serie multivariată are MAI MULTE valori per timestamp:
    timestamp    | var1  | var2  | var3  | ...
    -------------|-------|-------|-------|-----
    1458031648545| 20.48 | 42.33 | 185.7 | ...
    1458031652680| 20.49 | 42.33 | 185.7 | ...

STRATEGIA DE COMPRESIE:
-----------------------
- UN SINGUR stream de timestamps (delta-of-delta)
- CÂTE UN stream separat pentru fiecare variabilă (XOR compression)

Aceasta evită duplicarea timestamp-urilor și menține compresia XOR optimă
(fiecare valoare se compară cu valoarea anterioară a ACELEIAȘI variabile).
"""

from typing import List, Dict, Tuple, Optional, Iterator
from BitWriter import BitWriter
from BitReader import BitReader
from timestamp_compression import TimestampEncoder, TimestampDecoder
from value_compression import ValueEncoder, ValueDecoder


class MultiVariateBlock:
    """
    Bloc care stochează eficient date multivariate comprimate.

    Structura internă:
    - Un singur TimestampEncoder pentru toate punctele
    - Câte un ValueEncoder pentru fiecare variabilă

    Toate encoderele scriu în același BitWriter, deci datele sunt intercalate
    la nivel de bit, dar fiecare variabilă își menține propriul context XOR.
    """

    __slots__ = ("_writer", "_ts_encoder", "_val_encoders", "_var_names",
                 "_count", "_closed", "_compressed_data", "_start_timestamp")

    def __init__(self, variable_names: List[str], start_timestamp: Optional[int] = None):
        """
        Inițializează un bloc multivariate.

        Args:
            variable_names: Lista numelor variabilelor (ex: ["temp", "humidity", "light"])
            start_timestamp: Timestamp-ul de start al blocului (opțional, pentru organizare)
        """
        if not variable_names:
            raise ValueError("Lista de variabile nu poate fi goală!")

        self._var_names = list(variable_names)  # Copie pentru a evita modificări externe
        self._writer = BitWriter()
        self._ts_encoder = TimestampEncoder(self._writer)

        # Creăm câte un ValueEncoder pentru fiecare variabilă
        # IMPORTANT: Toate scriu în același BitWriter
        self._val_encoders = {
            name: ValueEncoder(self._writer)
            for name in self._var_names
        }

        self._count = 0
        self._closed = False
        self._compressed_data: Optional[bytes] = None
        self._start_timestamp = start_timestamp

    def add(self, timestamp: int, values: Dict[str, float]) -> None:
        """
        Adaugă un punct multivariate în bloc.

        Args:
            timestamp: Timestamp-ul punctului (int64, ex: milisecunde Unix)
            values: Dicționar cu valorile pentru fiecare variabilă
                    Ex: {"temp": 20.5, "humidity": 42.3, "light": 185.7}

        Raises:
            ValueError: Dacă lipsesc variabile sau blocul e închis
        """
        if self._closed:
            raise ValueError("Blocul este închis, nu se mai pot adăuga date!")

        # Verificăm că avem toate variabilele
        missing = set(self._var_names) - set(values.keys())
        if missing:
            raise ValueError(f"Lipsesc variabilele: {missing}")

        # Setăm start_timestamp dacă e primul punct
        if self._count == 0 and self._start_timestamp is None:
            self._start_timestamp = timestamp

        # 1. Encodăm timestamp-ul O SINGURĂ DATĂ
        self._ts_encoder.add_timestamp(timestamp)

        # 2. Encodăm fiecare valoare în encoder-ul său dedicat
        #    IMPORTANT: Ordinea trebuie să fie constantă!
        for name in self._var_names:
            self._val_encoders[name].add_value(float(values[name]))

        self._count += 1

    def seal(self) -> bytes:
        """
        Închide blocul și returnează datele comprimate.

        După seal(), blocul devine read-only și memoria encoderelor e eliberată.

        Returns:
            bytes: Datele comprimate
        """
        if self._closed:
            return self._compressed_data

        self._compressed_data = self._writer.to_bytes()
        self._closed = True

        # Eliberăm memoria encoderelor
        self._writer = None
        self._ts_encoder = None
        self._val_encoders = None

        return self._compressed_data

    @property
    def count(self) -> int:
        """Numărul de puncte din bloc."""
        return self._count

    @property
    def variable_names(self) -> List[str]:
        """Lista numelor variabilelor."""
        return list(self._var_names)

    @property
    def is_closed(self) -> bool:
        """True dacă blocul e închis."""
        return self._closed

    @property
    def start_timestamp(self) -> Optional[int]:
        """Timestamp-ul de start al blocului."""
        return self._start_timestamp

    def get_compressed_data(self) -> Optional[bytes]:
        """Returnează datele comprimate (sau None dacă blocul nu e închis)."""
        if not self._closed:
            # Returnăm o copie fără a închide blocul
            return self._writer.to_bytes()
        return self._compressed_data


class MultiVariateDecoder:
    """
    Decoder pentru blocuri multivariate.

    Citește datele comprimate și reconstruiește punctele originale.
    """

    __slots__ = ("_reader", "_ts_decoder", "_val_decoders", "_var_names", "_count")

    def __init__(self, data: bytes, variable_names: List[str]):
        """
        Inițializează decoder-ul.

        Args:
            data: Datele comprimate (bytes)
            variable_names: Lista numelor variabilelor (TREBUIE să fie în aceeași ordine ca la encoding!)
        """
        self._var_names = list(variable_names)
        self._reader = BitReader(data)
        self._ts_decoder = TimestampDecoder(self._reader)

        # Creăm câte un ValueDecoder pentru fiecare variabilă
        self._val_decoders = {
            name: ValueDecoder(self._reader)
            for name in self._var_names
        }

        self._count = 0

    def read_point(self) -> Tuple[int, Dict[str, float]]:
        """
        Citește următorul punct multivariate.

        Returns:
            Tuple de (timestamp, {var_name: value, ...})

        Raises:
            EOFError: Dacă nu mai sunt date
        """
        # 1. Citim timestamp-ul
        timestamp = self._ts_decoder.read_timestamp()

        # 2. Citim valorile în aceeași ordine ca la encoding
        values = {}
        for name in self._var_names:
            values[name] = self._val_decoders[name].read_value()

        self._count += 1
        return timestamp, values

    def read_all(self, count: int) -> List[Tuple[int, Dict[str, float]]]:
        """
        Citește toate punctele din bloc.

        Args:
            count: Numărul de puncte de citit

        Returns:
            Lista de (timestamp, {var_name: value, ...})
        """
        points = []
        for _ in range(count):
            points.append(self.read_point())
        return points

    @property
    def points_read(self) -> int:
        """Numărul de puncte citite până acum."""
        return self._count


class MultiVariateSeries:
    """
    Gestionează o serie temporală multivariată completă.

    Organizează datele în blocuri de durată fixă (default 2 ore),
    similar cu implementarea originală Gorilla.
    """

    __slots__ = ("_var_names", "_block_duration", "_open_block", "_closed_blocks")

    def __init__(self, variable_names: List[str], block_duration_ms: int = 7200000):
        """
        Inițializează seria multivariată.

        Args:
            variable_names: Lista numelor variabilelor
            block_duration_ms: Durata unui bloc în milisecunde (default: 2 ore = 7200000 ms)
        """
        self._var_names = list(variable_names)
        self._block_duration = block_duration_ms
        self._open_block: Optional[MultiVariateBlock] = None
        self._closed_blocks: List[Tuple[int, int, bytes]] = []  # (start_ts, count, data)

    def insert(self, timestamp: int, values: Dict[str, float]) -> None:
        """
        Inserează un punct în serie.

        Creează automat blocuri noi când e necesar.

        Args:
            timestamp: Timestamp-ul punctului
            values: Valorile pentru fiecare variabilă
        """
        # Verificăm dacă avem nevoie de un bloc nou
        if self._open_block is None:
            self._create_new_block(timestamp)
        elif timestamp >= self._open_block.start_timestamp + self._block_duration:
            # Închidem blocul curent și creăm unul nou
            self._close_current_block()
            self._create_new_block(timestamp)

        self._open_block.add(timestamp, values)

    def _create_new_block(self, timestamp: int) -> None:
        """Creează un bloc nou aliniat la block_duration."""
        aligned_start = (timestamp // self._block_duration) * self._block_duration
        self._open_block = MultiVariateBlock(self._var_names, aligned_start)

    def _close_current_block(self) -> None:
        """Închide blocul curent și îl mută în lista de blocuri închise."""
        if self._open_block and self._open_block.count > 0:
            data = self._open_block.seal()
            self._closed_blocks.append((
                self._open_block.start_timestamp,
                self._open_block.count,
                data
            ))
        self._open_block = None

    def flush(self) -> None:
        """Închide blocul curent (util la finalul inserării)."""
        self._close_current_block()

    def query(self, t_start: int, t_end: int) -> List[Tuple[int, Dict[str, float]]]:
        """
        Interogare pe un interval de timp.

        Args:
            t_start: Timestamp start (inclusiv)
            t_end: Timestamp end (inclusiv)

        Returns:
            Lista de puncte în intervalul specificat
        """
        results = []

        # Căutăm în blocurile închise
        for block_start, count, data in self._closed_blocks:
            block_end = block_start + self._block_duration

            # Verificăm dacă blocul se suprapune cu intervalul cerut
            if block_start <= t_end and block_end >= t_start:
                decoder = MultiVariateDecoder(data, self._var_names)
                for _ in range(count):
                    ts, values = decoder.read_point()
                    if t_start <= ts <= t_end:
                        results.append((ts, values))

        # Căutăm în blocul deschis (dacă există)
        if self._open_block and self._open_block.count > 0:
            block_start = self._open_block.start_timestamp
            block_end = block_start + self._block_duration

            if block_start <= t_end and block_end >= t_start:
                data = self._open_block.get_compressed_data()
                decoder = MultiVariateDecoder(data, self._var_names)
                for _ in range(self._open_block.count):
                    ts, values = decoder.read_point()
                    if t_start <= ts <= t_end:
                        results.append((ts, values))

        return results

    def query_all(self) -> List[Tuple[int, Dict[str, float]]]:
        """Returnează toate punctele din serie."""
        return self.query(0, 2**63 - 1)

    @property
    def variable_names(self) -> List[str]:
        """Lista numelor variabilelor."""
        return list(self._var_names)

    @property
    def total_points(self) -> int:
        """Numărul total de puncte din serie."""
        total = sum(count for _, count, _ in self._closed_blocks)
        if self._open_block:
            total += self._open_block.count
        return total

    @property
    def num_blocks(self) -> int:
        """Numărul total de blocuri (închise + deschis)."""
        return len(self._closed_blocks) + (1 if self._open_block else 0)

    def get_compression_stats(self) -> Dict:
        """
        Calculează statistici despre compresie.

        Returns:
            Dicționar cu statistici
        """
        # Închidem temporar pentru statistici precise
        was_open = self._open_block is not None
        if was_open:
            open_block_data = self._open_block.get_compressed_data()
            open_block_count = self._open_block.count

        total_points = self.total_points
        num_variables = len(self._var_names)

        # Dimensiune originală: timestamp (8 bytes) + valori (8 bytes * num_vars)
        bytes_per_point = 8 + (8 * num_variables)
        original_size = total_points * bytes_per_point

        # Dimensiune comprimată
        compressed_size = sum(len(data) for _, _, data in self._closed_blocks)
        if was_open:
            compressed_size += len(open_block_data)

        compression_ratio = original_size / compressed_size if compressed_size > 0 else 0

        return {
            "total_points": total_points,
            "num_variables": num_variables,
            "num_blocks": self.num_blocks,
            "original_bytes": original_size,
            "compressed_bytes": compressed_size,
            "compression_ratio": compression_ratio,
            "savings_percent": (1 - compressed_size / original_size) * 100 if original_size > 0 else 0,
            "bits_per_point": (compressed_size * 8) / total_points if total_points > 0 else 0,
        }


class MultiVariateStore:
    """
    Store pentru multiple serii multivariate.

    Permite gestionarea mai multor serii (ex: mai multe noduri/senzori),
    fiecare cu propriile variabile.
    """

    def __init__(self):
        self._series: Dict[str, MultiVariateSeries] = {}

    def create_series(self, series_key: str, variable_names: List[str],
                      block_duration_ms: int = 7200000) -> MultiVariateSeries:
        """
        Creează o serie nouă.

        Args:
            series_key: Identificator unic pentru serie (ex: "node_1", "sensor_A")
            variable_names: Lista variabilelor pentru această serie
            block_duration_ms: Durata unui bloc

        Returns:
            Seria creată
        """
        if series_key in self._series:
            raise ValueError(f"Seria '{series_key}' există deja!")

        series = MultiVariateSeries(variable_names, block_duration_ms)
        self._series[series_key] = series
        return series

    def get_series(self, series_key: str) -> Optional[MultiVariateSeries]:
        """Returnează seria cu cheia specificată (sau None)."""
        return self._series.get(series_key)

    def insert(self, series_key: str, timestamp: int, values: Dict[str, float]) -> None:
        """
        Inserează un punct într-o serie existentă.

        Args:
            series_key: Cheia seriei
            timestamp: Timestamp-ul
            values: Valorile

        Raises:
            KeyError: Dacă seria nu există
        """
        if series_key not in self._series:
            raise KeyError(f"Seria '{series_key}' nu există! Creează-o mai întâi cu create_series().")

        self._series[series_key].insert(timestamp, values)

    def query(self, series_key: str, t_start: int, t_end: int) -> List[Tuple[int, Dict[str, float]]]:
        """Interogare pe o serie."""
        if series_key not in self._series:
            return []
        return self._series[series_key].query(t_start, t_end)

    def scan_all(self) -> Iterator[Tuple[str, MultiVariateSeries]]:
        """Iterează prin toate seriile."""
        for key, series in self._series.items():
            yield key, series

    def list_series(self) -> List[str]:
        """Returnează lista cheilor tuturor seriilor."""
        return list(self._series.keys())

    def flush_all(self) -> None:
        """Închide toate blocurile deschise."""
        for series in self._series.values():
            series.flush()

    def get_total_stats(self) -> Dict:
        """Statistici agregate pentru toate seriile."""
        total_points = 0
        total_original = 0
        total_compressed = 0

        for series in self._series.values():
            stats = series.get_compression_stats()
            total_points += stats["total_points"]
            total_original += stats["original_bytes"]
            total_compressed += stats["compressed_bytes"]

        return {
            "num_series": len(self._series),
            "total_points": total_points,
            "total_original_bytes": total_original,
            "total_compressed_bytes": total_compressed,
            "overall_compression_ratio": total_original / total_compressed if total_compressed > 0 else 0,
            "overall_savings_percent": (1 - total_compressed / total_original) * 100 if total_original > 0 else 0,
        }


# =============================================================================
# FUNCȚII HELPER PENTRU ÎNCĂRCARE CSV
# =============================================================================

def load_room_climate_csv(filepath: str, variable_names: Optional[List[str]] = None) -> MultiVariateSeries:
    """
    Incarca un fisier CSV Room Climate intr-o serie multivariata.

    Presupune ca toate randurile au acelasi NID (Node ID).

    Format CSV (cu header):
    EID, AbsT(ms), RelT(s), NID, Temp, RelH, L1, L2, Occ, Act, Door, Win

    Args:
        filepath: Calea catre fisierul CSV
        variable_names: Numele variabilelor (default: cele standard din dataset)

    Returns:
        MultiVariateSeries cu datele incarcate
    """
    import csv

    # Numele default ale variabilelor din Room Climate Dataset
    if variable_names is None:
        variable_names = ["temp", "humidity", "light1", "light2",
                         "occupancy", "activity", "door", "window"]

    # Indexurile coloanelor in CSV (0-based)
    # EID=0, AbsT=1, RelT=2, NID=3, Temp=4, RelH=5, L1=6, L2=7, Occ=8, Act=9, Door=10, Win=11
    COL_TIMESTAMP = 1

    series = MultiVariateSeries(variable_names, block_duration_ms=7200000)

    with open(filepath, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)

        # Sarim header-ul (prima linie)
        header = next(reader, None)

        for row in reader:
            if not row or len(row) < 12:
                continue

            try:
                # Extragem timestamp-ul (coloana 1 = AbsT in milisecunde)
                timestamp = int(row[COL_TIMESTAMP].strip())

                # Extragem valorile (coloanele 4-11)
                values = {
                    variable_names[0]: float(row[4].strip()),   # temp
                    variable_names[1]: float(row[5].strip()),   # humidity
                    variable_names[2]: float(row[6].strip()),   # light1
                    variable_names[3]: float(row[7].strip()),   # light2
                    variable_names[4]: float(row[8].strip()),   # occupancy
                    variable_names[5]: float(row[9].strip()),   # activity
                    variable_names[6]: float(row[10].strip()),  # door
                    variable_names[7]: float(row[11].strip()),  # window
                }

                series.insert(timestamp, values)

            except (ValueError, IndexError) as e:
                # Skip randuri invalide
                continue

    return series


# =============================================================================
# TEST / DEMO
# =============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("TEST: Stocare Multivariată Gorilla")
    print("=" * 70)

    # Test 1: Creare și populare bloc simplu
    print("\n[Test 1] Bloc multivariate simplu")
    print("-" * 40)

    var_names = ["temp", "humidity", "light"]
    block = MultiVariateBlock(var_names)

    # Adăugăm câteva puncte
    test_data = [
        (1000, {"temp": 22.5, "humidity": 45.0, "light": 500.0}),
        (2000, {"temp": 22.5, "humidity": 45.1, "light": 500.0}),  # temp identic
        (3000, {"temp": 22.6, "humidity": 45.1, "light": 501.0}),
        (4000, {"temp": 22.6, "humidity": 45.2, "light": 499.0}),
        (5000, {"temp": 22.7, "humidity": 45.2, "light": 500.0}),
    ]

    for ts, values in test_data:
        block.add(ts, values)

    compressed = block.seal()
    print(f"  Puncte: {block.count}")
    print(f"  Original: {block.count * (8 + 8*3)} bytes (ts + 3 valori * 8 bytes)")
    print(f"  Comprimat: {len(compressed)} bytes")
    print(f"  Ratio: {(block.count * 32) / len(compressed):.2f}x")

    # Decodare
    decoder = MultiVariateDecoder(compressed, var_names)
    decoded = decoder.read_all(block.count)

    print("\n  Verificare round-trip:")
    all_ok = True
    for (orig_ts, orig_vals), (dec_ts, dec_vals) in zip(test_data, decoded):
        if orig_ts != dec_ts:
            all_ok = False
            print(f"    FAIL: timestamp {orig_ts} != {dec_ts}")
        for var in var_names:
            if abs(orig_vals[var] - dec_vals[var]) > 1e-10:
                all_ok = False
                print(f"    FAIL: {var} {orig_vals[var]} != {dec_vals[var]}")

    if all_ok:
        print("  [OK] Toate valorile sunt identice!")

    # Test 2: Serie completă cu blocuri
    print("\n[Test 2] Serie multivariată cu blocuri automate")
    print("-" * 40)

    series = MultiVariateSeries(["temp", "humidity"], block_duration_ms=10000)

    # Inserăm date pe 30 de secunde (3 blocuri de 10s)
    import random
    random.seed(42)

    for i in range(30):
        ts = 1000000 + i * 1000  # La fiecare secundă
        series.insert(ts, {
            "temp": 22.0 + random.gauss(0, 0.1),
            "humidity": 45.0 + random.gauss(0, 0.5)
        })

    series.flush()
    stats = series.get_compression_stats()

    print(f"  Total puncte: {stats['total_points']}")
    print(f"  Blocuri: {stats['num_blocks']}")
    print(f"  Original: {stats['original_bytes']} bytes")
    print(f"  Comprimat: {stats['compressed_bytes']} bytes")
    print(f"  Ratio: {stats['compression_ratio']:.2f}x")
    print(f"  Economie: {stats['savings_percent']:.1f}%")

    # Test query
    results = series.query(1000005, 1000015)
    print(f"\n  Query [1000005, 1000015]: {len(results)} puncte găsite")

    print("\n" + "=" * 70)
    print("TOATE TESTELE AU TRECUT!")
    print("=" * 70)