from typing import List, Dict, Tuple, Optional, Iterator
from BitWriter import BitWriter
from BitReader import BitReader
from timestamp_compression import TimestampEncoder, TimestampDecoder
from value_compression import ValueEncoder, ValueDecoder

# Stocare eficienta pentru serii temporale multivariate (merge si pt univariate) folosind compresia descrisa in alg. Gorilla
#
# - UN singur stream de timestamps (delta-of-delta)
# - Cate un stream separat pentru fiecare variabila (compresia cu xor)
#
# nu duplicam timestamp-urile!
# (fiecare valoare se compara cu valoarea anterioara a ACELEIASI variabile)


# Bloc care stocheaza dtatpoints multivariate comprimate
# Structura:
# - Un singur TimestampEncoder pentru toate punctele
# - Cate un ValueEncoder pentru fiecare variabila
# Toate encoderele scriu in acelasi BitWriter, deci datele sunt intercalate
# la nivel de bit, dar fiecare variabila isi mentine propriul context XOR
class MultiVariateBlock:

    __slots__ = ("_writer", #bit writer-ul
                "_ts_encoder", #timestamps encoder
                "_val_encoders", #value_encoders - 1 per variabila
                "_var_names",
                "_count", 
                "_closed", 
                "_compressed_data", 
                "_start_timestamp")

    def __init__(self, variable_names: List[str], start_timestamp: Optional[int] = None):
        if not variable_names:
            raise ValueError("Lista de variabile nu poate fi goala")

        self._var_names = list(variable_names)
        self._writer = BitWriter()
        self._ts_encoder = TimestampEncoder(self._writer)

        # Cream cate un ValueEncoder pentru fiecare variabila
        # Toate ValueEncoders scriu in acelasi BitWriter
        self._val_encoders = { name: ValueEncoder(self._writer) for name in self._var_names }

        self._count = 0
        self._closed = False
        self._compressed_data: Optional[bytes] = None
        self._start_timestamp = start_timestamp

    # Adauga un punct multivariate in bloc
    def add(self, timestamp: int, values: Dict[str, float]) -> None:
        if self._closed:
            raise ValueError("Blocul este inchis, nu se mai pot adauga date!")

        # Verificam ca avem toate variabilele
        missing = set(self._var_names) - set(values.keys())
        if missing:
            raise ValueError(f"Lipsesc variabilele: {missing}")

        # Setam start_timestamp daca e primul punct
        if self._count == 0 and self._start_timestamp is None:
            self._start_timestamp = timestamp

        # Encodam timestamp-ul O SINGURA DATA
        self._ts_encoder.add_timestamp(timestamp)

        # Encodam fiecare valoare in encoder-ul sau dedicat
        #    IMPORTANT: Ordinea trebuie sa fie constanta
        for name in self._var_names:
            self._val_encoders[name].add_value(float(values[name]))

        self._count += 1

    # Adauga un punct multivariate in bloc (versiunea lui Muscalu cu verificare)
    # Diferenta f.d. add simplu:
    # -> add_value_verification()
    def add_verification(self, timestamp: int, values: Dict[str, float]) -> None:
        if self._closed:
            raise ValueError("Blocul este inchis, nu se mai pot adauga date!")

        # Verificam ca avem toate variabilele
        missing = set(self._var_names) - set(values.keys())
        if missing:
            raise ValueError(f"Lipsesc variabilele: {missing}")

        # Setam start_timestamp daca e primul punct
        if self._count == 0 and self._start_timestamp is None:
            self._start_timestamp = timestamp

        # Encodam timestamp-ul O SINGURA DATA
        self._ts_encoder.add_timestamp(timestamp)

        # Encodam fiecare valoare in encoder-ul sau dedicat
        # ! in aceeasi ordine mereu, ca sa nu scriu in encoderul gresit
        for name in self._var_names:
            self._val_encoders[name].add_value_verification(float(values[name]))

        self._count += 1

    # Inchide blocul si returneaza datele comprimate
    # Dupa seal(), blocul devine read-only si memoria encoderelor e eliberata
    def seal(self) -> bytes:
        if self._closed:
            return self._compressed_data

        self._compressed_data = self._writer.to_bytes()
        self._closed = True

        # Eliberam memoria encoderelor
        self._writer = None
        self._ts_encoder = None
        self._val_encoders = None

        return self._compressed_data

    @property
    # Numarul de puncte din bloc
    def count(self) -> int:
        return self._count

    @property
    # Lista numelor variabilelor
    def variable_names(self) -> List[str]:
        return list(self._var_names)

    @property
    # True daca blocul e inchis
    def is_closed(self) -> bool:
        return self._closed

    @property
    # Timestamp-ul de start al blocului
    def start_timestamp(self) -> Optional[int]:
        return self._start_timestamp

    # Returneaza datele comprimate (sau None daca blocul nu e inchis)
    def get_compressed_data(self) -> Optional[bytes]:
        if not self._closed:
            # Returnam o copie fara a inchide blocul
            return self._writer.to_bytes()
        return self._compressed_data


# Decoder pentru blocuri multivariate
# Citeste datele comprimate si reconstruieste punctele originale
class MultiVariateDecoder:

    __slots__ = ("_reader", # BitReader
                 "_ts_decoder", # TimestampDecoder
                 "_val_decoders", # ValueDecoder (1 per variabila)
                 "_var_names", 
                 "_count" # Numarul de timestamp-uri 
                 )

    def __init__(self, data: bytes, variable_names: List[str]):
        self._var_names = list(variable_names)
        self._reader = BitReader(data)
        self._ts_decoder = TimestampDecoder(self._reader)

        # Cream cate un ValueDecoder pentru fiecare variabila
        self._val_decoders = {
            name: ValueDecoder(self._reader)
            for name in self._var_names
        }

        self._count = 0

    # Citeste urmatorul punct multivariate
    def read_point(self) -> Tuple[int, Dict[str, float]]:
        # 1. Citim timestamp-ul
        timestamp = self._ts_decoder.read_timestamp()

        # 2. Citim valorile in aceeasi ordine ca la encoding
        values = {}
        for name in self._var_names:
            values[name] = self._val_decoders[name].read_value()

        self._count += 1
        return timestamp, values

    # Citeste toate punctele din bloc
    def read_all(self, count: int) -> List[Tuple[int, Dict[str, float]]]:
        points = []
        for _ in range(count):
            points.append(self.read_point())
        return points

    @property
    # Numarul de puncte citite pana acum
    def points_read(self) -> int:
        return self._count


# Gestioneaza o serie temporala multivariata completa
# Organizeaza datele in blocuri de durata fixa (default 2 ore)
class MultiVariateSeries:

    __slots__ = ("_var_names", 
                "_block_duration", 
                "_open_block", 
                "_closed_blocks"
                )

    def __init__(self, variable_names: List[str], block_duration_ms: int = 7200000): # 2h
        self._var_names = list(variable_names)
        self._block_duration = block_duration_ms
        self._open_block: Optional[MultiVariateBlock] = None
        self._closed_blocks: List[Tuple[int, int, bytes]] = []  # (start_ts, count, data)

    # Insereaza un punct in serie
    # Creeaza automat blocuri noi cand e necesar
    def insert(self, timestamp: int, values: Dict[str, float]) -> None:
        # Verificam daca avem nevoie de un bloc nou
        if self._open_block is None:
            self._create_new_block(timestamp)
        elif timestamp >= self._open_block.start_timestamp + self._block_duration:
            # Inchidem blocul curent si cream unul nou
            self._close_current_block()
            self._create_new_block(timestamp)

        self._open_block.add(timestamp, values)

    # Creeaza un bloc nou aliniat la block_duration
    def _create_new_block(self, timestamp: int) -> None:
        aligned_start = (timestamp // self._block_duration) * self._block_duration
        self._open_block = MultiVariateBlock(self._var_names, aligned_start)

    # Inchide blocul curent si il muta in lista de blocuri inchise
    def _close_current_block(self) -> None:
        if self._open_block and self._open_block.count > 0:
            data = self._open_block.seal()
            self._closed_blocks.append((
                self._open_block.start_timestamp,
                self._open_block.count,
                data
            ))
        self._open_block = None

    # Inchide blocul curent (util la finalul inserarii)
    def flush(self) -> None:
        self._close_current_block()

    # Interogare pe un interval de timp
    def query(self, t_start: int, t_end: int) -> List[Tuple[int, Dict[str, float]]]:
        results = []

        # Cautam in blocurile inchise
        for block_start, count, data in self._closed_blocks:
            block_end = block_start + self._block_duration

            # Verificam daca blocul se suprapune cu intervalul cerut
            if block_start <= t_end and block_end >= t_start:
                decoder = MultiVariateDecoder(data, self._var_names)
                for _ in range(count):
                    ts, values = decoder.read_point()
                    if t_start <= ts <= t_end:
                        results.append((ts, values))

        # Cautam in blocul deschis (daca exista)
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

    # Returneaza toate punctele din serie
    def query_all(self) -> List[Tuple[int, Dict[str, float]]]:
        return self.query(0, 2**63 - 1)

    @property
    # Lista numelor variabilelor
    def variable_names(self) -> List[str]:
        return list(self._var_names)

    @property
    # Numarul total de puncte din serie
    def total_points(self) -> int:
        total = sum(count for _, count, _ in self._closed_blocks)
        if self._open_block:
            total += self._open_block.count
        return total

    @property
    # Numarul total de blocuri (inchise + deschis)
    def num_blocks(self) -> int:
        return len(self._closed_blocks) + (1 if self._open_block else 0)

    # Calculeaza statistici despre compresie
    def get_compression_stats(self) -> Dict:
        # Inchidem temporar pentru statistici precise
        was_open = self._open_block is not None
        if was_open:
            open_block_data = self._open_block.get_compressed_data()
            open_block_count = self._open_block.count

        total_points = self.total_points
        num_variables = len(self._var_names)

        # Dimensiune originala: timestamp (8 bytes) + valori (8 bytes * num_vars)
        bytes_per_point = 8 + (8 * num_variables)
        original_size = total_points * bytes_per_point

        # Dimensiune comprimata
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


# Store pentru mai multe serii multivariate
class MultiVariateStore:

    def __init__(self):
        self._series: Dict[str, MultiVariateSeries] = {}

    # Creeaza o serie noua
    def create_series(self, series_key: str, variable_names: List[str],
                      block_duration_ms: int = 7200000) -> MultiVariateSeries:
        if series_key in self._series:
            raise ValueError(f"Seria '{series_key}' exista deja!")

        series = MultiVariateSeries(variable_names, block_duration_ms)
        self._series[series_key] = series
        return series

    # Returneaza seria cu cheia specificata (sau None)
    def get_series(self, series_key: str) -> Optional[MultiVariateSeries]:
        return self._series.get(series_key)

    # Insereaza un punct intr-o serie existenta
    def insert(self, series_key: str, timestamp: int, values: Dict[str, float]) -> None:
        if series_key not in self._series:
            raise KeyError(f"Seria '{series_key}' nu exista! Creeaz-o mai intai cu create_series()")

        self._series[series_key].insert(timestamp, values)

    # Interogare pe o serie
    def query(self, series_key: str, t_start: int, t_end: int) -> List[Tuple[int, Dict[str, float]]]:
        if series_key not in self._series:
            return []
        return self._series[series_key].query(t_start, t_end)

    # Itereaza prin toate seriile
    def scan_all(self) -> Iterator[Tuple[str, MultiVariateSeries]]:
        for key, series in self._series.items():
            yield key, series

    # Returneaza lista cheilor tuturor seriilor
    def list_series(self) -> List[str]:
        return list(self._series.keys())

    # Inchide toate blocurile deschise
    def flush_all(self) -> None:
        for series in self._series.values():
            series.flush()

    # Statistici agregate pentru toate seriile
    def get_total_stats(self) -> Dict:
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
# FUNCTII HELPER PENTRU INCARCARE CSV
# =============================================================================

def load_room_climate_csv(filepath: str, variable_names: Optional[List[str]] = None) -> MultiVariateSeries:
    import csv

    if variable_names is None:
        variable_names = ["temp", "humidity", "light1", "light2",
                         "occupancy", "activity", "door", "window"]

    # EID=0, AbsT=1, RelT=2, NID=3, Temp=4, RelH=5, L1=6, L2=7, Occ=8, Act=9, Door=10, Win=11
    COL_TIMESTAMP = 1

    series = MultiVariateSeries(variable_names, block_duration_ms=7200000)

    with open(filepath, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)

        header = next(reader, None)
        for row in reader:
            if not row or len(row) < 12:
                continue

            try:
                timestamp = int(row[COL_TIMESTAMP].strip())

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