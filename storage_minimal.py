import time
from typing import List, Tuple, Optional
from BitWriter import BitWriter
from BitReader import BitReader
from timestamp_compression import TimestampEncoder, TimestampDecoder
from value_compression import ValueEncoder, ValueDecoder
# Presupunem că ai clasele de la Ziua 2 și 3 în fișiere separate
# from Timestamps import TimestampEncoder, TimestampDecoder
# from Values import ValueEncoder, ValueDecoder

class SeriesBlock:
    """
    Reprezintă un bloc de date comprimat (de obicei pentru o fereastră de 2 ore)[cite: 8, 9, 13].
    """
    def __init__(self, start_timestamp: int):
        self.start_timestamp = start_timestamp # 
        self.writer = BitWriter()
        self.ts_encoder = TimestampEncoder(self.writer)
        self.val_encoder = ValueEncoder(self.writer)
        self.count = 0
        self.closed = False
        self.compressed_data: Optional[bytes] = None

    def add(self, ts: int, val: float):
        """Adaugă un punct în bloc[cite: 11]."""
        self.ts_encoder.add_timestamp(ts)
        self.val_encoder.add_value(val)
        self.count += 1

    def seal(self):
        """Închide blocul și eliberează memoria writer-ului."""
        self.compressed_data = self.writer.to_bytes()
        self.closed = True
        # Eliberăm obiectele de encoding pentru a economisi memorie
        self.ts_encoder = None
        self.val_encoder = None
        self.writer = None

class TimeSeries:
    """
    Gestionează blocurile unei singure serii (ex: 'temperatura_camera_1')[cite: 10].
    """
    def __init__(self, block_duration_sec: int = 7200): # 7200s = 2h 
        self.block_duration = block_duration_sec
        self.open_block: Optional[SeriesBlock] = None
        self.closed_blocks: List[SeriesBlock] = []

    def insert(self, ts: int, val: float):
        # Determinăm dacă avem nevoie de un bloc nou 
        if self.open_block is None or ts >= self.open_block.start_timestamp + self.block_duration:
            if self.open_block:
                self.open_block.seal()
                self.closed_blocks.append(self.open_block)
            
            # Startul blocului aliniat (ex: la fix, la 2h, 4h etc.) 
            aligned_start = (ts // self.block_duration) * self.block_duration
            self.open_block = SeriesBlock(aligned_start)
        
        self.open_block.add(ts, val)

    def query(self, t_start: int, t_end: int) -> List[Tuple[int, float]]:
        """Identifică blocurile relevante și decodează doar acele intervale."""
        results = []
        
        # Verificăm blocurile închise 
        for block in self.closed_blocks:
            if self._block_overlaps(block, t_start, t_end):
                results.extend(self._decode_block(block, t_start, t_end))
        
        # Verificăm blocul deschis curent 
        if self.open_block and self._block_overlaps(self.open_block, t_start, t_end):
            # Pentru simplitate, sigilăm temporar sau folosim o copie
            # În producție, open_block se citește cu grijă
            temp_data = self.open_block.writer.to_bytes()
            results.extend(self._decode_data(temp_data, self.open_block.count, t_start, t_end))
            
        return results

    def _block_overlaps(self, block: SeriesBlock, t_start: int, t_end: int) -> bool:
        block_end = block.start_timestamp + self.block_duration
        return not (block.start_timestamp > t_end or block_end < t_start)

    def _decode_block(self, block: SeriesBlock, t_start: int, t_end: int):
        return self._decode_data(block.compressed_data, block.count, t_start, t_end)

    def _decode_data(self, data: bytes, count: int, t_start: int, t_end: int):
        reader = BitReader(data)
        ts_decoder = TimestampDecoder(reader)
        val_decoder = ValueDecoder(reader)
        
        points = []
        for _ in range(count):
            ts = ts_decoder.read_timestamp()
            val = val_decoder.read_value()
            if t_start <= ts <= t_end:
                points.append((ts, val))
        return points

class GorillaStore:
    def __init__(self):
        self.series_map = {}

    def insert(self, series_key: str, timestamp: int, value: float): 
        if series_key not in self.series_map:
            self.series_map[series_key] = TimeSeries()
        self.series_map[series_key].insert(timestamp, value)

    def query(self, series_key: str, t_start: int, t_end: int): 
        if series_key not in self.series_map:
            return []
        return self.series_map[series_key].query(t_start, t_end)
    
if __name__ == "__main__":
    # 1. Inițializăm stocarea Gorilla
    store = GorillaStore()
    nume_senzor = "senzor_temperatura_01"
    
    # Alegem un timestamp de start (ora 10:00:00)
    start_time = 1700000000 
    
    print(f"--- Incepere Test Gorilla Store pentru {nume_senzor} ---")

    # 2. Inserăm date pentru 5 ore (pentru a genera cel puțin 3 blocuri de câte 2h)
    # Introducem un punct la fiecare 1 minut (60 secunde)
    # 5 ore * 60 minute = 300 de puncte
    print("Se insereaza date pentru 5 ore (300 puncte)...")
    for i in range(300):
        current_ts = start_time + (i * 60)
        # Generăm o valoare care variază ușor
        current_val = 22.0 + (i % 10) * 0.1 
        store.insert(nume_senzor, current_ts, current_val)

    # 3. Test Query: Cerem datele dintr-un interval specific (ex: o fereastră de 15 min)
    # Alegem un interval care se află la mijlocul datelor inserate
    t_query_start = start_time + 7200 + 600  # Ora 12:10 (după 2 ore și 10 min)
    t_query_end = t_query_start + 900        # Până la 12:25 (15 minute mai târziu)

    print(f"\nExecutie Query intre timestamps: {t_query_start} si {t_query_end}")
    
    t0 = time.time()
    rezultate = store.query(nume_senzor, t_query_start, t_query_end)
    t1 = time.time()

    # 4. Verificarea rezultatelor (Cerința 51, 53)
    print(f"Query finalizat in {(t1-t0)*1000:.2f} ms.")
    print(f"Puncte gasite: {len(rezultate)}")
    
    if rezultate:
        print("\nPrimele 5 puncte returnate:")
        for ts, val in rezultate[:5]:
            print(f"  TS: {ts} | Valoare: {val:.2f}")

    # 5. Analiza stocării (Cerința 12, 13)
    # Accesăm direct structura internă pentru a vedea câte blocuri s-au creat
    serie = store.series_map[nume_senzor]
    print(f"\n--- Statisici Stocare ---")
    print(f"Blocuri inchise (imutabile): {len(serie.closed_blocks)}")
    print(f"Blocuri deschise (active): {1 if serie.open_block else 0}")
    
    # Calculăm dimensiunea totală a biților comprimați
    total_bytes = sum(len(b.compressed_data) for b in serie.closed_blocks)
    if serie.open_block:
        total_bytes += len(serie.open_block.writer.to_bytes())
        
    print(f"Dimensiune totala comprimata: {total_bytes} bytes")
    print(f"Dimensiune originala estimata (16 bytes/punct): {300 * 16} bytes")
    print(f"Rata de compresie: {(300 * 16) / total_bytes:.2f}x")

    print("\n--- Test Finalizat cu Succes ---")