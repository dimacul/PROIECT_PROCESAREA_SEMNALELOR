from typing import List, Tuple, Optional
from BitWriter import BitWriter
from BitReader import BitReader
from timestamp_compression import TimestampEncoder, TimestampDecoder
from value_compression import ValueEncoder, ValueDecoder



class SeriesBlock:

    def __init__(self, start_timestamp: int):
        self.start_timestamp = start_timestamp # 
        self.writer = BitWriter()
        self.ts_encoder = TimestampEncoder(self.writer)
        self.val_encoder = ValueEncoder(self.writer)
        self.count = 0
        self.closed = False
        self.compressed_data: Optional[bytes] = None

    def add(self, ts: int, val: float):
        self.ts_encoder.add_timestamp(ts)
        self.val_encoder.add_value(val)
        self.count += 1

    def seal(self):
        self.compressed_data = self.writer.to_bytes()
        self.closed = True
        # Eliberăm obiectele de encoding pentru a economisi memorie
        self.ts_encoder = None
        self.val_encoder = None
        self.writer = None

class TimeSeries:

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
    
