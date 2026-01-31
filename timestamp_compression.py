"""
Compresia Timestamp-urilor folosind algoritmul Delta-of-Delta din Gorilla.

CONCEPTUL DELTA-OF-DELTA:
-------------------------
Seriile temporale au de obicei timestamp-uri aproape periodice:
    t0=1000, t1=1010, t2=1020, t3=1030, t4=1035

Delta simplu = diferenta dintre 2 timestampuri consecutive
    delta1 = t1-t0 = 10
    delta2 = t2-t1 = 10
    delta3 = t3-t2 = 10
    delta4 = t4-t3 = 5

Delta-of-delta (diferenta intre delta-uri consecutive):
    delta_of_delta1 = delta1 - 0      = 10  (primul delta comparat cu 0)
    delta_of_delta2 = delta2 - delta1 = 0   (perfect periodic!)
    delta_of_delta3 = delta3 - delta2 = 0   (perfect periodic!)
    delta_of_delta4 = delta4 - delta3 = -5  (deviatie mica)

OBSERVATIE: Daca timestamp-urile sunt periodice, delta-of-delta = 0 foarte des!
=> Putem comprima 0 ca un singur bit: "0"

SCHEMA DE CODARE VARIABILA (din articolul Gorilla):
---------------------------------------------------
delta_of_delta = 0:           scriem "0"                    (1 bit)
delta_of_delta in [-63, 64]:  scriem "10" + 7 biti         (9 biti)
delta_of_delta in [-255, 256]: scriem "110" + 9 biti       (12 biti)
delta_of_delta in [-2047, 2048]: scriem "1110" + 12 biti   (16 biti)
altfel:                       scriem "1111" + 32 biti       (36 biti)

Nota: Intervalele sunt adaptate pentru numere cu semn in Two's Complement.
"""

from BitWriter import BitWriter
from BitReader import BitReader


class TimestampEncoder:
    """
    Encoder pentru compresia timestamp-urilor folosind Delta-of-Delta.
    """

    __slots__ = ("_writer", "_prev_timestamp", "_prev_delta", "_count")

    def __init__(self, writer: BitWriter):
        """
        Initializeaza encoder-ul.

        Args:
            writer: BitWriter pentru scrierea bitilor comprimati
        """
        self._writer = writer
        self._prev_timestamp = None  # Timestamp-ul anterior (pentru calculul delta)
        self._prev_delta = None      # Delta anterior (pentru calculul delta-of-delta)
        self._count = 0              # Numarul de timestamp-uri adaugate

    def add_timestamp(self, timestamp: int) -> None:
        """
        Adauga un timestamp in flux si il comprima. //de fapt, scrie delta of delta 

        Args:
            timestamp: Timestamp-ul de adaugat (int64, de ex. secunde sau milisecunde)
        """
        if self._count == 0:
            # Primul timestamp: il scriem complet (64 biti)
            self._writer.write_i64(timestamp)
            self._prev_timestamp = timestamp
            self._count = 1
            return

        # Calculam delta (diferenta fata de timestamp-ul anterior)
        delta = timestamp - self._prev_timestamp

        if self._count == 1:
            # Al doilea timestamp: scriem delta complet (64 biti signed)
            # Nu putem calcula delta-of-delta inca (avem nevoie de 2 delta-uri)
            self._writer.write_i64(delta)
            self._prev_timestamp = timestamp
            self._prev_delta = delta
            self._count = 2
            return

        # De la al treilea timestamp inainte: folosim delta-of-delta
        delta_of_delta = delta - self._prev_delta

        # Codare variabila conform schemei Gorilla
        self._encode_delta_of_delta(delta_of_delta)

        # Actualizam starea
        self._prev_timestamp = timestamp
        self._prev_delta = delta
        self._count += 1

    def _encode_delta_of_delta(self, dod: int) -> None:
        """
        Encodeaza delta-of-delta folosind codare variabila.

        Schema:
        - dod == 0:              "0"                (1 bit)
        - dod in [-63, 64]:      "10" + 7 biti     (9 biti)
        - dod in [-255, 256]:    "110" + 9 biti    (12 biti)
        - dod in [-2047, 2048]:  "1110" + 12 biti  (16 biti)
        - altfel:                "1111" + 32 biti  (36 biti)

        Args:
            dod: Delta-of-delta (diferenta intre delta-uri consecutive)
        """
        if dod == 0: #dif intre delta_cur - delta_ant = 0
            # Cazul cel mai frecvent: timestamp perfect periodic
            self._writer.write_bit(0)

        elif -63 <= dod <= 64:
            # Deviatie mica: 2 biti control + 7 biti valoare
            self._writer.write_bit(1)
            self._writer.write_bit(0)
            self._writer.write_signed(dod, 7)

        elif -255 <= dod <= 256:
            # Deviatie medie: 3 biti control + 9 biti valoare
            self._writer.write_bit(1)
            self._writer.write_bit(1)
            self._writer.write_bit(0)
            self._writer.write_signed(dod, 9)

        elif -2047 <= dod <= 2048:
            # Deviatie mare: 4 biti control + 12 biti valoare
            self._writer.write_bit(1)
            self._writer.write_bit(1)
            self._writer.write_bit(1)
            self._writer.write_bit(0)
            self._writer.write_signed(dod, 12)

        else:
            # Deviatie foarte mare: 4 biti control + 32 biti valoare
            self._writer.write_bit(1)
            self._writer.write_bit(1)
            self._writer.write_bit(1)
            self._writer.write_bit(1)
            self._writer.write_signed(dod, 32)

    @property
    def count(self) -> int:
        """Returneaza numarul de timestamp-uri adaugate."""
        return self._count


class TimestampDecoder:
    """
    Decoder pentru decompresarea timestamp-urilor comprimate cu Delta-of-Delta.
    """

    __slots__ = ("_reader", "_prev_timestamp", "_prev_delta", "_count")

    def __init__(self, reader: BitReader):
        """
        Initializeaza decoder-ul.

        Args:
            reader: BitReader pentru citirea bitilor comprimati
        """
        self._reader = reader
        self._prev_timestamp = None
        self._prev_delta = None
        self._count = 0 # cate timestampuri am citit deja

    def read_timestamp(self) -> int:
        """
        Citeste si decomprima urmatorul timestamp.

        Returns:
            Timestamp-ul decomprimat (int64)

        Raises:
            EOFError: Daca nu mai sunt date de citit
        """
        if self._count == 0:
            # Primul timestamp: citim complet (64 biti)
            timestamp = self._reader.read_i64()
            self._prev_timestamp = timestamp
            self._count = 1
            return timestamp

        if self._count == 1:
            # Al doilea timestamp: citim delta complet (64 biti)
            delta = self._reader.read_i64()
            timestamp = self._prev_timestamp + delta
            self._prev_timestamp = timestamp
            self._prev_delta = delta
            self._count = 2
            return timestamp

        # De la al treilea timestamp inainte: decodam delta-of-delta
        delta_of_delta = self._decode_delta_of_delta()

        # Reconstruim delta si timestamp-ul
        delta = self._prev_delta + delta_of_delta
        timestamp = self._prev_timestamp + delta

        # Actualizam starea
        self._prev_timestamp = timestamp
        self._prev_delta = delta
        self._count += 1

        return timestamp

    def _decode_delta_of_delta(self) -> int:
        """
        Decodeaza delta-of-delta din flux.

        Citeste biti de control si apoi valoarea, conform schemei de codare variabila.

        Returns:
            Delta-of-delta decodificat
            
        Schema:
        - dod == 0:              "0"                (1 bit)
        - dod in [-63, 64]:      "10" + 7 biti     (9 biti)
        - dod in [-255, 256]:    "110" + 9 biti    (12 biti)
        - dod in [-2047, 2048]:  "1110" + 12 biti  (16 biti)
        - altfel:                "1111" + 32 biti  (36 biti)
        """
        # Citim primul bit de control
        first_bit = self._reader.read_bit()

        if first_bit == 0:
            # "0" => delta_of_delta = 0
            return 0

        # Citim al doilea bit de control
        second_bit = self._reader.read_bit()

        if second_bit == 0:
            # "10" => citim 7 biti signed
            return self._reader.read_signed(7)

        # Citim al treilea bit de control
        third_bit = self._reader.read_bit()

        if third_bit == 0:
            # "110" => citim 9 biti signed
            return self._reader.read_signed(9)

        # Citim al patrulea bit de control
        fourth_bit = self._reader.read_bit()

        if fourth_bit == 0:
            # "1110" => citim 12 biti signed
            return self._reader.read_signed(12)

        # "1111" => citim 32 biti signed
        return self._reader.read_signed(32)

    @property
    def count(self) -> int:
        """Returneaza numarul de timestamp-uri citite."""
        return self._count


def compress_timestamps(timestamps: list[int]) -> bytes:
    """
    Comprima o lista de timestamp-uri folosind Delta-of-Delta.

    Args:
        timestamps: Lista de timestamp-uri (int64)

    Returns:
        Bytes comprimati

    Example:
        >>> timestamps = [1000, 1010, 1020, 1030, 1035]
        >>> compressed = compress_timestamps(timestamps)
        >>> print(f"Original: {len(timestamps) * 8} bytes")
        >>> print(f"Compressed: {len(compressed)} bytes")
    """
    writer = BitWriter()
    encoder = TimestampEncoder(writer)

    for ts in timestamps:
        encoder.add_timestamp(ts)

    return writer.to_bytes() # returneaza timestamp0, delta01, dod, dod, dod, ....


def decompress_timestamps(data: bytes, count: int) -> list[int]:
    """
    Decomprima timestamp-uri comprimate cu Delta-of-Delta.

    Args:
        data: Bytes comprimati
        count: Numarul de timestamp-uri de decomprimat

    Returns:
        Lista de timestamp-uri decomprimati

    Example:
        >>> compressed = compress_timestamps([1000, 1010, 1020, 1030, 1035])
        >>> original = decompress_timestamps(compressed, 5)
        >>> print(original)  # [1000, 1010, 1020, 1030, 1035]
    """
    reader = BitReader(data)
    decoder = TimestampDecoder(reader)

    timestamps = []
    for _ in range(count):
        timestamps.append(decoder.read_timestamp())

    return timestamps


# ============================================================================
# FUNCTII HELPER PENTRU ANALIZA COMPRESIEI
# ============================================================================

def analyze_compression(timestamps: list[int]) -> dict:
    """
    Analizeaza eficienta compresiei pentru o lista de timestamp-uri.

    Args:
        timestamps: Lista de timestamp-uri

    Returns:
        Dictionar cu statistici despre compresie
    """
    if not timestamps:
        return {
            "count": 0,
            "original_bytes": 0,
            "compressed_bytes": 0,
            "compression_ratio": 0,
            "bits_per_timestamp": 0,
        }

    # Comprima timestamp-urile
    compressed = compress_timestamps(timestamps)

    # Calculeaza statistici
    original_bytes = len(timestamps) * 8  # 8 bytes (64 biti) per timestamp  = cati bytes ocupa seria necomprimata
    compressed_bytes = len(compressed) # cati bytes ocupa seria comprimata
    compression_ratio = original_bytes / compressed_bytes if compressed_bytes > 0 else 0 # de cate ori mai eficient e sa comprim fata de a scrie timestamps complete
    bits_per_timestamp = (compressed_bytes * 8) / len(timestamps) # pe cati biti memorez un timestamp folosind comprimarea

    return {
        "count": len(timestamps),
        "original_bytes": original_bytes,
        "compressed_bytes": compressed_bytes,
        "compression_ratio": compression_ratio,
        "bits_per_timestamp": bits_per_timestamp,
        "savings_percent": (1 - compressed_bytes / original_bytes) * 100 if original_bytes > 0 else 0,
    }


if __name__ == "__main__":
    # Test rapid
    print("=" * 60)
    print("TEST: Compresia Timestamp-urilor (Delta-of-Delta)")
    print("=" * 60)

    # Test 1: Timestamp-uri perfect periodice
    print("\nTest 1: Timestamp-uri perfect periodice (interval=10)")
    timestamps = [1000 + i * 10 for i in range(100)]
    stats = analyze_compression(timestamps)
    print(f"  Timestamp-uri: {stats['count']}")
    print(f"  Original: {stats['original_bytes']} bytes")
    print(f"  Comprimat: {stats['compressed_bytes']} bytes")
    print(f"  Ratio: {stats['compression_ratio']:.2f}x")
    print(f"  Biti/timestamp: {stats['bits_per_timestamp']:.2f}")
    print(f"  Economie: {stats['savings_percent']:.1f}%")

    # Verificare round-trip
    compressed = compress_timestamps(timestamps)
    decompressed = decompress_timestamps(compressed, len(timestamps))
    assert timestamps == decompressed, "Round-trip failed!"
    print("  [OK] Round-trip test passed!")





    # Test 2: Timestamp-uri cu jitter mic
    print("\nTest 2: Timestamp-uri cu jitter mic")
    
#     Jitter = variație aleatorie, mică, neregulată în timing sau valori

# În contextul nostru (serii temporale), jitter-ul reprezintă devierile mici de la un pattern perfect periodic.
    import random
    random.seed(42)
    timestamps = [1000 + i * 10 + random.randint(-2, 2) for i in range(100)]
    stats = analyze_compression(timestamps)
    print(f"  Timestamp-uri: {stats['count']}")
    print(f"  Original: {stats['original_bytes']} bytes")
    print(f"  Comprimat: {stats['compressed_bytes']} bytes")
    print(f"  Ratio: {stats['compression_ratio']:.2f}x")
    print(f"  Biti/timestamp: {stats['bits_per_timestamp']:.2f}")
    print(f"  Economie: {stats['savings_percent']:.1f}%")

    compressed = compress_timestamps(timestamps)
    decompressed = decompress_timestamps(compressed, len(timestamps))
    assert timestamps == decompressed, "Round-trip failed!"
    print("  [OK] Round-trip test passed!")

    # Test 3: Timestamp-uri random (worst case)
    print("\nTest 3: Timestamp-uri random (worst case)")
    random.seed(123)
    timestamps = sorted([random.randint(0, 1000000) for _ in range(100)])
    stats = analyze_compression(timestamps)
    print(f"  Timestamp-uri: {stats['count']}")
    print(f"  Original: {stats['original_bytes']} bytes")
    print(f"  Comprimat: {stats['compressed_bytes']} bytes")
    print(f"  Ratio: {stats['compression_ratio']:.2f}x")
    print(f"  Biti/timestamp: {stats['bits_per_timestamp']:.2f}")
    print(f"  Economie: {stats['savings_percent']:.1f}%")

    compressed = compress_timestamps(timestamps)
    decompressed = decompress_timestamps(compressed, len(timestamps))
    assert timestamps == decompressed, "Round-trip failed!"
    print("  [OK] Round-trip test passed!")

    print("\n" + "=" * 60)
    print("TOATE TESTELE AU TRECUT!")
    print("=" * 60)
