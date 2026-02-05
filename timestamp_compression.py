# Compresia Timestamp-urilor folosind algoritmul Delta-of-Delta din Gorilla
#
# CONCEPTUL DELTA-OF-DELTA:
# -------------------------
# Seriile de timp au de obicei timestamp-uri cvasi periodice:
#     t0=1000, t1=1010, t2=1020, t3=1030, t4=1035
#
# Delta  = diferenta dintre 2 timestampuri consecutive
#     delta1 = t1-t0 = 10
#     delta2 = t2-t1 = 10
#     delta3 = t3-t2 = 10
#     delta4 = t4-t3 = 5
#
# Delta-of-delta (diferenta intre delta-uri consecutive):
#     delta_of_delta1 = delta1 - 0      = 10  (primul delta comparat cu 0)
#     delta_of_delta2 = delta2 - delta1 = 0   (perfect periodic)
#     delta_of_delta3 = delta3 - delta2 = 0   (perfect periodic)
#     delta_of_delta4 = delta4 - delta3 = -5  (deviatie mica)
#
# OBS: Daca timestamp-urile sunt periodice, delta-of-delta = 0 foarte des => Putem comprima 0 ca un singur bit: "0"
#
# CODARE VARIABILA din GORILLA:
# delta_of_delta = 0:             scriem "0"                    (1 bit)
# delta_of_delta in [-63, 64]:   scriem "10" + 7 biti            (9 biti)
# delta_of_delta in [-255, 256]: scriem "110" + 9 biti           (12 biti)
# delta_of_delta in [-2047, 2048]: scriem "1110" + 12 biti         (16 biti)
# altfel:                          scriem "1111" + 32 biti       (36 biti)


from BitWriter import BitWriter
from BitReader import BitReader


# Encoder pentru compresia timestamp-urilor folosind Delta-of-Delta
class TimestampEncoder:

    __slots__ = ("_writer", "_prev_timestamp", "_prev_delta", "_count")

    def __init__(self, writer: BitWriter):
        self._writer = writer
        self._prev_timestamp = None  # timestamp-ul anterior (pentru calculul delta)
        self._prev_delta = None      # delta anterior (pentru calculul delta-of-delta)
        self._count = 0              # numarul de timestamp-uri adaugate pana acum
        
    def _encode_delta_of_delta(self, dod: int) -> None:
        if dod == 0:  # dif intre delta_cur - delta_ant = 0
            # Cazul cel mai frecvent: timestamp perfect periodic
            self._writer.write_bit(0)

        elif -64 <= dod <= 63:
            # Deviatie mica: 2 biti control + 7 biti valoare
            # 7 biti signed: [-64, 63]
            self._writer.write_bit(1)
            self._writer.write_bit(0)
            self._writer.write_signed(dod, 7)

        elif -256 <= dod <= 255:
            # Deviatie medie: 3 biti control + 9 biti valoare
            # 9 biti signed: [-256, 255]
            self._writer.write_bit(1)
            self._writer.write_bit(1)
            self._writer.write_bit(0)
            self._writer.write_signed(dod, 9)

        elif -2048 <= dod <= 2047:
            # Deviatie mare: 4 biti control + 12 biti valoare
            # 12 biti signed: [-2048, 2047]
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

    def add_timestamp(self, timestamp: int) -> None: # Adauga un timestamp in flux si il comprima (de fapt, scrie delta of delta)
        if self._count == 0: # Primul timestamp- il scriem complet (64 biti)
            self._writer.write_i64(timestamp)
            self._prev_timestamp = timestamp
            self._count = 1
            return

        delta = timestamp - self._prev_timestamp # Calculam delta (diferenta fata de timestamp-ul anterior)

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

        # Codare variabila
        self._encode_delta_of_delta(delta_of_delta)

        # Actualizam starea
        self._prev_timestamp = timestamp
        self._prev_delta = delta
        self._count += 1
        


    @property # Decorator care imi permite sa definesc o metoda, dar sa o accesez ca si cum ar fi un atribut public al clasei
    # Returneaza numarul de timestamp-uri adaugate
    def count(self) -> int:
        return self._count


# Decoder pentru decompresarea timestamp-urilor comprimate cu Delta-of-Delta
class TimestampDecoder:

    __slots__ = ("_reader", "_prev_timestamp", "_prev_delta", "_count")

    def __init__(self, reader: BitReader):
        self._reader = reader
        self._prev_timestamp = None
        self._prev_delta = None
        self._count = 0 # cate timestampuri am citit deja
        

    # - dod == 0:              "0"                (1 bit)
    # - dod in [-63, 64]:      "10" + 7 biti       (9 biti)
    # - dod in [-255, 256]:    "110" + 9 biti       (12 biti)
    # - dod in [-2047, 2048]:  "1110" + 12 biti       (16 biti)
    # - altfel:                "1111" + 32 biti       (36 biti)
    def _decode_delta_of_delta(self) -> int:
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


    # Citeste si decomprima urmatorul timestamp
    def read_timestamp(self) -> int:
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

        delta = self._prev_delta + delta_of_delta
        timestamp = self._prev_timestamp + delta

        self._prev_timestamp = timestamp
        self._prev_delta = delta
        self._count += 1

        return timestamp

    @property
    # Returneaza numarul de timestamp-uri citite
    def count(self) -> int:
        return self._count



