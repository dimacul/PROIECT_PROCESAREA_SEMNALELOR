import struct

# Clasa complementara pentru BitWriter
# Permite citirea bit cu bit sau in grupuri de n biti dintr-un flux de bytes
class BitReader:
    __slots__ = ("_data", "_byte_pos", "_bit_pos")

    def __init__(self, data: bytes):
        self._data = data        # Sursa de date (bytes imutabili)
        self._byte_pos = 0       # Indexul byte-ului curent din _data
        self._bit_pos = 0        # Indexul bitului curent din byte-ul curent (0 la 7)

    # Citeste un singur bit si returneaza 0 sau 1
    def read_bit(self) -> int:
        if self._byte_pos >= len(self._data):
            raise EOFError("S-a atins sfarsitul fluxului de date (End of Stream).")

        # Extragem bitul de la pozitia curenta (MSB first)
        # Shiftam byte-ul la dreapta astfel incat bitul dorit sa ajunga pe ultima pozitie
        bit = (self._data[self._byte_pos] >> (7 - self._bit_pos)) & 1

        self._bit_pos += 1
        if self._bit_pos == 8:
            self._bit_pos = 0
            self._byte_pos += 1

        return bit

    # Citeste n biti si ii returneaza ca un singur intreg
    def read_bits(self, n: int) -> int:
        if n < 0:
            raise ValueError(f"Numarul de biti de citit trebuie sa fie >= 0. Am primit: {n}")
        if n == 0:
            return 0

        val = 0
        for _ in range(n):
            val = (val << 1) | self.read_bit()
        return val

    # Citeste un numar cu semn (Two's Complement) pe un numar fix de biti
    def read_signed(self, bits: int) -> int:
        val = self.read_bits(bits)
        # Daca bitul cel mai semnificativ (MSB) este 1, numarul este negativ
        if val & (1 << (bits - 1)):
            val -= (1 << bits)
        return val

    # Sare peste bitii ramasi din byte-ul curent si trece la urmatorul byte intreg
    def align_to_byte(self) -> None:
        if self._bit_pos > 0:
            self._bit_pos = 0
            self._byte_pos += 1

    # Citeste un unsigned int pe 32 biti (aliniat la byte)
    def read_u32(self) -> int:
        self.align_to_byte()
        res = struct.unpack(">I", self._data[self._byte_pos:self._byte_pos + 4])[0]
        self._byte_pos += 4
        return res

    # Citeste un signed int pe 64 biti (aliniat la byte)
    def read_i64(self) -> int:
        self.align_to_byte()
        res = struct.unpack(">q", self._data[self._byte_pos:self._byte_pos + 8])[0]
        self._byte_pos += 8
        return res

    # Citeste un unsigned int pe 64 biti (aliniat la byte)
    def read_u64(self) -> int:
        self.align_to_byte()
        res = struct.unpack(">Q", self._data[self._byte_pos:self._byte_pos + 8])[0]
        self._byte_pos += 8
        return res

    # Citeste un bit fara a avansa cursorul (util pentru headere de control)
    def peek_bit(self) -> int:
        if self._byte_pos >= len(self._data):
            raise EOFError("End of Stream")
        return (self._data[self._byte_pos] >> (7 - self._bit_pos)) & 1

    # Citeste n bytes (aliniat la byte)
    def read_bytes(self, n: int) -> bytes:
        self.align_to_byte()
        if self._byte_pos + n > len(self._data):
            raise EOFError(f"Nu sunt suficienti bytes: ceruti {n}, disponibili {len(self._data) - self._byte_pos}")
        res = self._data[self._byte_pos:self._byte_pos + n]
        self._byte_pos += n
        return res

    @property
    # Returneaza cati biti mai sunt disponibili in flux
    def bits_remaining(self) -> int:
        return (len(self._data) - self._byte_pos) * 8 - self._bit_pos