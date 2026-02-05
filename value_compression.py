import struct
from BitWriter import BitWriter
from BitReader import BitReader

class ValueEncoder:
    __slots__ = ("_writer", "_prev_value_bits", "_prev_leading", "_prev_trailing", "_count")

    def __init__(self, writer: BitWriter):
        self._writer = writer
        self._prev_value_bits = 0
        self._prev_leading = 255
        self._prev_trailing = 255
        self._count = 0

    def _float_to_bits(self, val: float) -> int:
        return struct.unpack(">Q", struct.pack(">d", val))[0]

    def add_value(self, val: float) -> None:
        v_bits = self._float_to_bits(val)

        if self._count == 0:
            # Prima valoare se scrie mereu pe 64 de biti
            self._writer.write_u64(v_bits)
            self._prev_value_bits = v_bits
            self._count = 1
            return

        xor = v_bits ^ self._prev_value_bits

        if xor == 0:
            # Cazul ideal: valoarea este identica cu cea anterioara
            self._writer.write_bit(0)
        else:
            # Exista o diferenta: scriem bit de control '1'
            self._writer.write_bit(1)
            # Calculam leading si trailing zeros pentru a gasi bitii semnificativi
            bin_xor = bin(xor)[2:].zfill(64)
            leading = len(bin_xor) - len(bin_xor.lstrip('0'))
            trailing = len(bin_xor) - len(bin_xor.rstrip('0'))

            # Limitam la 31 pentru a incapea pe 5 biti (asa e in Gorilla)
            if leading > 31: leading = 31

            # Verificam daca putem refolosi fereastra anterioara de biti semnificativi
            if (self._prev_leading != 255 and
                leading >= self._prev_leading and
                trailing >= self._prev_trailing):

                # Bit control '0': refolosim fereastra
                self._writer.write_bit(0)
                meaningful_bits = 64 - self._prev_leading - self._prev_trailing
                self._writer.write_bits(xor >> self._prev_trailing, meaningful_bits)
            else:
                # Bit control '1': definim o fereastra noua
                self._writer.write_bit(1)
                self._writer.write_bits(leading, 5)

                meaningful_bits = 64 - leading - trailing
                # IMPORTANT: Stocam (meaningful_bits - 1) pe 6 biti
                # Aceasta permite reprezentarea valorilor 1-64 ca 0-63
                # (meaningful_bits e minim 1 cand XOR != 0)
                self._writer.write_bits(meaningful_bits - 1, 6)
                self._writer.write_bits(xor >> trailing, meaningful_bits)

                # Actualizam parametrii ferestrei pentru urmatoarea valoare
                self._prev_leading = leading
                self._prev_trailing = trailing

        self._prev_value_bits = v_bits
        self._count += 1

    def add_value_verification(self, val: float) -> None:
        v_bits = self._float_to_bits(val)

        if self._count == 0:
            # Prima valoare se scrie mereu pe 64 de biti
            self._writer.write_u64(v_bits)
            self._prev_value_bits = v_bits
            self._count = 1
            return

        xor = v_bits ^ self._prev_value_bits

        if xor == 0:
            # Cazul ideal: valoarea este identica cu cea anterioara
            self._writer.write_bit(0)
        else:
            # Exista o diferenta: scriem bit de control '1'
            self._writer.write_bit(1)
            # Calculam leading si trailing zeros pentru a gasi bitii semnificativi
            bin_xor = bin(xor)[2:].zfill(64)
            leading = len(bin_xor) - len(bin_xor.lstrip('0'))
            trailing = len(bin_xor) - len(bin_xor.rstrip('0'))

            # Limitam la 31 pentru a incapea pe 5 biti conform algoritmului
            if leading > 31: leading = 31

            # Verificam daca putem refolosi fereastra anterioara de biti semnificativi
            if (self._prev_leading != 255 and # Ma asigur ca calculez o fereastra pentru al doilea numar
                leading >= self._prev_leading and # Zerouri din fata al curentului - mai multe decat la cel anterior
                trailing >= self._prev_trailing and  # Zerourile din coada sunt mai multe decat cele ale anteriorului
                
                # Verificam daca fereastra anterioara este mai mare cu cel putin 11 biti fata de fereastra actuala
                # Daca da, atunci e mai eficient sa cream o fereastra noua
                not ((64 - self._prev_trailing - self._prev_leading) - ( 64 - trailing - leading) > 11)): 

                # Bit control '0': refolosim fereastra
                self._writer.write_bit(0)
                meaningful_bits = 64 - self._prev_leading - self._prev_trailing
                self._writer.write_bits(xor >> self._prev_trailing, meaningful_bits)
            else:
                # Bit control '1': definim o fereastra noua
                self._writer.write_bit(1)
                self._writer.write_bits(leading, 5)

                meaningful_bits = 64 - leading - trailing
                # Stocam (meaningful_bits - 1) pe 6 biti
                # Asta permite reprezentarea valorilor 1-64 ca 0-63
                # (meaningful_bits e minim 1 cand XOR != 0)
                self._writer.write_bits(meaningful_bits - 1, 6)
                self._writer.write_bits(xor >> trailing, meaningful_bits)

                # Actualizam parametrii ferestrei pentru urmatoarea valoare
                self._prev_leading = leading
                self._prev_trailing = trailing

        self._prev_value_bits = v_bits
        self._count += 1

class ValueDecoder:
    __slots__ = ("_reader", "_prev_value_bits", "_prev_leading", "_prev_trailing", "_count")

    def __init__(self, reader: BitReader):
        self._reader = reader
        self._prev_value_bits = 0
        self._prev_leading = 0
        self._prev_trailing = 0
        self._count = 0

    def _bits_to_float(self, bits: int) -> float:
        return struct.unpack(">d", struct.pack(">Q", bits & 0xFFFFFFFFFFFFFFFF))[0]

    def read_value(self) -> float:
        if self._count == 0:
            bits = self._reader.read_u64()
            self._prev_value_bits = bits
            self._count = 1
            return self._bits_to_float(bits)

        # Citim primul bit de control (Diferenta?)
        if self._reader.read_bit() == 0:
            # xor este 0 => valoarea e identica
            return self._bits_to_float(self._prev_value_bits)

        # Citim al doilea bit de control (refolosire sau nou?)
        if self._reader.read_bit() == 0:
            # Refolosim fereastra anterioara (cea salvata in self._prev_leading/trailing)
            meaningful_bits = 64 - self._prev_leading - self._prev_trailing
        else:
            # Definim o fereastra noua (citim 5 biti + 6 biti)
            self._prev_leading = self._reader.read_bits(5)
            length_bits = self._reader.read_bits(6)
            meaningful_bits = length_bits + 1
            # ACTUALIZAM trailing pentru a fi folosit la viitoarele refolosiri
            self._prev_trailing = 64 - self._prev_leading - meaningful_bits

        # Citim bitii semnificativi
        if meaningful_bits < 0 or meaningful_bits > 64:
            # s-a pierdut alinierea bitilor
            raise ValueError(f"Eroare: meaningful_bits invalid ({meaningful_bits})")

        xor_val = self._reader.read_bits(meaningful_bits)
        xor_val <<= self._prev_trailing

        current_bits = self._prev_value_bits ^ xor_val
        self._prev_value_bits = current_bits
        self._count += 1
        return self._bits_to_float(current_bits)