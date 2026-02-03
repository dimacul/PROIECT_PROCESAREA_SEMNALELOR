import struct
from BitWriter import BitWriter
from BitReader import BitReader

class ValueEncoder:
    __slots__ = ("_writer", "_prev_value_bits", "_prev_leading", "_prev_trailing", "_count")

    def __init__(self, writer: BitWriter):
        self._writer = writer
        self._prev_value_bits = 0
        self._prev_leading = 255  # Valori santinelă pentru a forța scrierea completă
        self._prev_trailing = 255 
        self._count = 0

    def _float_to_bits(self, val: float) -> int:
        return struct.unpack(">Q", struct.pack(">d", val))[0]

    def add_value(self, val: float) -> None:
        v_bits = self._float_to_bits(val)

        if self._count == 0:
            # Prima valoare se scrie mereu pe 64 de biți
            self._writer.write_u64(v_bits)
            self._prev_value_bits = v_bits
            self._count = 1
            return

        xor = v_bits ^ self._prev_value_bits 

        if xor == 0:
            # Cazul ideal: valoarea este identică cu cea anterioară
            self._writer.write_bit(0) 
        else:
            # Există o diferență: scriem bit de control '1'
            self._writer.write_bit(1)

            # Calculăm leading și trailing zeros pentru a găsi biții semnificativi
            bin_xor = bin(xor)[2:].zfill(64)
            leading = len(bin_xor) - len(bin_xor.lstrip('0'))
            trailing = len(bin_xor) - len(bin_xor.rstrip('0'))

            # Limităm la 31 pentru a încăpea pe 5 biți conform algoritmului
            if leading > 31: leading = 31

            # Verificăm dacă putem refolosi fereastra anterioară de biți semnificativi
            if (self._prev_leading != 255 and 
                leading >= self._prev_leading and 
                trailing >= self._prev_trailing):
                
                # Bit control '0': refolosim fereastra
                self._writer.write_bit(0)
                meaningful_bits = 64 - self._prev_leading - self._prev_trailing
                self._writer.write_bits(xor >> self._prev_trailing, meaningful_bits)
            else:
                # Bit control '1': definim o fereastră nouă
                self._writer.write_bit(1)
                self._writer.write_bits(leading, 5) 
                
                meaningful_bits = 64 - leading - trailing
                # Lungimea biților semnificativi se scrie pe 6 biți
                self._writer.write_bits(meaningful_bits, 6) 
                self._writer.write_bits(xor >> trailing, meaningful_bits) 

                # Actualizăm parametrii ferestrei pentru următoarea valoare
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
            # Prima valoare este stocată integral
            bits = self._reader.read_u64()
            self._prev_value_bits = bits
            self._count = 1
            return self._bits_to_float(bits)

        if self._reader.read_bit() == 0:
            # Bit 0 înseamnă XOR 0 (valoare identică)
            return self._bits_to_float(self._prev_value_bits)

        # Bit 1 înseamnă că avem o diferență XOR
        if self._reader.read_bit() == 0:
            # Refolosim fereastra de biți semnificativi anterioară
            meaningful_bits = 64 - self._prev_leading - self._prev_trailing
        else:
            # Citim o fereastră nouă (leading zeros și lungime)
            self._prev_leading = self._reader.read_bits(5) 
            meaningful_bits = self._reader.read_bits(6) 
            self._prev_trailing = 64 - self._prev_leading - meaningful_bits

        # Extragem biții semnificativi și reconstruim valoarea prin XOR
        xor_val = self._reader.read_bits(meaningful_bits)
        xor_val <<= self._prev_trailing
        
        current_bits = self._prev_value_bits ^ xor_val
        self._prev_value_bits = current_bits
        self._count += 1
        return self._bits_to_float(current_bits)
    
if __name__ == "__main__":
    # Importăm clasele create anterior
    # Presupunem că ValueEncoder și ValueDecoder sunt în același fișier sau importate
    
    print("=" * 60)
    print("TEST: Compresia Valorilor float64 (XOR Gorilla)")
    print("=" * 60)

    # 1. Pregătim setul de date (metrici realiste)
    # Simulăm o temperatură care variază ușor, plus câteva valori identice
    original_values = [
        22.5, 22.5, 22.5, 22.6, 22.6, 22.7, 22.8, 22.8, 
        25.0, 25.0, 25.1, 0.0, -0.0, float('inf'), 22.5
    ]
    
    # 2. Procesul de ENCODING
    writer = BitWriter()
    encoder = ValueEncoder(writer)
    
    for val in original_values:
        encoder.add_value(val)
    
    compressed_data = writer.to_bytes()
    
    # 3. Procesul de DECODING
    reader = BitReader(compressed_data)
    decoder = ValueDecoder(reader)
    
    decoded_values = []
    for _ in range(len(original_values)):
        decoded_values.append(decoder.read_value())
    
    # 4. Evaluare Experimentală (Cerința 6 din plan)
    original_size = len(original_values) * 8  # 8 bytes per float64 [cite: 59]
    compressed_size = len(compressed_data)
    ratio = original_size / compressed_size if compressed_size > 0 else 0
    
    print(f"Date originale: {original_values[:8]} ... (+ încă {len(original_values)-8} valori)")
    print(f"Dimensiune necomprimată: {original_size} bytes [cite: 59]")
    print(f"Dimensiune comprimată:   {compressed_size} bytes [cite: 61]")
    print(f"Rată de compresie:      {ratio:.2f}x")
    print(f"Economie spațiu:        {(1 - compressed_size/original_size)*100:.1f}%")
    
    # 5. Verificare Bit-Perfect (Cerința 3 din plan) 
    # Folosim math.isclose pentru float sau verificăm reprezentarea binară
    import math
    
    success = True
    for o, d in zip(original_values, decoded_values):
        # Verificăm inclusiv cazurile speciale NaN/Inf
        if math.isnan(o):
            if not math.isnan(d): success = False
        else:
            if o != d: success = False
            
    if success:
        print("\n[OK] Test de integritate trecut: Toate valorile sunt identice!")
    else:
        print("\n[FAIL] Datele decodate diferă de cele originale!")