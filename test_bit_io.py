"""
Teste pentru BitWriter si BitReader.
Verificam ca operatiile de scriere/citire bit cu bit functioneaza corect (round-trip tests).
"""

from BitWriter import BitWriter, to_twos_complement
from BitReader import BitReader


def test_single_bit():
    """Test: scriere si citire de biti individuali"""
    print("Test 1: Single bit write/read...")

    bw = BitWriter()
    bw.write_bit(1)
    bw.write_bit(0)
    bw.write_bit(1)
    bw.write_bit(1)
    bw.write_bit(0)

    data = bw.to_bytes()
    br = BitReader(data)

    assert br.read_bit() == 1, "Primul bit ar trebui sa fie 1"
    assert br.read_bit() == 0, "Al doilea bit ar trebui sa fie 0"
    assert br.read_bit() == 1, "Al treilea bit ar trebui sa fie 1"
    assert br.read_bit() == 1, "Al patrulea bit ar trebui sa fie 1"
    assert br.read_bit() == 0, "Al cincilea bit ar trebui sa fie 0"

    print("[OK] Single bit test passed!")


def test_write_read_bits():
    """Test: scriere si citire de grupuri de biti"""
    print("\nTest 2: Multi-bit write/read...")

    bw = BitWriter()
    bw.write_bits(5, 3)    # 101 (3 biti)
    bw.write_bits(7, 3)    # 111 (3 biti)
    bw.write_bits(0, 2)    # 00  (2 biti)

    data = bw.to_bytes()
    br = BitReader(data)

    assert br.read_bits(3) == 5, "Primii 3 biti ar trebui sa fie 5 (101)"
    assert br.read_bits(3) == 7, "Urmatorii 3 biti ar trebui sa fie 7 (111)"
    assert br.read_bits(2) == 0, "Ultimii 2 biti ar trebui sa fie 0 (00)"

    print("[OK] Multi-bit test passed!")


def test_signed_numbers():
    """Test: scriere si citire de numere cu semn (Two's Complement)"""
    print("\nTest 3: Signed numbers (Two's Complement)...")

    test_cases = [
        (0, 8),
        (5, 8),
        (-5, 8),
        (127, 8),
        (-128, 8),
        (10, 5),
        (-10, 5),
    ]

    for value, bits in test_cases:
        bw = BitWriter()
        bw.write_signed(value, bits)

        data = bw.to_bytes()
        br = BitReader(data)

        result = br.read_signed(bits)
        assert result == value, f"Expected {value}, got {result} (on {bits} bits)"

    print("[OK] Signed numbers test passed!")


def test_alignment():
    """Test: alinierea la byte (align_to_byte)"""
    print("\nTest 4: Byte alignment...")

    bw = BitWriter()
    bw.write_bits(7, 3)    # 111 (3 biti) -> _cur = 111, _nbits = 3
    bw.align_to_byte()      # Completeaza cu 0: 11100000 -> adauga in buf
    bw.write_bits(255, 8)  # 11111111 (byte complet)

    data = bw.to_bytes()

    # Ar trebui sa avem 2 bytes: 11100000 si 11111111
    assert len(data) == 2, f"Expected 2 bytes, got {len(data)}"
    assert data[0] == 0b11100000, f"First byte should be 0b11100000 (224), got {data[0]}"
    assert data[1] == 0b11111111, f"Second byte should be 0b11111111 (255), got {data[1]}"

    print("[OK] Byte alignment test passed!")


def test_u32_i64_u64():
    """Test: scriere si citire de intregi mari (u32, i64, u64)"""
    print("\nTest 5: Large integers (u32, i64, u64)...")

    bw = BitWriter()
    bw.write_u32(0x12345678)
    bw.write_i64(-1234567890)
    bw.write_u64(0xFEDCBA9876543210)

    data = bw.to_bytes()
    br = BitReader(data)

    assert br.read_u32() == 0x12345678, "u32 mismatch"
    assert br.read_i64() == -1234567890, "i64 mismatch"
    assert br.read_u64() == 0xFEDCBA9876543210, "u64 mismatch"

    print("[OK] Large integers test passed!")


def test_mixed_operations():
    """Test: mix de operatii (biti + bytes + intregi)"""
    print("\nTest 6: Mixed operations...")

    bw = BitWriter()
    bw.write_bit(1)
    bw.write_bits(5, 3)      # 101
    bw.write_signed(-7, 4)   # Two's complement pe 4 biti
    bw.align_to_byte()
    bw.write_u32(42)
    bw.write_bits(15, 4)     # 1111

    data = bw.to_bytes()
    br = BitReader(data)

    assert br.read_bit() == 1
    assert br.read_bits(3) == 5
    assert br.read_signed(4) == -7
    br.align_to_byte()
    assert br.read_u32() == 42
    assert br.read_bits(4) == 15

    print("[OK] Mixed operations test passed!")


def test_edge_cases():
    """Test: cazuri extrema (0, max values, etc.)"""
    print("\nTest 7: Edge cases...")

    # Test 0 bits
    bw = BitWriter()
    bw.write_bits(999, 0)  # Nu ar trebui sa scrie nimic
    assert bw.bit_length() == 0

    # Test 1 bit
    bw = BitWriter()
    bw.write_bits(1, 1)
    assert bw.bit_length() == 1

    # Test 64 de biti
    bw = BitWriter()
    bw.write_bits(0xFFFFFFFFFFFFFFFF, 64)
    data = bw.to_bytes()
    br = BitReader(data)
    assert br.read_bits(64) == 0xFFFFFFFFFFFFFFFF

    # Test valori mari trunchiare
    bw = BitWriter()
    bw.write_bits(0xFF, 4)  # 11111111 -> trunchiază la 1111
    data = bw.to_bytes()
    br = BitReader(data)
    assert br.read_bits(4) == 0x0F  # 1111 = 15

    print("[OK] Edge cases test passed!")


def test_peek_bit():
    """Test: peek_bit (citire fara avansare cursor)"""
    print("\nTest 8: Peek bit...")

    bw = BitWriter()
    bw.write_bits(0b10110011, 8)

    data = bw.to_bytes()
    br = BitReader(data)

    # Peek ar trebui sa returneze primul bit fara sa avanseze
    assert br.peek_bit() == 1
    assert br.peek_bit() == 1  # Tot 1, nu a avansat

    # Acum citim efectiv
    assert br.read_bit() == 1
    assert br.peek_bit() == 0  # Al doilea bit
    assert br.read_bit() == 0

    print("[OK] Peek bit test passed!")


def test_bits_remaining():
    """Test: bits_remaining property"""
    print("\nTest 9: Bits remaining...")

    bw = BitWriter()
    bw.write_bits(0xFF, 8)
    bw.write_bits(0xAA, 8)

    data = bw.to_bytes()
    br = BitReader(data)

    assert br.bits_remaining == 16
    br.read_bits(5)
    assert br.bits_remaining == 11
    br.read_bits(11)
    assert br.bits_remaining == 0

    print("[OK] Bits remaining test passed!")


def test_reserve_and_patch():
    """Test: reserve_u32 si patch_u32"""
    print("\nTest 10: Reserve and patch u32...")

    bw = BitWriter()
    bw.write_u32(100)
    offset = bw.reserve_u32()  # Rezerva spatiu
    bw.write_u32(200)

    # Acum patch-uim valoarea rezervata
    bw.patch_u32(offset, 999)

    data = bw.to_bytes()
    br = BitReader(data)

    assert br.read_u32() == 100
    assert br.read_u32() == 999  # Valoarea patch-uita
    assert br.read_u32() == 200

    print("[OK] Reserve and patch test passed!")


def test_twos_complement_function():
    """Test: functia to_twos_complement"""
    print("\nTest 11: Two's complement function...")

    assert to_twos_complement(0, 8) == 0
    assert to_twos_complement(5, 8) == 5
    assert to_twos_complement(-1, 8) == 255  # 11111111
    assert to_twos_complement(-128, 8) == 128  # 10000000
    assert to_twos_complement(127, 8) == 127
    assert to_twos_complement(-10, 5) == 22  # 10110 (ca in comentariu)

    print("[OK] Two's complement function test passed!")


def test_eof_handling():
    """Test: gestionarea EOF (End of File)"""
    print("\nTest 12: EOF handling...")

    bw = BitWriter()
    bw.write_bits(0xFF, 8)

    data = bw.to_bytes()
    br = BitReader(data)

    br.read_bits(8)  # Citim toti bitii

    try:
        br.read_bit()  # Ar trebui sa dea EOFError
        assert False, "Ar trebui sa arunce EOFError"
    except EOFError:
        pass  # Corect

    print("[OK] EOF handling test passed!")


def run_all_tests():
    """Ruleaza toate testele"""
    print("=" * 60)
    print("RULARE TESTE BitWriter & BitReader")
    print("=" * 60)

    tests = [
        test_single_bit,
        test_write_read_bits,
        test_signed_numbers,
        test_alignment,
        test_u32_i64_u64,
        test_mixed_operations,
        test_edge_cases,
        test_peek_bit,
        test_bits_remaining,
        test_reserve_and_patch,
        test_twos_complement_function,
        test_eof_handling,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"[FAIL] {test.__name__} FAILED: {e}")
            failed += 1
        except Exception as e:
            print(f"[FAIL] {test.__name__} ERROR: {e}")
            failed += 1

    print("\n" + "=" * 60)
    print(f"REZULTATE: {passed} passed, {failed} failed")
    print("=" * 60)

    if failed == 0:
        print("\nGOOD TOATE TESTELE AU TRECUT! BitWriter si BitReader functioneaza corect!")
    else:
        print(f"\nBAD  {failed} teste au esuat. Verifica implementarea.")

    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)
