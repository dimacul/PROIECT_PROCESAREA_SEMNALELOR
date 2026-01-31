#------------------------------------------------------------------------------------------------------------------------------------
# In Gorilla, aproape toate campurile sunt codate pe lungimi nealiniate la byte, adica:
# - uneori am nevoie sa scriu in memorie un singur bit
# - alteori 5, sau 7, sau 12 (deci nu multipli de 8b = 1 byte)

# Daca as scrie pe cate un byte in loc sa scriu punctual pe cati biti am nevoie, as irosi spatiu.
# De aceea, am nevoie de un BitWriter care sa imi permita sa scriu secvente de biti de lungime arbitrara intr-un buffer de tip bytearray.
#------------------------------------------------------------------------------------------------------------------------------------
# IMPORTANT:

# In aceasta implementare: MSB first!
# Adica: ex: cifra 6 e scrisa pe biti ca 1 1 0 (cel mai semnificativ bit e cel din stanga - aici: 1).
#------------------------------------------------------------------------------------------------------------------------------------

from __future__ import annotations
import struct
from typing import Optional

MASK64 = (1 << 64) - 1 #2**64 - 1 = 111....111 de 64 de ori


def to_twos_complement(x: int, num_bits: int) -> int: # primeste ca argument un intreg (+ sau -) si un numar de biti "num_bits"
                                                    # si reprezinta intregul pe "num_bits" biti ca two's complement
                                                    # MSB (primul bit de la stanga) e bitul de semn
                                                    
    # exemplu:
    # x = -10 pe 5 biti
    # Obs: boundaries reprezentare numere cu semn pe 5 biti: [-2^(num_bits-1) , 2^(num_bits - 1) - 1] = [-2^4, 2^4 - 1]
    # twos_complement(x, pe 5 biti) = twos_complement(-10, pe 5 biti) = 1 << 5 + x = 2^5 + (-10) = 32 - 10 = 22 = 10110
    
    if num_bits <= 0:
        raise ValueError(f"Numarul de biti pentru reprezentarea lui {x} in two's complement trebuie sa fie >0. Am primit: num_bits={num_bits} !")
    
    minv = -(1 << (num_bits - 1)) # cea mai mica valoare care se poate reprezenta pe num_bits biti
    maxv = (1 << (num_bits - 1)) - 1 # cea mai mare
    if x < minv or x > maxv:
        raise ValueError(f"{x} nu incape pe {num_bits} biti in reprezentarea two's complement !")
    if x < 0:
        x = (1 << num_bits) + x
    return x


class BitWriter:
    # clasa care se ocupa cu scriere bit cu bit intr-un bytearray
    
    # _numeVariabila <=> „acest atribut este intern / privat, nu face parte din API-ul public” al clasei

    __slots__ = ("_buf", "_cur", "_nbits")
    
    # Ce este __slots__ ?
    # = class attribute citita de interpretorul Python la definirea clasei. Ea spune acestuia ce atribute vor avea instantele clasei.
    # = mecanism de control al memoriei si al modelului de obiecte, definit la nivel de clasa.
    
    # La noi, __slots__ = (buf, cur, nbits) spune interpretorului ca:
    #       - un obiect de tipul BitWriter poate avea doar aceste atribute si nici unul in plus.
    #       - un obiect de tipul BitWriter nu va avea __dict__ 
    
    # Implicit, fiecare obiect Python are un:    
    #       obj.__dict__   =  un dictionar ce mapeaza numele fiecarui atribut al clasei la valoarea sa
    # 
    # Fara __slots__ specificat:
    # EXEMPLU:
    
    # class A:
    #     pass
    
    # a = A()
    # a.x = 10
    # a.y = 20
    
    # Intern, Python creeaza variabilele de clasa pe masura ce le adaug dinamic si intern ajung sa am:
    # a.__dict__ == {"x": 10, "y": 20}

    # Problemele acestui model
    #  - accesul la atribute implică lookup in dict
    #  - fiecare obiect are overhead mare de memorie
    #  - putem adauga atribute din greseala (typos)
    
    # Cu __slots__ specificat, la crearea obiectului, Python nu ii mai atribuie un __dict__, ci:
    # - alocă spațiu fix pentru atribute, ca in C
    # - accesul la câmpuri devine mai rapid (nu mai am lookups in acel dicTionar)
    # - memoria necesara per obiect scade drastic
    
    # De ce e important pentru noi?
    # Pentru ca obiectul nostru BitWriter va fi folosit extrem de des
    #  - Fara slots, ar trebui sa am lookup in dictionar de fiecare data cand accesez un atribut, ceea ce ar scadea performanta => EFICIENTA DPDV TIMP DE EXECUTIE
    #  - Pt ca folosim __slots__, obiectul meu BitWriter e mai compact (limitat in memorie)                                     => EFICIENTA DPDV MEMORIE
    #  - Pt ca folosim __slots__, obiectul meu nu poate primi atribute noi => un typo al unui atribut al clasei va fi detectat imediat => PROTECTIE BUGS
    #           ex: bw._nbtis = 5   # typo!
    #           FARA slots: Python ar crea un atribut nou _nbtis
    #           CU   slots: AttributeError: 'BitWriter' object has no attribute '_nbtis'



    def __init__(self, initial_capacity: int = 0):
        
        self._buf = bytearray() # un bytearray care contine toti bytes-ii finalizati complet (8 biti) scrisi pana in acel moment
                                # fiecare element din _buf e un byte complet, stabil
                                # nu contine niciodata biti in asteptare
                                
                                # De ce bytearray:
                                    # mutabil (pot adauga efic. bytes)
                                    # mai eficient decât list[int]
                                    # usor de convertit la bytes la final
                                    
        if initial_capacity > 0: # daca stiu ca voi scrie mult in bufferul de bytes,

            self._buf.extend(b"\x00" * initial_capacity) # pre-aloc memorie extinzand cu zerouri fara a-i creste lungimea logica
            del self._buf[:]                             # bufferul e din nou gol, dar capacitatea interna ramane
            
        self._cur = 0 # (int) -> reprezinta byte-ul curent partial completat, 
                        # in care se acumuleaza biti inainte de a forma un byte complet
                        # Stocheaza temporar biti pana ajung la 8 biti
                        
        self._nbits = 0 # numarul de biti scrisi in _cur (valori posibile: 0 -> 7)
        
        # Cand _nbits == 8:
        #  - _cur este complet => este mutat în _buf
        #  - _cur si _nbits sunt resetate

    def bit_length(self) -> int: #returneaza numarul de biti scrisi pana la momentul respectiv
        return len(self._buf) * 8 + self._nbits # numarul de bytes completi * 8 biti per byte   +    nr biti care nu formeaza inca un byte complet

    def byte_length(self) -> int: # returneaza numarul de bytes pe care i-ar avea iesirea acum, daca as inchide fluxul
        return len(self._buf) + (1 if self._nbits else 0) # numerul de bytes deja umpluti complet + 1 (daca exista un byte partial in lucru)

    def write_bit(self, bit: int) -> None: # scrie un singur bit (0 sau 1)
        
        self._cur = (self._cur << 1) | (bit & 1) # mut bitii deja scrisi cu o pozitie la stanga si fac "SAU pe biti" cu valoarea de 0 sau 1 pe care vreau sa o adaug
                                                # => valoarea noua e adaugata pe ultima pozitie din drepata (cea pe care am eliberat-o)
        self._nbits += 1 # actualizez numarul de biti completati ai byte-ului in lucru
        if self._nbits == 8: # daca am ajuns la byte complet, bag in buffer si resetez _curr si _nbits
            self._buf.append(self._cur & 0xFF)
            self._cur = 0
            self._nbits = 0

    # ok de folosit cand numarul pe care vreau sa-l scriu incape pe 1B
    def write_bits(self, x: int, n: int) -> None: #scrie exact n biti din numarul x in fluxul de iesire, 
                                                    # in ordine MSB-first (de la bitul cel mai semnificativ la cel mai putin semnificativ)
        
        if n < 0: # nu are sens sa scriu un numar negativ de biti
            raise ValueError(f"Numarul de biti doriti a fi scrisi trebuie sa fie > 0. Am primit: n={n} !")
        if n == 0: # nu am nimic de scris
            return
        if x < 0:
            raise ValueError(f"x trebuie sa fie pozitiv pentru a fi argument valid al metodei. Am primit: x={x} (Foloseste write_signed!).")
        
        if x.bit_length() > n: #daca x are mai mult de n biti, pastrez doar cei mai putin semnificativi n biti din x
            x = x & (1 << n) - 1
                    # 2^n -1 = 111....111 de n ori
                # and pe biti care imi da ultimii n biti din x

        for i in range(n - 1, -1, -1): #parcurg bitii lui x de la stg la dr
            bit = (x >> i) & 1 # bitul curent din x
            self._cur = (self._cur << 1) | bit # il mut pe cur cu o poz spre stg pt a face loc noului bit si adaug bitul din x
            self._nbits += 1
            if self._nbits == 8: # daca am umplut byte-ul, adaug in buf si resetez
                self._buf.append(self._cur & 0xFF)
                self._cur = 0
                self._nbits = 0

    def write_signed(self, x: int, bits: int) -> None: # scrie un numar cu semn (x) in flux, pe un numar fix de "bits" biti
        self.write_bits(to_twos_complement(x, bits), bits)

    def align_to_byte(self) -> None: #completeaza byte-ul in lucru cu zerouri, ex: 111|00000 si il impinge in buff
        if self._nbits: # daca am byte inclomplet in lucru
            self._cur = self._cur << (8 - self._nbits) # shift la stanga cu cati biti mai aveam de completat si pun zerouri pe acele pozitii din dreapta
            self._buf.append(self._cur & 0xFF)
            self._cur = 0
            self._nbits = 0

    def write_bytes(self, secventa_bytes: bytes) -> None: # scrie o secventa de bytes (secventa_bytes) in flux
        
        if not secventa_bytes:
            return
        self.align_to_byte()                                # dar se asigura intai ca incep de la marginea stanga a unui byte gol
                                                            # adica impinge in buf byte-ul incomplet (daca exista)
        self._buf.extend(secventa_bytes)


#-----------------------------------------------------------------------------------------------------------------------------------
# struct.pack() = fctie din modulul standard struct care transforma valori Python in bytes, 
# conform unui format binar specificat explicit.

# struct.pack(format, value1, value2, ...)
#-----------------------------------------------------------------------------------------------------------------------------------

    def write_u32(self, x: int) -> None: # scrie un unsigned int pe 32 de biti
        self.write_bytes(struct.pack(">I", x & 0xFFFFFFFF))
        
        # ">I" = big-endian, unsigned 32-bit.

        # and pe biti cu 0xFFFFFFFF pastreaza doar ultimii 32 de biti din scrierea pe biti a lui x
        #      - forțează valoarea să fie interpretată ca unsigned
        #      - taie tot ce depășește 32 de biți
        
        # Ex: write_u32(10) scrie bytes-ii: 00 00 00 0A.

    def write_i64(self, x: int) -> None: # scrie un signed int pe 64 de biti
        self.write_bytes(struct.pack(">q", int(x)))
        
        #">q" = big-endian, signed 64-bit

    def write_u64(self, x: int) -> None: #scrie un unsigned int pe 64 biti
        self.write_bytes(struct.pack(">Q", x & MASK64))

    def reserve_u32(self) -> int: #rezerva un spatiu de 4 bytes (32 biti) in flux, pe care il voi completa mai tarziu, si imi spune unde e acel spatiu
        # las loc liber pentru un uint32 care va fi scris ulterior 

        self.align_to_byte() # imping byte-ul curent necompletat in buf (ma aliniez la granita de byte)
        offset = len(self._buf) # retin pozitia curenta in flux
        # offset = indexul (in bytes) din buffer unde începe campul rezervat
        self._buf.extend(b"\x00\x00\x00\x00") # adaug 4 bytes de zero pentru uint32-ul ce va veni aici mai tarziu
        return offset

    def patch_u32(self, offset: int, x: int) -> None: #completare spatiu rezervat pentru un uint32 cu un uint32
        if offset < 0 or offset + 4 > len(self._buf):
            raise ValueError(f"Offset invalid pentru patchul ce se doreste a fi completat! Am primit: {offset}. len(buf) = {len(self._buf)}")
        self._buf[offset:offset + 4] = struct.pack(">I", x & 0xFFFFFFFF)

    def to_bytes(self) -> bytes: # inchid byte-ul partial si returnez sirul de bytes scrisi ca bytes imutabil
        self.align_to_byte()
        return bytes(self._buf)


