import os
import pandas as pd
import chardet # Do próby detekcji kodowania

# --- Konfiguracja ---
ROOT_DATA_DIR = "pobrane_dane_imgw"
# Lista przykładowych plików do inspekcji (dodaj/zmień wg potrzeb)
# Wybierz pliki, które są reprezentatywne lub budzą najwięcej pytań
# Upewnij się, że te ścieżki są poprawne w Twojej strukturze folderów
EXAMPLE_FILES_TO_INSPECT = [
    # Klimatologiczne
    "meteo/dobowe/klimat/2018/extracted_files/k_d_01_2018.csv",
    "meteo/dobowe/klimat/2018/extracted_files/k_d_t_01_2018.csv", # Ważne!
    # Opadowe
    "meteo/dobowe/opad/2019/extracted_files/o_d_01_2019.csv",
    # Synoptyczne - historyczne
    "meteo/dobowe/synop/2020/extracted_files/s_d_100_2020.csv", # Zastąp '100' rzeczywistym kodem stacji
    "meteo/dobowe/synop/2020/extracted_files/s_d_t_100_2020.csv", # Ważne! Zastąp '100'
    # Synoptyczne - "bieżący" rok (jeśli masz dane dla 2025)
    "meteo/dobowe/synop/2025/extracted_files/s_d_01_2025.csv",
    # Hydrologiczne - przykładowy plik, który masz
    # Musisz podać dokładną nazwę pliku, jeśli masz rozpakowany
    # "ost_hydro/2022/extracted_files/codz_2022_01.csv" # Zastąp rzeczywistą nazwą
]

# Potencjalne kodowania do sprawdzenia
COMMON_ENCODINGS = ['utf-8', 'cp1250', 'iso-8859-2', 'latin1']

def detect_encoding(file_path, sample_size=10000):
    """Próbuje wykryć kodowanie pliku na podstawie próbki bajtów."""
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read(sample_size)
        result = chardet.detect(raw_data)
        encoding = result['encoding']
        confidence = result['confidence']
        print(f"  Chardet wykrył kodowanie: {encoding} z pewnością: {confidence:.2f}")
        return encoding if confidence > 0.7 else None # Zwracaj tylko przy wysokiej pewności
    except Exception as e:
        print(f"  Błąd podczas detekcji kodowania (chardet) dla {file_path}: {e}")
        return None

def inspect_csv_file(file_path):
    """Wczytuje i wyświetla informacje o pliku CSV."""
    print(f"\n--- Inspekcja pliku: {file_path} ---")

    if not os.path.exists(file_path):
        print("  BŁĄD: Plik nie istnieje!")
        return

    detected_encoding_by_chardet = detect_encoding(file_path)
    
    loaded_successfully = False
    used_encoding = None

    if detected_encoding_by_chardet:
        encodings_to_try = [detected_encoding_by_chardet] + [enc for enc in COMMON_ENCODINGS if enc != detected_encoding_by_chardet]
    else:
        encodings_to_try = COMMON_ENCODINGS

    # Dodatkowa logika dla plików klimatologicznych
    if "k_d_" in os.path.basename(file_path).lower():
        if 'cp1250' not in encodings_to_try:
            encodings_to_try.insert(0, 'cp1250') # Spróbuj cp1250 jako pierwszy
        elif encodings_to_try[0] != 'cp1250':
            encodings_to_try.remove('cp1250')
            encodings_to_try.insert(0, 'cp1250')


    for encoding in encodings_to_try:
        try:
            # Wersja bez nagłówków, bo ich nie ma w plikach
            df = pd.read_csv(file_path, encoding=encoding, header=None, nrows=10, on_bad_lines='warn', low_memory=False)
            print(f"  Użyte kodowanie do wczytania: {encoding} (header=None)")
            # df.columns = [f"col_{i}" for i in range(df.shape[1])] # Tymczasowe nazwy kolumn
            print("  Domyślne nazwy kolumn (nadane przez Pandas, bo header=None):")
            print(f"    {list(df.columns)}")
            print("\n  Pierwsze 5 wierszy:")
            print(df.head().to_string())
            
            try:
                df_info = pd.read_csv(file_path, encoding=encoding, header=None, low_memory=False, nrows=1000)
                # df_info.columns = [f"col_{i}" for i in range(df_info.shape[1])]
                print("\n  Informacje o typach danych (Pandas):")
                df_info.info(verbose=True, show_counts=True)
            except Exception as e_info:
                print(f"  Nie udało się wczytać pliku dla df.info() z kodowaniem {encoding}: {e_info}")
                print("  Próba wczytania df.info() z nrows=100:")
                try:
                    df_info_small = pd.read_csv(file_path, encoding=encoding, nrows=100, low_memory=False)
                    print("\n  Informacje o typach danych (Pandas, próbka 100 wierszy):")
                    df_info_small.info(verbose=True, show_counts=True)
                except Exception as e_info_small:
                     print(f"  Nie udało się wczytać pliku dla df_info() (próbka 100 wierszy) z kodowaniem {encoding}: {e_info_small}")


            loaded_successfully = True
            used_encoding = encoding
            break  # Jeśli się udało, nie próbuj innych kodowań
        except UnicodeDecodeError:
            print(f"  Nie udało się wczytać z kodowaniem: {encoding} (UnicodeDecodeError)")
        except FileNotFoundError:
            print(f"  BŁĄD: Plik {file_path} nie został znaleziony.")
            return # Nie ma sensu próbować dalej
        except pd.errors.EmptyDataError:
            print(f"  BŁĄD: Plik {file_path} jest pusty.")
            return
        except pd.errors.ParserError as pe:
            print(f"  Błąd parsowania z kodowaniem {encoding} dla {file_path}: {pe}")
            print(f"  Plik może nie być standardowym CSV lub separator jest inny niż przecinek.")
        except Exception as e:
            print(f"  Inny błąd podczas wczytywania {file_path} z kodowaniem {encoding}: {e}")

    if not loaded_successfully:
        print("  Nie udało się pomyślnie wczytać pliku przy użyciu żadnego ze standardowych kodowań.")
    print("--- Koniec inspekcji ---")

# --- Główna część skryptu ---
if __name__ == "__main__":
    # Utwórz pełne ścieżki do plików na podstawie ROOT_DATA_DIR
    full_paths_to_inspect = []
    for rel_path in EXAMPLE_FILES_TO_INSPECT:
        # Sprawdź, czy ścieżka już zawiera ROOT_DATA_DIR (na wszelki wypadek)
        if rel_path.startswith(ROOT_DATA_DIR):
            full_paths_to_inspect.append(rel_path)
        else:
            full_paths_to_inspect.append(os.path.join(ROOT_DATA_DIR, rel_path.replace("/", os.sep))) # Poprawka dla różnych systemów

    if not os.path.isdir(ROOT_DATA_DIR):
        print(f"Błąd: Główny katalog danych '{ROOT_DATA_DIR}' nie istnieje.")
    else:
        for file_path_to_check in full_paths_to_inspect:
            inspect_csv_file(file_path_to_check)

        print("\nZakończono inspekcję wybranych plików.")
        print("Przejrzyj wyniki powyżej. Zwróć szczególną uwagę na:")
        print("- Poprawność wykrytego kodowania (czy polskie znaki są OK).")
        print("- Nazwy kolumn.")
        print("- Formaty danych w pierwszych wierszach.")
        print("- Typy danych zidentyfikowane przez Pandas.")
        print("- Czy pliki '_t_' zawierają dane terminowe (np. więcej wierszy, kolumna z godziną).")