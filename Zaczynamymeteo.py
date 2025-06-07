import os
import pandas as pd

# --- Konfiguracja ---
ROOT_METEO_KLIMAT_DIR = os.path.join("pobrane_dane_imgw", "meteo", "dobowe", "klimat")
OUTPUT_FILENAME_KLIMAT_KD = "przetworzone_dane_klimat_kd.csv"
# Na podstawie wcześniejszej inspekcji, cp1250 wydaje się dobrym kandydatem
# ale dodajemy też inne na wszelki wypadek
KLIMAT_KD_ENCODINGS_TO_TRY = ['cp1250', 'utf-8', 'iso-8859-2', 'latin1']

# Nazwy kolumn na podstawie Twojego opisu "k d format"
KLIMAT_KD_COLUMN_NAMES = [
    "KodStacji", "NazwaStacji", "Rok", "Miesiac", "Dzien",
    "TMAX_C", "Status_TMAX",
    "TMIN_C", "Status_TMIN",
    "STD_C", "Status_STD", # Średnia temperatura dobowa
    "TMNG_C", "Status_TMNG", # Temperatura minimalna przy gruncie
    "SMDB_mm", "Status_SMDB", # Suma dobowa opadów
    "RodzajOpadu", # S/W/spacja
    "PKSN_cm", "Status_PKSN" # Wysokość pokrywy śnieżnej
]

# Wartości, które oznaczają NaN (nie dotyczy statusów, bo one są informacją)
# Na razie nie definiujemy specyficznych na_values, bo statusy '8' i '9'
# będziemy obsługiwać inaczej - zamieniając wartość pomiaru na NaN jeśli status to '8'
# a '9' oznacza brak zjawiska, co może być zerem dla niektórych pomiarów (np. opad)

def process_single_klimat_kd_file(file_path):
    """Wczytuje i przetwarza pojedynczy plik danych klimat_kd."""
    print(f"Przetwarzanie pliku: {file_path}")
    df = None
    used_encoding = None

    for encoding_attempt in KLIMAT_KD_ENCODINGS_TO_TRY:
        try:
            # Sprawdzenie liczby kolumn
            with open(file_path, 'r', encoding=encoding_attempt) as f_check:
                first_line = f_check.readline().strip()
            num_fields = len(first_line.split(',')) # Zakładamy przecinek jako separator dla tych plików

            if num_fields != len(KLIMAT_KD_COLUMN_NAMES):
                # print(f"  Ostrzeżenie: Plik {file_path} (kod: {encoding_attempt}) ma {num_fields} pól, oczekiwano {len(KLIMAT_KD_COLUMN_NAMES)}. Próba kolejnego kodowania.")
                continue

            temp_df = pd.read_csv(
                file_path,
                encoding=encoding_attempt,
                header=None,
                names=KLIMAT_KD_COLUMN_NAMES,
                sep=',', # Zakładamy przecinek
                dtype=str # Wczytaj wszystko jako string na początku
            )
            df = temp_df
            used_encoding = encoding_attempt
            print(f"  Pomyślnie wczytano z kodowaniem: {used_encoding}")
            break
        except UnicodeDecodeError:
            # print(f"  Nie udało się wczytać z kodowaniem: {encoding_attempt}")
            continue
        except FileNotFoundError:
            print(f"  BŁĄD: Plik {file_path} nie został znaleziony.")
            return None
        except pd.errors.EmptyDataError:
            print(f"  BŁĄD: Plik {file_path} jest pusty.")
            return None
        except Exception as e:
            # print(f"  Inny błąd podczas próby wczytania pliku {file_path} (kod: {encoding_attempt}): {e}")
            pass
            
    if df is None:
        print(f"  NIEPOWODZENIE: Nie udało się wczytać pliku {file_path} przy użyciu żadnego ze zdefiniowanych kodowań.")
        return None

    try:
        # Czyszczenie
        for col in ["KodStacji", "NazwaStacji", "RodzajOpadu"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        # Kolumny pomiarowe do konwersji i obsługi statusów
        measurement_cols_status = {
            "TMAX_C": "Status_TMAX", "TMIN_C": "Status_TMIN", "STD_C": "Status_STD",
            "TMNG_C": "Status_TMNG", "SMDB_mm": "Status_SMDB", "PKSN_cm": "Status_PKSN"
        }

        for meas_col, status_col in measurement_cols_status.items():
            if meas_col in df.columns and status_col in df.columns:
                # Najpierw konwersja na numeryczny, błędy zamienią na NaN
                df[meas_col] = pd.to_numeric(df[meas_col].str.replace(',', '.', regex=False), errors='coerce')
                
                # Jeśli status to '8' (brak pomiaru), ustaw wartość na NaN
                df.loc[df[status_col].astype(str).str.strip() == '8', meas_col] = pd.NA
                
                # Jeśli status to '9' (brak zjawiska), dla opadów i pokrywy śnieżnej to 0
                if meas_col in ["SMDB_mm", "PKSN_cm"]:
                    df.loc[df[status_col].astype(str).str.strip() == '9', meas_col] = 0.0
                # Dla temperatur, status '9' jest mniej jasny, na razie zostawiamy NaN jeśli pd.to_numeric tak zrobiło,
                # lub jeśli wartość była nie-numeryczna. Można by rozważyć logikę specyficzną dla temperatur.

        # Tworzenie kolumny Data
        date_components_valid = df[['Rok', 'Miesiac', 'Dzien']].notna().all(axis=1)
        df.loc[date_components_valid, "Data"] = pd.to_datetime(
            df.loc[date_components_valid, "Rok"].astype(str) + '-' +
            df.loc[date_components_valid, "Miesiac"].astype(str) + '-' +
            df.loc[date_components_valid, "Dzien"].astype(str),
            format='%Y-%m-%d',
            errors='coerce'
        )
        df.loc[~date_components_valid, "Data"] = pd.NaT
        
        cols_to_drop = ["Rok", "Miesiac", "Dzien"] # Kolumny statusowe można zostawić dla informacji lub też usunąć
        df = df.drop(columns=[col for col in cols_to_drop if col in df.columns], errors='ignore')
        
        if 'Data' in df.columns:
            data_col = df.pop('Data')
            df.insert(0, 'Data', data_col)
            # Sortowanie po dacie i kodzie stacji dla spójności
            df = df.sort_values(by=['Data', 'KodStacji']).reset_index(drop=True)

        return df

    except Exception as e_processing:
        print(f"  Błąd podczas przetwarzania danych po wczytaniu pliku {file_path} (kodowanie: {used_encoding}): {e_processing}")
        return None

# --- Główna część skryptu ---
if __name__ == "__main__":
    all_klimat_kd_files = []
    for root, _, files in os.walk(ROOT_METEO_KLIMAT_DIR):
        for filename in files:
            # Szukamy plików k_d_MM_RRRR.csv, ale nie k_d_t_MM_RRRR.csv
            if filename.startswith("k_d_") and "_t_" not in filename and filename.lower().endswith(".csv"):
                all_klimat_kd_files.append(os.path.join(root, filename))

    if not all_klimat_kd_files:
        print(f"Nie znaleziono żadnych plików 'k_d_MM_RRRR.csv' w katalogu: {ROOT_METEO_KLIMAT_DIR}")
    else:
        print(f"Znaleziono {len(all_klimat_kd_files)} plików danych klimat_kd do przetworzenia.")
        
        list_of_dataframes_klimat_kd = []
        for f_path in sorted(all_klimat_kd_files):
            df_single = process_single_klimat_kd_file(f_path)
            if df_single is not None and not df_single.empty:
                list_of_dataframes_klimat_kd.append(df_single)

        if list_of_dataframes_klimat_kd:
            print("\nŁączenie wszystkich przetworzonych danych klimat_kd...")
            final_klimat_kd_df = pd.concat(list_of_dataframes_klimat_kd, ignore_index=True)
            
            print("\n--- Wynikowa ramka danych klimat_kd ---")
            final_klimat_kd_df.info(verbose=True, show_counts=True)
            print("\nPierwsze 5 wierszy wynikowych danych klimat_kd:")
            print(final_klimat_kd_df.head().to_string())
            print("\nOstatnie 5 wierszy wynikowych danych klimat_kd:")
            print(final_klimat_kd_df.tail().to_string())

            try:
                final_klimat_kd_df.to_csv(OUTPUT_FILENAME_KLIMAT_KD, index=False, encoding='utf-8-sig')
                print(f"\nPrzetworzone dane klimat_kd zapisano do: {OUTPUT_FILENAME_KLIMAT_KD}")
            except Exception as e:
                print(f"Błąd podczas zapisywania pliku {OUTPUT_FILENAME_KLIMAT_KD}: {e}")
        else:
            print("Nie udało się przetworzyć żadnych plików klimat_kd.")