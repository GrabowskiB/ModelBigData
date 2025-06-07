import os
import pandas as pd

# --- Konfiguracja ---
ROOT_METEO_KLIMAT_DIR = os.path.join("pobrane_dane_imgw", "meteo", "dobowe", "klimat")
OUTPUT_FILENAME_KLIMAT_KDT = "przetworzone_dane_klimat_kdt.csv"
KLIMAT_KDT_ENCODINGS_TO_TRY = ['cp1250', 'utf-8', 'iso-8859-2', 'latin1']

# Nazwy kolumn na podstawie Twojego opisu "k d t format"
KLIMAT_KDT_COLUMN_NAMES = [
    "KodStacji", "NazwaStacji", "Rok", "Miesiac", "Dzien",
    "TEMP_Srednia_C", "Status_TEMP",         # Średnia dobowa temperatura
    "WLGS_Srednia_proc", "Status_WLGS",      # Średnia dobowa wilgotność względna
    "FWS_Srednia_ms", "Status_FWS",          # Średnia dobowa prędkość wiatru
    "NOS_Srednie_okt", "Status_NOS"          # Średnie dobowe zachmurzenie ogólne
]

def process_single_klimat_kdt_file(file_path):
    """Wczytuje i przetwarza pojedynczy plik danych klimat_kdt."""
    print(f"Przetwarzanie pliku: {file_path}")
    df = None
    used_encoding = None

    for encoding_attempt in KLIMAT_KDT_ENCODINGS_TO_TRY:
        try:
            with open(file_path, 'r', encoding=encoding_attempt) as f_check:
                first_line = f_check.readline().strip()
            num_fields = len(first_line.split(','))

            if num_fields != len(KLIMAT_KDT_COLUMN_NAMES):
                continue

            temp_df = pd.read_csv(
                file_path,
                encoding=encoding_attempt,
                header=None,
                names=KLIMAT_KDT_COLUMN_NAMES,
                sep=',',
                dtype=str
            )
            df = temp_df
            used_encoding = encoding_attempt
            print(f"  Pomyślnie wczytano z kodowaniem: {used_encoding}")
            break
        except UnicodeDecodeError:
            continue
        except FileNotFoundError:
            print(f"  BŁĄD: Plik {file_path} nie został znaleziony.")
            return None
        except pd.errors.EmptyDataError:
            print(f"  BŁĄD: Plik {file_path} jest pusty.")
            return None
        except Exception:
            pass
            
    if df is None:
        print(f"  NIEPOWODZENIE: Nie udało się wczytać pliku {file_path} przy użyciu żadnego ze zdefiniowanych kodowań.")
        return None

    try:
        for col in ["KodStacji", "NazwaStacji"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        measurement_cols_status = {
            "TEMP_Srednia_C": "Status_TEMP",
            "WLGS_Srednia_proc": "Status_WLGS",
            "FWS_Srednia_ms": "Status_FWS",
            "NOS_Srednie_okt": "Status_NOS"
        }

        for meas_col, status_col in measurement_cols_status.items():
            if meas_col in df.columns and status_col in df.columns:
                df[meas_col] = pd.to_numeric(df[meas_col].str.replace(',', '.', regex=False), errors='coerce')
                df.loc[df[status_col].astype(str).str.strip() == '8', meas_col] = pd.NA
                # Dla tych parametrów status '9' (brak zjawiska) jest mniej typowy,
                # więc na razie tylko obsługa '8'. Można by dodać logikę dla '9' jeśli potrzebne.

        date_components_valid = df[['Rok', 'Miesiac', 'Dzien']].notna().all(axis=1)
        df.loc[date_components_valid, "Data"] = pd.to_datetime(
            df.loc[date_components_valid, "Rok"].astype(str) + '-' +
            df.loc[date_components_valid, "Miesiac"].astype(str) + '-' +
            df.loc[date_components_valid, "Dzien"].astype(str),
            format='%Y-%m-%d',
            errors='coerce'
        )
        df.loc[~date_components_valid, "Data"] = pd.NaT
        
        cols_to_drop = ["Rok", "Miesiac", "Dzien"]
        df = df.drop(columns=[col for col in cols_to_drop if col in df.columns], errors='ignore')
        
        if 'Data' in df.columns:
            data_col = df.pop('Data')
            df.insert(0, 'Data', data_col)
            df = df.sort_values(by=['Data', 'KodStacji']).reset_index(drop=True)

        return df

    except Exception as e_processing:
        print(f"  Błąd podczas przetwarzania danych po wczytaniu pliku {file_path} (kodowanie: {used_encoding}): {e_processing}")
        return None

# --- Główna część skryptu ---
if __name__ == "__main__":
    all_klimat_kdt_files = []
    for root, _, files in os.walk(ROOT_METEO_KLIMAT_DIR):
        for filename in files:
            # Szukamy plików k_d_t_MM_RRRR.csv
            if filename.startswith("k_d_t_") and filename.lower().endswith(".csv"):
                all_klimat_kdt_files.append(os.path.join(root, filename))

    if not all_klimat_kdt_files:
        print(f"Nie znaleziono żadnych plików 'k_d_t_MM_RRRR.csv' w katalogu: {ROOT_METEO_KLIMAT_DIR}")
    else:
        print(f"Znaleziono {len(all_klimat_kdt_files)} plików danych klimat_kdt do przetworzenia.")
        
        list_of_dataframes_klimat_kdt = []
        for f_path in sorted(all_klimat_kdt_files):
            df_single = process_single_klimat_kdt_file(f_path)
            if df_single is not None and not df_single.empty:
                list_of_dataframes_klimat_kdt.append(df_single)

        if list_of_dataframes_klimat_kdt:
            print("\nŁączenie wszystkich przetworzonych danych klimat_kdt...")
            final_klimat_kdt_df = pd.concat(list_of_dataframes_klimat_kdt, ignore_index=True)
            
            print("\n--- Wynikowa ramka danych klimat_kdt ---")
            final_klimat_kdt_df.info(verbose=True, show_counts=True)
            print("\nPierwsze 5 wierszy wynikowych danych klimat_kdt:")
            print(final_klimat_kdt_df.head().to_string())
            print("\nOstatnie 5 wierszy wynikowych danych klimat_kdt:")
            print(final_klimat_kdt_df.tail().to_string())

            try:
                final_klimat_kdt_df.to_csv(OUTPUT_FILENAME_KLIMAT_KDT, index=False, encoding='utf-8-sig')
                print(f"\nPrzetworzone dane klimat_kdt zapisano do: {OUTPUT_FILENAME_KLIMAT_KDT}")
            except Exception as e:
                print(f"Błąd podczas zapisywania pliku {OUTPUT_FILENAME_KLIMAT_KDT}: {e}")
        else:
            print("Nie udało się przetworzyć żadnych plików klimat_kdt.")