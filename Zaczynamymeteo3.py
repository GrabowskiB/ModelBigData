import os
import pandas as pd

# --- Konfiguracja ---
ROOT_METEO_OPAD_DIR = os.path.join("pobrane_dane_imgw", "meteo", "dobowe", "opad")
OUTPUT_FILENAME_OPAD_OD = "przetworzone_dane_opad_od.csv"
# Na podstawie inspekcji pliku o_d_01_2019.csv, ISO-8859-1 było dobrym kandydatem,
# ale dodajemy też inne, w tym cp1250, które często występuje.
OPAD_OD_ENCODINGS_TO_TRY = ['iso-8859-1', 'cp1250', 'utf-8', 'latin1']

# Nazwy kolumn na podstawie Twojego opisu "o d format"
OPAD_OD_COLUMN_NAMES = [
    "KodStacji", "NazwaStacji", "Rok", "Miesiac", "Dzien",
    "SMDB_mm", "Status_SMDB",           # Suma dobowa opadów
    "RodzajOpadu",                     # S/W/spacja
    "PKSN_cm", "Status_PKSN",           # Wysokość pokrywy śnieżnej
    "HSS_cm", "Status_HSS",             # Wysokość świeżospałego śniegu
    "GatunekSniegu_kod", "Status_GATS",  # Gatunek śniegu
    "RodzajPokrywy_kod", "Status_RPSN"   # Rodzaj pokrywy śnieżnej
]

def process_single_opad_od_file(file_path):
    """Wczytuje i przetwarza pojedynczy plik danych opad_od."""
    print(f"Przetwarzanie pliku: {file_path}")
    df = None
    used_encoding = None

    for encoding_attempt in OPAD_OD_ENCODINGS_TO_TRY:
        try:
            with open(file_path, 'r', encoding=encoding_attempt) as f_check:
                first_line = f_check.readline().strip()
            num_fields = len(first_line.split(','))

            if num_fields != len(OPAD_OD_COLUMN_NAMES):
                continue

            temp_df = pd.read_csv(
                file_path,
                encoding=encoding_attempt,
                header=None,
                names=OPAD_OD_COLUMN_NAMES,
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
        for col in ["KodStacji", "NazwaStacji", "RodzajOpadu", "GatunekSniegu_kod", "RodzajPokrywy_kod"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        measurement_cols_status = {
            "SMDB_mm": "Status_SMDB",
            "PKSN_cm": "Status_PKSN",
            "HSS_cm": "Status_HSS"
            # GatunekSniegu_kod i RodzajPokrywy_kod to kody, nie wartości numeryczne do uśredniania/sumowania,
            # więc ich statusy (GATS, RPSN) informują o poprawności samego kodu.
        }

        for meas_col, status_col in measurement_cols_status.items():
            if meas_col in df.columns and status_col in df.columns:
                df[meas_col] = pd.to_numeric(df[meas_col].str.replace(',', '.', regex=False), errors='coerce')
                df.loc[df[status_col].astype(str).str.strip() == '8', meas_col] = pd.NA
                # Dla opadów i śniegu, status '9' (brak zjawiska) oznacza 0
                df.loc[df[status_col].astype(str).str.strip() == '9', meas_col] = 0.0
        
        # Dla kolumn kodowych, jeśli status to '8' (brak pomiaru) lub '9' (brak zjawiska),
        # sam kod można by ustawić na pusty string lub specjalny wskaźnik, jeśli jest taka potrzeba.
        # Na razie zostawiamy je tak, jak zostały wczytane, jeśli nie są puste.
        code_status_cols = {"GatunekSniegu_kod": "Status_GATS", "RodzajPokrywy_kod": "Status_RPSN"}
        for code_col, status_col in code_status_cols.items():
            if code_col in df.columns and status_col in df.columns:
                # Jeśli status to 8 (brak pomiaru) lub 9 (brak zjawiska), kod może nie mieć znaczenia
                df.loc[df[status_col].astype(str).str.strip().isin(['8', '9']), code_col] = "" # lub pd.NA


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
    all_opad_od_files = []
    for root, _, files in os.walk(ROOT_METEO_OPAD_DIR):
        for filename in files:
            if filename.startswith("o_d_") and filename.lower().endswith(".csv"):
                all_opad_od_files.append(os.path.join(root, filename))

    if not all_opad_od_files:
        print(f"Nie znaleziono żadnych plików 'o_d_MM_RRRR.csv' w katalogu: {ROOT_METEO_OPAD_DIR}")
    else:
        print(f"Znaleziono {len(all_opad_od_files)} plików danych opad_od do przetworzenia.")
        
        list_of_dataframes_opad_od = []
        for f_path in sorted(all_opad_od_files):
            df_single = process_single_opad_od_file(f_path)
            if df_single is not None and not df_single.empty:
                list_of_dataframes_opad_od.append(df_single)

        if list_of_dataframes_opad_od:
            print("\nŁączenie wszystkich przetworzonych danych opad_od...")
            final_opad_od_df = pd.concat(list_of_dataframes_opad_od, ignore_index=True)
            
            print("\n--- Wynikowa ramka danych opad_od ---")
            final_opad_od_df.info(verbose=True, show_counts=True)
            print("\nPierwsze 5 wierszy wynikowych danych opad_od:")

            print("\nPierwsze 5 wierszy wynikowych danych opad_od (wybrane kolumny):")
            print(final_opad_od_df[['Data', 'KodStacji', 'SMDB_mm', 'PKSN_cm', 'HSS_cm']].head().to_string())

            print("\nOstatnie 5 wierszy wynikowych danych opad_od (wybrane kolumny):")
            print(final_opad_od_df[['Data', 'KodStacji', 'SMDB_mm', 'PKSN_cm', 'HSS_cm']].tail().to_string())
            print("\nOstatnie 5 wierszy wynikowych danych opad_od:")
            #print(final_opad_od_df.tail().to_string())

            try:
                final_opad_od_df.to_csv(OUTPUT_FILENAME_OPAD_OD, index=False, encoding='utf-8-sig')
                print(f"\nPrzetworzone dane opad_od zapisano do: {OUTPUT_FILENAME_OPAD_OD}")
            except Exception as e:
                print(f"Błąd podczas zapisywania pliku {OUTPUT_FILENAME_OPAD_OD}: {e}")
        else:
            print("Nie udało się przetworzyć żadnych plików opad_od.")