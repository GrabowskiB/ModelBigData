import os
import pandas as pd

# --- Konfiguracja ---
ROOT_METEO_SYNOP_DIR = os.path.join("pobrane_dane_imgw", "meteo", "dobowe", "synop")
OUTPUT_FILENAME_SYNOP_SDT = "przetworzone_dane_synop_sdt.csv"
# Na podstawie inspekcji wcześniejszych plików synop, cp1250 jest dobrym kandydatem
SYNOP_SDT_ENCODINGS_TO_TRY = ['cp1250', 'utf-8', 'iso-8859-2', 'latin1']

# Nazwy kolumn na podstawie Twojego opisu "s d t format"
SYNOP_SDT_COLUMN_NAMES = [
    "KodStacji", "NazwaStacji", "Rok", "Miesiac", "Dzien",
    "NOS_Srednie_okt", "Status_NOS",         # Średnie dobowe zachmurzenie ogólne
    "FWS_Srednia_ms", "Status_FWS",          # Średnia dobowa prędkość wiatru
    "TEMP_Srednia_C", "Status_TEMP",         # Średnia dobowa temperatura
    "CPW_Srednie_hPa", "Status_CPW",         # Średnie dobowe ciśnienie pary wodnej
    "WLGS_Srednia_proc", "Status_WLGS",      # Średnia dobowa wilgotność względna
    "PPPS_Srednie_hPa", "Status_PPPS",       # Średnie dobowe ciśnienie na poziomie stacji
    "PPPM_Srednie_hPa", "Status_PPPM",       # Średnie dobowe ciśnienie na poziomie morza
    "WODZ_SumaOpaduDzien_mm", "Status_WODZ", # Suma opadu dzień
    "WONO_SumaOpaduNoc_mm", "Status_WONO"    # Suma opadu noc
]

def process_single_synop_sdt_file(file_path):
    """Wczytuje i przetwarza pojedynczy plik danych synop_sdt."""
    print(f"Przetwarzanie pliku: {file_path}")
    df = None
    used_encoding = None

    for encoding_attempt in SYNOP_SDT_ENCODINGS_TO_TRY:
        try:
            with open(file_path, 'r', encoding=encoding_attempt) as f_check:
                first_line = f_check.readline().strip()
            num_fields = len(first_line.split(','))

            if num_fields != len(SYNOP_SDT_COLUMN_NAMES):
                continue

            temp_df = pd.read_csv(
                file_path,
                encoding=encoding_attempt,
                header=None,
                names=SYNOP_SDT_COLUMN_NAMES,
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
            "NOS_Srednie_okt": "Status_NOS",
            "FWS_Srednia_ms": "Status_FWS",
            "TEMP_Srednia_C": "Status_TEMP",
            "CPW_Srednie_hPa": "Status_CPW",
            "WLGS_Srednia_proc": "Status_WLGS",
            "PPPS_Srednie_hPa": "Status_PPPS",
            "PPPM_Srednie_hPa": "Status_PPPM",
            "WODZ_SumaOpaduDzien_mm": "Status_WODZ",
            "WONO_SumaOpaduNoc_mm": "Status_WONO"
        }

        for meas_col, status_col in measurement_cols_status.items():
            if meas_col in df.columns and status_col in df.columns:
                df[meas_col] = pd.to_numeric(df[meas_col].str.replace(',', '.', regex=False), errors='coerce')
                df.loc[df[status_col].astype(str).str.strip() == '8', meas_col] = pd.NA
                # Dla większości tych parametrów status '9' (brak zjawiska) może oznaczać 0
                # (np. opad, prędkość wiatru). Dla temperatury, ciśnienia, wilgotności '9' jest mniej jasne.
                if meas_col in ["FWS_Srednia_ms", "WODZ_SumaOpaduDzien_mm", "WONO_SumaOpaduNoc_mm", "NOS_Srednie_okt"]: # Zachmurzenie 0-8
                    df.loc[df[status_col].astype(str).str.strip() == '9', meas_col] = 0.0

        date_components_valid = df[['Rok', 'Miesiac', 'Dzien']].notna().all(axis=1) & \
                                df['Rok'].str.match(r'^\d{4}$') & \
                                df['Miesiac'].str.match(r'^\d{1,2}$') & \
                                df['Dzien'].str.match(r'^\d{1,2}$')
        
        df.loc[date_components_valid, "Data"] = pd.to_datetime(
            df.loc[date_components_valid, "Rok"] + '-' +
            df.loc[date_components_valid, "Miesiac"] + '-' +
            df.loc[date_components_valid, "Dzien"],
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
    all_synop_sdt_files = []
    for root, _, files in os.walk(ROOT_METEO_SYNOP_DIR):
        for filename in files:
            # Szukamy plików s_d_t_KODSTACJI_RRRR.csv
            if filename.startswith("s_d_t_") and filename.lower().endswith(".csv"):
                all_synop_sdt_files.append(os.path.join(root, filename))

    if not all_synop_sdt_files:
        print(f"Nie znaleziono żadnych plików 's_d_t_KODSTACJI_RRRR.csv' w katalogu: {ROOT_METEO_SYNOP_DIR}")
    else:
        print(f"Znaleziono {len(all_synop_sdt_files)} plików danych synop_sdt do przetworzenia.")
        
        list_of_dataframes_synop_sdt = []
        for f_path in sorted(all_synop_sdt_files):
            df_single = process_single_synop_sdt_file(f_path)
            if df_single is not None and not df_single.empty:
                list_of_dataframes_synop_sdt.append(df_single)

        if list_of_dataframes_synop_sdt:
            print("\nŁączenie wszystkich przetworzonych danych synop_sdt...")
            final_synop_sdt_df = pd.concat(list_of_dataframes_synop_sdt, ignore_index=True)
            
            print("\n--- Wynikowa ramka danych synop_sdt ---")
            final_synop_sdt_df.info(verbose=True, show_counts=True)
            print("\nPierwsze 5 wierszy wynikowych danych synop_sdt:")
            print(final_synop_sdt_df.head().to_string())
            print("\nOstatnie 5 wierszy wynikowych danych synop_sdt:")
            print(final_synop_sdt_df.tail().to_string())

            try:
                final_synop_sdt_df.to_csv(OUTPUT_FILENAME_SYNOP_SDT, index=False, encoding='utf-8-sig')
                print(f"\nPrzetworzone dane synop_sdt zapisano do: {OUTPUT_FILENAME_SYNOP_SDT}")
            except Exception as e:
                print(f"Błąd podczas zapisywania pliku {OUTPUT_FILENAME_SYNOP_SDT}: {e}")
        else:
            print("Nie udało się przetworzyć żadnych plików synop_sdt.")