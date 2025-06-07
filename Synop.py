import os
import pandas as pd

# --- Konfiguracja ---
ROOT_METEO_SYNOP_DIR = os.path.join("pobrane_dane_imgw", "meteo", "dobowe", "synop")
OUTPUT_FILENAME_SYNOP_SD = "przetworzone_dane_synop_sd.csv"
# Na podstawie inspekcji, cp1250 wydaje się dobrym kandydatem
SYNOP_SD_ENCODINGS_TO_TRY = ['cp1250', 'utf-8', 'iso-8859-2', 'latin1']

# Nazwy kolumn na podstawie Twojego opisu "s d format" (pierwsze 5 + reszta)
# Musimy mieć dokładnie 65 nazw
SYNOP_SD_COLUMN_NAMES = [
    "KodStacji", "NazwaStacji", "Rok", "Miesiac", "Dzien",
    "TMAX_C", "Status_TMAX",
    "TMIN_C", "Status_TMIN",
    "STD_C", "Status_STD",                       # Średnia temperatura dobowa
    "TMNG_C", "Status_TMNG",                     # Temperatura minimalna przy gruncie
    "SMDB_mm", "Status_SMDB",                   # Suma dobowa opadów
    "RodzajOpadu",                             # S/W/spacja
    "PKSN_cm", "Status_PKSN",                   # Wysokość pokrywy śnieżnej
    "RWSN_mm_cm", "Status_RWSN",               # Równoważnik wodny śniegu
    "USL_godz", "Status_USL",                   # Usłonecznienie
    "CzasOpaduDeszcz_godz", "Status_DESZ",
    "CzasOpaduSnieg_godz", "Status_SNEG",
    "CzasOpaduDeszczSnieg_godz", "Status_DISN",
    "CzasGradu_godz", "Status_GRAD",
    "CzasMgly_godz", "Status_MGLA",
    "CzasZamglenia_godz", "Status_ZMGL",
    "CzasSadzi_godz", "Status_SADZ",
    "CzasGololedzi_godz", "Status_GOLO",
    "CzasZamieciNiskiej_godz", "Status_ZMNI",
    "CzasZamieciWysokiej_godz", "Status_ZMWS",
    "CzasZmetnienia_godz", "Status_ZMET",
    "CzasWiatru_ge10ms_godz", "Status_FF10",    # Wiatr >=10m/s
    "CzasWiatru_gt15ms_godz", "Status_FF15",    # Wiatr >15m/s (UWAGA: w opisie >15, w logu FF15)
    "CzasBurzy_godz", "Status_BRZA",
    "CzasRosy_godz", "Status_ROSA",
    "CzasSzronu_godz", "Status_SZRO",
    "WystPokrywySnieznej_01", "Status_DZPS",     # 0/1
    "WystBlyskawicy_01", "Status_DZBL",          # 0/1
    "StanGruntu_ZR",                             # Z/R (Status nie był jawnie podany obok, zakładamy brak)
    "IzotermaDolna_cm", "Status_IZD",
    "IzotermaGorna_cm", "Status_IZG",
    "Aktynometria_Jcm2", "Status_AKTN"
]

def process_single_synop_sd_file(file_path):
    """Wczytuje i przetwarza pojedynczy plik danych synop_sd."""
    print(f"Przetwarzanie pliku: {file_path}")
    df = None
    used_encoding = None

    for encoding_attempt in SYNOP_SD_ENCODINGS_TO_TRY:
        try:
            with open(file_path, 'r', encoding=encoding_attempt) as f_check:
                first_line = f_check.readline().strip()
            num_fields = len(first_line.split(','))

            if num_fields != len(SYNOP_SD_COLUMN_NAMES):
                # print(f"  Ostrzeżenie: Plik {file_path} (kod: {encoding_attempt}) ma {num_fields} pól, oczekiwano {len(SYNOP_SD_COLUMN_NAMES)}. Próba kolejnego kodowania.")
                continue

            temp_df = pd.read_csv(
                file_path,
                encoding=encoding_attempt,
                header=None,
                names=SYNOP_SD_COLUMN_NAMES,
                sep=',',
                dtype=str # Wczytaj wszystko jako string na początku
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
        except Exception: # Ogólny wyjątek, jeśli inne zawiodą
            pass
            
    if df is None:
        print(f"  NIEPOWODZENIE: Nie udało się wczytać pliku {file_path} przy użyciu żadnego ze zdefiniowanych kodowań.")
        return None

    try:
        # Czyszczenie kolumn tekstowych
        text_cols_to_strip = ["KodStacji", "NazwaStacji", "RodzajOpadu", "StanGruntu_ZR"]
        for col in text_cols_to_strip:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        # Lista kolumn pomiarowych i ich odpowiadających kolumn statusowych
        # Pomijamy kolumny, które są kodami (0/1, Z/R) lub nie mają jawnego statusu obok w definicji
        measurement_cols_status = {
            "TMAX_C": "Status_TMAX", "TMIN_C": "Status_TMIN", "STD_C": "Status_STD",
            "TMNG_C": "Status_TMNG", "SMDB_mm": "Status_SMDB", "PKSN_cm": "Status_PKSN",
            "RWSN_mm_cm": "Status_RWSN", "USL_godz": "Status_USL",
            "CzasOpaduDeszcz_godz": "Status_DESZ", "CzasOpaduSnieg_godz": "Status_SNEG",
            "CzasOpaduDeszczSnieg_godz": "Status_DISN", "CzasGradu_godz": "Status_GRAD",
            "CzasMgly_godz": "Status_MGLA", "CzasZamglenia_godz": "Status_ZMGL",
            "CzasSadzi_godz": "Status_SADZ", "CzasGololedzi_godz": "Status_GOLO",
            "CzasZamieciNiskiej_godz": "Status_ZMNI", "CzasZamieciWysokiej_godz": "Status_ZMWS",
            "CzasZmetnienia_godz": "Status_ZMET", "CzasWiatru_ge10ms_godz": "Status_FF10",
            "CzasWiatru_gt15ms_godz": "Status_FF15", "CzasBurzy_godz": "Status_BRZA",
            "CzasRosy_godz": "Status_ROSA", "CzasSzronu_godz": "Status_SZRO",
            "IzotermaDolna_cm": "Status_IZD", "IzotermaGorna_cm": "Status_IZG",
            "Aktynometria_Jcm2": "Status_AKTN",
            # Kolumny 0/1:
            "WystPokrywySnieznej_01": "Status_DZPS", "WystBlyskawicy_01": "Status_DZBL"
        }

        for meas_col, status_col in measurement_cols_status.items():
            if meas_col in df.columns and status_col in df.columns:
                df[meas_col] = pd.to_numeric(df[meas_col].str.replace(',', '.', regex=False), errors='coerce')
                # Ustaw NaN jeśli status to '8' (brak pomiaru)
                df.loc[df[status_col].astype(str).str.strip() == '8', meas_col] = pd.NA
                # Dla wielu z tych pomiarów, '9' (brak zjawiska) oznacza 0
                if meas_col not in ["TMAX_C", "TMIN_C", "STD_C", "TMNG_C", "IzotermaDolna_cm", "IzotermaGorna_cm", "Aktynometria_Jcm2"]: # Temperatury i specjalne
                     df.loc[df[status_col].astype(str).str.strip() == '9', meas_col] = 0.0

        # Kolumna StanGruntu_ZR jest kodem, nie konwertujemy na numeryczny

        # Tworzenie kolumny Data
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
    all_synop_sd_files = []
    for root, _, files in os.walk(ROOT_METEO_SYNOP_DIR):
        for filename in files:
            # Szukamy plików s_d_...csv, ale nie s_d_t_...csv
            if filename.startswith("s_d_") and "_t_" not in filename and filename.lower().endswith(".csv"):
                all_synop_sd_files.append(os.path.join(root, filename))

    if not all_synop_sd_files:
        print(f"Nie znaleziono żadnych plików 's_d_...' (bez '_t_') w katalogu: {ROOT_METEO_SYNOP_DIR}")
    else:
        print(f"Znaleziono {len(all_synop_sd_files)} plików danych synop_sd do przetworzenia.")
        
        list_of_dataframes_synop_sd = []
        for f_path in sorted(all_synop_sd_files):
            df_single = process_single_synop_sd_file(f_path)
            if df_single is not None and not df_single.empty:
                list_of_dataframes_synop_sd.append(df_single)

        if list_of_dataframes_synop_sd:
            print("\nŁączenie wszystkich przetworzonych danych synop_sd...")
            final_synop_sd_df = pd.concat(list_of_dataframes_synop_sd, ignore_index=True)
            
            print("\n--- Wynikowa ramka danych synop_sd ---")
            final_synop_sd_df.info(verbose=True, show_counts=True)
            print("\nPierwsze 5 wierszy wynikowych danych synop_sd:")
            print(final_synop_sd_df.head().to_string())
            print("\nOstatnie 5 wierszy wynikowych danych synop_sd:")
            print(final_synop_sd_df.tail().to_string())

            try:
                final_synop_sd_df.to_csv(OUTPUT_FILENAME_SYNOP_SD, index=False, encoding='utf-8-sig')
                print(f"\nPrzetworzone dane synop_sd zapisano do: {OUTPUT_FILENAME_SYNOP_SD}")
            except Exception as e:
                print(f"Błąd podczas zapisywania pliku {OUTPUT_FILENAME_SYNOP_SD}: {e}")
        else:
            print("Nie udało się przetworzyć żadnych plików synop_sd.")