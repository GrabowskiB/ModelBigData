import os
import pandas as pd

# --- Konfiguracja ---
ROOT_HYDRO_DATA_DIR = os.path.join("pobrane_dane_imgw", "hydro", "dobowe_pomiarowe")
OUTPUT_FILENAME_HYDRO = "przetworzone_dane_hydrologiczne.csv"
# Lista kodowań do wypróbowania
HYDRO_ENCODINGS_TO_TRY = ['utf-8', 'cp1250', 'iso-8859-2', 'latin1'] # Dodaj UTF-8 jako pierwszy do próby

# Nazwy kolumn na podstawie Twojego opisu "codz_info"
HYDRO_COLUMN_NAMES = [
    "KodStacji",
    "NazwaStacji",
    "NazwaRzekiJeziora",
    "RokHydrologiczny",
    "WskaznikMiesiacaRokHydrologiczny",
    "Dzien",
    "StanWody_cm",
    "Przeplyw_m3s",
    "TemperaturaWody_C",
    "MiesiacKalendarzowy"
]

HYDRO_NA_VALUES = {
    "StanWody_cm": [9999, "9999"], # Dodajemy stringi na wszelki wypadek
    "Przeplyw_m3s": [99999.999, "99999.999"],
    "TemperaturaWody_C": [99.9, "99.9"]
}

def determine_calendar_year(row):
    try:
        # W plikach rocznych (np. codz_2023.csv) RokHydrologiczny może być już rokiem kalendarzowym
        # lub wymagać tej samej logiki co wcześniej. Zakładamy na razie tę samą logikę.
        # Jeśli MiesiacKalendarzowy to 11 lub 12, RokKalendarzowy = RokHydrologiczny - 1
        # W przeciwnym razie RokKalendarzowy = RokHydrologiczny
        # Ważne: upewnij się, że kolumny są jako int przed operacjami arytmetycznymi
        miesiac_kal = int(row['MiesiacKalendarzowy'])
        rok_hydro = int(row['RokHydrologiczny'])

        if miesiac_kal in [11, 12]:
            return rok_hydro - 1
        else:
            return rok_hydro
    except (ValueError, TypeError):
        return pd.NA

def process_single_hydro_file(file_path):
    print(f"Przetwarzanie pliku: {file_path}")
    df = None
    used_encoding = None
    used_separator = None

    # Najpierw spróbujmy ze średnikiem, potem z przecinkiem
    separators_to_try = [';', ',']

    for separator in separators_to_try:
        for encoding_attempt in HYDRO_ENCODINGS_TO_TRY:
            try:
                # Sprawdzenie liczby kolumn/separatorów
                with open(file_path, 'r', encoding=encoding_attempt) as f_check:
                    first_line = f_check.readline().strip() # strip() na wszelki wypadek
                
                # Liczymy ile jest kolumn na podstawie pierwszego separatora z listy
                # który pasuje. Jeśli żaden nie pasuje, a jest tylko jedna "kolumna", to źle.
                num_fields = len(first_line.split(separator))

                if num_fields != len(HYDRO_COLUMN_NAMES):
                    # print(f"  Ostrzeżenie: Plik {file_path} (kod: {encoding_attempt}, sep: '{separator}') ma {num_fields} pól, oczekiwano {len(HYDRO_COLUMN_NAMES)}. Próba kolejnej kombinacji.")
                    continue

                temp_df = pd.read_csv(
                    file_path,
                    encoding=encoding_attempt,
                    header=None,
                    names=HYDRO_COLUMN_NAMES,
                    sep=separator, # Używamy testowanego separatora
                    na_values=HYDRO_NA_VALUES,
                    dtype=str # Wczytaj wszystko jako string na początku, potem konwertuj
                )
                df = temp_df
                used_encoding = encoding_attempt
                used_separator = separator
                print(f"  Pomyślnie wczytano z kodowaniem: {used_encoding}, separatorem: '{used_separator}'")
                break # Przerwij pętlę kodowań
            except UnicodeDecodeError:
                # print(f"  Nie udało się wczytać z kodowaniem: {encoding_attempt} (sep: '{separator}')")
                continue # Spróbuj następnego kodowania
            except FileNotFoundError:
                print(f"  BŁĄD: Plik {file_path} nie został znaleziony.")
                return None
            except pd.errors.EmptyDataError:
                print(f"  BŁĄD: Plik {file_path} jest pusty.")
                return None
            except Exception as e:
                # print(f"  Inny błąd podczas próby wczytania pliku {file_path} (kod: {encoding_attempt}, sep: '{separator}'): {e}")
                pass # Spróbuj dalej
        if df is not None:
            break # Przerwij pętlę separatorów, jeśli się udało

    if df is None:
        print(f"  NIEPOWODZENIE: Nie udało się wczytać pliku {file_path} przy użyciu żadnej z kombinacji kodowań/separatorów.")
        return None

    try:
        # Czyszczenie i konwersje po udanym wczytaniu
        for col in ["KodStacji", "NazwaStacji", "NazwaRzekiJeziora"]: # Dodajemy NazwaRzekiJeziora
            if col in df.columns:
                 df[col] = df[col].astype(str).str.strip()


        for col in ["StanWody_cm", "Przeplyw_m3s", "TemperaturaWody_C"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].str.replace(',', '.', regex=False), errors='coerce') # Zamień przecinek na kropkę dla liczb dziesiętnych

        # Konwersja kolumn daty na string przed próbą konwersji na int, a potem na datetime
        for col in ['RokHydrologiczny', 'MiesiacKalendarzowy', 'Dzien', 'WskaznikMiesiacaRokHydrologiczny']:
             if col in df.columns:
                df[col] = df[col].astype(str).str.strip()


        df['RokKalendarzowy'] = df.apply(determine_calendar_year, axis=1)
        
        date_components_valid = df[['RokKalendarzowy', 'MiesiacKalendarzowy', 'Dzien']].notna().all(axis=1)
        
        df.loc[date_components_valid, "Data"] = pd.to_datetime(
            df.loc[date_components_valid, "RokKalendarzowy"].astype(str) + '-' +
            df.loc[date_components_valid, "MiesiacKalendarzowy"].astype(str) + '-' +
            df.loc[date_components_valid, "Dzien"].astype(str),
            format='%Y-%m-%d',
            errors='coerce'
        )
        df.loc[~date_components_valid, "Data"] = pd.NaT

        cols_to_drop = ["RokHydrologiczny", "WskaznikMiesiacaRokHydrologiczny", "Dzien", "MiesiacKalendarzowy", "RokKalendarzowy"]
        df = df.drop(columns=[col for col in cols_to_drop if col in df.columns], errors='ignore')
        
        if 'Data' in df.columns:
            data_col = df.pop('Data')
            df.insert(0, 'Data', data_col)
        
        return df

    except Exception as e_processing:
        print(f"  Błąd podczas przetwarzania danych po wczytaniu pliku {file_path} (kod: {used_encoding}, sep: '{used_separator}'): {e_processing}")
        return None

# --- Główna część skryptu (bez zmian) ---
if __name__ == "__main__":
    all_hydro_files = []
    for root, _, files in os.walk(ROOT_HYDRO_DATA_DIR):
        for filename in files:
            if (filename.startswith("codz_") and filename.lower().endswith(".csv")):
                all_hydro_files.append(os.path.join(root, filename))

    if not all_hydro_files:
        print(f"Nie znaleziono żadnych plików 'codz_*.csv' w katalogu: {ROOT_HYDRO_DATA_DIR}")
    else:
        print(f"Znaleziono {len(all_hydro_files)} plików danych hydrologicznych do przetworzenia.")
        
        list_of_dataframes = []
        for f_path in sorted(all_hydro_files):
            df_single = process_single_hydro_file(f_path)
            if df_single is not None and not df_single.empty:
                list_of_dataframes.append(df_single)

        if list_of_dataframes:
            print("\nŁączenie wszystkich przetworzonych danych hydrologicznych...")
            final_hydro_df = pd.concat(list_of_dataframes, ignore_index=True)
            
            print("\n--- Wynikowa ramka danych hydrologicznych ---")
            final_hydro_df.info(verbose=True, show_counts=True)
            print("\nPierwsze 5 wierszy wynikowych danych:")
            print(final_hydro_df.head().to_string())
            print("\nOstatnie 5 wierszy wynikowych danych:")
            print(final_hydro_df.tail().to_string())

            try:
                final_hydro_df.to_csv(OUTPUT_FILENAME_HYDRO, index=False, encoding='utf-8-sig')
                print(f"\nPrzetworzone dane hydrologiczne zapisano do: {OUTPUT_FILENAME_HYDRO}")
            except Exception as e:
                print(f"Błąd podczas zapisywania pliku {OUTPUT_FILENAME_HYDRO}: {e}")
        else:
            print("Nie udało się przetworzyć żadnych plików hydrologicznych.")