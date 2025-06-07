import pandas as pd
import numpy as np # Dla np.nan
import os

# --- Konfiguracja ---
INPUT_HYDRO_STACJE_Z_POWIATAMI_REDUCED = "dane_hydro_stacje_z_powiatami_redukcja.csv"
OUTPUT_HYDRO_POWIAT_DZIEN = "dane_hydro_powiat_dzien.csv"

# Kolumny numeryczne do agregacji (zostały nam StanWody_cm i Przeplyw_m3s)
HYDRO_NUMERIC_COLS_TO_AGGREGATE = [
    'StanWody_cm',
    'Przeplyw_m3s'
]

if __name__ == "__main__":
    print(f"Wczytywanie zredukowanych danych hydrologicznych stacji z powiatami: {INPUT_HYDRO_STACJE_Z_POWIATAMI_REDUCED}...")
    try:
        df_hydro_stacje = pd.read_csv(INPUT_HYDRO_STACJE_Z_POWIATAMI_REDUCED,
                                      parse_dates=['Data'],
                                      dtype={'KodStacji': str, 'Powiat': str, 'NazwaStacji': str},
                                      low_memory=False)
        print(f"Wczytano {len(df_hydro_stacje)} wierszy.")
    except FileNotFoundError:
        print(f"BŁĄD: Plik {INPUT_HYDRO_STACJE_Z_POWIATAMI_REDUCED} nie został znaleziony.")
        exit()
    except Exception as e:
        print(f"BŁĄD podczas wczytywania {INPUT_HYDRO_STACJE_Z_POWIATAMI_REDUCED}: {e}")
        exit()

    if df_hydro_stacje.empty:
        print("Wczytana ramka danych hydrologicznych jest pusta.")
        exit()

    # Usunięcie wierszy, gdzie 'Powiat' jest NaN lub naszym placeholderem, zanim zagregujemy
    df_hydro_stacje_filtered = df_hydro_stacje[
        ~df_hydro_stacje['Powiat'].isin(['brak_przypisanego_powiatu_hydro', 'niezidentyfikowany_powiat', np.nan, None, 'nan'])
    ].copy()
    
    if df_hydro_stacje_filtered.empty:
        print("Brak danych hydrologicznych z przypisanymi powiatami do agregacji.")
        exit()

    print(f"\nAgregowanie danych hydrologicznych na poziom (Data, Powiat)... Liczba wierszy przed agregacją: {len(df_hydro_stacje_filtered)}")

    # Definicja funkcji agregujących - dla obu chcemy średnią
    agg_functions_hydro = {}
    for col in HYDRO_NUMERIC_COLS_TO_AGGREGATE:
        if col in df_hydro_stacje_filtered.columns: # Sprawdź, czy kolumna istnieje
            agg_functions_hydro[col] = 'mean' 

    if not agg_functions_hydro:
        print("BŁĄD: Żadna z kolumn do agregacji nie została znaleziona w pliku wejściowym.")
        exit()
        
    print("\nDefinicje agregacji dla danych hydrologicznych:")
    for k,v in agg_functions_hydro.items(): print(f"  {k}: {v}")

    # Grupujemy i agregujemy
    # Używamy pętli dla lepszej kontroli nad skipna i potencjalnie min_count, jeśli będzie potrzebne
    grouped_hydro = df_hydro_stacje_filtered.groupby(['Data', 'Powiat'])
    aggregated_hydro_data_list = []

    for group_keys, group_df in grouped_hydro:
        single_aggregated_row = {'Data': group_keys[0], 'Powiat': group_keys[1]}
        for col_to_aggregate, agg_func_name in agg_functions_hydro.items():
            series_to_aggregate = group_df[col_to_aggregate]
            if series_to_aggregate.notna().any(): # Agreguj tylko jeśli są jakieś wartości nie-NaN
                if agg_func_name == 'mean':
                    single_aggregated_row[col_to_aggregate] = series_to_aggregate.mean(skipna=True)
                # Można dodać inne funkcje w przyszłości
                else:
                    single_aggregated_row[col_to_aggregate] = series_to_aggregate.agg(agg_func_name) # Ogólne wywołanie
            else:
                single_aggregated_row[col_to_aggregate] = np.nan
        aggregated_hydro_data_list.append(single_aggregated_row)
        
    df_hydro_powiat_dzien = pd.DataFrame(aggregated_hydro_data_list)
    
    print(f"Liczba wierszy po agregacji: {len(df_hydro_powiat_dzien)}")

    print("\n--- Zagregowana ramka danych hydrologicznych (Powiat-Dzień) ---")
    df_hydro_powiat_dzien.info(verbose=False, show_counts=True)
    
    print("\nPierwsze 5 wierszy zagregowanych danych hydrologicznych:")
    print(df_hydro_powiat_dzien.head().to_string())

    try:
        df_hydro_powiat_dzien.to_csv(OUTPUT_HYDRO_POWIAT_DZIEN, index=False, encoding='utf-8-sig')
        print(f"\nZagregowane dane hydrologiczne (powiat-dzień) zapisano do: {OUTPUT_HYDRO_POWIAT_DZIEN}")
    except Exception as e:
        print(f"Błąd podczas zapisywania pliku {OUTPUT_HYDRO_POWIAT_DZIEN}: {e}")