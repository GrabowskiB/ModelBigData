import pandas as pd
import numpy as np

# --- Konfiguracja ---
INPUT_METEO_STACJE_Z_POWIATAMI = "dane_meteo_stacje_z_powiatami_final.csv"
OUTPUT_METEO_POWIAT_DZIEN = "dane_meteo_powiat_dzien.csv"

if __name__ == "__main__":
    print(f"Wczytywanie danych meteorologicznych stacji z powiatami: {INPUT_METEO_STACJE_Z_POWIATAMI}...")
    try:
        df_meteo_stacje = pd.read_csv(INPUT_METEO_STACJE_Z_POWIATAMI,
                                      parse_dates=['Data'],
                                      dtype={'KodStacji': str, 'Powiat': str},
                                      low_memory=False)
        print(f"Wczytano {len(df_meteo_stacje)} wierszy.")
    except FileNotFoundError:
        print(f"BŁĄD: Plik {INPUT_METEO_STACJE_Z_POWIATAMI} nie został znaleziony.")
        exit()
    except Exception as e:
        print(f"BŁĄD podczas wczytywania {INPUT_METEO_STACJE_Z_POWIATAMI}: {e}")
        exit()

    # Kolumny, które nie będą agregowane numerycznie (pomijamy je w słowniku agg_functions)
    # lub dla których chcemy specjalnej agregacji
    non_numeric_or_special_agg_cols = ['Data', 'KodStacji', 'NazwaStacji', 'Powiat', 
                                       'RodzajOpadu', 'StanGruntu_ZR', # To są kody/tekst
                                       'GatunekSniegu_kod', 'RodzajPokrywy_kod'] # Również kody

    # Automatyczne tworzenie słownika funkcji agregujących
    # Dla większości kolumn numerycznych użyjemy 'mean'
    # Dla kolumn binarnych (0/1) jak WystPokrywySnieznej_01 użyjemy 'max'
    agg_functions = {}
    for col in df_meteo_stacje.columns:
        if col not in non_numeric_or_special_agg_cols:
            if df_meteo_stacje[col].dtype in [np.float64, np.int64]: # Sprawdzamy czy kolumna jest numeryczna
                if col in ["WystPokrywySnieznej_01", "WystBlyskawicy_01"]:
                    agg_functions[col] = 'max'  # Jeśli wystąpiło na jednej stacji, przyjmujemy że wystąpiło w powiecie
                elif "Opady" in col or "WODZ" in col or "WONO" in col or "SMDB" in col: # Wszystkie typy opadów
                    agg_functions[col] = 'mean' # Średnia suma opadów
                else:
                    agg_functions[col] = 'mean'
            # Można dodać logikę dla kolumn tekstowych/kodowych, jeśli chcemy je jakoś zagregować
            # np. 'first' lub lambda x: x.mode()[0] if not x.mode().empty else np.nan

    print("\nDefinicje agregacji:")
    for k,v in agg_functions.items(): print(f"  {k}: {v}")

    # Usunięcie wierszy, gdzie 'Powiat' jest NaN lub naszym placeholderem, zanim zagregujemy
    # chyba że chcemy je traktować jako osobny "powiat"
    df_meteo_stacje_filtered = df_meteo_stacje[
        ~df_meteo_stacje['Powiat'].isin(['brak_przypisanego_powiatu_meteo', 'niezidentyfikowany_powiat', np.nan, None, 'nan'])
    ].copy()
    
    if df_meteo_stacje_filtered.empty:
        print("Brak danych z przypisanymi powiatami do agregacji.")
        exit()

    print(f"\nAgregowanie danych na poziom (Data, Powiat)... Liczba wierszy przed agregacją: {len(df_meteo_stacje_filtered)}")
    
    # Grupujemy i agregujemy, używając min_count=1, aby NaN był wynikiem tylko jeśli wszystkie wartości w grupie są NaN
    # df_meteo_powiat_dzien = df_meteo_stacje_filtered.groupby(['Data', 'Powiat'], as_index=False).agg(
    #     pd.NamedAgg(column=key, aggfunc=val) for key, val in agg_functions.items()
    # )
    # W nowszych Pandas można by użyć:
    # df_meteo_powiat_dzien = df_meteo_stacje_filtered.groupby(['Data', 'Powiat'], as_index=False).agg(**agg_functions)
    # Ale NamedAgg jest bardziej uniwersalne dla starszych wersji.
    # Jeszcze prościej, jeśli agg_functions jest prostym słownikiem:
    # df_meteo_powiat_dzien = df_meteo_stacje_filtered.groupby(['Data', 'Powiat'], as_index=False).agg(agg_functions)
    # Pandas > 1.1.0 powinien sobie poradzić z tym ostatnim.
    # Aby uniknąć problemów z wersjami i ostrzeżeniami o Future поведение, można zrobić tak:
    
    # Tworzymy pusty DataFrame do przechowywania zagregowanych wyników
    grouped = df_meteo_stacje_filtered.groupby(['Data', 'Powiat'])
    aggregated_data = []

    for (date, powiat), group in grouped:
        agg_row = {'Data': date, 'Powiat': powiat}
        for col, func in agg_functions.items():
            if col in group:
                # Używamy min_count=1 dla mean, sum, min, max aby NaN był tylko gdy wszystkie są NaN
                if func in ['mean', 'sum', 'min', 'max', 'median'] and group[col].notna().any():
                    if func == 'mean': agg_row[col] = group[col].mean(skipna=True)
                    elif func == 'sum': agg_row[col] = group[col].sum(skipna=True, min_count=1)
                    elif func == 'min': agg_row[col] = group[col].min(skipna=True)
                    elif func == 'max': agg_row[col] = group[col].max(skipna=True)
                    elif func == 'median': agg_row[col] = group[col].median(skipna=True)
                elif group[col].notna().any(): # Dla innych funkcji, jeśli są jakieś dane
                     agg_row[col] = group[col].agg(func) # Ogólne wywołanie, może wymagać dostosowania
                else:
                    agg_row[col] = np.nan # Jeśli wszystkie są NaN w grupie
            else:
                agg_row[col] = np.nan # Jeśli kolumna nie istnieje w grupie (nie powinno się zdarzyć)
        aggregated_data.append(agg_row)
    
    df_meteo_powiat_dzien = pd.DataFrame(aggregated_data)


    print(f"Liczba wierszy po agregacji: {len(df_meteo_powiat_dzien)}")

    print("\n--- Zagregowana ramka danych meteorologicznych (Powiat-Dzień) ---")
    df_meteo_powiat_dzien.info(verbose=False, show_counts=True)
    
    print("\nPierwsze 5 wierszy zagregowanych danych:")
    print(df_meteo_powiat_dzien.head().to_string())

    try:
        df_meteo_powiat_dzien.to_csv(OUTPUT_METEO_POWIAT_DZIEN, index=False, encoding='utf-8-sig')
        print(f"\nZagregowane dane meteorologiczne (powiat-dzień) zapisano do: {OUTPUT_METEO_POWIAT_DZIEN}")
    except Exception as e:
        print(f"Błąd podczas zapisywania pliku {OUTPUT_METEO_POWIAT_DZIEN}: {e}")