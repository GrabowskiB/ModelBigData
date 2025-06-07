import pandas as pd
import numpy as np

# --- Konfiguracja ---
INPUT_METEO_POWIAT_DZIEN = "dane_meteo_powiat_dzien.csv"
OUTPUT_METEO_POWIAT_DZIEN_REDUCED = "dane_meteo_powiat_dzien_redukcja_brakow.csv"
THRESHOLD_MISSING_PERCENT = 70.0 # Próg procentowy braków do usunięcia kolumny

if __name__ == "__main__":
    print(f"Wczytywanie zagregowanych danych meteorologicznych: {INPUT_METEO_POWIAT_DZIEN}...")
    try:
        df_meteo_powiat = pd.read_csv(INPUT_METEO_POWIAT_DZIEN,
                                      parse_dates=['Data'],
                                      dtype={'Powiat': str},
                                      low_memory=False)
        print(f"Wczytano {len(df_meteo_powiat)} wierszy i {len(df_meteo_powiat.columns)} kolumn.")
    except FileNotFoundError:
        print(f"BŁĄD: Plik {INPUT_METEO_POWIAT_DZIEN} nie został znaleziony.")
        exit()
    except Exception as e:
        print(f"BŁĄD podczas wczytywania {INPUT_METEO_POWIAT_DZIEN}: {e}")
        exit()

    if df_meteo_powiat.empty:
        print("Wczytana ramka danych jest pusta.")
        exit()

    # Obliczanie procentu brakujących danych dla każdej kolumny
    missing_percentage = (df_meteo_powiat.isnull().sum() * 100) / len(df_meteo_powiat)
    
    # Identyfikacja kolumn do usunięcia
    cols_to_drop = missing_percentage[missing_percentage > THRESHOLD_MISSING_PERCENT].index.tolist()
    
    # Upewnijmy się, że nie usuwamy kluczowych kolumn jak 'Data' czy 'Powiat',
    # nawet jeśli jakimś cudem miałyby dużo braków (co nie powinno się zdarzyć)
    essential_cols = ['Data', 'Powiat']
    cols_to_drop = [col for col in cols_to_drop if col not in essential_cols]

    if cols_to_drop:
        print(f"\nKolumny do usunięcia (ponad {THRESHOLD_MISSING_PERCENT}% brakujących danych):")
        for col in cols_to_drop:
            print(f"  - {col} ({missing_percentage[col]:.2f}%)")
        
        df_meteo_powiat_reduced = df_meteo_powiat.drop(columns=cols_to_drop)
        print(f"\nUsunięto {len(cols_to_drop)} kolumn.")
        print(f"Nowa liczba kolumn: {len(df_meteo_powiat_reduced.columns)}")
    else:
        print(f"\nNie znaleziono kolumn z ponad {THRESHOLD_MISSING_PERCENT}% brakujących danych do usunięcia.")
        df_meteo_powiat_reduced = df_meteo_powiat.copy() # Kontynuuj z oryginalną ramką, jeśli nic nie usunięto

    print("\n--- Ramka danych po redukcji kolumn z brakami ---")
    df_meteo_powiat_reduced.info(verbose=False, show_counts=True)
    
    print("\nPierwsze 5 wierszy (pierwsze 10 kolumn, jeśli dostępne):")
    print(df_meteo_powiat_reduced.iloc[:, :min(10, len(df_meteo_powiat_reduced.columns))].head().to_string())

    try:
        df_meteo_powiat_reduced.to_csv(OUTPUT_METEO_POWIAT_DZIEN_REDUCED, index=False, encoding='utf-8-sig')
        print(f"\nDane meteorologiczne po redukcji kolumn zapisano do: {OUTPUT_METEO_POWIAT_DZIEN_REDUCED}")
    except Exception as e:
        print(f"Błąd podczas zapisywania pliku {OUTPUT_METEO_POWIAT_DZIEN_REDUCED}: {e}")