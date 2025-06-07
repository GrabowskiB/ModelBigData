import os
import pandas as pd
import re # Do wyrażeń regularnych

# --- Konfiguracja ---
INPUT_STACJE_METEO = "kody_stacji.csv" # Plik, który właśnie wysłałeś
OUTPUT_STACJE_METEO_PRZETWORZONE = "stacje_meteo_wspolrzedne_przetworzone.csv"
# Załóżmy cp1250 lub utf-8 jako prawdopodobne
ENCODING_STACJE_METEO = 'cp1250' # Spróbujemy też utf-8, jeśli to zawiedzie

def dms_to_decimal(dms_str):
    """Konwertuje string stopni, minut, sekund (np. "49 59 37") na stopnie dziesiętne."""
    if pd.isna(dms_str) or not isinstance(dms_str, str):
        return None
    
    parts = re.findall(r'\d+', dms_str)
    if len(parts) == 3:
        degrees, minutes, seconds = map(int, parts)
        decimal_degrees = degrees + minutes / 60 + seconds / 3600
        return decimal_degrees
    elif len(parts) == 2:
        degrees, minutes = map(int, parts)
        decimal_degrees = degrees + minutes / 60
        return decimal_degrees
    elif len(parts) == 1:
        degrees = int(parts[0])
        return float(degrees)
    return None

# --- Główna część skryptu ---
if __name__ == "__main__":
    print(f"Przetwarzanie pliku stacji meteorologicznych: {INPUT_STACJE_METEO}")
    
    if not os.path.exists(INPUT_STACJE_METEO):
        print(f"BŁĄD: Plik {INPUT_STACJE_METEO} nie został znaleziony.")
    else:
        try:
            df_stacje_meteo = None
            encodings_to_try_meteo = [ENCODING_STACJE_METEO, 'utf-8', 'iso-8859-2', 'latin1']
            if 'cp1250' not in encodings_to_try_meteo:
                 encodings_to_try_meteo.insert(0,'cp1250')


            for enc in encodings_to_try_meteo:
                try:
                    # Zakładamy, że kolumny mają te same nazwy co w pliku hydro
                    df_stacje_meteo = pd.read_csv(INPUT_STACJE_METEO, sep=';', encoding=enc, dtype=str)
                    print(f"  Pomyślnie wczytano plik stacji meteo z kodowaniem: {enc}")
                    break 
                except UnicodeDecodeError:
                    print(f"  Nie udało się wczytać stacji meteo z kodowaniem: {enc}")
                except Exception as e_read:
                    print(f"  Błąd podczas wczytywania stacji meteo z kodowaniem {enc}: {e_read}")
            
            if df_stacje_meteo is None:
                print("  NIEPOWODZENIE: Nie udało się wczytać pliku stacji meteo przy użyciu żadnego kodowania.")
                exit()

            if df_stacje_meteo.columns[0].strip().upper() == 'LP.':
                df_stacje_meteo = df_stacje_meteo.drop(columns=df_stacje_meteo.columns[0])

            df_stacje_meteo.columns = df_stacje_meteo.columns.str.strip().str.replace('.', '', regex=False)
            
            print("Oryginalne nazwy kolumn stacji meteo po oczyszczeniu:", df_stacje_meteo.columns.tolist())

            # Mapowanie nazw kolumn do standardowych nazw
            column_mapping_meteo = {
                'Szerokość geograficzna': 'Szerokośćgeograficzna',
                'Długość geograficzna': 'Długośćgeograficzna',
                'Wysokość npm': 'Wysokośćnpm'
            }
            
            # Zastosowanie mapowania
            df_stacje_meteo = df_stacje_meteo.rename(columns=column_mapping_meteo)
            
            print("Nazwy kolumn stacji meteo po mapowaniu:", df_stacje_meteo.columns.tolist())

            # Sprawdzenie wymaganych kolumn do konwersji współrzędnych
            # W pliku meteo nie ma kolumny 'Powiat'
            required_cols_meteo = ['ID', 'Nazwa', 'Szerokośćgeograficzna', 'Długośćgeograficzna']
            missing_cols_meteo = [col for col in required_cols_meteo if col not in df_stacje_meteo.columns]
            if missing_cols_meteo:
                print(f"BŁĄD: Brakuje wymaganych kolumn w pliku stacji meteo: {missing_cols_meteo}")
                print(f"Dostępne kolumny: {df_stacje_meteo.columns.tolist()}")
                exit()

            df_stacje_meteo['lat_dec'] = df_stacje_meteo['Szerokośćgeograficzna'].apply(dms_to_decimal)
            df_stacje_meteo['lon_dec'] = df_stacje_meteo['Długośćgeograficzna'].apply(dms_to_decimal)

            df_stacje_meteo['ID'] = df_stacje_meteo['ID'].str.strip()
            
            if 'Wysokośćnpm' in df_stacje_meteo.columns:
                 df_stacje_meteo['Wysokośćnpm'] = pd.to_numeric(df_stacje_meteo['Wysokośćnpm'], errors='coerce')


            print("\n--- Przetworzona ramka danych stacji meteorologicznych ---")
            df_stacje_meteo.info()
            print("\nPierwsze 5 wierszy:")
            # Wyświetlamy tylko relevantne kolumny, bo nie ma tu powiatu
            cols_to_show = ['ID', 'Nazwa', 'Szerokośćgeograficzna', 'lat_dec', 'Długośćgeograficzna', 'lon_dec']
            if 'Rzeka' in df_stacje_meteo.columns: # Rzeka może być lub nie
                cols_to_show.insert(2, 'Rzeka')
            print(df_stacje_meteo[cols_to_show].head().to_string())

            df_stacje_meteo.to_csv(OUTPUT_STACJE_METEO_PRZETWORZONE, index=False, encoding='utf-8-sig', sep=';')
            print(f"\nPrzetworzone dane stacji meteorologicznych zapisano do: {OUTPUT_STACJE_METEO_PRZETWORZONE}")

        except Exception as e:
            print(f"Wystąpił ogólny błąd przy przetwarzaniu stacji meteo: {e}")