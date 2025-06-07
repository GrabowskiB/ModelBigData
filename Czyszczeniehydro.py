import os
import pandas as pd
import re # Do wyrażeń regularnych

# --- Konfiguracja ---
INPUT_STACJE_HYDRO_POWIATY = "kody_stacji_hydro_z_powiatami.csv"
OUTPUT_STACJE_HYDRO_POWIATY_PRZETWORZONE = "stacje_hydro_z_powiatami_przetworzone.csv"
# Zakładamy, że plik jest w CP1250, jeśli VS Code dobrze go wyświetla, może być też UTF-8
ENCODING_STACJE = 'cp1250' # lub 'utf-8'

def dms_to_decimal(dms_str):
    """Konwertuje string stopni, minut, sekund (np. "49 59 37") na stopnie dziesiętne."""
    if pd.isna(dms_str) or not isinstance(dms_str, str):
        return None
    
    parts = re.findall(r'\d+', dms_str) # Znajdź wszystkie liczby
    if len(parts) == 3:
        degrees, minutes, seconds = map(int, parts)
        decimal_degrees = degrees + minutes / 60 + seconds / 3600
        return decimal_degrees
    elif len(parts) == 2: # Czasem może brakować sekund
        degrees, minutes = map(int, parts)
        decimal_degrees = degrees + minutes / 60
        return decimal_degrees
    elif len(parts) == 1: # Tylko stopnie
        degrees = int(parts[0])
        return float(degrees)
    return None


# --- Główna część skryptu ---
if __name__ == "__main__":
    print(f"Przetwarzanie pliku: {INPUT_STACJE_HYDRO_POWIATY}")
    
    if not os.path.exists(INPUT_STACJE_HYDRO_POWIATY):
        print(f"BŁĄD: Plik {INPUT_STACJE_HYDRO_POWIATY} nie został znaleziony.")
    else:
        try:
            # Próba wczytania z różnymi popularnymi kodowaniami, jeśli domyślne zawiedzie
            df_stacje = None
            encodings_to_try = [ENCODING_STACJE, 'utf-8', 'iso-8859-2', 'latin1']
            if 'cp1250' not in encodings_to_try: # Upewnij się, że cp1250 jest na liście
                 encodings_to_try.insert(0,'cp1250')


            for enc in encodings_to_try:
                try:
                    df_stacje = pd.read_csv(INPUT_STACJE_HYDRO_POWIATY, sep=';', encoding=enc, dtype=str)
                    print(f"  Pomyślnie wczytano plik z kodowaniem: {enc}")
                    break 
                except UnicodeDecodeError:
                    print(f"  Nie udało się wczytać z kodowaniem: {enc}")
                except Exception as e_read: # Ogólny błąd wczytywania
                    print(f"  Błąd podczas wczytywania z kodowaniem {enc}: {e_read}")
            
            if df_stacje is None:
                print("  NIEPOWODZENIE: Nie udało się wczytać pliku stacji przy użyciu żadnego kodowania.")
                exit()

            # Usunięcie pierwszej kolumny 'LP.', jeśli istnieje i jest tylko indeksem
            if df_stacje.columns[0].strip().upper() == 'LP.':
                df_stacje = df_stacje.drop(columns=df_stacje.columns[0])

            # Oczyszczenie nazw kolumn (usuwanie spacji, kropek)
            df_stacje.columns = df_stacje.columns.str.strip().str.replace('.', '', regex=False)
            
            print("Oryginalne nazwy kolumn po oczyszczeniu:", df_stacje.columns.tolist())

            # Mapowanie nazw kolumn do standardowych nazw
            column_mapping = {
                'Szerokość geograficzna': 'Szerokośćgeograficzna',
                'Długość geograficzna': 'Długośćgeograficzna',
                'Wysokość npm': 'Wysokośćnpm'
            }
            
            # Zastosowanie mapowania
            df_stacje = df_stacje.rename(columns=column_mapping)
            
            print("Nazwy kolumn po mapowaniu:", df_stacje.columns.tolist())

            # Sprawdzenie, czy kluczowe kolumny istnieją
            required_cols = ['ID', 'Nazwa', 'Rzeka', 'Szerokośćgeograficzna', 'Długośćgeograficzna', 'Powiat']
            missing_cols = [col for col in required_cols if col not in df_stacje.columns]
            if missing_cols:
                print(f"BŁĄD: Brakuje wymaganych kolumn: {missing_cols}")
                print(f"Dostępne kolumny: {df_stacje.columns.tolist()}")
                exit()

            # Konwersja współrzędnych
            df_stacje['lat_dec'] = df_stacje['Szerokośćgeograficzna'].apply(dms_to_decimal)
            df_stacje['lon_dec'] = df_stacje['Długośćgeograficzna'].apply(dms_to_decimal)

            # Oczyszczenie ID stacji i Powiatu
            df_stacje['ID'] = df_stacje['ID'].str.strip()
            df_stacje['Powiat'] = df_stacje['Powiat'].str.strip().str.lower() # do małych liter dla spójności
            
            # Wysokość n.p.m. na numeryczny
            if 'Wysokośćnpm' in df_stacje.columns:
                df_stacje['Wysokośćnpm'] = pd.to_numeric(df_stacje['Wysokośćnpm'], errors='coerce')

            print("\n--- Przetworzona ramka danych stacji hydrologicznych ---")
            df_stacje.info()
            print("\nPierwsze 5 wierszy:")
            print(df_stacje[['ID', 'Nazwa', 'Rzeka', 'Powiat', 'Szerokośćgeograficzna', 'lat_dec', 'Długośćgeograficzna', 'lon_dec']].head().to_string())

            # Zapis do pliku
            df_stacje.to_csv(OUTPUT_STACJE_HYDRO_POWIATY_PRZETWORZONE, index=False, encoding='utf-8-sig', sep=';')
            print(f"\nPrzetworzone dane stacji hydrologicznych zapisano do: {OUTPUT_STACJE_HYDRO_POWIATY_PRZETWORZONE}")

        except Exception as e:
            print(f"Wystąpił ogólny błąd: {e}")