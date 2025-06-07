import os
import pandas as pd

# --- Konfiguracja ---
INPUT_METEO_STACJE_OCZYSZCZONE = "dane_meteo_stacje_oczyszczone.csv"
# WAŻNE: Podaj poprawną nazwę pliku CSV z listą stacji METEOROLOGICZNYCH,
# który zawiera kolumnę 'Powiat'
INPUT_STACJE_METEO_Z_POWIATAMI_CSV = "kody_stacji_z_powiatami.csv" # <--- ZMIEŃ NA WŁAŚCIWĄ NAZWĘ, JEŚLI INNA
# Zakładamy, że kodowanie jest takie samo jak dla hydro z powiatami
ENCODING_STACJE_METEO_POWIATY = 'cp1250' # lub 'utf-8'

OUTPUT_METEO_STACJE_Z_POWIATAMI_FINAL = "dane_meteo_stacje_z_powiatami_final.csv"

if __name__ == "__main__":
    print(f"Wczytywanie oczyszczonych danych meteorologicznych ze stacji: {INPUT_METEO_STACJE_OCZYSZCZONE}...")
    try:
        df_meteo_oczyszczone = pd.read_csv(INPUT_METEO_STACJE_OCZYSZCZONE,
                                           parse_dates=['Data'],
                                           dtype={'KodStacji': str},
                                           low_memory=False)
        print(f"Wczytano {len(df_meteo_oczyszczone)} wierszy z danych oczyszczonych.")
    except FileNotFoundError:
        print(f"BŁĄD: Plik {INPUT_METEO_STACJE_OCZYSZCZONE} nie został znaleziony.")
        exit()
    except Exception as e:
        print(f"BŁĄD podczas wczytywania {INPUT_METEO_STACJE_OCZYSZCZONE}: {e}")
        exit()

    print(f"\nWczytywanie danych o stacjach meteorologicznych z powiatami: {INPUT_STACJE_METEO_Z_POWIATAMI_CSV}...")
    try:
        df_stacje_meteo_powiaty = None
        encodings_to_try = [ENCODING_STACJE_METEO_POWIATY, 'utf-8', 'iso-8859-2', 'latin1']
        if 'cp1250' not in encodings_to_try: encodings_to_try.insert(0,'cp1250')

        for enc in encodings_to_try:
            try:
                # Zakładamy separator ';' i obecność kolumn 'ID', 'Nazwa', 'Powiat'
                df_stacje_meteo_powiaty_temp = pd.read_csv(INPUT_STACJE_METEO_Z_POWIATAMI_CSV, sep=';', encoding=enc, dtype=str)
                
                # Sprawdzenie, czy wczytano poprawnie i czy są potrzebne kolumny
                # (dostosuj 'ID' i 'Powiat' jeśli nazwy kolumn w pliku są inne)
                if 'ID' in df_stacje_meteo_powiaty_temp.columns and \
                   'Nazwa' in df_stacje_meteo_powiaty_temp.columns and \
                   'Powiat' in df_stacje_meteo_powiaty_temp.columns:
                    df_stacje_meteo_powiaty = df_stacje_meteo_powiaty_temp
                    print(f"  Pomyślnie wczytano plik stacji meteo z powiatami (kodowanie: {enc})")
                    break
                else:
                    print(f"  Nie znaleziono wymaganych kolumn ('ID', 'Nazwa', 'Powiat') przy kodowaniu {enc}. Dostępne: {df_stacje_meteo_powiaty_temp.columns.tolist()}")
            except UnicodeDecodeError:
                print(f"  Nie udało się wczytać stacji meteo z powiatami (kodowanie: {enc})")
            except Exception as e_read:
                print(f"  Błąd podczas wczytywania stacji meteo z powiatami (kodowanie {enc}): {e_read}")
        
        if df_stacje_meteo_powiaty is None:
            print(f"  NIEPOWODZENIE: Nie udało się wczytać pliku {INPUT_STACJE_METEO_Z_POWIATAMI_CSV} lub brakuje w nim wymaganych kolumn.")
            exit()

        # Usunięcie pierwszej kolumny 'LP.', jeśli istnieje
        if df_stacje_meteo_powiaty.columns[0].strip().upper() == 'LP.':
            df_stacje_meteo_powiaty = df_stacje_meteo_powiaty.drop(columns=df_stacje_meteo_powiaty.columns[0])
        
        # Oczyszczenie nazw kolumn (na wszelki wypadek)
        df_stacje_meteo_powiaty.columns = df_stacje_meteo_powiaty.columns.str.strip().str.replace('.', '', regex=False)

        # Wybieramy tylko potrzebne kolumny i zmieniamy nazwę ID na KodStacji
        # Upewnij się, że nazwy kolumn ('ID', 'Nazwa', 'Powiat') są poprawne dla Twojego pliku
        df_stacje_info_final = df_stacje_meteo_powiaty[['ID', 'Nazwa', 'Powiat']].rename(
            columns={'ID': 'KodStacji', 'Nazwa': 'NazwaStacjiOficjalna'}
        )
        df_stacje_info_final['KodStacji'] = df_stacje_info_final['KodStacji'].str.strip()
        df_stacje_info_final['Powiat'] = df_stacje_info_final['Powiat'].astype(str).str.lower().str.strip()
        df_stacje_info_final['NazwaStacjiOficjalna'] = df_stacje_info_final['NazwaStacjiOficjalna'].astype(str).str.strip()

        print(f"Przetworzono informacje o {len(df_stacje_info_final)} stacjach meteo z powiatami.")

    except FileNotFoundError:
        print(f"BŁĄD: Plik {INPUT_STACJE_METEO_Z_POWIATAMI_CSV} nie został znaleziony.")
        exit()
    except Exception as e:
        print(f"BŁĄD podczas wczytywania lub przetwarzania {INPUT_STACJE_METEO_Z_POWIATAMI_CSV}: {e}")
        exit()

    # 1. Połączenie danych pomiarowych z informacjami o stacjach (oficjalna nazwa, powiat)
    print("\nŁączenie danych pomiarowych z informacjami o stacjach i powiatach...")
    df_meteo_final = pd.merge(df_meteo_oczyszczone, df_stacje_info_final, on="KodStacji", how="left")

    # Usunięcie tymczasowej kolumny NazwaStacji_Skonsolidowana
    if 'NazwaStacji_Skonsolidowana' in df_meteo_final.columns:
        df_meteo_final = df_meteo_final.drop(columns=['NazwaStacji_Skonsolidowana'])
    
    # Przeniesienie NazwaStacjiOficjalna i Powiat bliżej początku
    if 'NazwaStacjiOficjalna' in df_meteo_final.columns:
        nazwa_col = df_meteo_final.pop('NazwaStacjiOficjalna')
        df_meteo_final.insert(2, 'NazwaStacji', nazwa_col) # Zmieniamy nazwę na spójną 'NazwaStacji'
    if 'Powiat' in df_meteo_final.columns:
        powiat_col = df_meteo_final.pop('Powiat')
        df_meteo_final.insert(3, 'Powiat', powiat_col)

    print(f"Liczba wierszy po połączeniu: {len(df_meteo_final)}")
    print(f"Liczba wierszy bez dopasowanego powiatu (przed wypełnieniem): {df_meteo_final['Powiat'].isna().sum()}")
    
    # Wypełnienie NaN w Powiat (jeśli stacja nie została znaleziona w pliku z powiatami)
    df_meteo_final['Powiat'] = df_meteo_final['Powiat'].fillna('brak_przypisanego_powiatu_meteo')

    print("\n--- Finalna ramka danych meteorologicznych (stacje z powiatami) ---")
    df_meteo_final.info(verbose=False, show_counts=True)
    
    cols_to_show = ['Data', 'KodStacji', 'NazwaStacji', 'Powiat'] + \
                   [col for col in df_meteo_final.columns if col.endswith('_Final_C') or col.endswith('_Final_mm')][:2] # Przykładowe
    print("\nPierwsze 5 wierszy (wybrane kolumny):")
    print(df_meteo_final[[col for col in cols_to_show if col in df_meteo_final.columns]].head().to_string())

    try:
        df_meteo_final.to_csv(OUTPUT_METEO_STACJE_Z_POWIATAMI_FINAL, index=False, encoding='utf-8-sig')
        print(f"\nDane meteorologiczne stacji z przypisanymi powiatami zapisano do: {OUTPUT_METEO_STACJE_Z_POWIATAMI_FINAL}")
    except Exception as e:
        print(f"Błąd podczas zapisywania pliku {OUTPUT_METEO_STACJE_Z_POWIATAMI_FINAL}: {e}")
        import traceback
        traceback.print_exc()