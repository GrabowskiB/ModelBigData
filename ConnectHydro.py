import pandas as pd
import os

# --- Konfiguracja ---
INPUT_HYDRO_PRZETWORZONE = "przetworzone_dane_hydrologiczne.csv"
INPUT_STACJE_HYDRO_Z_POWIATAMI = "stacje_hydro_z_powiatami_przetworzone.csv" 

OUTPUT_HYDRO_STACJE_Z_POWIATAMI_FINAL = "dane_hydro_stacje_z_powiatami_final.csv" # Zmieniona nazwa pliku wyjściowego

if __name__ == "__main__":
    print(f"Wczytywanie przetworzonych danych hydrologicznych: {INPUT_HYDRO_PRZETWORZONE}...")
    try:
        df_hydro = pd.read_csv(INPUT_HYDRO_PRZETWORZONE,
                               parse_dates=['Data'],
                               dtype={'KodStacji': str},
                               low_memory=False)
        print(f"Wczytano {len(df_hydro)} wierszy z danych hydrologicznych.")
    except FileNotFoundError:
        print(f"BŁĄD: Plik {INPUT_HYDRO_PRZETWORZONE} nie został znaleziony.")
        exit()
    except Exception as e:
        print(f"BŁĄD podczas wczytywania {INPUT_HYDRO_PRZETWORZONE}: {e}")
        exit()

    # ... (początek skryptu bez zmian) ...

    print(f"\nWczytywanie przetworzonych informacji o stacjach hydrologicznych z powiatami: {INPUT_STACJE_HYDRO_Z_POWIATAMI}...")
    try:
        df_stacje_info_raw = None # Zmieniamy nazwę zmiennej tymczasowej
        encodings_to_try = ['utf-8', 'iso-8859-2', 'latin1'] # ENCODING_STACJE_HYDRO_POWIATY zdefiniuj wcześniej
        if 'cp1250' not in encodings_to_try: encodings_to_try.insert(0,'cp1250')


        for enc in encodings_to_try:
            try:
                df_stacje_info_temp = pd.read_csv(INPUT_STACJE_HYDRO_Z_POWIATAMI, sep=';', encoding=enc, dtype=str)
                # Sprawdź, czy są wymagane oryginalne nazwy kolumn przed zmianą nazwy
                # Oryginalne nazwy to 'ID', 'Nazwa', 'Powiat' z pliku CSV
                # 'Rzeka' też jest ważna, jeśli chcemy ją przenieść
                required_original_cols = ['ID', 'Nazwa', 'Powiat']
                if 'Rzeka' in df_stacje_info_temp.columns: # Jeśli jest kolumna Rzeka, dodaj ją do wymaganych
                    required_original_cols.append('Rzeka')

                if all(col in df_stacje_info_temp.columns for col in ['ID', 'Nazwa', 'Powiat']): # Minimalny zestaw
                    df_stacje_info_raw = df_stacje_info_temp
                    print(f"  Pomyślnie wczytano plik stacji hydro z powiatami (kodowanie: {enc})")
                    break
                else:
                    print(f"  Nie znaleziono wymaganych kolumn ('ID', 'Nazwa', 'Powiat') przy kodowaniu {enc}. Dostępne: {df_stacje_info_temp.columns.tolist()}")
            except UnicodeDecodeError:
                print(f"  Nie udało się wczytać stacji hydro z powiatami (kodowanie: {enc})")
            except Exception as e_read:
                print(f"  Błąd podczas wczytywania stacji hydro z powiatami (kodowanie {enc}): {e_read}")
        
        if df_stacje_info_raw is None:
            print(f"  NIEPOWODZENIE: Nie udało się wczytać pliku {INPUT_STACJE_HYDRO_Z_POWIATAMI} lub brakuje w nim wymaganych kolumn.")
            exit()
        
        # Teraz dopiero zmieniamy nazwy kolumn i wybieramy te potrzebne
        df_stacje_info_selected = df_stacje_info_raw.rename(columns={'ID': 'KodStacji', 'Nazwa': 'NazwaStacjiOficjalna'})
        
        # Kolumny do wybrania po zmianie nazwy
        cols_to_select = ['KodStacji', 'NazwaStacjiOficjalna', 'Powiat']
        if 'Rzeka' in df_stacje_info_selected.columns: # Jeśli Rzeka jest w pliku stacji
            cols_to_select.append('Rzeka')
        
        # Upewnij się, że wybieramy tylko istniejące kolumny po renamingu
        df_stacje_info_selected = df_stacje_info_selected[[col for col in cols_to_select if col in df_stacje_info_selected.columns]].copy()

        # Czyszczenie danych
        df_stacje_info_selected['KodStacji'] = df_stacje_info_selected['KodStacji'].str.strip()
        df_stacje_info_selected['Powiat'] = df_stacje_info_selected['Powiat'].astype(str).str.lower().str.strip()
        df_stacje_info_selected['NazwaStacjiOficjalna'] = df_stacje_info_selected['NazwaStacjiOficjalna'].astype(str).str.strip()
        if 'Rzeka' in df_stacje_info_selected.columns:
            df_stacje_info_selected['Rzeka'] = df_stacje_info_selected['Rzeka'].astype(str).str.strip()


        print(f"Przetworzono informacje o {len(df_stacje_info_selected)} stacjach hydrologicznych z powiatami.")

    except FileNotFoundError:
        print(f"BŁĄD: Plik {INPUT_STACJE_HYDRO_Z_POWIATAMI} nie został znaleziony.")
        exit()
    except Exception as e:
        print(f"BŁĄD podczas wczytywania lub przetwarzania {INPUT_STACJE_HYDRO_Z_POWIATAMI}: {e}")
        import traceback
        traceback.print_exc() # Dodaj to, aby zobaczyć pełny traceback błędu
        exit()

    # Łączenie danych hydrologicznych z informacjami o stacjach i powiatach
    print("\nŁączenie danych hydrologicznych z informacjami o stacjach (powiat, nazwa)...")
    
    # Zapisz oryginalną NazwaStacji i NazwaRzekiJeziora z df_hydro, jeśli istnieją
    original_nazwa_stacji_in_df_hydro = df_hydro.get('NazwaStacji')
    original_rzeka_in_df_hydro = df_hydro.get('NazwaRzekiJeziora')

    # Usuń oryginalne kolumny NazwaStacji i NazwaRzekiJeziora z df_hydro przed merge, aby uniknąć duplikatów
    cols_to_drop_from_hydro = []
    if 'NazwaStacji' in df_hydro.columns:
        cols_to_drop_from_hydro.append('NazwaStacji')
    if 'NazwaRzekiJeziora' in df_hydro.columns:
        cols_to_drop_from_hydro.append('NazwaRzekiJeziora')
    
    if cols_to_drop_from_hydro:
        df_hydro = df_hydro.drop(columns=cols_to_drop_from_hydro)
    
    df_hydro_z_powiatami = pd.merge(df_hydro, df_stacje_info_selected, on="KodStacji", how="left")

    # Zmień nazwę 'NazwaStacjiOficjalna' na 'NazwaStacji'
    if 'NazwaStacjiOficjalna' in df_hydro_z_powiatami.columns:
        df_hydro_z_powiatami = df_hydro_z_powiatami.rename(columns={'NazwaStacjiOficjalna': 'NazwaStacji'})
    # Jeśli po merge nie ma 'NazwaStacji' (bo np. nie było jej w df_stacje_info_selected), 
    # a była oryginalnie w df_hydro, przywróć ją
    elif original_nazwa_stacji_in_df_hydro is not None and 'NazwaStacji' not in df_hydro_z_powiatami.columns:
        df_hydro_z_powiatami['NazwaStacji'] = original_nazwa_stacji_in_df_hydro
    
    # Podobnie dla Rzeki, jeśli nie przyszła z merge
    if 'Rzeka' not in df_hydro_z_powiatami.columns and original_rzeka_in_df_hydro is not None:
        df_hydro_z_powiatami['NazwaRzekiJeziora'] = original_rzeka_in_df_hydro


    # Ustawienie kolejności kolumn
    cols_order_start = ['Data', 'KodStacji', 'NazwaStacji', 'Powiat']
    if 'Rzeka' in df_hydro_z_powiatami.columns:
        cols_order_start.append('Rzeka')
    elif 'NazwaRzekiJeziora' in df_hydro_z_powiatami.columns: # Jeśli Rzeka nie została dodana, ale NazwaRzekiJeziora istnieje
        cols_order_start.append('NazwaRzekiJeziora')
        
    remaining_cols = [col for col in df_hydro_z_powiatami.columns if col not in cols_order_start]
    final_cols_order_existing = [col for col in (cols_order_start + remaining_cols) if col in df_hydro_z_powiatami.columns]

    df_hydro_z_powiatami = df_hydro_z_powiatami[final_cols_order_existing]

    # ... (reszta skryptu: print info, head, zapis do pliku - bez zmian) ...

    print(f"Liczba wierszy po połączeniu: {len(df_hydro_z_powiatami)}")
    missing_powiat_count = df_hydro_z_powiatami['Powiat'].isna().sum()
    print(f"Liczba wierszy bez dopasowanego powiatu (stacje hydro): {missing_powiat_count}")
    
    df_hydro_z_powiatami['Powiat'] = df_hydro_z_powiatami['Powiat'].fillna('brak_przypisanego_powiatu_hydro')

    print("\n--- Ramka danych hydrologicznych stacji z powiatami (bez współrzędnych) ---")
    df_hydro_z_powiatami.info(verbose=False, show_counts=True)
    
    print("\nPierwsze 5 wierszy:")
    print(df_hydro_z_powiatami.head().to_string())

    try:
        df_hydro_z_powiatami.to_csv(OUTPUT_HYDRO_STACJE_Z_POWIATAMI_FINAL, index=False, encoding='utf-8-sig')
        print(f"\nDane hydrologiczne stacji z powiatami zapisano do: {OUTPUT_HYDRO_STACJE_Z_POWIATAMI_FINAL}")
    except Exception as e:
        print(f"Błąd podczas zapisywania pliku {OUTPUT_HYDRO_STACJE_Z_POWIATAMI_FINAL}: {e}")