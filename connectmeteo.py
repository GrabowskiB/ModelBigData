import os
import pandas as pd

# --- Konfiguracja ---
# Ścieżki do przetworzonych plików CSV
PATH_KLIMAT_KD = "przetworzone_dane_klimat_kd.csv"
PATH_KLIMAT_KDT = "przetworzone_dane_klimat_kdt.csv"
PATH_OPAD_OD = "przetworzone_dane_opad_od.csv"
PATH_SYNOP_SD = "przetworzone_dane_synop_sd.csv"
PATH_SYNOP_SDT = "przetworzone_dane_synop_sdt.csv"

OUTPUT_METEO_SKONSOLIDOWANE = "dane_meteo_skonsolidowane.csv"

# Lista plików i ich "etykiet" dla sufiksów przy łączeniu
meteo_files_to_merge = [
    (PATH_KLIMAT_KD, "_klimatKD"),
    (PATH_KLIMAT_KDT, "_klimatKDT"),
    (PATH_OPAD_OD, "_opadOD"),
    (PATH_SYNOP_SD, "_synopSD"),
    (PATH_SYNOP_SDT, "_synopSDT")
]

def load_processed_csv(file_path, label_for_log):
    """Wczytuje przetworzony plik CSV."""
    print(f"Wczytywanie: {file_path} ({label_for_log})...")
    if not os.path.exists(file_path):
        print(f"  OSTRZEŻENIE: Plik {file_path} nie istnieje. Pomijanie.")
        return None
    try:
        # Ważne: 'Data' musi być sparsowana jako data, KodStacji jako string
        df = pd.read_csv(file_path, encoding='utf-8-sig', parse_dates=['Data'], dtype={'KodStacji': str})
        print(f"  Wczytano {len(df)} wierszy.")
        return df
    except Exception as e:
        print(f"  BŁĄD podczas wczytywania {file_path}: {e}")
        return None

# --- Główna część skryptu ---
if __name__ == "__main__":
    dataframes_to_merge = []
    
    # Wczytywanie wszystkich przetworzonych ramek danych
    df_klimat_kd = load_processed_csv(PATH_KLIMAT_KD, "Klimat KD")
    df_klimat_kdt = load_processed_csv(PATH_KLIMAT_KDT, "Klimat KDT")
    df_opad_od = load_processed_csv(PATH_OPAD_OD, "Opad OD")
    df_synop_sd = load_processed_csv(PATH_SYNOP_SD, "Synop SD")
    df_synop_sdt = load_processed_csv(PATH_SYNOP_SDT, "Synop SDT")

    # Lista DataFrame'ów, które zostały pomyślnie wczytane
    loaded_dfs_with_labels = [
        (df_klimat_kd, meteo_files_to_merge[0][1]),
        (df_klimat_kdt, meteo_files_to_merge[1][1]),
        (df_opad_od, meteo_files_to_merge[2][1]),
        (df_synop_sd, meteo_files_to_merge[3][1]),
        (df_synop_sdt, meteo_files_to_merge[4][1])
    ]
    
    # Filtrujemy tylko te, które nie są None
    valid_dfs_with_labels = [(df, label) for df, label in loaded_dfs_with_labels if df is not None and not df.empty]

    if not valid_dfs_with_labels:
        print("Nie wczytano żadnych danych meteorologicznych do połączenia. Kończenie.")
        exit()

    # Rozpocznij łączenie od pierwszego dostępnego DataFrame
    merged_df = valid_dfs_with_labels[0][0]
    print(f"\nRozpoczynanie łączenia od: {meteo_files_to_merge[[l for d,l in loaded_dfs_with_labels].index(valid_dfs_with_labels[0][1])][0]}")


    # Iteracyjne łączenie pozostałych DataFrame'ów
    for i in range(1, len(valid_dfs_with_labels)):
        df_to_merge = valid_dfs_with_labels[i][0]
        label_current = valid_dfs_with_labels[i][1] # Sufiks dla aktualnie łączonego df
        
        # Określenie sufiksów. Dla lewej strony (merged_df) nie dodajemy sufiksu za pierwszym razem,
        # ale jeśli kolumny już istnieją z poprzednich merge'ów, Pandas doda domyślne _x, _y.
        # Aby mieć kontrolę, lepiej zawsze dodawać sufiksy, chyba że to pierwsza ramka.
        # W naszym iteracyjnym podejściu, 'merged_df' będzie miało kolumny bez sufiksu (lub z sufiksami z poprzednich łączeń).
        # Kolumny z 'df_to_merge' dostaną sufiks 'label_current'.
        
        print(f"Łączenie z: {meteo_files_to_merge[[l for d,l in loaded_dfs_with_labels].index(label_current)][0]} (sufiks dla nowych kolumn: {label_current})")
        
        merged_df = pd.merge(
            merged_df,
            df_to_merge,
            on=['Data', 'KodStacji'],
            how='outer', # Zachowaj wszystkie wiersze z obu ramek
            suffixes=('', label_current) # Pierwszy pusty sufiks dla kolumn z merged_df, drugi dla df_to_merge
                                         # Jeśli kolumna już istnieje w merged_df (np. NazwaStacji), a także w df_to_merge,
                                         # to ta z merged_df pozostanie bez sufiksu, a ta z df_to_merge dostanie np. NazwaStacji_klimatKDT
        )
        print(f"  Rozmiar po połączeniu: {merged_df.shape}")

    print("\n--- Skonsolidowana ramka danych meteorologicznych ---")
    merged_df.info(verbose=True, show_counts=True) # verbose=True pokaże wszystkie kolumny
    
    # Wyświetlenie przykładowych wierszy może być trudne przy dużej liczbie kolumn
    # Możemy wyświetlić tylko pierwsze kilka kolumn lub informacje o zakresie dat
    if not merged_df.empty:
        print("\nZakres dat w połączonych danych:")
        print(f"  Od: {merged_df['Data'].min()}")
        print(f"  Do: {merged_df['Data'].max()}")
        
        print("\nPierwsze 5 wierszy (pierwsze 10 kolumn):")
        print(merged_df.iloc[:, :10].head().to_string())
        
        print("\nLiczba unikalnych Kodów Stacji:", merged_df['KodStacji'].nunique())

    # Zapis do pliku CSV
    try:
        merged_df.to_csv(OUTPUT_METEO_SKONSOLIDOWANE, index=False, encoding='utf-8-sig')
        print(f"\nSkonsolidowane dane meteorologiczne zapisano do: {OUTPUT_METEO_SKONSOLIDOWANE}")
    except Exception as e:
        print(f"Błąd podczas zapisywania pliku {OUTPUT_METEO_SKONSOLIDOWANE}: {e}")