import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# --- Konfiguracja ---
# Lista plików do analizy kompletności danych
FILES_TO_ANALYZE = {
    "Dane Hydro Stacje z Powiatami": "dane_hydro_stacje_z_powiatami_final.csv",
    "Dane Meteo Powiat-Dzień (po redukcji braków)": "dane_meteo_powiat_dzien_redukcja_brakow.csv"
}

# Kolumny, które zawsze powinny być kompletne i możemy je pominąć na wykresie
# (jeśli chcemy skupić się tylko na kolumnach pomiarowych)
# Na razie zostawmy je, aby zobaczyć pełny obraz.
# ALWAYS_COMPLETE_COLS = ['Data', 'Powiat', 'KodStacji', 'NazwaStacji']


def plot_completeness(df, title, output_image_filename):
    """Tworzy i wyświetla wykres kompletności danych dla DataFrame."""
    if df.empty:
        print(f"Ramka danych dla '{title}' jest pusta. Nie można wygenerować wykresu.")
        return

    # Obliczanie procentu NIEBRAKUJĄCYCH danych dla każdej kolumny
    completeness_percentage = (df.notna().sum() * 100) / len(df)
    
    completeness_df = pd.DataFrame({
        'Kolumna': completeness_percentage.index,
        'ProcentDostepnychDanych': completeness_percentage.values
    })
    completeness_df = completeness_df.sort_values(by='ProcentDostepnychDanych', ascending=True) # Sortujemy rosnąco

    print(f"\n--- Kompletność danych dla: {title} ---")
    print(completeness_df.to_string())

    plt.figure(figsize=(12, max(8, len(completeness_df) * 0.3))) # Dostosuj rozmiar
    sns.barplot(x='ProcentDostepnychDanych', y='Kolumna', data=completeness_df, palette="mako") # Inna paleta dla odmiany
    
    plt.title(f'Procent Dostępnych (Nie-NaN) Danych dla Każdej Kolumny\n({title})', fontsize=16)
    plt.xlabel('Procent Dostępnych Danych (%)', fontsize=12)
    plt.ylabel('Kolumna', fontsize=12)
    plt.xticks(np.arange(0, 101, 10), fontsize=10) # Skala od 0 do 100 co 10
    plt_yticks_fontsize = 10 if len(completeness_df) < 30 else 8 # Mniejsza czcionka dla wielu kolumn
    plt.yticks(fontsize=plt_yticks_fontsize)
    plt.xlim(0, 100) # Ustawienie zakresu osi X od 0 do 100
    plt.grid(axis='x', linestyle='--', alpha=0.7)
    plt.tight_layout()
    
    try:
        plt.savefig(output_image_filename, dpi=300)
        print(f"\nWykres zapisano do pliku: {output_image_filename}")
    except Exception as e_save:
        print(f"Nie udało się zapisać wykresu {output_image_filename}: {e_save}")
        
    plt.show()


if __name__ == "__main__":
    for title, filename in FILES_TO_ANALYZE.items():
        print(f"\nAnalizowanie pliku: {filename} ({title})")
        if not os.path.exists(filename):
            print(f"  BŁĄD: Plik {filename} nie został znaleziony.")
            continue
        
        try:
            # Założenie o typach kluczowych kolumn dla spójności
            dtype_spec = {}
            if "Powiat" in pd.read_csv(filename, nrows=0).columns: # Sprawdź czy kolumna istnieje
                dtype_spec['Powiat'] = str
            if "KodStacji" in pd.read_csv(filename, nrows=0).columns:
                dtype_spec['KodStacji'] = str

            df_to_analyze = pd.read_csv(filename,
                                        parse_dates=['Data'],
                                        dtype=dtype_spec if dtype_spec else None,
                                        low_memory=False)
            print(f"  Wczytano {len(df_to_analyze)} wierszy.")
            
            # Generowanie unikalnej nazwy pliku dla wykresu
            base_output_name = os.path.splitext(filename)[0] # Usuwa .csv
            output_plot_filename = f"wykres_kompletnosci_{base_output_name.replace('przetworzone_dane_', '').replace('dane_', '')}.png"
            
            plot_completeness(df_to_analyze, title, output_plot_filename)
            
        except Exception as e:
            print(f"  BŁĄD podczas przetwarzania pliku {filename}: {e}")
            import traceback
            traceback.print_exc()