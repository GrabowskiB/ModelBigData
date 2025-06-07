import pandas as pd
import numpy as np # Do użycia np.nan

# --- Konfiguracja ---
INPUT_METEO_SKONSOLIDOWANE = "dane_meteo_skonsolidowane.csv" # Wynik poprzedniego skryptu
OUTPUT_METEO_STACJE_OCZYSZCZONE = "dane_meteo_stacje_oczyszczone.csv"

# Definicja finalnych kolumn i hierarchii źródeł (kolumny z sufiksami)
# Klucz: Nazwa finalnej kolumny
# Wartość: Lista kolumn źródłowych w kolejności priorytetu (od najważniejszej)
PARAMETRY_PRIORYTETY = {
    "TMAX_Final_C": ['TMAX_C_synopSD', 'TMAX_C_klimatKD', 'TMAX_C'], # Ostatnie 'TMAX_C' to z pierwszego pliku, jeśli nie było sufiksu
    "TMIN_Final_C": ['TMIN_C_synopSD', 'TMIN_C_klimatKD', 'TMIN_C'],
    "TEMP_Srednia_Final_C": ['STD_C_synopSD', 'TEMP_Srednia_C_synopSDT', 'STD_C_klimatKD', 'TEMP_Srednia_C_klimatKDT', 'STD_C', 'TEMP_Srednia_C'],
    "TMNG_Final_C": ['TMNG_C_synopSD', 'TMNG_C_klimatKD', 'TMNG_C'],
    "Opady_SumaDobowa_Final_mm": ['SMDB_mm_synopSD', 'SMDB_mm_opadOD', 'SMDB_mm_klimatKD', 'SMDB_mm'],
    "PokrywaSniezna_Final_cm": ['PKSN_cm_synopSD', 'PKSN_cm_opadOD', 'PKSN_cm_klimatKD', 'PKSN_cm'],
    "Wilgotnosc_Srednia_Final_proc": ['WLGS_Srednia_proc_synopSDT', 'WLGS_Srednia_proc_klimatKDT', 'WLGS_Srednia_proc'],
    "Wiatr_PredkoscSrednia_Final_ms": ['FWS_Srednia_ms_synopSDT', 'FWS_Srednia_ms_klimatKDT', 'FWS_Srednia_ms'],
    "Zachmurzenie_Srednie_Final_okt": ['NOS_Srednie_okt_synopSDT', 'NOS_Srednie_okt_klimatKDT', 'NOS_Srednie_okt'],
    "CisnienieStacji_Srednie_Final_hPa": ['PPPS_Srednie_hPa_synopSDT', 'PPPS_Srednie_hPa'], # synopSDT jest jedynym źródłem tych z _sdt
    "CisnienieMorza_Srednie_Final_hPa": ['PPPM_Srednie_hPa_synopSDT', 'PPPM_Srednie_hPa']   # j.w.
}
# Dodatkowe parametry, które są tylko w synop_sd (jeśli chcemy je zachować bez scalania)
SYNOP_SD_SPECIFIC_PARAMS = [
    "RWSN_mm_cm", "USL_godz", "CzasOpaduDeszcz_godz", "CzasOpaduSnieg_godz",
    "CzasOpaduDeszczSnieg_godz", "CzasGradu_godz", "CzasMgly_godz", "CzasZamglenia_godz",
    "CzasSadzi_godz", "CzasGololedzi_godz", "CzasZamieciNiskiej_godz", "CzasZamieciWysokiej_godz",
    "CzasZmetnienia_godz", "CzasWiatru_ge10ms_godz", "CzasWiatru_gt15ms_godz", "CzasBurzy_godz",
    "CzasRosy_godz", "CzasSzronu_godz", "WystPokrywySnieznej_01", "WystBlyskawicy_01",
    "StanGruntu_ZR", "IzotermaDolna_cm", "IzotermaGorna_cm", "Aktynometria_Jcm2"
]
# Parametry tylko z synop_sdt
SYNOP_SDT_SPECIFIC_PARAMS = ["CPW_Srednie_hPa", "WODZ_SumaOpaduDzien_mm", "WONO_SumaOpaduNoc_mm"]


if __name__ == "__main__":
    print(f"Wczytywanie skonsolidowanych danych meteorologicznych: {INPUT_METEO_SKONSOLIDOWANE}...")
    try:
        df_merged = pd.read_csv(INPUT_METEO_SKONSOLIDOWANE, 
                                parse_dates=['Data'], 
                                dtype={'KodStacji': str},
                                low_memory=False) # low_memory=False dla uniknięcia ostrzeżeń o typach
        print(f"Wczytano {len(df_merged)} wierszy, {len(df_merged.columns)} kolumn.")
    except FileNotFoundError:
        print(f"BŁĄD: Plik {INPUT_METEO_SKONSOLIDOWANE} nie został znaleziony. Uruchom najpierw skrypt konsolidujący.")
        exit()
    except Exception as e:
        print(f"BŁĄD podczas wczytywania {INPUT_METEO_SKONSOLIDOWANE}: {e}")
        exit()

    df_final_stacje = df_merged[['Data', 'KodStacji']].copy()

    print("\nKonsolidowanie kolumn pomiarowych zgodnie z priorytetami...")
    for final_col_name, source_cols_priority in PARAMETRY_PRIORYTETY.items():
        df_final_stacje[final_col_name] = np.nan # Inicjalizuj nową kolumnę NaN-ami
        for source_col in source_cols_priority:
            if source_col in df_merged.columns:
                # Użyj combine_first do wypełnienia NaN w final_col_name wartościami z source_col
                df_final_stacje[final_col_name] = df_final_stacje[final_col_name].combine_first(df_merged[source_col])
        print(f"  Utworzono/zaktualizowano kolumnę: {final_col_name}")

    # Dodawanie specyficznych parametrów z synop_sd (jeśli istnieją i nie były częścią scalania)
    print("\nDodawanie specyficznych parametrów z danych synop_sd...")
    for param in SYNOP_SD_SPECIFIC_PARAMS:
        # Sprawdzamy, czy kolumna istnieje z sufiksem _synopSD lub bez (jeśli była z pierwszego pliku)
        col_synopSD = param + "_synopSD"
        if col_synopSD in df_merged.columns:
            df_final_stacje[param] = df_merged[col_synopSD]
            print(f"  Dodano kolumnę: {param} (ze źródła _synopSD)")
        elif param in df_merged.columns and not any(param + suf in df_merged.columns for suf in ['_klimatKD', '_klimatKDT', '_opadOD', '_synopSDT']):
             # Jeśli kolumna istnieje bez sufiksu i nie ma jej wersji z innymi sufiksami (czyli pochodzi z _synopSD jako pierwszego)
            df_final_stacje[param] = df_merged[param]
            print(f"  Dodano kolumnę: {param} (ze źródła _synopSD - bez sufiksu)")


    # Dodawanie specyficznych parametrów z synop_sdt
    print("\nDodawanie specyficznych parametrów z danych synop_sdt...")
    for param in SYNOP_SDT_SPECIFIC_PARAMS:
        col_synopSDT = param + "_synopSDT"
        if col_synopSDT in df_merged.columns:
            df_final_stacje[param] = df_merged[col_synopSDT]
            print(f"  Dodano kolumnę: {param} (ze źródła _synopSDT)")
        elif param in df_merged.columns and not any(param + suf in df_merged.columns for suf in ['_klimatKD', '_klimatKDT', '_opadOD', '_synopSD']):
            df_final_stacje[param] = df_merged[param]
            print(f"  Dodano kolumnę: {param} (ze źródła _synopSDT - bez sufiksu)")


    # Wybór jednej kolumny NazwaStacji
    # Priorytet: _synopSD, potem _klimatKD, potem _opadOD, potem _klimatKDT, potem _synopSDT, na końcu bez sufiksu
    nazwa_stacji_sources = ['NazwaStacji_synopSD', 'NazwaStacji_klimatKD', 'NazwaStacji_opadOD', 
                            'NazwaStacji_klimatKDT', 'NazwaStacji_synopSDT', 'NazwaStacji']
    df_final_stacje['NazwaStacji_Skonsolidowana'] = np.nan
    for col_nazwa in nazwa_stacji_sources:
        if col_nazwa in df_merged.columns:
            df_final_stacje['NazwaStacji_Skonsolidowana'] = df_final_stacje['NazwaStacji_Skonsolidowana'].combine_first(df_merged[col_nazwa])
    
    # Przeniesienie NazwaStacji_Skonsolidowana bliżej początku
    if 'NazwaStacji_Skonsolidowana' in df_final_stacje.columns:
        nazwa_col = df_final_stacje.pop('NazwaStacji_Skonsolidowana')
        df_final_stacje.insert(2, 'NazwaStacji_Skonsolidowana', nazwa_col)


    print("\n--- Oczyszczona ramka danych meteorologicznych na poziomie stacji ---")
    df_final_stacje.info(verbose=False, show_counts=True) # verbose=False, bo kolumn może być dużo
    print("\nPierwsze 5 wierszy danych oczyszczonych (wybrane kolumny):")
    cols_to_show_final = ['Data', 'KodStacji', 'NazwaStacji_Skonsolidowana'] + list(PARAMETRY_PRIORYTETY.keys())[:3] # Pokaż pierwsze 3 skonsolidowane
    #print(df_final_stacje[[col for col in cols_to_show_final if col in df_final_stacje.columns]].head().to_string())

    try:
        df_final_stacje.to_csv(OUTPUT_METEO_STACJE_OCZYSZCZONE, index=False, encoding='utf-8-sig')
        print(f"\nOczyszczone dane meteorologiczne (stacje) zapisano do: {OUTPUT_METEO_STACJE_OCZYSZCZONE}")
    except Exception as e:
        print(f"Błąd podczas zapisywania pliku {OUTPUT_METEO_STACJE_OCZYSZCZONE}: {e}")