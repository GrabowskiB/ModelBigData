import pandas as pd
import requests
import time
from geopy.geocoders import Nominatim

def convert_dms_to_dd(dms_str):
    """
    Konwertuje współrzędne z formatu DMS (stopnie minuty sekundy) do DD (dziesiętne stopnie)
    """
    if pd.isna(dms_str) or dms_str.strip() == '':
        return None
    
    parts = dms_str.strip().split()
    if len(parts) != 3:
        return None
    
    try:
        degrees = float(parts[0])
        minutes = float(parts[1])
        seconds = float(parts[2])
        
        dd = degrees + minutes/60 + seconds/3600
        return dd
    except:
        return None

def get_county_from_coordinates(lat, lon, geolocator):
    """
    Pobiera informacje o powiecie na podstawie współrzędnych
    """
    try:
        location = geolocator.reverse(f"{lat}, {lon}", language='pl')
        if location and location.raw.get('address'):
            address = location.raw['address']
            
            # Próbujemy znaleźć powiat w różnych polach
            county = (address.get('county') or 
                     address.get('state_district') or
                     address.get('administrative_area_level_2') or
                     address.get('political'))
            
            if county:
                # Usuwamy przedrostki typu "powiat"
                county = county.replace('powiat ', '').replace('Powiat ', '')
                return county
            
            # Jeśli nie ma powiatu, spróbuj gminy lub miasta
            return (address.get('municipality') or 
                   address.get('city') or 
                   address.get('town') or 
                   address.get('village') or
                   'Nieznany')
        
        return 'Nieznany'
    
    except Exception as e:
        print(f"Błąd dla współrzędnych {lat}, {lon}: {e}")
        return 'Błąd'

def main():
    # Wczytaj dane
    df = pd.read_csv('kody_stacji.csv', sep=';', encoding='utf-8')
    
    # Inicjalizuj geolocator
    geolocator = Nominatim(user_agent="hydro_stations_app")
    
    # Dodaj kolumnę na powiat
    df['Powiat'] = ''
    
    # Przetwarzaj każdy wiersz
    for index, row in df.iterrows():
        
        # Konwertuj współrzędne
        lat = convert_dms_to_dd(row['Szerokość geograficzna'])
        lon = convert_dms_to_dd(row['Długość geograficzna'])
        
        if lat is not None and lon is not None:
            # Pobierz powiat
            county = get_county_from_coordinates(lat, lon, geolocator)
            df.at[index, 'Powiat'] = county
            
            print(f"  Współrzędne: {lat:.4f}, {lon:.4f} -> Powiat: {county}")
        else:
            df.at[index, 'Powiat'] = 'Brak współrzędnych'
            print(f"  Błąd konwersji współrzędnych")
        
        # Dodaj opóźnienie żeby nie przeciążyć API
        #time.sleep(0.05)  # 5 ms opóźnienia
        
        # Zapisuj co 50 wierszy (na wypadek przerwania)
        if (index + 1) % 50 == 0:
            df.to_csv('kody_stacji_z_powiatami_temp.csv', sep=';', index=False, encoding='utf-8')
            print(f"Zapisano tymczasowo po {index + 1} wierszach")
    
    # Zapisz końcowy plik
    df.to_csv('kody_stacji_z_powiatami.csv', sep=';', index=False, encoding='utf-8')
    print("Zakończono! Plik zapisany jako 'kody_stacji_z_powiatami.csv'")
    
    # Pokaż statystyki
    print(f"\nStatystyki:")
    print(f"Liczba stacji: {len(df)}")
    print(f"Stacje z powiatami: {len(df[df['Powiat'] != ''])}")
    print(f"Stacje bez współrzędnych: {len(df[df['Powiat'] == 'Brak współrzędnych'])}")
    print(f"Błędy: {len(df[df['Powiat'] == 'Błąd'])}")
    
    # Pokaż najczęstsze powiaty
    print(f"\nNajczęstsze powiaty:")
    county_counts = df['Powiat'].value_counts().head(10)
    print(county_counts)

if __name__ == "__main__":
    main()