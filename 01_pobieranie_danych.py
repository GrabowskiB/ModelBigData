import os
import requests
from bs4 import BeautifulSoup
import zipfile
from urllib.parse import urljoin, urlparse
import datetime

# --- Konfiguracja ---
BASE_SAVE_DIR = "pobrane_dane_imgw"
START_YEAR = 2018
CURRENT_YEAR = datetime.datetime.now().year
END_YEAR = 2025 # Możesz ustawić sztywno lub użyć CURRENT_YEAR

# Upewnij się, że END_YEAR nie jest większy niż faktycznie dostępny
if END_YEAR > CURRENT_YEAR + 1: # Dajemy margines na przyszły rok, jeśli dane są już publikowane
    ACTUAL_END_YEAR = CURRENT_YEAR + 1
else:
    ACTUAL_END_YEAR = END_YEAR

print(f"Pobieranie danych od {START_YEAR} do {ACTUAL_END_YEAR}")

# Definicje źródeł danych
# Klucz to nazwa folderu, wartość to base_url i typ (dla struktury folderów)
# ... (reszta importów i konfiguracji na początku skryptu pobierającego) ...

# Definicje źródeł danych
# Klucz to nazwa folderu, wartość to base_url i typ (dla struktury folderów)
SOURCES = {
    "ostrzezenia_hydrologiczne": {
        "url_template": "https://danepubliczne.imgw.pl/data/arch/ost_hydro/{year}/",
        "type": "ost_hydro" # Tutaj będą tylko pliki .TXT z ostrzeżeniami
    },
    "hydro_dobowe_pomiarowe": { # NOWE ŹRÓDŁO
        "url_template": "https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_hydrologiczne/dobowe/{year}/",
        "type": "hydro/dobowe_pomiarowe" # Tutaj będą pliki .zip z danymi codz_...csv
    },
    "meteo_dobowe_opad": {
        "url_template": "https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/dobowe/opad/{year}/",
        "type": "meteo/dobowe/opad"
    },
    "meteo_dobowe_klimat": {
        "url_template": "https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/dobowe/klimat/{year}/",
        "type": "meteo/dobowe/klimat"
    },
    "meteo_dobowe_synop": {
         "url_template": "https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/dobowe/synop/{year}/",
         "type": "meteo/dobowe/synop"
     }
}

# ... (reszta skryptu pobierającego: funkcje get_zip_links_from_url, download_and_extract_zip, główna pętla) ...

def get_zip_links_from_url(directory_url):
    """Pobiera listę linków do plików ZIP z podanego URL katalogu."""
    links = []
    try:
        response = requests.get(directory_url, timeout=30)
        response.raise_for_status() # Rzuci wyjątkiem dla kodów błędów HTTP
        soup = BeautifulSoup(response.content, 'html.parser')
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if href.lower().endswith('.zip'):
                # Tworzenie pełnego URL, jeśli href jest relatywny
                full_link = urljoin(directory_url, href)
                links.append(full_link)
        if not links:
            print(f"  Nie znaleziono plików .zip w {directory_url}")
    except requests.exceptions.RequestException as e:
        print(f"  Błąd podczas dostępu do {directory_url}: {e}")
    return links

def download_and_extract_zip(zip_url, save_path_base, year):
    """Pobiera plik ZIP, zapisuje go i wypakowuje."""
    try:
        # Ścieżka do zapisu pliku ZIP
        parsed_url = urlparse(zip_url)
        zip_filename = os.path.basename(parsed_url.path)
        zip_save_dir = os.path.join(save_path_base, str(year), "zips")
        os.makedirs(zip_save_dir, exist_ok=True)
        local_zip_path = os.path.join(zip_save_dir, zip_filename)

        # Ścieżka do wypakowania zawartości
        extract_dir = os.path.join(save_path_base, str(year), "extracted_files")
        os.makedirs(extract_dir, exist_ok=True)

        print(f"    Pobieranie {zip_url} do {local_zip_path}...")
        zip_response = requests.get(zip_url, stream=True, timeout=60)
        zip_response.raise_for_status()

        with open(local_zip_path, 'wb') as f:
            for chunk in zip_response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"    Pobrano: {zip_filename}")

        print(f"    Rozpakowywanie {local_zip_path} do {extract_dir}...")
        with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        print(f"    Rozpakowano: {zip_filename}")

    except requests.exceptions.RequestException as e:
        print(f"    Błąd pobierania {zip_url}: {e}")
    except zipfile.BadZipFile:
        print(f"    Błąd: {local_zip_path} nie jest poprawnym plikiem ZIP lub jest uszkodzony.")
    except Exception as e:
        print(f"    Wystąpił nieoczekiwany błąd podczas przetwarzania {zip_url}: {e}")


# --- Główna pętla skryptu ---
if not os.path.exists(BASE_SAVE_DIR):
    os.makedirs(BASE_SAVE_DIR)

for source_name, source_info in SOURCES.items():
    print(f"\nPrzetwarzanie źródła: {source_name} (typ: {source_info['type']})")
    current_save_path_base = os.path.join(BASE_SAVE_DIR, source_info['type'])
    os.makedirs(current_save_path_base, exist_ok=True)

    for year in range(START_YEAR, ACTUAL_END_YEAR + 1):
        print(f"  Rok: {year}")
        year_directory_url = source_info['url_template'].format(year=year)

        zip_file_urls = get_zip_links_from_url(year_directory_url)

        if not zip_file_urls:
            print(f"  Brak plików ZIP do pobrania dla {source_name} w roku {year}.")
            continue

        for zip_url in zip_file_urls:
            download_and_extract_zip(zip_url, current_save_path_base, year)

print("\nZakończono pobieranie i rozpakowywanie wszystkich plików.")