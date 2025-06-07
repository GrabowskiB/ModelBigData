import os

# --- Konfiguracja ---
ROOT_DATA_DIR = "pobrane_dane_imgw"  # Główny folder z pobranymi danymi
OUTPUT_FILE_NAME = "lista_plikow_rozpakowanych.txt"
EXCLUDE_EXTENSIONS = ['.zip'] # Lista rozszerzeń do wykluczenia (można dodać więcej)

def find_files_recursive(directory, exclude_ext):
    """
    Rekursywnie przeszukuje katalog i zwraca listę ścieżek do plików,
    wykluczając te z podanymi rozszerzeniami.
    """
    found_files = []
    for root, _, files in os.walk(directory):
        for filename in files:
            # Sprawdzamy, czy rozszerzenie pliku nie jest na liście wykluczonych
            file_ext = os.path.splitext(filename)[1].lower() # Pobieramy rozszerzenie i konwertujemy na małe litery
            if file_ext not in exclude_ext:
                full_path = os.path.join(root, filename)
                found_files.append(full_path)
    return found_files

# --- Główna część skryptu ---
if __name__ == "__main__":
    if not os.path.isdir(ROOT_DATA_DIR):
        print(f"Błąd: Katalog '{ROOT_DATA_DIR}' nie istnieje. Uruchom najpierw skrypt pobierający.")
    else:
        print(f"Przeszukiwanie katalogu: {ROOT_DATA_DIR}...")
        all_extracted_files = find_files_recursive(ROOT_DATA_DIR, EXCLUDE_EXTENSIONS)

        if not all_extracted_files:
            print("Nie znaleziono żadnych rozpakowanych plików (poza wykluczonymi rozszerzeniami).")
        else:
            print(f"Znaleziono {len(all_extracted_files)} plików. Zapisywanie listy do '{OUTPUT_FILE_NAME}'...")
            try:
                with open(OUTPUT_FILE_NAME, 'w', encoding='utf-8') as f:
                    for file_path in all_extracted_files:
                        f.write(file_path + '\n')
                print(f"Lista plików została pomyślnie zapisana w '{OUTPUT_FILE_NAME}'.")
            except IOError as e:
                print(f"Błąd podczas zapisywania pliku '{OUTPUT_FILE_NAME}': {e}")