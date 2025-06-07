import os
import re
import csv
from datetime import datetime

def extract_hydro_data(file_path):
    """Wyciąga kluczowe informacje z pliku hydrologicznego IMGW"""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            content = file.read()
        
        # Regex patterns dla różnych pól
        data_pattern = r'Data i godzina wydania:\s*(\d{2}\.\d{2}\.\d{4})\s*-\s*godz\.\s*(\d{2}:\d{2})'
        biuro_pattern = r'Nazwa biura prognoz hydrologicznych:\s*(.+?)(?:\n|$)'
        numer_pattern = r'INFORMACJA O NIEBEZPIECZNYM ZJAWISKU Nr\s*([^:\n]+)'
        zjawisko_pattern = r'Zjawisko:\s*(.+?)(?:\n|$)'
        stopien_pattern = r'Stopień zagrożenia:\s*(\d+)'
        waznosc_pattern = r'Ważność:\s*od godz\.\s*(\d{2}:\d{2})\s*dnia\s*(\d{2}\.\d{2}\.\d{4})\s*do godz\.\s*(\d{2}:\d{2})\s*dnia\s*(\d{2}\.\d{2}\.\d{4})'
        obszar_pattern = r'Obszar:\s*(.+?)(?:\n|Przebieg:)'
        prawdopodobienstwo_pattern = r'Prawdopodobieństwo wystąpienia zjawiska:\s*(\d+)%'
        hydrolog_pattern = r'Dyżurny synoptyk hydrolog:\s*(.+?)(?:\n|$)'
        
        # Wyciąganie danych
        data_match = re.search(data_pattern, content)
        biuro_match = re.search(biuro_pattern, content)
        numer_match = re.search(numer_pattern, content)
        zjawisko_match = re.search(zjawisko_pattern, content)
        stopien_match = re.search(stopien_pattern, content)
        waznosc_match = re.search(waznosc_pattern, content)
        obszar_match = re.search(obszar_pattern, content, re.DOTALL)
        prawdopodobienstwo_match = re.search(prawdopodobienstwo_pattern, content)
        hydrolog_match = re.search(hydrolog_pattern, content)
        
        return {
            'nazwa_pliku': os.path.basename(file_path),
            'data_wydania': data_match.group(1) if data_match else '',
            'godzina_wydania': data_match.group(2) if data_match else '',
            'biuro': biuro_match.group(1).strip() if biuro_match else '',
            'numer_informacji': numer_match.group(1).strip() if numer_match else '',
            'zjawisko': zjawisko_match.group(1).strip() if zjawisko_match else '',
            'stopien_zagrozenia': int(stopien_match.group(1)) if stopien_match else '',
            'waznosc_od_godzina': waznosc_match.group(1) if waznosc_match else '',
            'waznosc_od_data': waznosc_match.group(2) if waznosc_match else '',
            'waznosc_do_godzina': waznosc_match.group(3) if waznosc_match else '',
            'waznosc_do_data': waznosc_match.group(4) if waznosc_match else '',
            'obszar': obszar_match.group(1).strip().replace('\n', ' ') if obszar_match else '',
            'prawdopodobienstwo': int(prawdopodobienstwo_match.group(1)) if prawdopodobienstwo_match else '',
            'hydrolog': hydrolog_match.group(1).strip() if hydrolog_match else ''
        }
    except Exception as e:
        print(f"Błąd podczas przetwarzania pliku {file_path}: {e}")
        return None

def process_hydro_directory(base_path):
    """Przetwarza wszystkie pliki hydrologiczne w katalogu"""
    all_data = []
    
    # Przejdź przez wszystkie lata
    for year in os.listdir(base_path):
        year_path = os.path.join(base_path, year)
        if os.path.isdir(year_path):
            extracted_path = os.path.join(year_path, 'extracted_files')
            
            if os.path.exists(extracted_path):
                print(f"Przetwarzanie roku {year}...")
                
                for filename in os.listdir(extracted_path):
                    if filename.endswith('.TXT'):
                        file_path = os.path.join(extracted_path, filename)
                        data = extract_hydro_data(file_path)
                        
                        if data:
                            data['rok'] = year
                            all_data.append(data)
    
    return all_data

def save_to_csv(data, output_file):
    """Zapisuje dane do pliku CSV"""
    if not data:
        print("Brak danych do zapisania")
        return
    
    fieldnames = [
        'rok', 'nazwa_pliku', 'data_wydania', 'godzina_wydania',
        'biuro', 'numer_informacji', 'zjawisko', 'stopien_zagrozenia',
        'waznosc_od_data', 'waznosc_od_godzina', 'waznosc_do_data', 'waznosc_do_godzina',
        'obszar', 'prawdopodobienstwo', 'hydrolog'
    ]
    
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    
    print(f"Zapisano {len(data)} rekordów do pliku {output_file}")

# Główna część skryptu
if __name__ == "__main__":
    base_path = r"c:\Users\barte\OneDrive\Pulpit\ModelBigData\pobrane_dane_imgw\ost_hydro"
    output_file = r"c:\Users\barte\OneDrive\Pulpit\ModelBigData\dane_hydrologiczne.csv"
    
    print("Rozpoczynam przetwarzanie plików hydrologicznych...")
    hydro_data = process_hydro_directory(base_path)
    
    if hydro_data:
        save_to_csv(hydro_data, output_file)
        print(f"Przetworzono łącznie {len(hydro_data)} plików")
        
        # Pokaż przykładowe statystyki
        zjawiska = {}
        stopnie = {}
        for record in hydro_data:
            zjawisko = record.get('zjawisko', 'nieznane')
            zjawiska[zjawisko] = zjawiska.get(zjawisko, 0) + 1
            
            stopien = record.get('stopien_zagrozenia', 'nieznany')
            stopnie[stopien] = stopnie.get(stopien, 0) + 1
        
        print("\nStatystyki zjawisk:")
        for zjawisko, count in sorted(zjawiska.items()):
            print(f"  {zjawisko}: {count}")
            
        print("\nStatystyki stopni zagrożenia:")
        for stopien, count in sorted(stopnie.items()):
            print(f"  Stopień {stopien}: {count}")
    else:
        print("Nie znaleziono żadnych danych do przetworzenia")