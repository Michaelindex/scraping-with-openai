import csv
import io
import json
import re
import time
import os # Importado para manipulação de caminhos
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# --- Configurações ---
SEARXNG_URL = "http://124.81.6.163:8092/search"
SEARXNG_TIMEOUT = 30  # segundos
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'

# Regex para encontrar CEPs no formato XXXXX-XXX ou XXXXXXXX
CEP_REGEX = re.compile(r'\b(\d{5}-?\d{3})\b')

# --- Funções Auxiliares (sem alterações) ---

def sanitize_cep(cep_str):
    if cep_str:
        digits = re.sub(r'\D', '', cep_str)
        if len(digits) == 8:
            return f"{digits[:5]}-{digits[5:]}"
    return None

def extract_ceps_from_text(text):
    if not text:
        return []
    found_ceps = CEP_REGEX.findall(text)
    return [sanitize_cep(cep) for cep in found_ceps if sanitize_cep(cep)]

def find_cep_searxng(address, number, bairro, city, state):
    if not all([address, city, state]):
        return None
    query = f"CEP {address}"
    if number: query += f", {number}"
    if bairro: query += f", {bairro}"
    query += f" {city} {state}"
    params = {'q': query, 'format': 'json', 'engines': 'google,bing,duckduckgo', 'language': 'pt-BR'}
    headers = {'User-Agent': USER_AGENT}
    try:
        print(f"   [SearXNG] Buscando CEP para: {query}")
        response = requests.get(SEARXNG_URL, params=params, headers=headers, timeout=SEARXNG_TIMEOUT)
        response.raise_for_status()
        results = response.json()
        for item in results.get('results', []):
            text_to_search = item.get('title', '') + " " + item.get('content', '') + " " + item.get('snippet', '') + " " + item.get('description', '')
            ceps_found = extract_ceps_from_text(text_to_search)
            if ceps_found:
                print(f"      [SearXNG] CEP(s) encontrado(s): {ceps_found[0]}")
                return ceps_found[0]
        for infobox in results.get('infoboxes', []):
            text_to_search = infobox.get('content', '')
            if 'links' in infobox:
                for link_info in infobox.get('links', []):
                    text_to_search += " " + link_info.get('text', '') + " " + link_info.get('url', '')
            ceps_found = extract_ceps_from_text(text_to_search)
            if ceps_found:
                print(f"      [SearXNG] CEP(s) encontrado(s) em infobox: {ceps_found[0]}")
                return ceps_found[0]
    except requests.exceptions.RequestException as e:
        print(f"      [SearXNG] Erro ao buscar: {e}")
    except json.JSONDecodeError:
        print(f"      [SearXNG] Erro ao decodificar JSON da resposta.")
    return None

def setup_chromedriver():
    options = ChromeOptions()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument(f'user-agent={USER_AGENT}')
    options.add_argument("--blink-settings=imagesEnabled=false")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    try:
        driver = webdriver.Chrome(options=options)
        return driver
    except Exception as e:
        print(f"   [Selenium] Erro ao iniciar ChromeDriver: {e}")
        print("   [Selenium] Certifique-se de que o ChromeDriver está no PATH ou configurado corretamente.")
        return None

def find_cep_google_selenium(driver, address, number, bairro, city, state):
    if not driver or not all([address, city, state]): return None
    query = f"CEP {address}"
    if number: query += f" {number}"
    if bairro: query += f", {bairro}"
    query += f" {city} {state}"
    search_url = f"https://www.google.com/search?q={requests.utils.quote(query)}"
    try:
        print(f"   [Google Selenium] Buscando CEP para: {query}")
        driver.get(search_url)
        time.sleep(2)
        page_text = driver.find_element(By.TAG_NAME, 'body').text
        ceps_found = extract_ceps_from_text(page_text)
        if ceps_found:
            print(f"      [Google Selenium] CEP(s) encontrado(s): {ceps_found[0]}")
            return ceps_found[0]
        else:
            print(f"      [Google Selenium] Nenhum CEP encontrado na página para: {query}")
    except Exception as e:
        print(f"      [Google Selenium] Erro ao buscar: {e}")
    return None

def find_cep_correios_selenium(driver, address, number, bairro, city, state_uf):
    if not driver or not address: return None
    search_term = address
    if number: search_term += f", {number}"
    if city and state_uf: search_term += f", {city}, {state_uf}"
    elif city: search_term += f", {city}"
    try:
        print(f"   [Correios Selenium] Buscando CEP para: {search_term}")
        driver.get("https://buscacepinter.correios.com.br/app/endereco/index.php")
        endereco_input = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "endereco")))
        endereco_input.clear()
        endereco_input.send_keys(search_term)
        driver.find_element(By.ID, "btn_pesquisar").click()
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "navegacaoAbaixo")))
        time.sleep(1)
        try:
            erro_msg_element = driver.find_element(By.CSS_SELECTOR, "div.mensagem.alert.alert-danger")
            if erro_msg_element and ("não encontrado" in erro_msg_element.text.lower() or "nao existe" in erro_msg_element.text.lower()):
                print(f"      [Correios Selenium] Dados não encontrados no site dos Correios para: {search_term}")
                return None
        except NoSuchElementException:
            pass
        try:
            cep_elements = driver.find_elements(By.XPATH, "//table[@id='resultado-DNEC']/tbody/tr/td[4]")
            if cep_elements:
                for cep_el in cep_elements:
                    cep_text = cep_el.text.strip()
                    sanitized = sanitize_cep(cep_text)
                    if sanitized:
                        print(f"      [Correios Selenium] CEP encontrado na tabela: {sanitized}")
                        return sanitized
            if not cep_elements and driver.find_elements(By.ID, "resultado-DNEC"):
                 print(f"      [Correios Selenium] Tabela de resultados encontrada, mas CEP não localizado na 4a coluna para: {search_term}")
        except NoSuchElementException:
            print(f"      [Correios Selenium] Tabela de resultados não encontrada para: {search_term}")
    except TimeoutException:
        print(f"      [Correios Selenium] Timeout ao carregar página ou elementos para: {search_term}")
    except Exception as e:
        print(f"      [Correios Selenium] Erro ao buscar: {e} para: {search_term}")
    return None

# --- Script Principal ---
def main():
    # Define o caminho para o arquivo de entrada
    # __file__ é o caminho do script atual (buscar_cep2.py)
    # os.path.dirname(__file__) é o diretório onde o script está (ex: .../try-search-true-cep/)
    # os.path.join(..., '..', 'medicos-output.csv') volta um nível e acessa o arquivo
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_file_path = os.path.join(script_dir, '..', 'medicos-output.csv')
    
    # Nomes dos arquivos de saída (serão salvos na pasta do script)
    output_file_completo = os.path.join(script_dir, 'medicos_com_cep_completo.csv')
    output_file_simplificado = os.path.join(script_dir, 'ceps_encontrados_detalhado.csv')

    input_rows_list = []
    original_fieldnames = []

    try:
        with open(input_file_path, 'r', encoding='utf-8', newline='') as infile:
            reader = csv.DictReader(infile)
            if not reader.fieldnames:
                 print(f"Erro: Não foi possível ler os cabeçalhos do arquivo CSV: {input_file_path}")
                 return
            original_fieldnames = list(reader.fieldnames)
            for row in reader:
                input_rows_list.append(row)
        print(f"Arquivo '{input_file_path}' lido com sucesso. {len(input_rows_list)} linhas encontradas (sem contar cabeçalho).")
    except FileNotFoundError:
        print(f"Erro: Arquivo de entrada não encontrado em '{input_file_path}'")
        return
    except Exception as e:
        print(f"Erro ao ler o arquivo CSV '{input_file_path}': {e}")
        return

    if not input_rows_list:
        print("Nenhuma linha de dados para processar.")
        return

    # Define os cabeçalhos para o arquivo de saída completo
    # Garante que as novas colunas sejam adicionadas ao final
    extended_fieldnames = list(original_fieldnames)
    if 'CEP_Encontrado' not in extended_fieldnames:
        extended_fieldnames.append('CEP_Encontrado')
    if 'Status_Busca_CEP' not in extended_fieldnames:
        extended_fieldnames.append('Status_Busca_CEP')
    
    processed_output_rows = [] # Para o arquivo completo
    resultados_simplificados = [] # Para o arquivo detalhado/simplificado
    
    processed_doctors_count = 0
    selenium_driver = None

    for row_num, input_row_data in enumerate(input_rows_list):
        # Cria uma cópia para não modificar o dicionário original da lista lida
        current_row_data = input_row_data.copy()

        print(f"\nProcessando Linha {row_num+1}: {current_row_data.get('Firstname', '')} {current_row_data.get('LastName', '')} (CRM: {current_row_data.get('CRM','')}{current_row_data.get('UF','')})")
        
        # Extrai os campos necessários do CSV (ajuste os nomes das colunas se forem diferentes no seu CSV)
        address = current_row_data.get('Address A1', '').strip()
        number = current_row_data.get('Numero A1', '').strip()
        bairro = current_row_data.get('Bairro A1', '').strip()
        city = current_row_data.get('City A1', '').strip()
        state = current_row_data.get('State A1', '').strip()

        cep_encontrado = None
        status_busca = ""

        if not address or not city or not state:
            if "Segurado:" in address or "Matrícula:" in address :
                 print("   Endereço parece ser inválido (contém 'Segurado:' ou 'Matrícula:').")
                 status_busca = "Endereço Inválido"
            else:
                print("   Informações de endereço insuficientes (Rua, Cidade ou Estado faltando).")
                status_busca = "Info Insuficiente"
        else:
            cep_encontrado = find_cep_searxng(address, number, bairro, city, state)
            if cep_encontrado:
                status_busca = "SearXNG OK"
            else:
                status_busca = "SearXNG Falhou"
                print("   [Fallback 1] SearXNG falhou ou não retornou CEP.")
                if processed_doctors_count % 5 == 0 or selenium_driver is None:
                    if selenium_driver:
                        selenium_driver.quit()
                        print("   [Selenium] Driver reiniciado.")
                    selenium_driver = setup_chromedriver()
                if selenium_driver:
                    cep_encontrado = find_cep_google_selenium(selenium_driver, address, number, bairro, city, state)
                    if cep_encontrado:
                        status_busca = "Google Selenium OK"
                    else:
                        status_busca = "Google Selenium Falhou"
                        print("   [Fallback 2] Google Selenium falhou ou não retornou CEP.")
                        cep_encontrado = find_cep_correios_selenium(selenium_driver, address, number, bairro, city, state)
                        if cep_encontrado:
                            status_busca = "Correios Selenium OK"
                        else:
                            status_busca = "Correios Selenium Falhou"
                            print("   [Fallback 3] Correios Selenium falhou ou não retornou CEP.")
                else:
                    status_busca += " / Selenium Driver Falhou"
            time.sleep(1)

        # Adiciona/Atualiza os campos na cópia da linha atual
        current_row_data['CEP_Encontrado'] = cep_encontrado if cep_encontrado else ""
        current_row_data['postal code A1'] = cep_encontrado if cep_encontrado else current_row_data.get('postal code A1', '') # Atualiza a coluna original também
        current_row_data['Status_Busca_CEP'] = status_busca
        
        processed_output_rows.append(current_row_data)

        if cep_encontrado:
            resultados_simplificados.append({
                'Rua': address, 'Numero': number, 'Bairro': bairro,
                'Cidade': city, 'UF': state, 'CEP': cep_encontrado,
                'Status_Busca': status_busca
            })
        processed_doctors_count += 1

    if selenium_driver:
        selenium_driver.quit()
        print("\n[Selenium] Driver finalizado.")

    # Salvar resultado completo
    print(f"\n\n--- SALVANDO RESULTADO COMPLETO ({len(processed_output_rows)} linhas) ---")
    try:
        with open(output_file_completo, 'w', newline='', encoding='utf-8') as f_comp:
            writer_comp = csv.DictWriter(f_comp, fieldnames=extended_fieldnames, extrasaction='ignore')
            writer_comp.writeheader()
            writer_comp.writerows(processed_output_rows)
        print(f"Arquivo '{output_file_completo}' criado com sucesso!")
    except Exception as e:
        print(f"Erro ao salvar o arquivo completo '{output_file_completo}': {e}")


    # Salvar resultados simplificados
    print(f"\n\n--- SALVANDO RESULTADOS SIMPLIFICADOS ({len(resultados_simplificados)} CEPs encontrados) ---")
    if resultados_simplificados:
        simplified_fieldnames = ['Rua', 'Numero', 'Bairro', 'Cidade', 'UF', 'CEP', 'Status_Busca']
        try:
            with open(output_file_simplificado, 'w', newline='', encoding='utf-8') as f_simp:
                writer_simple = csv.DictWriter(f_simp, fieldnames=simplified_fieldnames)
                writer_simple.writeheader()
                writer_simple.writerows(resultados_simplificados)
            print(f"Arquivo '{output_file_simplificado}' criado com sucesso!")
        except Exception as e:
            print(f"Erro ao salvar o arquivo simplificado '{output_file_simplificado}': {e}")

    else:
        print("Nenhum CEP foi encontrado para salvar no arquivo simplificado.")

if __name__ == "__main__":
    main()