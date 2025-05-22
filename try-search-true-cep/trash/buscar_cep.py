import csv
import io
import json
import re
import time
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- Configurações ---
SEARXNG_URL = "http://124.81.6.163:8092/search"
SEARXNG_TIMEOUT = 30  # segundos
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'

# Regex para encontrar CEPs no formato XXXXX-XXX ou XXXXXXXX
CEP_REGEX = re.compile(r'\b(\d{5}-?\d{3})\b')

# --- Funções Auxiliares ---

def sanitize_cep(cep_str):
    """Limpa e formata o CEP para XXXXX-XXX."""
    if cep_str:
        digits = re.sub(r'\D', '', cep_str)
        if len(digits) == 8:
            return f"{digits[:5]}-{digits[5:]}"
    return None

def extract_ceps_from_text(text):
    """Extrai todos os CEPs válidos de um texto."""
    if not text:
        return []
    found_ceps = CEP_REGEX.findall(text)
    return [sanitize_cep(cep) for cep in found_ceps if sanitize_cep(cep)]

def find_cep_searxng(address, number, city, state):
    """Tenta encontrar o CEP usando a API SearXNG."""
    if not all([address, city, state]): # Número pode ser opcional para uma busca mais ampla
        return None

    query = f"CEP {address}"
    if number:
        query += f", {number}"
    query += f" {city} {state}"

    params = {
        'q': query,
        'format': 'json',
        'engines': 'google,bing,duckduckgo', # Conforme exemplo, mas pode ser ajustado
        'language': 'pt-BR'
    }
    headers = {'User-Agent': USER_AGENT}

    try:
        print(f"   [SearXNG] Buscando CEP para: {query}")
        response = requests.get(SEARXNG_URL, params=params, headers=headers, timeout=SEARXNG_TIMEOUT)
        response.raise_for_status()
        results = response.json()

        # Procurar CEP nos resultados
        for item in results.get('results', []):
            text_to_search = ""
            if 'title' in item:
                text_to_search += item['title'] + " "
            if 'content' in item:
                text_to_search += item['content'] + " "
            # Algumas instâncias do SearXNG podem ter 'snippet' ou 'description'
            if 'snippet' in item:
                 text_to_search += item['snippet'] + " "
            if 'description' in item:
                 text_to_search += item['description'] + " "


            ceps_found = extract_ceps_from_text(text_to_search)
            if ceps_found:
                print(f"      [SearXNG] CEP(s) encontrado(s): {ceps_found[0]}")
                return ceps_found[0] # Retorna o primeiro CEP válido encontrado

        # Se não encontrou nos resultados principais, verificar se há infoboxes
        for infobox in results.get('infoboxes', []):
            text_to_search = ""
            if 'content' in infobox: # Conteúdo do infobox
                text_to_search += infobox['content'] + " "
            # Alguns infoboxes podem ter links ou outros campos com o CEP
            if 'links' in infobox:
                for link_info in infobox.get('links', []):
                    text_to_search += link_info.get('text', '') + " " + link_info.get('url', '') + " "

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
    """Configura e retorna uma instância do ChromeDriver."""
    options = ChromeOptions()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument(f'user-agent={USER_AGENT}')
    options.add_argument("--blink-settings=imagesEnabled=false") # Não carregar imagens
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    # prefs = {"profile.managed_default_content_settings.images": 2} # Desabilitar imagens
    # options.add_experimental_option("prefs", prefs)

    try:
        driver = webdriver.Chrome(options=options)
        return driver
    except Exception as e:
        print(f"   [Selenium] Erro ao iniciar ChromeDriver: {e}")
        print("   [Selenium] Certifique-se de que o ChromeDriver está no PATH ou configurado corretamente.")
        return None

def find_cep_selenium(driver, address, number, city, state):
    """Tenta encontrar o CEP usando Selenium e busca no Google."""
    if not driver or not all([address, city, state]):
        return None

    query = f"CEP {address}"
    if number:
        query += f" {number}"
    query += f" {city} {state}"

    search_url = f"https://www.google.com/search?q={requests.utils.quote(query)}" # URL encode query

    try:
        print(f"   [Selenium] Buscando CEP para: {query}")
        driver.get(search_url)
        # Espera um pouco para a página carregar (pode ser ajustado)
        # WebDriverWait(driver, 10).until(
        #     EC.presence_of_element_located((By.TAG_NAME, "body"))
        # )
        time.sleep(3) # Um sleep simples pode ser suficiente para o Google

        page_text = driver.find_element(By.TAG_NAME, 'body').text
        ceps_found = extract_ceps_from_text(page_text)

        if ceps_found:
            print(f"      [Selenium] CEP(s) encontrado(s): {ceps_found[0]}")
            return ceps_found[0]
        else:
            print(f"      [Selenium] Nenhum CEP encontrado na página para: {query}")

    except Exception as e:
        print(f"      [Selenium] Erro ao buscar: {e}")
    return None

# --- Script Principal ---
def main():
    input_csv_data = """Hash,CRM,UF,Firstname,LastName,Medical specialty,Endereco Completo A1,Address A1,Numero A1,Complement A1,Bairro A1,postal code A1,City A1,State A1,Phone A1,Phone A2,Cell phone A1,Cell phone A2,E-mail A1,E-mail A2,OPT-IN,STATUS,LOTE
,1655,CE,UBIRAJARA,JULIEUX DE MAGALHAES,,"Rua Monsenhor Coelho , 152",Rua Monsenhor Coelho,152,Saladas,Centro,,Iguatu,CE,85) 3198-3700,,,,contact@nesx.co,,,,
,25081,CE,FRANCISCO,DAVI FERNANDES BRILHANTE,,"Rua Tomas Acioli, 721",Rua Tomas Acioli,721,de 890/891 ao fim,Dionisio Torres,,Fortaleza,CE,85) 9713-0570,,,,davibrilhante02@gmail.com,,,,
,209301,SP,IGOR,GABRIEL MAIA MENDANHA AMARAL,,"Rua Perrella, 331",Rua Perrella,331,Apt 1210 Bairro: Fundacao Muni,,,SP,SP,11) 4386-1349,,,,dr.igoramaral@icloud.com,,,,
,16400,PR,DJALMA,MICHELE SILVA,,"Rua Dr Bernardo Ribeiro Vianna, 433",Rua Dr Bernardo Ribeiro Vianna,433,,,,Palmas,PR,41) 3240-4000,,,,protocolo@crmpr.org.br,,,,
,13800,PA,CLAUDIO,COSTA CARDOSO,,"Rua Sao Joao Del Rey, 123",Rua Sao Joao Del Rey,123,Sala 211 Bairro: Centro Munici,,,Belém,PA,13) 2102-3434,,,,recrutamento@hbpsantos.org.br,,,,
,22035,CE,JOSEMBERG,VIEIRA DE MENEZES FILHO,,"Rua Osvaldo Cruz, 1761",Rua Osvaldo Cruz,1761,Sala 1216 Centro,Aldeota,,Fortaleza,CE,81) 99909-4917,,,,josem@edu.unifor.br,,,,
,24698,CE,ADAM,VALENTE AMARAL,,Ruas; Segurado: Lucio Ruas De Oliveira; Matrícula: 054.936,Ruas; Segurado: Lucio Ruas De Oliveira; Matrícula: 054.936,,Sala 415,,,,,,31) 3916-7075,,,,adamvalenteamaral@yahoo.com.br,,,,
,145560,SP,JOAO,HENRIQUE DE SOUSA,,"Rua Frei Caneca, 1282",Rua Frei Caneca,1282,Conjunto D,,,,São Paulo,SP,11) 4349-9900,,,,cfm@portalmedico.org.br,,,,
,16759,PE,FRANCISCO,ROMERO CAMPELLO DE BIASE FILHO,,"Rua Carlos Gomes, 401",Rua Carlos Gomes,401,até 162/163,Madalena,,Recife,PE,81) 3671-3451,,,,,,,,
,4155,PA,ROBERTO,FARIAS LOPES,,"Rua Benedito Almeida, 621",Rua Benedito Almeida,621,Sala "Ponto de Afeto"." (NR),,,,Santarém,PA,19) 99951-0212,,,,contato@editorafoco.com.br,,,,
"""
    # Adicionei manualmente cidades e estados para os registros que estavam faltando
    # para melhor demonstrar o script. Se no seu dado real eles faltarem,
    # o script indicará "Informações insuficientes".
    # No registro "IGOR", alterei City A1 e State A1 para "SP", "SP"
    # No registro "CLAUDIO", alterei City A1 e State A1 para "Belém", "PA"
    # No registro "JOAO HENRIQUE", alterei City A1 e State A1 para "São Paulo", "SP"
    # No registro "ROBERTO", alterei City A1 e State A1 para "Santarém", "PA"

    # Para ler de um arquivo CSV real:
    # with open('seu_arquivo.csv', 'r', encoding='utf-8') as infile:
    #     input_csv_data = infile.read()

    f = io.StringIO(input_csv_data)
    reader = csv.DictReader(f)
    fieldnames = list(reader.fieldnames) + ['CEP_Encontrado', 'Status_Busca_CEP']
    
    output_rows = []
    processed_doctors_count = 0
    selenium_driver = None

    # Lista para armazenar os resultados simplificados
    resultados_simplificados = []

    for row in reader:
        print(f"\nProcessando: {row.get('Firstname', '')} {row.get('LastName', '')} (CRM: {row.get('CRM','')}{row.get('UF','')})")
        address = row.get('Address A1', '').strip()
        number = row.get('Numero A1', '').strip()
        city = row.get('City A1', '').strip()
        state = row.get('State A1', '').strip()

        cep_encontrado = None
        status_busca = ""

        if not address or not city or not state:
            print("   Informações de endereço insuficientes (Rua, Cidade ou Estado faltando).")
            status_busca = "Info Insuficiente"
        else:
            # Tenta com SearXNG primeiro
            cep_encontrado = find_cep_searxng(address, number, city, state)
            if cep_encontrado:
                status_busca = "SearXNG OK"
            else:
                status_busca = "SearXNG Falhou"
                print("   [Fallback] Tentando com Selenium...")
                if processed_doctors_count % 5 == 0 or selenium_driver is None:
                    if selenium_driver:
                        selenium_driver.quit()
                        print("   [Selenium] Driver reiniciado.")
                    selenium_driver = setup_chromedriver()

                if selenium_driver:
                    cep_encontrado = find_cep_selenium(selenium_driver, address, number, city, state)
                    if cep_encontrado:
                        status_busca = "Selenium OK"
                    else:
                        status_busca = "Selenium Falhou"
                else:
                    status_busca = "Selenium Driver Falhou"
            
            time.sleep(1) # Pequena pausa para não sobrecarregar as APIs/buscas

        # Limpar e garantir todos os campos
        cleaned_row = {}
        for field in fieldnames:
            value = row.get(field, '')
            if value is None:
                value = ''
            cleaned_row[field] = str(value).strip()

        cleaned_row['CEP_Encontrado'] = cep_encontrado if cep_encontrado else ""
        cleaned_row['postal code A1'] = cep_encontrado if cep_encontrado else cleaned_row.get('postal code A1', '')
        cleaned_row['Status_Busca_CEP'] = status_busca
        
        output_rows.append(cleaned_row)

        # Adicionar ao resultado simplificado se encontrou o CEP
        if cep_encontrado:
            resultados_simplificados.append({
                'Rua': address,
                'Numero': number,
                'CEP': cep_encontrado
            })

        processed_doctors_count += 1

    if selenium_driver:
        selenium_driver.quit()
        print("\n[Selenium] Driver finalizado.")

    # Imprimir resultado no formato CSV
    print("\n\n--- RESULTADO FINAL (CSV) ---")
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(output_rows)
    print(output.getvalue())

    # Salvar resultados simplificados em um arquivo CSV
    print("\n\n--- SALVANDO RESULTADOS SIMPLIFICADOS ---")
    with open('ceps_encontrados.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['Rua', 'Numero', 'CEP'])
        writer.writeheader()
        writer.writerows(resultados_simplificados)
    print(f"Arquivo 'ceps_encontrados.csv' criado com sucesso!")

if __name__ == "__main__":
    main()