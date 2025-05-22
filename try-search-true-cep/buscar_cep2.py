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
from selenium.common.exceptions import TimeoutException, NoSuchElementException

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
    # Prioritize CEPs that appear more complete or are common
    return [sanitize_cep(cep) for cep in found_ceps if sanitize_cep(cep)]

def find_cep_searxng(address, number, bairro, city, state):
    """Tenta encontrar o CEP usando a API SearXNG."""
    if not all([address, city, state]):
        return None

    query = f"CEP {address}"
    if number:
        query += f", {number}"
    if bairro:
        query += f", {bairro}"
    query += f" {city} {state}"

    params = {
        'q': query,
        'format': 'json',
        'engines': 'google,bing,duckduckgo',
        'language': 'pt-BR'
    }
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
    """Configura e retorna uma instância do ChromeDriver."""
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
    """Tenta encontrar o CEP usando Selenium e busca no Google."""
    if not driver or not all([address, city, state]):
        return None

    query = f"CEP {address}"
    if number:
        query += f" {number}"
    if bairro:
        query += f", {bairro}"
    query += f" {city} {state}"
    search_url = f"https://www.google.com/search?q={requests.utils.quote(query)}"

    try:
        print(f"   [Google Selenium] Buscando CEP para: {query}")
        driver.get(search_url)
        time.sleep(2) # Espera básica para carregamento
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
    """Tenta encontrar o CEP usando Selenium no site dos Correios."""
    if not driver or not address: # Cidade e Estado são cruciais para Correios
        return None

    # Correios espera o logradouro e opcionalmente Cidade/UF no mesmo campo
    # ou pode-se tentar apenas o logradouro se for muito específico.
    # Para maior precisão, usar cidade/UF é melhor.
    
    search_term = address
    if number:
        search_term += f", {number}"
    # O campo de busca dos Correios aceita "nome da rua, cidade, UF"
    # ou apenas "nome da rua" e ele tenta inferir ou dar opções.
    # Para uma busca mais direcionada:
    if city and state_uf:
         search_term += f", {city}, {state_uf}"
    elif city: # Se só tiver cidade
         search_term += f", {city}"


    try:
        print(f"   [Correios Selenium] Buscando CEP para: {search_term}")
        driver.get("https://buscacepinter.correios.com.br/app/endereco/index.php")
        
        endereco_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "endereco"))
        )
        endereco_input.clear()
        endereco_input.send_keys(search_term)
        
        driver.find_element(By.ID, "btn_pesquisar").click()
        
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "navegacaoAbaixo")) # Espera por um elemento que indica fim da busca
        )
        time.sleep(1) # Pequena pausa para renderização final da tabela/mensagem

        # Verificar se há mensagem de erro explícita
        try:
            erro_msg_element = driver.find_element(By.CSS_SELECTOR, "div.mensagem.alert.alert-danger") #  "div.erro h6" ou similar
            if erro_msg_element and ("não encontrado" in erro_msg_element.text.lower() or "nao existe" in erro_msg_element.text.lower()):
                print(f"      [Correios Selenium] Dados não encontrados no site dos Correios para: {search_term}")
                return None
        except NoSuchElementException:
            pass # Sem mensagem de erro visível, prosseguir para verificar tabela

        # Tentar extrair da tabela de resultados
        try:
            cep_elements = driver.find_elements(By.XPATH, "//table[@id='resultado-DNEC']/tbody/tr/td[4]")
            if cep_elements:
                for cep_el in cep_elements:
                    cep_text = cep_el.text.strip()
                    sanitized = sanitize_cep(cep_text)
                    if sanitized:
                        print(f"      [Correios Selenium] CEP encontrado na tabela: {sanitized}")
                        return sanitized
            # Se a tabela estiver lá mas vazia ou com estrutura diferente
            if not cep_elements and driver.find_elements(By.ID, "resultado-DNEC"):
                 print(f"      [Correios Selenium] Tabela de resultados encontrada, mas CEP não localizado na 4a coluna para: {search_term}")


        except NoSuchElementException:
            print(f"      [Correios Selenium] Tabela de resultados não encontrada para: {search_term}")
        
        # Fallback: Se não achou na tabela, verificar todo o corpo da página (menos provável)
        # Isto é mais um "desespero" e pode pegar CEPs irrelevantes se a página mostrar exemplos
        # page_text = driver.find_element(By.TAG_NAME, 'body').text
        # ceps_found_body = extract_ceps_from_text(page_text)
        # if ceps_found_body:
        #     print(f"      [Correios Selenium] CEP encontrado no corpo da página (genérico): {ceps_found_body[0]}")
        #     return ceps_found_body[0]

    except TimeoutException:
        print(f"      [Correios Selenium] Timeout ao carregar página ou elementos para: {search_term}")
    except Exception as e:
        print(f"      [Correios Selenium] Erro ao buscar: {e} para: {search_term}")
    return None

# --- Script Principal ---
def main():
    # Dados de entrada com correções nas vírgulas para JOAO e ROBERTO para teste
    # (assumindo Bairro A1 e postal code A1 originais como vazios para eles)
    input_csv_data = """Hash,CRM,UF,Firstname,LastName,Medical specialty,Endereco Completo A1,Address A1,Numero A1,Complement A1,Bairro A1,postal code A1,City A1,State A1,Phone A1,Phone A2,Cell phone A1,Cell phone A2,E-mail A1,E-mail A2,OPT-IN,STATUS,LOTE
,1655,CE,UBIRAJARA,JULIEUX DE MAGALHAES,,"Rua Monsenhor Coelho , 152",Rua Monsenhor Coelho,152,Saladas,Centro,,Iguatu,CE,85) 3198-3700,,,,contact@nesx.co,,,,
,25081,CE,FRANCISCO,DAVI FERNANDES BRILHANTE,,"Rua Tomas Acioli, 721",Rua Tomas Acioli,721,de 890/891 ao fim,Dionisio Torres,,Fortaleza,CE,85) 9713-0570,,,,davibrilhante02@gmail.com,,,,
,209301,SP,IGOR,GABRIEL MAIA MENDANHA AMARAL,,"Rua Perrella, 331",Rua Perrella,331,"Apt 1210 Bairro: Fundacao Muni",Fundacao,,São Caetano do Sul,SP,11) 4386-1349,,,,dr.igoramaral@icloud.com,,,,
,16400,PR,DJALMA,MICHELE SILVA,,"Rua Dr Bernardo Ribeiro Vianna, 433",Rua Dr Bernardo Ribeiro Vianna,433,,,,Palmas,PR,41) 3240-4000,,,,protocolo@crmpr.org.br,,,,
,13800,PA,CLAUDIO,COSTA CARDOSO,,"Rua Sao Joao Del Rey, 123",Rua Sao Joao Del Rey,123,"Sala 211 Bairro: Centro Munici",Centro,,Belém,PA,13) 2102-3434,,,,recrutamento@hbpsantos.org.br,,,,
,22035,CE,JOSEMBERG,VIEIRA DE MENEZES FILHO,,"Rua Osvaldo Cruz, 1761",Rua Osvaldo Cruz,1761,"Sala 1216 Centro",Aldeota,,Fortaleza,CE,81) 99909-4917,,,,josem@edu.unifor.br,,,,
,24698,CE,ADAM,VALENTE AMARAL,,Ruas; Segurado: Lucio Ruas De Oliveira; Matrícula: 054.936,Ruas; Segurado: Lucio Ruas De Oliveira; Matrícula: 054.936,,Sala 415,,,,,,31) 3916-7075,,,,adamvalenteamaral@yahoo.com.br,,,,
,145560,SP,JOAO,HENRIQUE DE SOUSA,,"Rua Frei Caneca, 1282",Rua Frei Caneca,1282,Conjunto D,,,São Paulo,SP,11) 4349-9900,,,,cfm@portalmedico.org.br,,,,
,16759,PE,FRANCISCO,ROMERO CAMPELLO DE BIASE FILHO,,"Rua Carlos Gomes, 401",Rua Carlos Gomes,401,até 162/163,Madalena,,Recife,PE,81) 3671-3451,,,,,,,,
,4155,PA,ROBERTO,FARIAS LOPES,,"Rua Benedito Almeida, 621",Rua Benedito Almeida,621,"Sala ""Ponto de Afeto""."" (NR)",,,Santarém,PA,19) 99951-0212,,,,contato@editorafoco.com.br,,,,
"""
    # Nota: Para IGOR, ajustei City A1 para "São Caetano do Sul" e Bairro A1 para "Fundacao", já que o CEP 09520-650 é de lá.
    # Para CLAUDIO, adicionei Bairro A1="Centro".
    # Estes são exemplos, os dados reais da sua fonte devem ser usados.

    f = io.StringIO(input_csv_data)
    reader = csv.DictReader(f)
    # Adicionando 'Bairro A1' explicitamente se não estiver nos fieldnames, para garantir consistência
    base_fieldnames = reader.fieldnames if reader.fieldnames else []
    extended_fieldnames = list(base_fieldnames)
    if 'CEP_Encontrado' not in extended_fieldnames: extended_fieldnames.append('CEP_Encontrado')
    if 'Status_Busca_CEP' not in extended_fieldnames: extended_fieldnames.append('Status_Busca_CEP')
    
    output_rows = []
    processed_doctors_count = 0
    selenium_driver = None
    resultados_simplificados = []

    for row_num, row in enumerate(reader):
        print(f"\nProcessando Linha {row_num+1}: {row.get('Firstname', '')} {row.get('LastName', '')} (CRM: {row.get('CRM','')}{row.get('UF','')})")
        address = row.get('Address A1', '').strip()
        number = row.get('Numero A1', '').strip()
        bairro = row.get('Bairro A1', '').strip() # Adicionado
        city = row.get('City A1', '').strip()
        state = row.get('State A1', '').strip()

        cep_encontrado = None
        status_busca = ""

        # Validação básica dos dados de endereço
        if not address or not city or not state: # Rua, Cidade e Estado são essenciais
            if "Segurado:" in address or "Matrícula:" in address : # Heurística para endereços inválidos
                 print("   Endereço parece ser inválido (contém 'Segurado:' ou 'Matrícula:').")
                 status_busca = "Endereço Inválido"
            else:
                print("   Informações de endereço insuficientes (Rua, Cidade ou Estado faltando).")
                status_busca = "Info Insuficiente"
        else:
            # 1. Tenta com SearXNG
            cep_encontrado = find_cep_searxng(address, number, bairro, city, state)
            if cep_encontrado:
                status_busca = "SearXNG OK"
            else:
                status_busca = "SearXNG Falhou"
                print("   [Fallback 1] SearXNG falhou ou não retornou CEP.")

                # Preparar Selenium Driver se ainda não estiver pronto ou se precisar reiniciar
                if processed_doctors_count % 5 == 0 or selenium_driver is None:
                    if selenium_driver:
                        selenium_driver.quit()
                        print("   [Selenium] Driver reiniciado.")
                    selenium_driver = setup_chromedriver()

                if selenium_driver:
                    # 2. Tenta com Google Selenium
                    cep_encontrado = find_cep_google_selenium(selenium_driver, address, number, bairro, city, state)
                    if cep_encontrado:
                        status_busca = "Google Selenium OK"
                    else:
                        status_busca = "Google Selenium Falhou"
                        print("   [Fallback 2] Google Selenium falhou ou não retornou CEP.")
                        
                        # 3. Tenta com Correios Selenium
                        cep_encontrado = find_cep_correios_selenium(selenium_driver, address, number, bairro, city, state)
                        if cep_encontrado:
                            status_busca = "Correios Selenium OK"
                        else:
                            status_busca = "Correios Selenium Falhou"
                            print("   [Fallback 3] Correios Selenium falhou ou não retornou CEP.")
                else:
                    status_busca += " / Selenium Driver Falhou" # Concatena ao status anterior
            
            time.sleep(1) # Pausa entre processamento de médicos

        # Limpeza e preparação da linha de saída
        cleaned_row = {}
        for field in extended_fieldnames: # Usar extended_fieldnames que inclui as novas colunas
            value = row.get(field) # Pega o valor original da linha lida
            if value is None: # Se o campo não existia na linha original (para novas colunas)
                value = ''
            cleaned_row[field] = str(value).strip()

        cleaned_row['CEP_Encontrado'] = cep_encontrado if cep_encontrado else ""
        # Atualiza a coluna 'postal code A1' original também, se encontrada.
        cleaned_row['postal code A1'] = cep_encontrado if cep_encontrado else cleaned_row.get('postal code A1', '')
        cleaned_row['Status_Busca_CEP'] = status_busca
        
        output_rows.append(cleaned_row)

        if cep_encontrado:
            resultados_simplificados.append({
                'Rua': address,
                'Numero': number,
                'Bairro': bairro,
                'Cidade': city,
                'UF': state,
                'CEP': cep_encontrado,
                'Status_Busca': status_busca
            })
        processed_doctors_count += 1

    if selenium_driver:
        selenium_driver.quit()
        print("\n[Selenium] Driver finalizado.")

    # Imprimir resultado completo no formato CSV
    print("\n\n--- RESULTADO FINAL (CSV) ---")
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=extended_fieldnames, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(output_rows)
    print(output.getvalue())

    # Salvar resultados simplificados em um arquivo CSV
    print("\n\n--- SALVANDO RESULTADOS SIMPLIFICADOS ---")
    if resultados_simplificados:
        simplified_fieldnames = ['Rua', 'Numero', 'Bairro', 'Cidade', 'UF', 'CEP', 'Status_Busca']
        with open('ceps_encontrados_detalhado.csv', 'w', newline='', encoding='utf-8') as f:
            writer_simple = csv.DictWriter(f, fieldnames=simplified_fieldnames)
            writer_simple.writeheader()
            writer_simple.writerows(resultados_simplificados)
        print(f"Arquivo 'ceps_encontrados_detalhado.csv' criado com sucesso!")
    else:
        print("Nenhum CEP foi encontrado para salvar no arquivo simplificado.")

if __name__ == "__main__":
    main()