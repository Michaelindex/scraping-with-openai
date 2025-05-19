#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import csv
import json
import time
import re
import requests
import logging
import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Configurações
SEARX_URL = "http://124.81.6.163:8092/search"
SEARX_JSON_URL = "http://124.81.6.163:8092/search?q={}&format=json"
OLLAMA_URL = "http://124.81.6.163:11434/api/generate"
OLLAMA_MODEL = "llama3.1:8b"
CSV_INPUT = "medicos.csv"
CSV_OUTPUT = "medicos-output.csv"
DATA_DIR = "data"
RAW_DATA_DIR = "raw_data"
LOG_DIR = "logs"
LOG_FILE = "scraping_log.txt"
MAX_RETRIES = 3
WAIT_TIME = 10

# Configuração do sistema de log
def setup_logging():
    """Configura o sistema de logging detalhado."""
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    
    log_file_path = os.path.join(LOG_DIR, LOG_FILE)
    
    # Configura o logger principal
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    # Remove handlers existentes para evitar duplicação
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Handler para arquivo
    file_handler = logging.FileHandler(log_file_path, mode='w')
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)
    
    # Handler para console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter('%(levelname)s: %(message)s')
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # Log de início da execução
    logging.info(f"Iniciando execução em {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"Arquivo de log: {log_file_path}")
    
    return logger

def check_csv_exists():
    """Verifica se o arquivo CSV existe e tem dados válidos."""
    logging.info(f"Verificando existência do arquivo {CSV_INPUT}")
    
    if not os.path.exists(CSV_INPUT):
        logging.error(f"Erro: O arquivo {CSV_INPUT} não foi encontrado.")
        print(f"Erro: O arquivo {CSV_INPUT} não foi encontrado.")
        return False
    
    try:
        with open(CSV_INPUT, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            try:
                header = next(reader)
                if not header:
                    logging.error(f"Erro: O arquivo {CSV_INPUT} não contém cabeçalho.")
                    print(f"Erro: O arquivo {CSV_INPUT} não contém cabeçalho.")
                    return False
                
                logging.info(f"Cabeçalho encontrado: {header}")
                
                # Verifica se há pelo menos uma linha de dados
                try:
                    first_row = next(reader)
                    if not any(first_row):
                        logging.error(f"Erro: O arquivo {CSV_INPUT} não contém dados válidos.")
                        print(f"Erro: O arquivo {CSV_INPUT} não contém dados válidos.")
                        return False
                    
                    logging.info(f"Primeira linha de dados: {first_row}")
                except StopIteration:
                    logging.error(f"Erro: O arquivo {CSV_INPUT} não contém dados além do cabeçalho.")
                    print(f"Erro: O arquivo {CSV_INPUT} não contém dados além do cabeçalho.")
                    return False
                    
            except Exception as e:
                logging.error(f"Erro ao ler o arquivo {CSV_INPUT}: {str(e)}")
                print(f"Erro ao ler o arquivo {CSV_INPUT}: {str(e)}")
                return False
    except Exception as e:
        logging.error(f"Erro ao abrir o arquivo {CSV_INPUT}: {str(e)}")
        print(f"Erro ao abrir o arquivo {CSV_INPUT}: {str(e)}")
        return False
    
    logging.info(f"Arquivo {CSV_INPUT} verificado com sucesso.")
    return True

def setup_selenium():
    """Configura e retorna uma instância do Selenium WebDriver."""
    logging.info("Configurando Selenium WebDriver")
    
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        logging.info("Selenium WebDriver inicializado com sucesso")
        return driver
    except Exception as e:
        logging.error(f"Erro ao inicializar o Selenium: {str(e)}")
        print(f"Erro ao inicializar o Selenium: {str(e)}")
        return None

def query_ollama(prompt, model=OLLAMA_MODEL):
    """Envia um prompt para a API do Ollama e retorna a resposta."""
    logging.info(f"Enviando prompt para Ollama (modelo: {model})")
    logging.debug(f"Prompt: {prompt}")
    
    for attempt in range(MAX_RETRIES):
        try:
            payload = {
                "model": model,
                "prompt": prompt,
                "stream": False
            }
            
            logging.debug(f"Tentativa {attempt+1}/{MAX_RETRIES} de conexão com Ollama")
            response = requests.post(OLLAMA_URL, json=payload)
            
            if response.status_code == 200:
                result = response.json().get('response', '')
                logging.info("Resposta recebida do Ollama com sucesso")
                logging.debug(f"Resposta: {result}")
                return result
            else:
                logging.warning(f"Erro na API do Ollama (tentativa {attempt+1}/{MAX_RETRIES}): {response.status_code}")
                logging.debug(f"Resposta de erro: {response.text}")
                print(f"Erro na API do Ollama (tentativa {attempt+1}/{MAX_RETRIES}): {response.status_code}")
                time.sleep(2)
        except Exception as e:
            logging.error(f"Erro ao conectar com Ollama (tentativa {attempt+1}/{MAX_RETRIES}): {str(e)}")
            print(f"Erro ao conectar com Ollama (tentativa {attempt+1}/{MAX_RETRIES}): {str(e)}")
            time.sleep(2)
    
    logging.error("Todas as tentativas de conexão com Ollama falharam")
    return ""

def create_context_prompt(headers, row):
    """Cria um prompt para a IA entender o contexto do CSV."""
    logging.info("Criando prompt de contexto para a IA")
    
    prompt = """
    Você é um assistente especializado em extração de dados precisos. Sua tarefa é analisar as colunas de um CSV e os dados parciais fornecidos para entender exatamente quais informações precisam ser buscadas.

    Regras importantes:
    1. Você deve identificar APENAS os dados faltantes com base nas colunas do CSV
    2. Você deve retornar APENAS os tipos de dados que precisam ser buscados, sem explicações adicionais
    3. Você deve ser extremamente preciso e específico
    4. Você NÃO deve inventar ou supor dados que não estão explicitamente mencionados

    Colunas do CSV:
    {headers}

    Dados parciais disponíveis:
    {data}

    Com base nas colunas e nos dados parciais, liste APENAS os tipos de dados específicos que precisam ser buscados para completar o registro, no formato:
    - [Nome do campo]: [Descrição exata do que buscar]
    """
    
    # Cria um dicionário com os cabeçalhos e valores da linha
    data_dict = {}
    for i, header in enumerate(headers):
        if i < len(row):
            data_dict[header] = row[i]
        else:
            data_dict[header] = ""
    
    # Formata os dados para o prompt
    headers_str = ", ".join(headers)
    data_str = "\n".join([f"{k}: {v}" for k, v in data_dict.items()])
    
    final_prompt = prompt.format(headers=headers_str, data=data_str)
    logging.debug(f"Prompt de contexto criado: {final_prompt}")
    
    return final_prompt

def create_validation_prompt(headers, row, search_results, field_to_find):
    """Cria um prompt para a IA validar e extrair dados específicos dos resultados de busca."""
    logging.info(f"Criando prompt de validação para o campo: {field_to_find}")
    
    prompt = """
    Você é um assistente especializado em extração de dados precisos. Sua tarefa é analisar os resultados de busca e extrair APENAS o dado específico solicitado.

    Regras importantes:
    1. Você deve retornar APENAS o dado solicitado, sem texto adicional, explicações ou formatação
    2. Se o dado não for encontrado nos resultados, responda apenas com "NÃO ENCONTRADO"
    3. Você deve ser extremamente preciso e específico
    4. Você NÃO deve inventar ou supor dados que não estão explicitamente mencionados nos resultados
    5. Você deve retornar o dado exatamente como aparece, sem adicionar ou remover informações
    6. Se houver múltiplas opções, escolha a mais relevante e precisa

    Contexto do registro:
    {context}

    Resultados da busca:
    {results}

    Dado específico a ser extraído:
    {field_to_find}

    Responda APENAS com o dado solicitado ou "NÃO ENCONTRADO".
    """
    
    # Cria um dicionário com os cabeçalhos e valores da linha
    context_dict = {}
    for i, header in enumerate(headers):
        if i < len(row):
            context_dict[header] = row[i]
        else:
            context_dict[header] = ""
    
    # Formata os dados para o prompt
    context_str = "\n".join([f"{k}: {v}" for k, v in context_dict.items() if v])
    
    final_prompt = prompt.format(context=context_str, results=search_results, field_to_find=field_to_find)
    logging.debug(f"Prompt de validação criado para {field_to_find}")
    
    return final_prompt

def get_doctor_filename(row, headers):
    """Gera um nome de arquivo para o médico baseado no nome."""
    logging.info("Gerando nome de arquivo para o médico")
    
    # Tenta encontrar os índices dos campos de nome
    first_name_idx = -1
    last_name_idx = -1
    
    for i, header in enumerate(headers):
        if "primeiro nome" in header.lower():
            first_name_idx = i
        elif "ultimo nome" in header.lower():
            last_name_idx = i
    
    # Se encontrou os campos de nome, usa-os para gerar o nome do arquivo
    if first_name_idx >= 0 and last_name_idx >= 0 and first_name_idx < len(row) and last_name_idx < len(row):
        first_name = row[first_name_idx].strip()
        last_name = row[last_name_idx].strip()
        
        if first_name and last_name:
            # Normaliza o nome para usar como nome de arquivo
            full_name = f"{first_name} {last_name}"
            normalized_name = re.sub(r'[^a-zA-Z0-9]', '-', full_name.lower())
            logging.info(f"Nome de arquivo gerado: {normalized_name}")
            return normalized_name
    
    # Fallback: usa o CRM se disponível
    if len(row) > 0 and row[0]:
        filename = f"medico-{row[0]}"
        logging.info(f"Nome de arquivo gerado (fallback CRM): {filename}")
        return filename
    
    # Último recurso: usa um timestamp
    filename = f"medico-{int(time.time())}"
    logging.info(f"Nome de arquivo gerado (fallback timestamp): {filename}")
    return filename

def save_search_result(doctor_filename, url, source, extracted_data, field):
    """Salva o resultado da busca em um arquivo txt para o médico."""
    logging.info(f"Salvando resultado da busca para {doctor_filename}")
    
    # Cria o diretório data se não existir
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        logging.info(f"Diretório {DATA_DIR} criado")
    
    file_path = os.path.join(DATA_DIR, f"{doctor_filename}.txt")
    
    # Formata a entrada conforme solicitado
    entry = f"URL : {url} [ {source} ]\n"
    entry += f"--Informação extraida e alocada no csv: {extracted_data if extracted_data else 'Nenhuma informação extraída'}\n\n"
    
    # Salva a entrada no arquivo (append)
    try:
        with open(file_path, 'a', encoding='utf-8') as f:
            f.write(entry)
        logging.info(f"Resultado salvo em {file_path}")
    except Exception as e:
        logging.error(f"Erro ao salvar resultado em {file_path}: {str(e)}")

def save_raw_search_data(doctor_filename, url, source, all_content):
    """Salva todas as informações captadas do site em um arquivo txt separado."""
    logging.info(f"Salvando dados brutos da busca para {doctor_filename}")
    
    # Cria o diretório raw_data se não existir
    if not os.path.exists(RAW_DATA_DIR):
        os.makedirs(RAW_DATA_DIR)
        logging.info(f"Diretório {RAW_DATA_DIR} criado")
    
    file_path = os.path.join(RAW_DATA_DIR, f"{doctor_filename}.txt")
    
    # Formata a entrada conforme solicitado
    entry = f"URL : {url} [ {source} ]\n"
    entry += f"--INFORMAÇÕES CAPTADAS DO SITE: {all_content}\n\n"
    
    # Salva a entrada no arquivo (append)
    try:
        with open(file_path, 'a', encoding='utf-8') as f:
            f.write(entry)
        logging.info(f"Dados brutos salvos em {file_path}")
    except Exception as e:
        logging.error(f"Erro ao salvar dados brutos em {file_path}: {str(e)}")

def extract_page_content(driver, url):
    """Extrai o conteúdo completo de uma página web."""
    logging.info(f"Extraindo conteúdo da página: {url}")
    
    try:
        # Tenta acessar a URL
        logging.debug(f"Navegando para {url}")
        driver.get(url)
        
        # Espera a página carregar
        try:
            WebDriverWait(driver, WAIT_TIME).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            logging.info("Página carregada com sucesso")
        except TimeoutException:
            logging.warning("Timeout ao esperar carregamento da página")
        
        # Tenta extrair o conteúdo da página
        try:
            # Tenta obter o título
            title = driver.title
            logging.debug(f"Título da página: {title}")
            
            # Tenta obter o conteúdo do corpo
            body_text = driver.find_element(By.TAG_NAME, "body").text
            
            # Tenta obter todos os parágrafos
            paragraphs = [p.text for p in driver.find_elements(By.TAG_NAME, "p")]
            
            # Tenta obter todos os cabeçalhos
            headings = []
            for h_tag in ["h1", "h2", "h3", "h4", "h5", "h6"]:
                headings.extend([h.text for h in driver.find_elements(By.TAG_NAME, h_tag)])
            
            # Combina todos os textos extraídos
            all_content = f"Título: {title} /// "
            all_content += f"Cabeçalhos: {' /// '.join(headings)} /// "
            all_content += f"Parágrafos: {' /// '.join(paragraphs)} /// "
            all_content += f"Texto completo: {body_text}"
            
            logging.info("Conteúdo da página extraído com sucesso")
            logging.debug(f"Tamanho do conteúdo extraído: {len(all_content)} caracteres")
            
            return all_content
        except Exception as e:
            logging.error(f"Erro ao extrair conteúdo da página: {str(e)}")
            return f"Erro na extração: {str(e)}"
    except Exception as e:
        logging.error(f"Erro ao acessar a URL {url}: {str(e)}")
        return f"Erro no acesso: {str(e)}"

def search_bing(driver, query, doctor_filename, field):
    """Realiza uma busca no Bing e retorna os resultados."""
    logging.info(f"Realizando busca no Bing: {query}")
    
    results = []
    try:
        url = f"https://www.bing.com/search?q={query}"
        logging.debug(f"URL de busca: {url}")
        
        driver.get(url)
        try:
            WebDriverWait(driver, WAIT_TIME).until(
                EC.presence_of_element_located((By.ID, "b_results"))
            )
            logging.info("Página de resultados do Bing carregada com sucesso")
        except TimeoutException:
            logging.warning("Timeout ao esperar carregamento dos resultados do Bing")
        
        # Extrai os resultados
        search_results = driver.find_elements(By.CSS_SELECTOR, "#b_results .b_algo")
        logging.info(f"Encontrados {len(search_results)} resultados no Bing")
        
        for i, result in enumerate(search_results[:5]):  # Limita aos primeiros 5 resultados
            try:
                title = result.find_element(By.CSS_SELECTOR, "h2").text
                snippet = result.find_element(By.CSS_SELECTOR, ".b_caption p").text
                
                # Tenta encontrar o link
                link_element = result.find_element(By.CSS_SELECTOR, "h2 a")
                link = link_element.get_attribute("href")
                
                logging.debug(f"Resultado {i+1}: {title} - {link}")
                
                results.append({
                    "title": title,
                    "snippet": snippet,
                    "url": link
                })
                
                # Tenta acessar a página e extrair o conteúdo completo
                try:
                    all_content = extract_page_content(driver, link)
                    save_raw_search_data(doctor_filename, link, "BING", all_content)
                except Exception as e:
                    logging.error(f"Erro ao extrair conteúdo da página {link}: {str(e)}")
                    save_raw_search_data(doctor_filename, link, "BING", f"Erro na extração: {str(e)}")
                
            except NoSuchElementException as e:
                logging.warning(f"Elemento não encontrado no resultado {i+1}: {str(e)}")
                continue
            except Exception as e:
                logging.error(f"Erro ao processar resultado {i+1}: {str(e)}")
                continue
        
        return results
    except Exception as e:
        logging.error(f"Erro ao buscar no Bing: {str(e)}")
        return []

def search_searx(query, doctor_filename, field):
    """Realiza uma busca no SearXNG e retorna os resultados."""
    logging.info(f"Realizando busca no SearXNG: {query}")
    
    results = []
    try:
        # Tenta primeiro a API JSON
        url = SEARX_JSON_URL.format(query)
        logging.debug(f"URL de busca JSON: {url}")
        
        try:
            response = requests.get(url)
            logging.debug(f"Status da resposta: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                logging.info(f"Encontrados {len(data.get('results', []))} resultados no SearXNG (JSON)")
                
                for i, result in enumerate(data.get('results', [])[:5]):
                    title = result.get('title', '')
                    snippet = result.get('content', '')
                    result_url = result.get('url', '')
                    
                    logging.debug(f"Resultado {i+1}: {title} - {result_url}")
                    
                    results.append({
                        "title": title,
                        "snippet": snippet,
                        "url": result_url
                    })
                    
                    # Para cada resultado, tenta acessar a página e extrair o conteúdo
                    try:
                        # Cria uma sessão temporária do Selenium para acessar a página
                        temp_options = Options()
                        temp_options.add_argument("--headless")
                        temp_options.add_argument("--no-sandbox")
                        temp_options.add_argument("--disable-dev-shm-usage")
                        temp_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
                        
                        with webdriver.Chrome(options=temp_options) as temp_driver:
                            all_content = extract_page_content(temp_driver, result_url)
                            save_raw_search_data(doctor_filename, result_url, "SearXNG", all_content)
                    except Exception as e:
                        logging.error(f"Erro ao extrair conteúdo da página {result_url}: {str(e)}")
                        save_raw_search_data(doctor_filename, result_url, "SearXNG", f"Erro na extração: {str(e)}")
            else:
                logging.warning(f"Erro na API JSON do SearXNG: {response.status_code}")
                logging.debug(f"Resposta de erro: {response.text}")
                
                # Fallback para a versão não-JSON
                url = f"{SEARX_URL}?q={query}"
                logging.debug(f"URL de busca HTML: {url}")
                
                response = requests.get(url)
                if response.status_code == 200:
                    content = response.text
                    logging.debug(f"Tamanho da resposta HTML: {len(content)} caracteres")
                    
                    # Implementação simplificada - em produção, usar BeautifulSoup
                    snippets = re.findall(r'<p class="content">(.*?)</p>', content)
                    titles = re.findall(r'<h4>(.*?)</h4>', content)
                    urls = re.findall(r'<a href="([^"]+)" class="url_link"', content)
                    
                    logging.info(f"Encontrados {min(len(titles), len(snippets), len(urls))} resultados no SearXNG (HTML)")
                    
                    for i in range(min(len(titles), len(snippets), len(urls), 5)):
                        logging.debug(f"Resultado {i+1}: {titles[i]} - {urls[i]}")
                        
                        results.append({
                            "title": titles[i],
                            "snippet": snippets[i],
                            "url": urls[i]
                        })
                        
                        # Para cada resultado, tenta acessar a página e extrair o conteúdo
                        try:
                            # Cria uma sessão temporária do Selenium para acessar a página
                            temp_options = Options()
                            temp_options.add_argument("--headless")
                            temp_options.add_argument("--no-sandbox")
                            temp_options.add_argument("--disable-dev-shm-usage")
                            temp_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
                            
                            with webdriver.Chrome(options=temp_options) as temp_driver:
                                all_content = extract_page_content(temp_driver, urls[i])
                                save_raw_search_data(doctor_filename, urls[i], "SearXNG", all_content)
                        except Exception as e:
                            logging.error(f"Erro ao extrair conteúdo da página {urls[i]}: {str(e)}")
                            save_raw_search_data(doctor_filename, urls[i], "SearXNG", f"Erro na extração: {str(e)}")
                else:
                    logging.error(f"Erro na versão HTML do SearXNG: {response.status_code}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Erro de conexão com SearXNG: {str(e)}")
        except json.JSONDecodeError as e:
            logging.error(f"Erro ao decodificar JSON do SearXNG: {str(e)}")
        except Exception as e:
            logging.error(f"Erro inesperado ao buscar no SearXNG: {str(e)}")
    except Exception as e:
        logging.error(f"Erro geral ao buscar no SearXNG: {str(e)}")
    
    return results

def extract_specific_data(text, field):
    """Usa regex para extrair dados específicos com base no tipo de campo."""
    logging.info(f"Extraindo dados específicos para o campo: {field}")
    
    # Padrões de regex para tipos comuns de dados
    patterns = {
        "CRM": r'\b[0-9]{5,8}\b',
        "CPF": r'\b\d{3}\.\d{3}\.\d{3}-\d{2}\b',
        "CNPJ": r'\b\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}\b',
        "CEP": r'\b\d{5}-\d{3}\b',
        "E-Mail": r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
        "Telephone": r'\b\(\d{2}\)\s*\d{4,5}-\d{4}\b',
        "Celphone": r'\b\(\d{2}\)\s*9\d{4}-\d{4}\b',
        "Endereço": r'\b[Rr]ua\s+[\w\s]+,?\s+\d+|\b[Aa]venida\s+[\w\s]+,?\s+\d+|\b[Aa]lameda\s+[\w\s]+,?\s+\d+|\b[Ee]strada\s+[\w\s]+,?\s+\d+',
        "Cidade": r'\b[A-Z][a-zÀ-ú]{2,}(?:\s+[A-Z][a-zÀ-ú]{2,})*\b',
        "Estado": r'\b(?:AC|AL|AP|AM|BA|CE|DF|ES|GO|MA|MT|MS|MG|PA|PB|PR|PE|PI|RJ|RN|RS|RO|RR|SC|SP|SE|TO)\b'
    }
    
    # Verifica se o campo corresponde a algum dos padrões conhecidos
    for pattern_name, pattern in patterns.items():
        if pattern_name.lower() in field.lower():
            logging.debug(f"Usando padrão regex para {pattern_name}: {pattern}")
            matches = re.findall(pattern, text)
            if matches:
                logging.info(f"Dado extraído com regex: {matches[0]}")
                return matches[0]
    
    logging.info("Nenhum dado extraído com regex")
    return None

def process_csv():
    """Processa o arquivo CSV, busca os dados faltantes e salva no arquivo de saída."""
    # Configura o sistema de logging
    logger = setup_logging()
    
    if not check_csv_exists():
        return
    
    driver = setup_selenium()
    if not driver:
        return
    
    try:
        # Lê o arquivo CSV de entrada
        with open(CSV_INPUT, 'r', encoding='utf-8') as f_in:
            reader = csv.reader(f_in)
            headers = next(reader)
            rows = list(reader)
        
        logging.info(f"Leitura do arquivo {CSV_INPUT} concluída: {len(rows)} registros encontrados")
        
        # Prepara o arquivo CSV de saída
        with open(CSV_OUTPUT, 'w', encoding='utf-8', newline='') as f_out:
            writer = csv.writer(f_out)
            writer.writerow(headers)
            logging.info(f"Arquivo de saída {CSV_OUTPUT} criado")
            
            # Processa cada linha do CSV
            for row_index, row in enumerate(rows):
                logging.info(f"Processando registro {row_index + 1}/{len(rows)}")
                print(f"Processando registro {row_index + 1}/{len(rows)}...")
                
                # Preenche a linha com valores vazios se necessário
                while len(row) < len(headers):
                    row.append("")
                
                # Gera o nome do arquivo para o médico
                doctor_filename = get_doctor_filename(row, headers)
                
                # Cria os diretórios se não existirem
                for directory in [DATA_DIR, RAW_DATA_DIR]:
                    if not os.path.exists(directory):
                        os.makedirs(directory)
                        logging.info(f"Diretório {directory} criado")
                
                # Limpa os arquivos anteriores se existirem
                for directory in [DATA_DIR, RAW_DATA_DIR]:
                    file_path = os.path.join(directory, f"{doctor_filename}.txt")
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(f"Arquivo de busca para: {doctor_filename}\n\n")
                    logging.info(f"Arquivo {file_path} inicializado")
                
                # Identifica campos vazios que precisam ser preenchidos
                empty_fields = []
                for i, value in enumerate(row):
                    if not value and i > 0:  # Ignora o campo CRM que é o identificador
                        empty_fields.append(headers[i])
                
                if not empty_fields:
                    logging.info("Todos os campos já estão preenchidos")
                    print("Todos os campos já estão preenchidos.")
                    writer.writerow(row)
                    continue
                
                logging.info(f"Campos vazios identificados: {empty_fields}")
                
                # Cria um prompt para a IA entender o contexto
                context_prompt = create_context_prompt(headers, row)
                context_response = query_ollama(context_prompt)
                
                # Extrai os campos a serem buscados da resposta da IA
                fields_to_search = []
                for field in empty_fields:
                    if field.lower() in context_response.lower():
                        fields_to_search.append(field)
                
                # Se a IA não identificou campos específicos, usa todos os campos vazios
                if not fields_to_search:
                    logging.info("IA não identificou campos específicos, usando todos os campos vazios")
                    fields_to_search = empty_fields
                
                logging.info(f"Campos a serem buscados: {fields_to_search}")
                
                # Cria uma query de busca com base nos dados disponíveis
                search_query = " ".join([v for v in row if v])
                logging.info(f"Query de busca: {search_query}")
                
                # Realiza buscas no Bing e SearXNG
                bing_results = search_bing(driver, search_query, doctor_filename, fields_to_search)
                searx_results = search_searx(search_query, doctor_filename, fields_to_search)
                
                logging.info(f"Resultados obtidos: {len(bing_results)} do Bing, {len(searx_results)} do SearXNG")
                
                # Combina os resultados para processamento
                all_results = []
                
                # Adiciona resultados do Bing
                for result in bing_results:
                    all_results.append({
                        "title": result.get("title", ""),
                        "snippet": result.get("snippet", ""),
                        "url": result.get("url", ""),
                        "source": "BING"
                    })
                
                # Adiciona resultados do SearXNG
                for result in searx_results:
                    all_results.append({
                        "title": result.get("title", ""),
                        "snippet": result.get("snippet", ""),
                        "url": result.get("url", ""),
                        "source": "SearXNG"
                    })
                
                # Prepara o texto para análise
                results_text = "\n\n".join([
                    f"Título: {result['title']}\nConteúdo: {result['snippet']}"
                    for result in all_results
                ])
                
                # Para cada campo vazio, tenta extrair o dado específico
                for field in fields_to_search:
                    field_index = headers.index(field)
                    extracted_data = None
                    
                    logging.info(f"Processando campo: {field}")
                    
                    # Primeiro tenta extrair com regex para tipos de dados conhecidos
                    extracted_data = extract_specific_data(results_text, field)
                    
                    # Se não conseguiu com regex, usa a IA para extrair
                    if not extracted_data:
                        logging.info(f"Regex não encontrou dados para {field}, tentando com IA")
                        validation_prompt = create_validation_prompt(
                            headers, row, results_text, field
                        )
                        extracted_data = query_ollama(validation_prompt)
                        
                        # Limpa a resposta da IA
                        if "NÃO ENCONTRADO" in extracted_data.upper():
                            logging.info(f"IA não encontrou dados para {field}")
                            extracted_data = ""
                        else:
                            logging.info(f"IA extraiu dados para {field}: {extracted_data}")
                    
                    # Atualiza o valor no registro
                    if extracted_data:
                        row[field_index] = extracted_data
                        logging.info(f"Campo {field} atualizado com: {extracted_data}")
                        
                        # Salva a informação para cada resultado que contribuiu
                        for result in all_results:
                            # Verifica se este resultado contribuiu para a extração
                            result_text = f"{result['title']} {result['snippet']}"
                            if extracted_data in result_text:
                                save_search_result(
                                    doctor_filename,
                                    result['url'],
                                    result['source'],
                                    f"{field}: {extracted_data}",
                                    field
                                )
                    else:
                        logging.warning(f"Nenhum dado extraído para o campo {field}")
                        # Mesmo sem extração, registra as tentativas
                        for result in all_results:
                            save_search_result(
                                doctor_filename,
                                result['url'],
                                result['source'],
                                "Nenhuma informação extraída para este campo",
                                field
                            )
                
                # Escreve a linha atualizada no arquivo de saída
                writer.writerow(row)
                logging.info(f"Registro {row_index + 1} processado e salvo no arquivo de saída")
                
                # Pausa para não sobrecarregar as APIs
                time.sleep(1)
        
        logging.info(f"Processamento concluído. Resultados salvos em {CSV_OUTPUT}")
        logging.info(f"Detalhes das buscas salvos nas pastas {DATA_DIR}/ e {RAW_DATA_DIR}/")
        print(f"Processamento concluído. Resultados salvos em {CSV_OUTPUT}")
        print(f"Detalhes das buscas salvos nas pastas {DATA_DIR}/ e {RAW_DATA_DIR}/")
    
    except Exception as e:
        logging.critical(f"Erro durante o processamento: {str(e)}")
        print(f"Erro durante o processamento: {str(e)}")
    
    finally:
        if driver:
            driver.quit()
            logging.info("Selenium WebDriver encerrado")

if __name__ == "__main__":
    process_csv()
