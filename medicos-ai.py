#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import csv
import json
import time
import re
import requests
import logging
import random
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
EXAMPLES_DIR = "examples"
MAX_SITES = 10
WAIT_TIME = 10
EXCLUDED_EXTENSIONS = ['.pdf', '.xlsx', '.xls', '.doc', '.docx', '.ppt', '.pptx', '.txt', '.csv']

# Lista de user agents realistas
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Edg/92.0.902.78"
]

def setup_logging():
    """Configura o sistema de logging básico."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def check_csv_exists():
    """Verifica se o arquivo CSV existe e tem dados válidos."""
    if not os.path.exists(CSV_INPUT):
        print(f"Erro: O arquivo {CSV_INPUT} não foi encontrado.")
        return False
    
    try:
        with open(CSV_INPUT, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            if not header:
                print(f"Erro: O arquivo {CSV_INPUT} não contém cabeçalho.")
                return False
            
            try:
                first_row = next(reader)
                if not any(first_row):
                    print(f"Erro: O arquivo {CSV_INPUT} não contém dados válidos.")
                    return False
            except StopIteration:
                print(f"Erro: O arquivo {CSV_INPUT} não contém dados além do cabeçalho.")
                return False
    except Exception as e:
        print(f"Erro ao ler o arquivo {CSV_INPUT}: {str(e)}")
        return False
    
    return True

def setup_selenium():
    """Configura e retorna uma instância do Selenium WebDriver."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument(f"--user-agent={random.choice(USER_AGENTS)}")
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        return driver
    except Exception as e:
        print(f"Erro ao inicializar o Selenium: {str(e)}")
        return None

def query_ollama(prompt, model=OLLAMA_MODEL):
    """Envia um prompt para a API do Ollama e retorna a resposta."""
    try:
        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False
        }
        
        response = requests.post(OLLAMA_URL, json=payload)
        
        if response.status_code == 200:
            result = response.json().get('response', '')
            return result
        else:
            print(f"Erro na API do Ollama: {response.status_code}")
            return ""
    except Exception as e:
        print(f"Erro ao conectar com Ollama: {str(e)}")
        return ""

def get_doctor_filename(row, headers):
    """Gera um nome de arquivo para o médico baseado no nome."""
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
            return normalized_name
    
    # Fallback: usa o CRM se disponível
    if len(row) > 0 and row[0]:
        filename = f"medico-{row[0]}"
        return filename
    
    # Último recurso: usa um timestamp
    filename = f"medico-{int(time.time())}"
    return filename

def save_search_result(doctor_filename, url, source, extracted_data, field):
    """Salva o resultado da busca em um arquivo txt para o médico."""
    # Cria o diretório data se não existir
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    
    file_path = os.path.join(DATA_DIR, f"{doctor_filename}.txt")
    
    # Formata a entrada conforme solicitado
    entry = f"URL : {url} [ {source} ]\n"
    entry += f"--Informação extraida e alocada no csv: {field}: {extracted_data}\n\n"
    
    # Salva a entrada no arquivo (append)
    try:
        with open(file_path, 'a', encoding='utf-8') as f:
            f.write(entry)
    except Exception as e:
        print(f"Erro ao salvar resultado em {file_path}: {str(e)}")

def save_raw_search_data(doctor_filename, url, source, all_content):
    """Salva todas as informações captadas do site em um arquivo txt separado."""
    # Cria o diretório raw_data se não existir
    if not os.path.exists(RAW_DATA_DIR):
        os.makedirs(RAW_DATA_DIR)
    
    file_path = os.path.join(RAW_DATA_DIR, f"{doctor_filename}.txt")
    
    # Formata a entrada conforme solicitado
    entry = f"URL : {url} [ {source} ]\n"
    entry += f"--INFORMAÇÕES CAPTADAS DO SITE: {all_content}\n\n"
    
    # Salva a entrada no arquivo (append)
    try:
        with open(file_path, 'a', encoding='utf-8') as f:
            f.write(entry)
    except Exception as e:
        print(f"Erro ao salvar dados brutos em {file_path}: {str(e)}")

def is_html_url(url):
    """Verifica se a URL é de uma página HTML (não PDF, XLSX, etc.)."""
    parsed_url = url.lower()
    
    # Verifica se a URL termina com uma extensão excluída
    for ext in EXCLUDED_EXTENSIONS:
        if parsed_url.endswith(ext):
            return False
    
    return True

def extract_page_content(driver, url):
    """Extrai o conteúdo completo de uma página web."""
    # Verifica se a URL é de uma página HTML
    if not is_html_url(url):
        print(f"URL ignorada (não é HTML): {url}")
        return "URL ignorada (não é HTML)"
    
    try:
        # Tenta acessar a URL
        driver.get(url)
        
        # Espera a página carregar
        try:
            WebDriverWait(driver, WAIT_TIME).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
        except TimeoutException:
            print("Timeout ao esperar carregamento da página")
        
        # Tenta extrair o conteúdo da página
        try:
            # Tenta obter o título
            title = driver.title
            
            # Tenta obter o conteúdo do corpo
            body_text = driver.find_element(By.TAG_NAME, "body").text
            
            # Combina os textos extraídos
            all_content = f"Título: {title} /// Texto completo: {body_text}"
            
            return all_content
        except Exception as e:
            print(f"Erro ao extrair conteúdo da página: {str(e)}")
            return f"Erro na extração: {str(e)}"
    except Exception as e:
        print(f"Erro ao acessar a URL {url}: {str(e)}")
        return f"Erro no acesso: {str(e)}"

def search_top_sites(driver, query):
    """Realiza uma busca e retorna os top sites."""
    print(f"Buscando por: {query}")
    
    all_results = []
    
    # Tenta buscar no SearXNG primeiro
    try:
        url = SEARX_JSON_URL.format(query)
        
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/json"
        }
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            results = data.get('results', [])
            
            for result in results:
                title = result.get('title', '')
                snippet = result.get('content', '')
                result_url = result.get('url', '')
                
                # Verifica se a URL é de uma página HTML
                if is_html_url(result_url):
                    all_results.append({
                        "title": title,
                        "snippet": snippet,
                        "url": result_url,
                        "source": "SearXNG"
                    })
    except Exception as e:
        print(f"Erro ao buscar no SearXNG: {str(e)}")
    
    # Se não conseguiu resultados suficientes, tenta o Bing
    if len(all_results) < MAX_SITES:
        try:
            bing_url = f"https://www.bing.com/search?q={query}"
            
            driver.get(bing_url)
            time.sleep(2)  # Pequeno delay para carregar
            
            try:
                WebDriverWait(driver, WAIT_TIME).until(
                    EC.presence_of_element_located((By.ID, "b_results"))
                )
            except TimeoutException:
                print("Timeout ao esperar carregamento dos resultados do Bing")
            
            # Extrai os resultados
            search_results = driver.find_elements(By.CSS_SELECTOR, "#b_results .b_algo")
            
            for result in search_results:
                try:
                    title = result.find_element(By.CSS_SELECTOR, "h2").text
                    snippet = result.find_element(By.CSS_SELECTOR, ".b_caption p").text
                    
                    # Tenta encontrar o link
                    link_element = result.find_element(By.CSS_SELECTOR, "h2 a")
                    link = link_element.get_attribute("href")
                    
                    # Verifica se a URL é de uma página HTML
                    if is_html_url(link):
                        all_results.append({
                            "title": title,
                            "snippet": snippet,
                            "url": link,
                            "source": "BING"
                        })
                except Exception:
                    continue
        except Exception as e:
            print(f"Erro ao buscar no Bing: {str(e)}")
    
    # Limita aos primeiros MAX_SITES resultados
    return all_results[:MAX_SITES]

def generate_field_examples(field_name):
    """Gera exemplos fictícios para um campo específico usando a IA."""
    print(f"Gerando exemplos para o campo: {field_name}")
    
    # Cria um prompt para a IA gerar exemplos
    prompt = f"""
    Você é um assistente especializado em gerar exemplos de dados precisos. Sua tarefa é gerar 10 exemplos fictícios para o campo "{field_name}" de um médico.

    Regras importantes:
    1. Gere APENAS 10 exemplos, um por linha
    2. Os exemplos devem ser curtos, diretos e realistas
    3. Não inclua explicações, apenas os exemplos
    4. Não numere os exemplos
    5. Não use aspas ou formatação especial
    6. Cada exemplo deve ser apenas o dado puro, sem rótulos ou descrições

    Por exemplo, se o campo for "Endereço de Atendimento", os exemplos devem ser como:
    Rua das Flores, 123
    Avenida Paulista, 1000
    Praça da Liberdade, 50
    
    Gere 10 exemplos para o campo "{field_name}":
    """
    
    # Envia o prompt para a IA
    examples_text = query_ollama(prompt)
    
    # Processa a resposta para extrair apenas os exemplos
    examples = []
    for line in examples_text.strip().split('\n'):
        line = line.strip()
        if line and not line.startswith('#') and not line.startswith('-'):
            # Remove numeração se houver
            line = re.sub(r'^\d+[\.\)]\s*', '', line)
            # Remove aspas se houver
            line = line.strip('"\'')
            if line:
                examples.append(line)
    
    # Limita a 10 exemplos
    examples = examples[:10]
    
    # Cria o diretório de exemplos se não existir
    if not os.path.exists(EXAMPLES_DIR):
        os.makedirs(EXAMPLES_DIR)
    
    # Salva os exemplos em um arquivo
    examples_file = os.path.join(EXAMPLES_DIR, f"{field_name.replace(' ', '_')}.txt")
    with open(examples_file, 'w', encoding='utf-8') as f:
        for example in examples:
            f.write(f"{example}\n")
    
    print(f"Gerados {len(examples)} exemplos para o campo {field_name}")
    return examples

def get_field_examples(field_name):
    """Obtém exemplos para um campo específico, gerando-os se necessário."""
    examples_file = os.path.join(EXAMPLES_DIR, f"{field_name.replace(' ', '_')}.txt")
    
    if os.path.exists(examples_file):
        with open(examples_file, 'r', encoding='utf-8') as f:
            examples = [line.strip() for line in f.readlines() if line.strip()]
        return examples
    else:
        return generate_field_examples(field_name)

def extract_info_from_site(driver, url, field_name, doctor_filename, source):
    """Extrai informações específicas de um site para um campo usando exemplos."""
    print(f"Extraindo informações para {field_name} de {url}")
    
    # Obtém exemplos para o campo
    examples = get_field_examples(field_name)
    examples_text = "\n".join(examples)
    
    # Extrai o conteúdo da página
    content = extract_page_content(driver, url)
    
    # Salva o conteúdo bruto
    save_raw_search_data(doctor_filename, url, source, content)
    
    # Cria um prompt para a IA extrair a informação específica usando os exemplos
    prompt = f"""
    Você é um assistente especializado em extração de dados precisos. Sua tarefa é extrair APENAS a informação relacionada ao campo "{field_name}" do conteúdo fornecido.

    Regras importantes:
    1. Retorne APENAS a informação solicitada, sem texto adicional
    2. Se a informação não for encontrada, responda apenas "NÃO ENCONTRADO"
    3. Não invente ou suponha dados que não estão explicitamente mencionados
    4. Seja extremamente preciso e específico
    5. Não inclua explicações, apenas o dado extraído
    6. Não use aspas ou formatação especial
    7. Não inclua o nome do campo na resposta
    8. Não numere a resposta
    9. Não inclua observações ou notas
    10. Retorne apenas o texto puro, sem formatação

    Exemplos do formato esperado para o campo "{field_name}":
    {examples_text}

    Conteúdo da página:
    {content}

    Informação a extrair: {field_name}

    Responda APENAS com a informação extraída no formato dos exemplos ou "NÃO ENCONTRADO".
    """
    
    # Envia o prompt para a IA
    extracted_info = query_ollama(prompt)
    
    # Limpa a resposta
    extracted_info = extracted_info.strip()
    
    # Remove frases comuns que indicam explicações
    explanations = [
        "Aqui está", "Baseado no", "De acordo com", "Com base no", 
        "Extraído do", "Encontrei", "A informação", "O campo", 
        "Não foi possível", "Não encontrei", "Não há", "Não existe",
        "Observação:", "Nota:", "Importante:"
    ]
    
    for exp in explanations:
        if extracted_info.startswith(exp):
            # Tenta encontrar o primeiro ponto e remove tudo antes dele
            dot_pos = extracted_info.find('.')
            if dot_pos > 0:
                extracted_info = extracted_info[dot_pos+1:].strip()
    
    # Remove aspas se houver
    extracted_info = extracted_info.strip('"\'')
    
    # Verifica se a resposta é válida
    if "NÃO ENCONTRADO" in extracted_info.upper():
        return None
    
    # Salva a informação extraída
    if extracted_info:
        save_search_result(doctor_filename, url, source, extracted_info, field_name)
    
    return extracted_info

def process_csv():
    """Processa o arquivo CSV, busca os dados faltantes e salva no arquivo de saída."""
    setup_logging()
    
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
        
        print(f"Leitura do arquivo {CSV_INPUT} concluída: {len(rows)} registros encontrados")
        
        # Cria o diretório de exemplos se não existir
        if not os.path.exists(EXAMPLES_DIR):
            os.makedirs(EXAMPLES_DIR)
        
        # Gera exemplos para todos os campos
        print("Gerando exemplos para todos os campos...")
        for header in headers[1:]:  # Ignora o campo CRM
            get_field_examples(header)
        
        # Prepara o arquivo CSV de saída
        with open(CSV_OUTPUT, 'w', encoding='utf-8', newline='') as f_out:
            writer = csv.writer(f_out)
            writer.writerow(headers)
            
            # Processa cada linha do CSV
            for row_index, row in enumerate(rows):
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
                
                # Limpa os arquivos anteriores se existirem
                for directory in [DATA_DIR, RAW_DATA_DIR]:
                    file_path = os.path.join(directory, f"{doctor_filename}.txt")
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(f"Arquivo de busca para: {doctor_filename}\n\n")
                
                # Identifica campos vazios que precisam ser preenchidos
                empty_fields = []
                for i, value in enumerate(row):
                    if not value and i > 0:  # Ignora o campo CRM que é o identificador
                        empty_fields.append(headers[i])
                
                if not empty_fields:
                    print("Todos os campos já estão preenchidos.")
                    writer.writerow(row)
                    continue
                
                print(f"Campos vazios identificados: {empty_fields}")
                
                # Cria uma query de busca com base nos dados disponíveis
                search_query = " ".join([v for v in row if v])
                
                # Busca os top sites
                top_sites = search_top_sites(driver, search_query)
                print(f"Encontrados {len(top_sites)} sites para análise")
                
                # Para cada site, tenta extrair informações para os campos vazios
                for site in top_sites:
                    url = site["url"]
                    source = site["source"]
                    
                    print(f"Analisando site: {url}")
                    
                    # Para cada campo vazio, tenta extrair a informação
                    for field in list(empty_fields):  # Cria uma cópia para poder modificar durante o loop
                        field_index = headers.index(field)
                        
                        # Extrai a informação usando exemplos
                        info = extract_info_from_site(driver, url, field, doctor_filename, source)
                        
                        # Se encontrou informação, atualiza o registro e remove o campo da lista de vazios
                        if info:
                            row[field_index] = info
                            empty_fields.remove(field)
                            print(f"Campo {field} preenchido com: {info}")
                    
                    # Se todos os campos foram preenchidos, para de processar sites
                    if not empty_fields:
                        print("Todos os campos foram preenchidos!")
                        break
                    
                    # Pequeno delay entre sites
                    time.sleep(1)
                
                # Escreve a linha atualizada no arquivo de saída
                writer.writerow(row)
                print(f"Registro {row_index + 1} processado e salvo no arquivo de saída")
        
        print(f"Processamento concluído. Resultados salvos em {CSV_OUTPUT}")
        print(f"Detalhes das buscas salvos nas pastas {DATA_DIR}/ e {RAW_DATA_DIR}/")
        print(f"Exemplos para os campos salvos na pasta {EXAMPLES_DIR}/")
    
    except Exception as e:
        print(f"Erro durante o processamento: {str(e)}")
    
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    process_csv()
