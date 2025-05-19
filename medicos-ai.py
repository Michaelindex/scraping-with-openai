#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import csv
import json
import time
import re
import requests
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
MAX_RETRIES = 3
WAIT_TIME = 10

def check_csv_exists():
    """Verifica se o arquivo CSV existe e tem dados válidos."""
    if not os.path.exists(CSV_INPUT):
        print(f"Erro: O arquivo {CSV_INPUT} não foi encontrado.")
        return False
    
    with open(CSV_INPUT, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
            if not header:
                print(f"Erro: O arquivo {CSV_INPUT} não contém cabeçalho.")
                return False
            
            # Verifica se há pelo menos uma linha de dados
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
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        return driver
    except Exception as e:
        print(f"Erro ao inicializar o Selenium: {str(e)}")
        return None

def query_ollama(prompt, model=OLLAMA_MODEL):
    """Envia um prompt para a API do Ollama e retorna a resposta."""
    for attempt in range(MAX_RETRIES):
        try:
            payload = {
                "model": model,
                "prompt": prompt,
                "stream": False
            }
            
            response = requests.post(OLLAMA_URL, json=payload)
            
            if response.status_code == 200:
                return response.json().get('response', '')
            else:
                print(f"Erro na API do Ollama (tentativa {attempt+1}/{MAX_RETRIES}): {response.status_code}")
                time.sleep(2)
        except Exception as e:
            print(f"Erro ao conectar com Ollama (tentativa {attempt+1}/{MAX_RETRIES}): {str(e)}")
            time.sleep(2)
    
    return ""

def create_context_prompt(headers, row):
    """Cria um prompt para a IA entender o contexto do CSV."""
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
    
    return prompt.format(headers=headers_str, data=data_str)

def create_validation_prompt(headers, row, search_results, field_to_find):
    """Cria um prompt para a IA validar e extrair dados específicos dos resultados de busca."""
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
    
    return prompt.format(context=context_str, results=search_results, field_to_find=field_to_find)

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
        return f"medico-{row[0]}"
    
    # Último recurso: usa um timestamp
    return f"medico-{int(time.time())}"

def save_search_result(doctor_filename, url, source, extracted_data, field):
    """Salva o resultado da busca em um arquivo txt para o médico."""
    # Cria o diretório data se não existir
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    
    file_path = os.path.join(DATA_DIR, f"{doctor_filename}.txt")
    
    # Formata a entrada conforme solicitado
    entry = f"URL : {url} [ {source} ]\n"
    entry += f"--Informação extraida e alocada no csv: {extracted_data if extracted_data else 'Nenhuma informação extraída'}\n\n"
    
    # Salva a entrada no arquivo (append)
    with open(file_path, 'a', encoding='utf-8') as f:
        f.write(entry)

def search_bing(driver, query, doctor_filename, field):
    """Realiza uma busca no Bing e retorna os resultados."""
    results = []
    try:
        url = f"https://www.bing.com/search?q={query}"
        driver.get(url)
        WebDriverWait(driver, WAIT_TIME).until(
            EC.presence_of_element_located((By.ID, "b_results"))
        )
        
        # Extrai os resultados
        search_results = driver.find_elements(By.CSS_SELECTOR, "#b_results .b_algo")
        for result in search_results[:5]:  # Limita aos primeiros 5 resultados
            try:
                title = result.find_element(By.CSS_SELECTOR, "h2").text
                snippet = result.find_element(By.CSS_SELECTOR, ".b_caption p").text
                
                # Tenta encontrar o link
                link_element = result.find_element(By.CSS_SELECTOR, "h2 a")
                link = link_element.get_attribute("href")
                
                results.append({
                    "title": title,
                    "snippet": snippet,
                    "url": link
                })
            except NoSuchElementException:
                continue
        
        return results
    except Exception as e:
        print(f"Erro ao buscar no Bing: {str(e)}")
        return []

def search_searx(query, doctor_filename, field):
    """Realiza uma busca no SearXNG e retorna os resultados."""
    results = []
    try:
        # Tenta primeiro a API JSON
        url = SEARX_JSON_URL.format(query)
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            for result in data.get('results', [])[:5]:
                results.append({
                    "title": result.get('title', ''),
                    "snippet": result.get('content', ''),
                    "url": result.get('url', '')
                })
        else:
            # Fallback para a versão não-JSON
            url = f"{SEARX_URL}?q={query}"
            response = requests.get(url)
            if response.status_code == 200:
                # Extrai resultados do HTML (simplificado)
                content = response.text
                # Implementação simplificada - em produção, usar BeautifulSoup
                snippets = re.findall(r'<p class="content">(.*?)</p>', content)
                titles = re.findall(r'<h4>(.*?)</h4>', content)
                urls = re.findall(r'<a href="([^"]+)" class="url_link"', content)
                
                for i in range(min(len(titles), len(snippets), len(urls), 5)):
                    results.append({
                        "title": titles[i],
                        "snippet": snippets[i],
                        "url": urls[i]
                    })
    except Exception as e:
        print(f"Erro ao buscar no SearXNG: {str(e)}")
    
    return results

def extract_specific_data(text, field):
    """Usa regex para extrair dados específicos com base no tipo de campo."""
    # Padrões de regex para tipos comuns de dados
    patterns = {
        "CRM": r'\b[0-9]{5,8}\b',
        "CPF": r'\b\d{3}\.\d{3}\.\d{3}-\d{2}\b',
        "CNPJ": r'\b\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}\b',
        "CEP": r'\b\d{5}-\d{3}\b',
        "E-Mail": r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
        "Telephone": r'\b\(\d{2}\)\s*\d{4,5}-\d{4}\b',
        "Celphone": r'\b\(\d{2}\)\s*9\d{4}-\d{4}\b'
    }
    
    # Verifica se o campo corresponde a algum dos padrões conhecidos
    for pattern_name, pattern in patterns.items():
        if pattern_name.lower() in field.lower():
            matches = re.findall(pattern, text)
            if matches:
                return matches[0]
    
    return None

def process_csv():
    """Processa o arquivo CSV, busca os dados faltantes e salva no arquivo de saída."""
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
                
                # Cria o diretório data se não existir
                if not os.path.exists(DATA_DIR):
                    os.makedirs(DATA_DIR)
                
                # Limpa o arquivo anterior se existir
                file_path = os.path.join(DATA_DIR, f"{doctor_filename}.txt")
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
                    fields_to_search = empty_fields
                
                # Cria uma query de busca com base nos dados disponíveis
                search_query = " ".join([v for v in row if v])
                
                # Realiza buscas no Bing e SearXNG
                bing_results = search_bing(driver, search_query, doctor_filename, fields_to_search)
                searx_results = search_searx(search_query, doctor_filename, fields_to_search)
                
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
                    
                    # Primeiro tenta extrair com regex para tipos de dados conhecidos
                    extracted_data = extract_specific_data(results_text, field)
                    
                    # Se não conseguiu com regex, usa a IA para extrair
                    if not extracted_data:
                        validation_prompt = create_validation_prompt(
                            headers, row, results_text, field
                        )
                        extracted_data = query_ollama(validation_prompt)
                        
                        # Limpa a resposta da IA
                        if "NÃO ENCONTRADO" in extracted_data.upper():
                            extracted_data = ""
                    
                    # Atualiza o valor no registro
                    if extracted_data:
                        row[field_index] = extracted_data
                        
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
                
                # Pausa para não sobrecarregar as APIs
                time.sleep(1)
        
        print(f"Processamento concluído. Resultados salvos em {CSV_OUTPUT}")
        print(f"Detalhes das buscas salvos na pasta {DATA_DIR}/")
    
    except Exception as e:
        print(f"Erro durante o processamento: {str(e)}")
    
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    process_csv()
