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

def search_bing(driver, query):
    """Realiza uma busca no Bing e retorna os resultados."""
    results = []
    try:
        driver.get(f"https://www.bing.com/search?q={query}")
        WebDriverWait(driver, WAIT_TIME).until(
            EC.presence_of_element_located((By.ID, "b_results"))
        )
        
        # Extrai os resultados
        search_results = driver.find_elements(By.CSS_SELECTOR, "#b_results .b_algo")
        for result in search_results[:5]:  # Limita aos primeiros 5 resultados
            try:
                title = result.find_element(By.CSS_SELECTOR, "h2").text
                snippet = result.find_element(By.CSS_SELECTOR, ".b_caption p").text
                results.append({"title": title, "snippet": snippet})
            except NoSuchElementException:
                continue
        
        return results
    except Exception as e:
        print(f"Erro ao buscar no Bing: {str(e)}")
        return []

def search_searx(query):
    """Realiza uma busca no SearXNG e retorna os resultados."""
    results = []
    try:
        # Tenta primeiro a API JSON
        response = requests.get(SEARX_JSON_URL.format(query))
        if response.status_code == 200:
            data = response.json()
            for result in data.get('results', [])[:5]:
                results.append({
                    "title": result.get('title', ''),
                    "snippet": result.get('content', '')
                })
        else:
            # Fallback para a versão não-JSON
            response = requests.get(f"{SEARX_URL}?q={query}")
            if response.status_code == 200:
                # Extrai resultados do HTML (simplificado)
                content = response.text
                # Implementação simplificada - em produção, usar BeautifulSoup
                snippets = re.findall(r'<p class="content">(.*?)</p>', content)
                titles = re.findall(r'<h4>(.*?)</h4>', content)
                
                for i in range(min(len(titles), len(snippets), 5)):
                    results.append({
                        "title": titles[i],
                        "snippet": snippets[i]
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
                bing_results = search_bing(driver, search_query)
                searx_results = search_searx(search_query)
                
                # Combina os resultados
                all_results = bing_results + searx_results
                results_text = "\n\n".join([
                    f"Título: {result['title']}\nConteúdo: {result['snippet']}"
                    for result in all_results
                ])
                
                # Para cada campo vazio, tenta extrair o dado específico
                for field in fields_to_search:
                    field_index = headers.index(field)
                    
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
                
                # Escreve a linha atualizada no arquivo de saída
                writer.writerow(row)
                
                # Pausa para não sobrecarregar as APIs
                time.sleep(1)
        
        print(f"Processamento concluído. Resultados salvos em {CSV_OUTPUT}")
    
    except Exception as e:
        print(f"Erro durante o processamento: {str(e)}")
    
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    process_csv()
