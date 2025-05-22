#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
buscador_medicos.v12.py

Versão 12.0 do buscador de médicos com:
- Integração do script buscar_cep2.py para captação de CEPs
- Remoção de todas as rotinas antigas de captação de CEP
- Manutenção da lógica de extração de endereços da v10
- Uso exclusivo da estrutura do buscar_cep2.py para CEPs

Aprimoramentos:
- Captação de CEP via SearXNG, Google Selenium e Correios Selenium
- Prompts específicos por tipo de campo
- Limpeza de texto mais agressiva para evitar campos verbosos
- Validação cruzada de dados entre diferentes fontes
"""
import sys
import csv
import re
import requests
import logging
import time
import os
import json
import threading
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from collections import Counter
import math
import tempfile
import shutil
import traceback
import hashlib
import psutil
from datetime import datetime
import urllib.parse
import unicodedata

# Configurações
SEARX_URL   = "http://124.81.6.163:8092/search"
OLLAMA_URL  = "http://124.81.6.163:11434/api/generate"
USER_AGENT  = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/114.0.0.0 Safari/537.36"
)
MAX_RESULTS = 15

# Caminhos dos arquivos
DATA_DIR = 'data'
ESPECIALIDADES_FILE = os.path.join(DATA_DIR, 'especialidades.txt')
TEXTOS_REMOVER_FILE = os.path.join(DATA_DIR, 'textos_remover.txt')
EXEMPLOS_FILE = os.path.join(DATA_DIR, 'exemplos_treinamento.txt')
EMAIL_BLACKLIST_FILE = os.path.join(DATA_DIR, 'email_blacklist.txt')
SITE_BLACKLIST_FILE = os.path.join(DATA_DIR, 'site_blacklist.txt')
LOG_DIR = os.path.join(DATA_DIR, 'logmulti')
DEBUG_HTML_DIR = os.path.join(DATA_DIR, 'debug_html_v12') # Atualizado para v12

# Criar diretórios necessários
for dir_path in [DATA_DIR, DEBUG_HTML_DIR, LOG_DIR]:
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

# Padrões regex (mantidos da v10)
PATTERNS = {
    'address': re.compile(r"(?:Av\.|Avenida|Rua|Travessa|Estrada|R\.)[^,\n]{5,100},?\s*\d{1,5}"),
    'phone':   re.compile(r"\(\d{2}\)\s?\d{4,5}-\d{4}"),
    'email':   re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"),
    'complement': re.compile(r"(?:Sala|Bloco|Apt\.?|Conjunto)[^,\n]{1,50}"),
    'cep':     re.compile(r"\d{5}-\d{3}|\d{8}")
}

# Configuração de logging para multiprocessamento
def setup_logger(process_id):
    logger = logging.getLogger(f"process_{process_id}")
    logger.setLevel(logging.INFO)
    
    # Cria um handler para arquivo
    file_handler = logging.FileHandler(os.path.join(LOG_DIR, f'buscador_medicos_v12_p{process_id}.log'), 'w', 'utf-8') # Atualizado para v12
    file_handler.setLevel(logging.INFO)
    
    # Cria um handler para console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    # Define o formato
    formatter = logging.Formatter('%(asctime)s - P%(process)d - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Adiciona os handlers ao logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def carregar_lista_arquivo(nome_arquivo):
    """Carrega uma lista de um arquivo de texto"""
    try:
        with open(nome_arquivo, 'r', encoding='utf-8') as f:
            return [linha.strip() for linha in f if linha.strip()]
    except Exception as e:
        print(f"Erro ao carregar arquivo {nome_arquivo}: {e}")
        return []

# Carrega as listas dos arquivos externos
TEXTOS_REMOVER = carregar_lista_arquivo(TEXTOS_REMOVER_FILE)
ESPECIALIDADES = carregar_lista_arquivo(ESPECIALIDADES_FILE)
EMAIL_BLACKLIST = carregar_lista_arquivo(EMAIL_BLACKLIST_FILE)
SITE_BLACKLIST = carregar_lista_arquivo(SITE_BLACKLIST_FILE)

# Se os arquivos não existirem, cria com valores padrão
if not TEXTOS_REMOVER:
    with open(TEXTOS_REMOVER_FILE, 'w', encoding='utf-8') as f:
        f.write("Endereço para correspondência\nEndereço para atendimento\nEndereço para consulta\nMunicípio/UF\nBairro:\nCEP\nSala\nApt\nConjunto\nCxpst")
    TEXTOS_REMOVER = carregar_lista_arquivo(TEXTOS_REMOVER_FILE)

if not ESPECIALIDADES:
    with open(ESPECIALIDADES_FILE, 'w', encoding='utf-8') as f:
        f.write("Clínico Geral\nPediatra\nGinecologista\nCardiologista\nDermatologista")
    ESPECIALIDADES = carregar_lista_arquivo(ESPECIALIDADES_FILE)

if not EMAIL_BLACKLIST:
    with open(EMAIL_BLACKLIST_FILE, 'w', encoding='utf-8') as f:
        f.write("@pixeon.com\n@boaconsulta.com\n@example.com\n@dominio.com")
    EMAIL_BLACKLIST = carregar_lista_arquivo(EMAIL_BLACKLIST_FILE)

if not SITE_BLACKLIST:
    with open(SITE_BLACKLIST_FILE, 'w', encoding='utf-8') as f:
        f.write("google.com\nbing.com\nyahoo.com\nfacebook.com\nlinkedin.com\ninstagram.com\ntwitter.com")
    SITE_BLACKLIST = carregar_lista_arquivo(SITE_BLACKLIST_FILE)

def normalizar_endereco(endereco):
    """Normaliza o endereço para busca"""
    if not endereco:
        return ""
    
    # Remove acentos
    endereco = unicodedata.normalize('NFKD', endereco).encode('ASCII', 'ignore').decode('ASCII')
    
    # Padroniza abreviações
    endereco = re.sub(r'\bR\.\b', 'Rua', endereco, flags=re.IGNORECASE)
    endereco = re.sub(r'\bAv\.\b', 'Avenida', endereco, flags=re.IGNORECASE)
    endereco = re.sub(r'\bTrav\.\b', 'Travessa', endereco, flags=re.IGNORECASE)
    endereco = re.sub(r'\bAl\.\b', 'Alameda', endereco, flags=re.IGNORECASE)
    endereco = re.sub(r'\bPc\.\b', 'Praca', endereco, flags=re.IGNORECASE)
    
    # Remove caracteres especiais
    endereco = re.sub(r'[^\w\s,-]', '', endereco)
    
    # Remove múltiplos espaços
    endereco = re.sub(r'\s+', ' ', endereco).strip()
    
    return endereco

def normalizar_cidade(cidade):
    """Normaliza o nome da cidade para busca"""
    if not cidade:
        return ""
    
    # Remove acentos
    cidade = unicodedata.normalize('NFKD', cidade).encode('ASCII', 'ignore').decode('ASCII')
    
    # Remove caracteres especiais
    cidade = re.sub(r'[^\w\s,-]', '', cidade)
    
    # Remove múltiplos espaços
    cidade = re.sub(r'\s+', ' ', cidade).strip()
    
    return cidade

def formatar_cep(cep):
    """Formata o CEP para o padrão XXXXX-XXX"""
    if not cep:
        return ""
    
    # Remove caracteres não numéricos
    cep_limpo = re.sub(r'\D', '', cep)
    
    # Verifica se tem 8 dígitos
    if len(cep_limpo) != 8:
        return ""
    
    # Verifica se não é um CEP inválido (00000000)
    if cep_limpo == "00000000":
        return ""
    
    # Formata para XXXXX-XXX
    return f"{cep_limpo[:5]}-{cep_limpo[5:]}"

def criar_driver():
    """Cria uma instância do Chrome WebDriver"""
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument(f'user-agent={USER_AGENT}')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-notifications')
    options.add_argument('--disable-popup-blocking')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-web-security')
    options.add_argument('--disable-features=IsolateOrigins,site-per-process')
    options.add_argument('--disable-site-isolation-trials')
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Desativa imagens e JavaScript para melhorar performance
    prefs = {
        'profile.default_content_setting_values': {
            'images': 2,  # 2 = block
            'javascript': 1,  # 1 = allow
            'notifications': 2,  # 2 = block
            'plugins': 2  # 2 = block
        }
    }
    options.add_experimental_option('prefs', prefs)
    
    driver = webdriver.Chrome(options=options)
    
    # Executa JavaScript para esconder que estamos usando automação
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

def buscar_no_searx(query, logger):
    """Busca no SearX e retorna os resultados"""
    try:
        logger.info(f"Buscando no SearX: {query}")
        
        # Faz a requisição
        response = requests.get(
            SEARX_URL,
            params={"q": query, "format": "json"},
            headers={"User-Agent": USER_AGENT},
            timeout=10
        )
        
        # Verifica se a resposta foi bem-sucedida
        if response.status_code == 200:
            data = response.json()
            results = data.get('results', [])
            
            # Filtra resultados de sites na blacklist
            filtered_results = []
            for result in results:
                url = result.get('url', '')
                if not any(site in url for site in SITE_BLACKLIST):
                    filtered_results.append(result)
            
            logger.info(f"Resultados encontrados no SearX: {len(filtered_results)}")
            return filtered_results[:MAX_RESULTS]
        
        logger.warning(f"SearX retornou status {response.status_code}")
        return []
    
    except Exception as e:
        logger.error(f"Erro ao buscar no SearX: {e}")
        return []

def buscar_no_bing(query, driver, logger):
    """Busca no Bing e retorna os resultados"""
    try:
        logger.info(f"Buscando no Bing: {query}")
        
        # Acessa o Bing
        driver.get(f"https://www.bing.com/search?q={urllib.parse.quote(query)}")
        time.sleep(2)
        
        # Extrai os resultados
        results = []
        elements = driver.find_elements(By.CSS_SELECTOR, "li.b_algo")
        
        for element in elements:
            try:
                title_element = element.find_element(By.CSS_SELECTOR, "h2")
                link_element = title_element.find_element(By.TAG_NAME, "a")
                url = link_element.get_attribute("href")
                title = title_element.text
                
                # Verifica se o URL não está na blacklist
                if not any(site in url for site in SITE_BLACKLIST):
                    results.append({
                        "url": url,
                        "title": title
                    })
            except Exception as e:
                logger.warning(f"Erro ao extrair resultado do Bing: {e}")
        
        logger.info(f"Resultados encontrados no Bing: {len(results)}")
        return results[:MAX_RESULTS]
    
    except Exception as e:
        logger.error(f"Erro ao buscar no Bing: {e}")
        return []

def buscar_no_google(query, driver, logger):
    """Busca no Google e retorna os resultados"""
    try:
        logger.info(f"Buscando no Google: {query}")
        
        # Acessa o Google
        driver.get(f"https://www.google.com/search?q={urllib.parse.quote(query)}")
        time.sleep(2)
        
        # Extrai os resultados
        results = []
        elements = driver.find_elements(By.CSS_SELECTOR, "div.g")
        
        for element in elements:
            try:
                link_element = element.find_element(By.CSS_SELECTOR, "a")
                url = link_element.get_attribute("href")
                title_element = element.find_element(By.CSS_SELECTOR, "h3")
                title = title_element.text
                
                # Verifica se o URL não está na blacklist
                if url and not any(site in url for site in SITE_BLACKLIST):
                    results.append({
                        "url": url,
                        "title": title
                    })
            except Exception as e:
                logger.warning(f"Erro ao extrair resultado do Google: {e}")
        
        logger.info(f"Resultados encontrados no Google: {len(results)}")
        return results[:MAX_RESULTS]
    
    except Exception as e:
        logger.error(f"Erro ao buscar no Google: {e}")
        return []

def baixar_html(url, driver, logger):
    """Baixa o HTML de uma URL"""
    try:
        logger.info(f"Baixando HTML de {url}")
        
        # Gera um hash da URL para identificação única
        url_hash = hashlib.md5(url.encode()).hexdigest()
        
        # Acessa a URL
        driver.get(url)
        time.sleep(2)
        
        # Obtém o HTML
        html = driver.page_source
        
        # Limita o tamanho do HTML para evitar problemas de memória
        if len(html) > 3 * 1024 * 1024:
            logger.warning(f"HTML muito grande ({len(html) / 1024 / 1024:.2f} MB), truncando...")
            html = html[:3 * 1024 * 1024]
        
        # Salva o HTML para debug
        debug_file = os.path.join(DEBUG_HTML_DIR, f"{url_hash}.html")
        with open(debug_file, 'w', encoding='utf-8', errors='ignore') as f:
            f.write(html)
        logger.info(f"HTML salvo para debug: {debug_file}")
        
        return html
    
    except Exception as e:
        logger.error(f"Erro ao baixar HTML de {url}: {e}")
        return None

def limpar_endereco(endereco):
    """Limpa o endereço removendo textos indesejados"""
    if not endereco:
        return ""
    
    # Limpa textos específicos
    for texto in TEXTOS_REMOVER:
        endereco = endereco.replace(texto, '')
    
    # Remove padrões de CEP
    endereco = re.sub(r'\b\d{5}-\d{3}\b', '', endereco)
    endereco = re.sub(r'\b\d{8}\b', '', endereco)
    
    # Remove múltiplos espaços
    endereco = re.sub(r'\s+', ' ', endereco)
    
    # Remove informações de cidade/estado no formato "Cidade - UF"
    endereco = re.sub(r'\s+[-–]\s+[A-Z]{2}\b', '', endereco)
    
    # Remove informações de CEP no formato "CEP XXXXX-XXX"
    endereco = re.sub(r'CEP\s+\d{5}-\d{3}', '', endereco)
    
    # Remove textos entre parênteses
    endereco = re.sub(r'\([^)]*\)', '', endereco)
    
    return endereco.strip()

def validar_endereco(endereco):
    """Valida se o endereço parece válido"""
    if not endereco:
        return False
    
    # Verifica se tem número
    if not re.search(r'\d', endereco):
        return False
    
    # Verifica se tem pelo menos uma palavra com mais de 3 letras
    if not re.search(r'\b\w{4,}\b', endereco):
        return False
    
    # Verifica se começa com palavras típicas de endereço
    if not re.search(r'^(Rua|Avenida|Av\.|R\.|Travessa|Estrada|Alameda|Al\.|Praça|Pç\.)', endereco, re.IGNORECASE):
        return False
    
    return True

def validar_telefone(telefone):
    """Valida se o telefone parece válido"""
    if not telefone:
        return False
    
    # Verifica se não é uma resposta de IA ou texto explicativo
    if any(termo in telefone.lower() for termo in ['não posso', 'não é possível', 'ajudar', 'exemplo']):
        return False
    
    # Remove caracteres não numéricos
    digits = re.sub(r"\D", "", telefone)
    
    # Verifica se tem pelo menos 10 dígitos (mínimo para um telefone válido)
    if len(digits) < 10:
        return False
    
    # Verifica se tem no máximo 11 dígitos (máximo para um telefone válido)
    if len(digits) > 11:
        return False
    
    # Verifica se começa com DDD válido (11-99)
    ddd = int(digits[:2])
    if ddd < 11 or ddd > 99:
        return False
    
    return True

def validar_email(email):
    """Valida se o email parece válido"""
    if not email:
        return False
    
    # Verifica se está na blacklist
    if any(domain in email.lower() for domain in EMAIL_BLACKLIST):
        return False
    
    # Verifica se tem formato básico de email
    if not re.match(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$', email):
        return False
    
    # Verifica se não é uma resposta de IA ou texto explicativo
    if any(termo in email.lower() for termo in ['não posso', 'não é possível', 'ajudar', 'exemplo']):
        return False
    
    return True

def extrair_candidatos(html, logger):
    """Extrai candidatos para cada campo do HTML"""
    if not html:
        return {
            'address': [],
            'phone': [],
            'email': [],
            'complement': [],
            'cep': []
        }
    
    try:
        # Parseia o HTML
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove scripts e estilos
        for script in soup(['script', 'style']):
            script.decompose()
        
        # Obtém o texto
        text = soup.get_text(' ')
        
        # Extrai candidatos usando regex
        candidates = {
            'address': PATTERNS['address'].findall(text),
            'phone': PATTERNS['phone'].findall(text),
            'email': PATTERNS['email'].findall(text),
            'complement': PATTERNS['complement'].findall(text),
            'cep': PATTERNS['cep'].findall(text)
        }
        
        # Extrai links de telefone e email
        for a in soup.find_all('a', href=True):
            href = a['href']
            if href.startswith('tel:'):
                phone = href[4:].strip()
                candidates['phone'].append(phone)
            elif href.startswith('mailto:'):
                email = href[7:].strip()
                candidates['email'].append(email)
        
        # Limpa e valida os candidatos
        clean_candidates = {
            'address': [limpar_endereco(c) for c in candidates['address']],
            'phone': candidates['phone'],
            'email': candidates['email'],
            'complement': candidates['complement'],
            'cep': [formatar_cep(c) for c in candidates['cep'] if formatar_cep(c)]
        }
        
        # Registra os candidatos encontrados
        for field, values in clean_candidates.items():
            logger.info(f"Candidatos para {field}: {len(values)}")
            if values:
                logger.info(f"Exemplos: {values[:3]}")
        
        return clean_candidates
    
    except Exception as e:
        logger.error(f"Erro ao extrair candidatos: {e}")
        return {
            'address': [],
            'phone': [],
            'email': [],
            'complement': [],
            'cep': []
        }

def aggregate_and_rank(all_c, logger):
    """Agrega e ranqueia os candidatos"""
    ranked = {}
    for k,lst in all_c.items():
        ranked[k] = [item for item,_ in Counter(lst).most_common()]
        logger.info(f"Ranked {k}: {len(ranked[k])} items")
    return ranked

def extrair_numero_endereco(endereco):
    """Extrai o número do endereço"""
    # Procura por padrões comuns de número no final do endereço
    padrao = r',\s*(\d+[A-Za-z]?)$|,\s*(\d+[A-Za-z]?)\s*$|,\s*(\d+[A-Za-z]?)\s*[,.]'
    match = re.search(padrao, endereco)
    if match:
        # Retorna o primeiro grupo não nulo
        numero = next((g for g in match.groups() if g is not None), '')
        # Remove o número do endereço original
        endereco_sem_numero = re.sub(padrao, '', endereco).strip()
        return endereco_sem_numero, numero
    return endereco, ''

def descobrir_cidade(endereco, uf, driver, logger):
    """Descobre a cidade com base no endereço"""
    if not endereco:
        return ""
    
    try:
        # Busca no Google
        query = f"{endereco} cidade {uf}"
        logger.info(f"Buscando cidade: {query}")
        
        driver.get(f"https://www.google.com/search?q={urllib.parse.quote(query)}")
        time.sleep(2)
        
        # Extrai o texto da página
        page_text = driver.page_source
        soup = BeautifulSoup(page_text, 'html.parser')
        text = soup.get_text(' ')
        
        # Lista de cidades do estado
        cidades_encontradas = []
        
        # Busca por padrões como "em [Cidade]" ou "localizada em [Cidade]"
        patterns = [
            r'em\s+([A-Z][a-zÀ-ú]+(?:\s+[A-Z][a-zÀ-ú]+){0,2})\s*[,-]?\s*' + uf,
            r'localizada\s+em\s+([A-Z][a-zÀ-ú]+(?:\s+[A-Z][a-zÀ-ú]+){0,2})\s*[,-]?\s*' + uf,
            r'situada\s+em\s+([A-Z][a-zÀ-ú]+(?:\s+[A-Z][a-zÀ-ú]+){0,2})\s*[,-]?\s*' + uf,
            r'cidade\s+de\s+([A-Z][a-zÀ-ú]+(?:\s+[A-Z][a-zÀ-ú]+){0,2})\s*[,-]?\s*' + uf,
            r'município\s+de\s+([A-Z][a-zÀ-ú]+(?:\s+[A-Z][a-zÀ-ú]+){0,2})\s*[,-]?\s*' + uf
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text)
            cidades_encontradas.extend(matches)
        
        # Se encontrou alguma cidade, retorna a mais frequente
        if cidades_encontradas:
            counter = Counter(cidades_encontradas)
            cidade_mais_frequente = counter.most_common(1)[0][0]
            logger.info(f"Cidade encontrada: {cidade_mais_frequente}")
            return cidade_mais_frequente
        
        logger.warning("Cidade não encontrada")
        return ""
    
    except Exception as e:
        logger.error(f"Erro ao descobrir cidade: {e}")
        return ""

# Integração com buscar_cep2.py
# Funções adaptadas do script buscar_cep2.py para busca de CEP

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
    found_ceps = re.findall(r'\b(\d{5}-?\d{3})\b', text)
    return [sanitize_cep(cep) for cep in found_ceps if sanitize_cep(cep)]

def find_cep_searxng(address, number, bairro, city, state, logger):
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
        logger.info(f"[SearXNG] Buscando CEP para: {query}")
        response = requests.get(SEARX_URL, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        results = response.json()

        for item in results.get('results', []):
            text_to_search = item.get('title', '') + " " + item.get('content', '') + " " + item.get('snippet', '') + " " + item.get('description', '')
            ceps_found = extract_ceps_from_text(text_to_search)
            if ceps_found:
                logger.info(f"[SearXNG] CEP(s) encontrado(s): {ceps_found[0]}")
                return ceps_found[0]

        for infobox in results.get('infoboxes', []):
            text_to_search = infobox.get('content', '')
            if 'links' in infobox:
                for link_info in infobox.get('links', []):
                    text_to_search += " " + link_info.get('text', '') + " " + link_info.get('url', '')
            ceps_found = extract_ceps_from_text(text_to_search)
            if ceps_found:
                logger.info(f"[SearXNG] CEP(s) encontrado(s) em infobox: {ceps_found[0]}")
                return ceps_found[0]

    except requests.exceptions.RequestException as e:
        logger.error(f"[SearXNG] Erro ao buscar: {e}")
    except json.JSONDecodeError:
        logger.error(f"[SearXNG] Erro ao decodificar JSON da resposta.")
    return None

def find_cep_google_selenium(driver, address, number, bairro, city, state, logger):
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
        logger.info(f"[Google Selenium] Buscando CEP para: {query}")
        driver.get(search_url)
        time.sleep(2) # Espera básica para carregamento
        page_text = driver.find_element(By.TAG_NAME, 'body').text
        ceps_found = extract_ceps_from_text(page_text)
        if ceps_found:
            logger.info(f"[Google Selenium] CEP(s) encontrado(s): {ceps_found[0]}")
            return ceps_found[0]
        else:
            logger.warning(f"[Google Selenium] Nenhum CEP encontrado na página para: {query}")
    except Exception as e:
        logger.error(f"[Google Selenium] Erro ao buscar: {e}")
    return None

def find_cep_correios_selenium(driver, address, number, bairro, city, state_uf, logger):
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
        logger.info(f"[Correios Selenium] Buscando CEP para: {search_term}")
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
                logger.warning(f"[Correios Selenium] Dados não encontrados no site dos Correios para: {search_term}")
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
                        logger.info(f"[Correios Selenium] CEP encontrado na tabela: {sanitized}")
                        return sanitized
            # Se a tabela estiver lá mas vazia ou com estrutura diferente
            if not cep_elements and driver.find_elements(By.ID, "resultado-DNEC"):
                 logger.warning(f"[Correios Selenium] Tabela de resultados encontrada, mas CEP não localizado na 4a coluna para: {search_term}")

        except NoSuchElementException:
            logger.warning(f"[Correios Selenium] Tabela de resultados não encontrada para: {search_term}")

    except TimeoutException:
        logger.error(f"[Correios Selenium] Timeout ao carregar página ou elementos para: {search_term}")
    except Exception as e:
        logger.error(f"[Correios Selenium] Erro ao buscar: {e} para: {search_term}")
    return None

def buscar_cep_com_cascata(address, number, bairro, city, state, driver, logger):
    """Busca CEP usando sistema de cascata de fallbacks do buscar_cep2.py"""
    if not address or not city or not state:
        logger.warning("Dados insuficientes para busca de CEP")
        return None
    
    logger.info(f"Iniciando busca de CEP em cascata para: {address}, {city}, {state}")
    
    # 1. Tenta com SearXNG
    cep_encontrado = find_cep_searxng(address, number, bairro, city, state, logger)
    if cep_encontrado:
        logger.info(f"CEP encontrado via SearXNG: {cep_encontrado}")
        return cep_encontrado
    
    logger.warning("SearXNG falhou ou não retornou CEP.")
    
    # 2. Tenta com Google Selenium
    cep_encontrado = find_cep_google_selenium(driver, address, number, bairro, city, state, logger)
    if cep_encontrado:
        logger.info(f"CEP encontrado via Google Selenium: {cep_encontrado}")
        return cep_encontrado
    
    logger.warning("Google Selenium falhou ou não retornou CEP.")
    
    # 3. Tenta com Correios Selenium
    cep_encontrado = find_cep_correios_selenium(driver, address, number, bairro, city, state, logger)
    if cep_encontrado:
        logger.info(f"CEP encontrado via Correios Selenium: {cep_encontrado}")
        return cep_encontrado
    
    logger.warning("Correios Selenium falhou ou não retornou CEP.")
    
    logger.warning("Nenhum CEP encontrado após tentar todos os métodos")
    return None

# Exemplos específicos para cada tipo de campo
EXEMPLOS_CAMPOS = {
    'address': [
        "Rua Visconde do Rio Branco",
        "Avenida Paulista",
        "Rua Barão de Itapetininga",
        "Avenida Brasil",
        "Rua das Flores"
    ],
    'phone': [
        "(11) 3113-8000",
        "(21) 2222-3333",
        "(85) 3198-3700",
        "(41) 3240-4000",
        "(31) 99876-5432"
    ],
    'email': [
        "contato@empresa.com.br",
        "atendimento@clinica.med.br",
        "dr.nome@gmail.com",
        "secretaria@consultorio.com",
        "info@hospital.org.br"
    ],
    'complement': [
        "Sala 101",
        "Conjunto 304",
        "Bloco B",
        "Apto 1210",
        "Sala 22"
    ],
    'city': [
        "São Paulo",
        "Rio de Janeiro",
        "Fortaleza",
        "Curitiba",
        "Belo Horizonte"
    ],
    'state': [
        "SP",
        "RJ",
        "CE",
        "PR",
        "MG"
    ],
    'bairro': [
        "Centro",
        "Jardim Paulista",
        "Copacabana",
        "Boa Viagem",
        "Barra da Tijuca"
    ],
    'cep': [
        "01310-200",
        "20031-170",
        "60175-047",
        "80530-000",
        "30130-110"
    ]
}

def gerar_prompt_ollama(texto, tipo_campo, exemplos, logger):
    """Gera um prompt para o Ollama com exemplos específicos"""
    # Exemplos específicos para o tipo de campo
    exemplos_campo = EXEMPLOS_CAMPOS.get(tipo_campo, [])
    
    # Adiciona exemplos específicos ao prompt
    exemplos_texto = "\n".join([f"- {ex}" for ex in exemplos_campo])
    
    # Instruções específicas por tipo de campo
    instrucoes_especificas = {
        'address': "Extraia apenas o nome da rua/avenida, sem número, complemento, bairro, cidade ou estado.",
        'phone': "Extraia apenas o número de telefone no formato (XX) XXXX-XXXX ou (XX) XXXXX-XXXX.",
        'email': "Extraia apenas o endereço de e-mail completo, em minúsculas.",
        'complement': "Extraia apenas o complemento do endereço (sala, bloco, apartamento, etc.).",
        'city': "Extraia apenas o nome da cidade, sem estado ou país.",
        'state': "Extraia apenas a sigla do estado (2 letras maiúsculas).",
        'bairro': "Extraia apenas o nome do bairro, sem cidade ou estado.",
        'cep': "Extraia apenas o CEP no formato XXXXX-XXX."
    }
    
    instrucao = instrucoes_especificas.get(tipo_campo, "Extraia a informação solicitada.")
    
    # Regras para garantir respostas limpas
    regras = """
    REGRAS IMPORTANTES:
    1. Responda APENAS com a informação solicitada, sem explicações ou texto adicional.
    2. Se não encontrar a informação, responda apenas com uma string vazia.
    3. Não inclua frases como "A informação é" ou "O valor é".
    4. Não inclua marcadores de lista ou numeração.
    5. Não inclua aspas ou outros caracteres delimitadores.
    6. Não inclua observações ou notas.
    7. Não inclua o tipo de informação (ex: "Endereço: Rua X") - apenas o valor.
    8. Não inclua múltiplas opções ou alternativas.
    9. Não inclua texto explicativo antes ou depois da informação.
    10. Forneça apenas UMA resposta, a mais provável e relevante.
    """
    
    # Constrói o prompt completo
    prompt = f"""
    Analise o seguinte texto e extraia apenas a informação de {tipo_campo}:
    
    {texto}
    
    {instrucao}
    
    Exemplos do formato esperado:
    {exemplos_texto}
    
    {regras}
    """
    
    logger.info(f"Prompt gerado para {tipo_campo} com {len(exemplos_campo)} exemplos")
    return prompt

def consultar_ollama(prompt, logger):
    """Consulta o modelo Ollama"""
    try:
        logger.info("Consultando Ollama...")
        
        # Prepara os dados
        data = {
            "model": "llama3.1:8b",
            "prompt": prompt,
            "stream": False
        }
        
        # Faz a requisição
        response = requests.post(OLLAMA_URL, json=data, timeout=30)
        
        # Verifica se a resposta foi bem-sucedida
        if response.status_code == 200:
            result = response.json()
            response_text = result.get('response', '').strip()
            logger.info(f"Resposta do Ollama: {response_text}")
            return response_text
        
        logger.warning(f"Ollama retornou status {response.status_code}")
        return ""
    
    except Exception as e:
        logger.error(f"Erro ao consultar Ollama: {e}")
        return ""

def limpar_texto_extenso(texto, tipo_campo, logger):
    """Limpa texto extenso removendo informações irrelevantes"""
    if not texto:
        return ""
    
    logger.info(f"Limpando texto extenso para campo {tipo_campo}: {texto[:50]}...")
    
    # Remove textos explicativos comuns
    explicativos = [
        "Aqui está", "Aqui estão", "Encontrei", "Segue", "Baseado em", 
        "De acordo com", "Conforme", "Segundo", "A seguir", "Abaixo",
        "Informações", "Dados", "Detalhes", "Resultados", "Análise",
        "Observação", "Nota", "Importante", "Atenção", "Aviso",
        "Não foi possível", "Não encontrei", "Não há", "Não existe",
        "Não disponível", "Não informado", "Não consta", "Não identificado"
    ]
    
    for exp in explicativos:
        if texto.startswith(exp):
            # Remove o texto explicativo e qualquer pontuação ou espaço após ele
            texto = re.sub(f"^{exp}[:\s,.;-]*", "", texto)
    
    # Remove marcadores de lista e numeração
    texto = re.sub(r'^\s*[\*\-•◦‣⁃⁌⁍⦾⦿⁕⁘⁙⁚⁛⁜⁝⁞⁂⁃⁄⁅⁆⁇⁈⁉⁊⁋⁌⁍⁎⁏⁐⁑⁒⁓⁔⁕⁖⁗⁘⁙⁚⁛⁜⁝⁞⁰ⁱ⁲⁳⁴⁵⁶⁷⁸⁹⁺⁻⁼⁽⁾ⁿ₀₁₂₃₄₅₆₇₈₉₊₋₌₍₎ₐₑₒₓₔₕₖₗₘₙₚₛₜ]\s*', '', texto)
    texto = re.sub(r'^\s*\d+[\.\)]\s*', '', texto)
    
    # Remove aspas e parênteses
    texto = texto.strip('"\'()[]{}')
    
    # Tratamento específico por tipo de campo
    if tipo_campo == 'address':
        # Remove informações de CEP
        texto = re.sub(r'CEP:?\s*\d{5}-?\d{3}', '', texto)
        # Remove informações de cidade/estado
        texto = re.sub(r'\s+-\s+[A-Z]{2}$', '', texto)
        # Remove textos como "Endereço:" ou "Localização:"
        texto = re.sub(r'^(Endereço|Localização|Local|Sede|Consultório):\s*', '', texto)
        # Limita o tamanho do endereço
        texto = texto[:100]
    
    elif tipo_campo == 'phone':
        # Extrai apenas o número de telefone no formato (XX) XXXX-XXXX ou (XX) XXXXX-XXXX
        match = re.search(r'\(\d{2}\)\s?\d{4,5}-\d{4}', texto)
        if match:
            texto = match.group(0)
        else:
            # Tenta extrair números com DDD sem parênteses
            match = re.search(r'\d{2}\s?\d{4,5}-\d{4}', texto)
            if match:
                num = match.group(0)
                ddd = num[:2]
                resto = num[2:].strip()
                texto = f"({ddd}) {resto}"
    
    elif tipo_campo == 'email':
        # Extrai apenas o email
        match = re.search(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', texto)
        if match:
            texto = match.group(0)
        # Converte para minúsculas
        texto = texto.lower()
    
    elif tipo_campo == 'complement':
        # Limita o complemento a 30 caracteres
        texto = texto[:30]
        # Remove textos como "Complemento:" ou "Informações adicionais:"
        texto = re.sub(r'^(Complemento|Informações adicionais|Adicional|Obs):\s*', '', texto)
    
    elif tipo_campo == 'city':
        # Remove textos como "Cidade:" ou "Município:"
        texto = re.sub(r'^(Cidade|Município|Localidade):\s*', '', texto)
        # Remove qualquer texto após o nome da cidade (como estado ou país)
        texto = re.sub(r'\s+-\s+.*$', '', texto)
        # Limita o tamanho da cidade
        texto = texto[:30]
    
    elif tipo_campo == 'state':
        # Extrai apenas a sigla do estado (2 letras maiúsculas)
        match = re.search(r'\b[A-Z]{2}\b', texto)
        if match:
            texto = match.group(0)
        # Limita a 2 caracteres
        texto = texto[:2]
    
    elif tipo_campo == 'bairro':
        # Remove textos como "Bairro:" ou "Região:"
        texto = re.sub(r'^(Bairro|Região|Distrito|Setor):\s*', '', texto)
        # Limita o tamanho do bairro
        texto = texto[:30]
    
    # Remove múltiplos espaços
    texto = re.sub(r'\s+', ' ', texto).strip()
    
    logger.info(f"Texto limpo: {texto}")
    return texto

def processar_medico(medico, process_id, lock, logger):
    """Processa um médico para extrair informações"""
    try:
        # Extrai os campos do médico
        crm = medico.get('CRM', '')
        uf = medico.get('UF', '')
        firstname = medico.get('Firstname', '')
        lastname = medico.get('LastName', '')
        
        logger.info(f"Processando médico: {firstname} {lastname} (CRM: {crm}, UF: {uf})")
        
        # Cria um driver para este processo
        driver = criar_driver()
        
        try:
            # Constrói a query de busca
            query = f"{firstname} {lastname} médico {uf} CRM {crm}"
            logger.info(f"Query de busca: {query}")
            
            # Busca no SearX
            searx_results = buscar_no_searx(query, logger)
            
            # Busca no Bing
            bing_results = buscar_no_bing(query, driver, logger)
            
            # Busca no Google
            google_results = buscar_no_google(query, driver, logger)
            
            # Combina os resultados
            all_results = searx_results + bing_results + google_results
            
            # Filtra URLs duplicadas
            unique_urls = []
            seen_urls = set()
            for result in all_results:
                url = result.get('url', '')
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    unique_urls.append(url)
            
            logger.info(f"URLs únicas encontradas: {len(unique_urls)}")
            
            # Prioriza URLs que contêm o nome do médico
            nome_lower = f"{firstname} {lastname}".lower()
            prioritized_urls = []
            other_urls = []
            
            for url in unique_urls:
                if nome_lower in url.lower():
                    prioritized_urls.append(url)
                else:
                    other_urls.append(url)
            
            # Combina as listas, com as URLs prioritárias primeiro
            final_urls = prioritized_urls + other_urls
            logger.info(f"URLs prioritárias: {len(prioritized_urls)}")
            
            # Limita o número de URLs para processamento
            urls_to_process = final_urls[:MAX_RESULTS]
            
            # Inicializa os candidatos
            all_candidates = {
                'address': [],
                'phone': [],
                'email': [],
                'complement': [],
                'cep': []
            }
            
            # Processa cada URL
            for url in urls_to_process:
                try:
                    logger.info(f"Processando URL: {url}")
                    
                    # Verifica se é um arquivo não-HTML
                    if any(ext in url.lower() for ext in ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.zip', '.rar']):
                        logger.info(f"Pulando arquivo não-HTML: {url}")
                        continue
                    
                    # Baixa o HTML
                    html = baixar_html(url, driver, logger)
                    if not html:
                        logger.warning(f"Não foi possível baixar o HTML de {url}")
                        continue
                    
                    # Extrai candidatos
                    candidates = extrair_candidatos(html, logger)
                    
                    # Adiciona os candidatos à lista geral
                    for field, values in candidates.items():
                        all_candidates[field].extend(values)
                
                except Exception as e:
                    logger.error(f"Erro ao processar URL {url}: {e}")
                    continue
            
            # Agrega e ranqueia os candidatos
            ranked_candidates = aggregate_and_rank(all_candidates, logger)
            
            # Inicializa os resultados
            results = {
                'address': "",
                'number': "",
                'complement': "",
                'bairro': "",
                'cep': "",
                'city': "",
                'state': "",
                'phone': "",
                'phone2': "",
                'cellphone': "",
                'cellphone2': "",
                'email': "",
                'email2': ""
            }
            
            # Processa o endereço
            if ranked_candidates['address']:
                address_candidate = ranked_candidates['address'][0]
                logger.info(f"Candidato a endereço: {address_candidate}")
                
                # Extrai o número do endereço
                address_without_number, number = extrair_numero_endereco(address_candidate)
                
                # Consulta o Ollama para validar o endereço
                prompt = gerar_prompt_ollama(address_without_number, 'address', [], logger)
                address_validated = consultar_ollama(prompt, logger)
                address_validated = limpar_texto_extenso(address_validated, 'address', logger)
                
                if address_validated and validar_endereco(address_validated):
                    results['address'] = address_validated
                    results['number'] = number
                else:
                    results['address'] = address_without_number
                    results['number'] = number
            
            # Processa o complemento
            if ranked_candidates['complement']:
                complement_candidate = ranked_candidates['complement'][0]
                logger.info(f"Candidato a complemento: {complement_candidate}")
                
                # Consulta o Ollama para validar o complemento
                prompt = gerar_prompt_ollama(complement_candidate, 'complement', [], logger)
                complement_validated = consultar_ollama(prompt, logger)
                complement_validated = limpar_texto_extenso(complement_validated, 'complement', logger)
                
                results['complement'] = complement_validated
            
            # Processa a cidade e estado
            if results['address']:
                # Tenta descobrir a cidade
                city = descobrir_cidade(results['address'], uf, driver, logger)
                if city:
                    results['city'] = city
                    results['state'] = uf
            
            # Processa o telefone
            if ranked_candidates['phone']:
                # Prioriza telefones celulares (com 9 dígitos)
                cell_phones = []
                landline_phones = []
                
                for phone in ranked_candidates['phone']:
                    # Remove caracteres não numéricos
                    digits = re.sub(r"\D", "", phone)
                    
                    # Verifica se é um celular (9 dígitos após o DDD)
                    if len(digits) == 11 and digits[2] == '9':
                        cell_phones.append(phone)
                    else:
                        landline_phones.append(phone)
                
                # Preenche os campos de telefone
                if cell_phones:
                    results['cellphone'] = cell_phones[0]
                    if len(cell_phones) > 1:
                        results['cellphone2'] = cell_phones[1]
                
                if landline_phones:
                    results['phone'] = landline_phones[0]
                    if len(landline_phones) > 1:
                        results['phone2'] = landline_phones[1]
                
                # Se não encontrou celular, usa o telefone fixo
                if not results['cellphone'] and results['phone']:
                    results['cellphone'] = results['phone']
                    results['phone'] = ""
            
            # Processa o email
            if ranked_candidates['email']:
                # Filtra emails na blacklist
                valid_emails = []
                for email in ranked_candidates['email']:
                    if validar_email(email):
                        valid_emails.append(email)
                
                if valid_emails:
                    results['email'] = valid_emails[0]
                    if len(valid_emails) > 1:
                        results['email2'] = valid_emails[1]
            
            # Busca o CEP usando o script buscar_cep2.py integrado
            if results['address'] and (results['city'] or uf):
                cep_encontrado = buscar_cep_com_cascata(
                    results['address'], 
                    results['number'], 
                    results.get('bairro', ''), 
                    results.get('city', ''), 
                    uf, 
                    driver, 
                    logger
                )
                
                if cep_encontrado:
                    results['cep'] = cep_encontrado
            
            # Limpa os resultados
            for field in results:
                if results[field]:
                    results[field] = limpar_texto_extenso(results[field], field, logger)
            
            # Fecha o driver
            driver.quit()
            
            # Retorna os resultados
            return results
        
        except Exception as e:
            logger.error(f"Erro ao processar médico {firstname} {lastname}: {e}")
            
            # Fecha o driver em caso de erro
            try:
                driver.quit()
            except:
                pass
            
            return {}
    
    except Exception as e:
        logger.error(f"Erro crítico ao processar médico: {e}")
        return {}

def processar_chunk(chunk, process_id):
    """Processa um chunk de médicos"""
    # Configura o logger para este processo
    logger = setup_logger(process_id)
    logger.info(f"Iniciando processo {process_id} com {len(chunk)} médicos")
    
    # Cria um lock para este processo
    lock = multiprocessing.Manager().Lock()
    
    # Processa cada médico no chunk
    results = []
    for medico in chunk:
        try:
            # Marca o tempo de início
            start_time = time.time()
            
            # Processa o médico
            result = processar_medico(medico, process_id, lock, logger)
            
            # Registra o tempo de execução
            elapsed = time.time() - start_time
            logger.info(f"Tempo de execução: {elapsed:.2f} segundos")
            
            # Adiciona o resultado à lista
            results.append((medico, result))
            
        except Exception as e:
            logger.error(f"Erro ao processar médico {medico.get('Firstname', '')} {medico.get('LastName', '')}: {e}")
            results.append((medico, {}))
    
    return results

def main():
    """Função principal"""
    # Verifica os argumentos
    if len(sys.argv) != 3:
        print("Uso: python buscador_medicos.v12.py input.csv output.csv")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    # Verifica se o arquivo de entrada existe
    if not os.path.exists(input_file):
        print(f"Arquivo de entrada {input_file} não encontrado")
        sys.exit(1)
    
    # Configura o logger principal
    logger = setup_logger("main")
    logger.info("Iniciando processamento sequencial")
    
    # Carrega os médicos do arquivo CSV
    medicos = []
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                medicos.append(row)
        
        logger.info(f"Carregados {len(medicos)} médicos do arquivo {input_file}")
    except Exception as e:
        logger.error(f"Erro ao carregar arquivo {input_file}: {e}")
        sys.exit(1)
    
    # Processa cada médico sequencialmente
    all_results = []
    for medico in medicos:
        # Cria um lock simples (não multiprocessing)
        lock = threading.Lock()
        
        # Processa o médico
        result = processar_medico(medico, 0, lock, logger)
        all_results.append((medico, result))
    
    logger.info(f"Processamento concluído, salvando resultados em {output_file}")
    
    # Salva os resultados no arquivo CSV
    try:
        with open(output_file, 'w', encoding='utf-8', newline='') as f:
            # Define os campos do CSV
            fieldnames = [
                'Hash', 'CRM', 'UF', 'Firstname', 'LastName', 'Medical specialty',
                'Endereco Completo A1', 'Address A1', 'Numero A1', 'Complement A1', 'Bairro A1',
                'postal code A1', 'City A1', 'State A1', 'Phone A1', 'Phone A2',
                'Cell phone A1', 'Cell phone A2', 'E-mail A1', 'E-mail A2',
                'OPT-IN', 'STATUS', 'LOTE'
            ]
            
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for medico, result in all_results:
                # Cria uma linha para o CSV
                row = {
                    'Hash': '',
                    'CRM': medico.get('CRM', ''),
                    'UF': medico.get('UF', ''),
                    'Firstname': medico.get('Firstname', ''),
                    'LastName': medico.get('LastName', ''),
                    'Medical specialty': medico.get('Medical specialty', ''),
                    'Endereco Completo A1': f"{result.get('address', '')}, {result.get('number', '')}" if result.get('address') else '',
                    'Address A1': result.get('address', ''),
                    'Numero A1': result.get('number', ''),
                    'Complement A1': result.get('complement', ''),
                    'Bairro A1': result.get('bairro', ''),
                    'postal code A1': result.get('cep', ''),
                    'City A1': result.get('city', ''),
                    'State A1': result.get('state', ''),
                    'Phone A1': result.get('phone', ''),
                    'Phone A2': result.get('phone2', ''),
                    'Cell phone A1': result.get('cellphone', ''),
                    'Cell phone A2': result.get('cellphone2', ''),
                    'E-mail A1': result.get('email', ''),
                    'E-mail A2': result.get('email2', ''),
                    'OPT-IN': '',
                    'STATUS': '',
                    'LOTE': ''
                }
                
                writer.writerow(row)
        
        logger.info(f"Resultados salvos em {output_file}")
    
    except Exception as e:
        logger.error(f"Erro ao salvar resultados em {output_file}: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
