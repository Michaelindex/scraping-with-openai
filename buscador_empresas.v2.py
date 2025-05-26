#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
buscador_empresas.v2.py - Revisado para Validação Rigorosa

Versão 2.1 (baseada na v2.0) com:
- Prompts de IA (Ollama) MUITO mais rigorosos para validação e associação com a empresa alvo.
- Validação Pós-IA implementada para descartar dados suspeitos/genéricos.
- Ênfase em retornar null para dados incertos ou não associados.
- Sem fallbacks para candidatos brutos não validados pela IA.
"""
import sys
import csv
import re
import requests
import logging
import time
import os
import json
import multiprocessing
from multiprocessing import Pool, Manager, Lock
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import StaleElementReferenceException, WebDriverException
from collections import Counter, defaultdict
import math
import tempfile
import shutil
import traceback
import hashlib
import psutil
from datetime import datetime
import urllib.parse
import unicodedata

# ===================== CONFIGURAÇÕES =====================
FOCO_CONTATO = "TI"  # <-- MODIFIQUE AQUI O FOCO DO CONTATO DESEJADO

# URLs de Serviços
SEARX_URL       = os.environ.get("SEARX_URL", "http://124.81.6.163:8092/search")
OLLAMA_URL      = os.environ.get("OLLAMA_URL", "http://124.81.6.163:11434/api/generate")
BRASILAPI_CNPJ_URL = "https://brasilapi.com.br/api/cnpj/v1/{cnpj}"

# Configurações Gerais
USER_AGENT  = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/114.0.0.0 Safari/537.36"
)
MAX_SEARCH_RESULTS = 10
REQUEST_TIMEOUT = 30
SELENIUM_TIMEOUT = 45
OLLAMA_TIMEOUT = 120
OLLAMA_MODEL = "llama3.1:8b"

# Configurações de Paralelismo
NUM_PROCESSES = max(1, multiprocessing.cpu_count() // 2)
CHUNK_SIZE = 5

# Caminhos
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data_empresas')
TIPOS_EMPRESA_FILE = os.path.join(DATA_DIR, 'tipos_empresa.txt')
TEXTOS_REMOVER_FILE = os.path.join(DATA_DIR, 'textos_remover.txt')
EMAIL_BLACKLIST_FILE = os.path.join(DATA_DIR, 'email_blacklist.txt')
SITE_BLACKLIST_FILE = os.path.join(DATA_DIR, 'site_blacklist.txt')
CARGOS_RELEVANTES_FILE = os.path.join(DATA_DIR, 'cargos_relevantes.json')
LOG_DIR = os.path.join(BASE_DIR, 'logs_empresas')
DEBUG_HTML_DIR = os.path.join(BASE_DIR, 'debug_html_empresas')
CACHE_DIR = os.path.join(BASE_DIR, 'cache_empresas')
API_CACHE_FILE = os.path.join(CACHE_DIR, 'api_cache.json')
SEARX_CACHE_FILE = os.path.join(CACHE_DIR, 'searx_cache.json')
HTML_CACHE_DIR = os.path.join(CACHE_DIR, 'html')

# Criar diretórios necessários
for dir_path in [DATA_DIR, LOG_DIR, DEBUG_HTML_DIR, CACHE_DIR, HTML_CACHE_DIR]:
    os.makedirs(dir_path, exist_ok=True)

# ===================== FUNÇÕES DE UTILIDADE E CARREGAMENTO =====================
def carregar_lista_arquivo(nome_arquivo, criar_padrao=None):
    try:
        if not os.path.exists(nome_arquivo) and criar_padrao:
            with open(nome_arquivo, 'w', encoding='utf-8') as f:
                f.write(criar_padrao)
        with open(nome_arquivo, 'r', encoding='utf-8') as f:
            return [linha.strip() for linha in f if linha.strip()]
    except Exception as e:
        print(f"Erro ao carregar arquivo {nome_arquivo}: {e}")
        return []

def carregar_json_arquivo(nome_arquivo, criar_padrao=None):
    try:
        if not os.path.exists(nome_arquivo) and criar_padrao:
            with open(nome_arquivo, 'w', encoding='utf-8') as f:
                json.dump(criar_padrao, f, ensure_ascii=False, indent=2)
        with open(nome_arquivo, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Erro ao carregar arquivo JSON {nome_arquivo}: {e}")
        return {}

def salvar_json_arquivo(dados, nome_arquivo):
    try:
        with open(nome_arquivo, 'w', encoding='utf-8') as f:
            json.dump(dados, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Erro ao salvar arquivo JSON {nome_arquivo}: {e}")

# Carregar dados externos
TIPOS_EMPRESA = carregar_lista_arquivo(TIPOS_EMPRESA_FILE, criar_padrao="Ltda\nS.A.\nEireli\nME\nEPP\nSociedade Simples\nMEI")
TEXTOS_REMOVER = carregar_lista_arquivo(TEXTOS_REMOVER_FILE, criar_padrao="CNPJ\nRazão Social\nInscrição Estadual\nEndereço\nTelefone\nContato")
EMAIL_BLACKLIST = carregar_lista_arquivo(EMAIL_BLACKLIST_FILE, criar_padrao="@gmail.com\n@yahoo.com\n@hotmail.com\n@outlook.com\n@example.com\ncontato@\nfaleconosco@\nsuporte@")
SITE_BLACKLIST = carregar_lista_arquivo(SITE_BLACKLIST_FILE, criar_padrao="google.com\nfacebook.com\nyoutube.com\nwikipedia.org\nreclameaqui.com.br\napontador.com.br\ninstagram.com\ntwitter.com")
CARGOS_RELEVANTES = carregar_json_arquivo(CARGOS_RELEVANTES_FILE, criar_padrao={
    "TI": ["Gerente de TI", "Diretor de TI", "Coordenador de TI", "Analista de Sistemas", "Desenvolvedor", "Infraestrutura", "CIO", "CTO"],
    "Comercial": ["Gerente Comercial", "Diretor Comercial", "Vendedor", "Executivo de Contas"],
    "Marketing": ["Gerente de Marketing", "Diretor de Marketing", "Analista de Marketing"],
    "RH": ["Gerente de RH", "Diretor de RH", "Analista de RH", "Recrutador"],
    "Financeiro": ["Gerente Financeiro", "Diretor Financeiro", "CFO", "Contador", "Analista Financeiro"]
})

# Cache (Carregar no início)
API_CACHE = carregar_json_arquivo(API_CACHE_FILE, criar_padrao={})
SEARX_CACHE = carregar_json_arquivo(SEARX_CACHE_FILE, criar_padrao={})

# ===================== PADRÕES REGEX =====================
PATTERNS = {
    'cnpj': re.compile(r"\b\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}\b"),
    'telefone': re.compile(r"\b\(?\d{2}\)?\s?\d{4,5}-?\d{4}\b"),
    'celular': re.compile(r"\b\(?\d{2}\)?\s?9\d{4}-?\d{4}\b"),
    'email': re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
    'cep': re.compile(r"\b\d{5}-?\d{3}\b"),
    'linkedin_profile': re.compile(r"https://[a-z]{2,3}\.linkedin\.com/in/([a-zA-Z0-9_-]+)"), # Captura o username
    'linkedin_company': re.compile(r"https://[a-z]{2,3}\.linkedin\.com/company/[a-zA-Z0-9_-]+")
}

# ===================== LOGGING =====================
def setup_logger(process_id):
    logger = logging.getLogger(f"process_{process_id}")
    if logger.hasHandlers():
        return logger
    logger.setLevel(logging.INFO)
    log_file = os.path.join(LOG_DIR, f'buscador_empresas_p{process_id}.log')
    file_handler = logging.FileHandler(log_file, 'a', 'utf-8')
    file_handler.setLevel(logging.INFO)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - P%(process)d - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

# ===================== FUNÇÕES DE NORMALIZAÇÃO E VALIDAÇÃO =====================
def normalizar_texto(texto):
    if not texto:
        return ""
    try:
        texto = unicodedata.normalize('NFKD', str(texto)).encode('ASCII', 'ignore').decode('ASCII')
        texto = texto.lower()
        texto = re.sub(r'[^\w\s@.-]', ' ', texto)
        texto = re.sub(r'\s+', ' ', texto).strip()
        return texto
    except Exception as e:
        return str(texto).lower().strip()

def formatar_cnpj(cnpj):
    if not cnpj:
        return ""
    cnpj_limpo = re.sub(r'\D', '', str(cnpj))
    if len(cnpj_limpo) == 14:
        return f"{cnpj_limpo[:2]}.{cnpj_limpo[2:5]}.{cnpj_limpo[5:8]}/{cnpj_limpo[8:12]}-{cnpj_limpo[12:]}"
    return cnpj

def validar_cnpj(cnpj):
    cnpj_limpo = re.sub(r'\D', '', str(cnpj))
    if len(cnpj_limpo) != 14 or len(set(cnpj_limpo)) == 1:
        return False
    try:
        pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        pesos2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        soma1 = sum(int(d) * p for d, p in zip(cnpj_limpo[:12], pesos1))
        dv1 = (11 - (soma1 % 11)) % 10
        soma2 = sum(int(d) * p for d, p in zip(cnpj_limpo[:13], pesos2))
        dv2 = (11 - (soma2 % 11)) % 10
        return int(cnpj_limpo[12]) == dv1 and int(cnpj_limpo[13]) == dv2
    except:
        return False

def validar_email(email, dominio_empresa=None):
    if not email or not isinstance(email, str):
        return False
    email_lower = email.lower()
    # 1. Blacklist básico
    if any(domain in email_lower for domain in EMAIL_BLACKLIST):
        return False
    # 2. Regex geral
    if not re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", email):
        return False
    # 3. Validação de domínio (se fornecido)
    if dominio_empresa:
        dominio_email = extrair_dominio(email_lower)
        # Considerar domínios ligeiramente diferentes? (ex: .com vs .com.br)
        # Por enquanto, exigir correspondência exata ou subdomínio
        if dominio_email != dominio_empresa and not dominio_email.endswith('.' + dominio_empresa):
            return False
    return True

def formatar_telefone(telefone):
    if not telefone:
        return ""
    digits = re.sub(r"\D", "", str(telefone))
    if len(digits) == 10:
        return f"({digits[:2]}) {digits[2:6]}-{digits[6:]}"
    elif len(digits) == 11 and digits[2] == '9':
        return f"({digits[:2]}) {digits[2:7]}-{digits[7:]}"
    return telefone

def validar_telefone(telefone):
    if not telefone:
        return False
    digits = re.sub(r"\D", "", str(telefone))
    return 10 <= len(digits) <= 11

def extrair_dominio(url_ou_email):
    if not url_ou_email:
        return None
    try:
        if '@' in url_ou_email:
            return url_ou_email.split('@')[1].lower()
        else:
            parsed_uri = urllib.parse.urlparse(url_ou_email)
            domain = '{uri.netloc}'.format(uri=parsed_uri)
            if domain.startswith('www.'):
                domain = domain[4:]
            # Tentar normalizar domínios (ex: .com.br -> .com)
            # parts = domain.split('.')
            # if len(parts) > 2 and parts[-2] in ['com', 'net', 'org', 'gov']:
            #     domain = '.'.join(parts[-3:])
            # elif len(parts) > 1:
            #     domain = '.'.join(parts[-2:])
            return domain.lower()
    except Exception:
        return None

def validar_linkedin_profile(url):
    """Valida se a URL do LinkedIn parece ser um perfil de pessoa válido e não genérico."""
    if not url or not isinstance(url, str):
        return False
    match = PATTERNS['linkedin_profile'].match(url)
    if not match:
        return False
    # Verificar se o username capturado não é muito curto ou genérico
    username = match.group(1)
    if not username or len(username) < 3 or username.isdigit():
        return False
    # Evitar URLs que terminam com números (podem ser IDs genéricos)
    if re.search(r'\d+$', username):
        return False
    # Adicionar outras heurísticas se necessário (ex: verificar se contém nome/sobrenome)
    return True

# ===================== FUNÇÕES DE CACHE =====================
def get_from_cache(cache_dict, key):
    return cache_dict.get(key)

def save_to_cache(cache_dict, key, value):
    cache_dict[key] = value

def get_html_from_cache(url):
    url_hash = hashlib.md5(url.encode()).hexdigest()
    cache_file = os.path.join(HTML_CACHE_DIR, f"{url_hash}.html")
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r', encoding='utf-8', errors='ignore') as f:
                return f.read()
        except Exception as e:
            print(f"Erro ao ler cache HTML {cache_file}: {e}")
    return None

def save_html_to_cache(url, html):
    if not html:
        return
    url_hash = hashlib.md5(url.encode()).hexdigest()
    cache_file = os.path.join(HTML_CACHE_DIR, f"{url_hash}.html")
    try:
        with open(cache_file, 'w', encoding='utf-8', errors='ignore') as f:
            f.write(html)
    except Exception as e:
        print(f"Erro ao salvar cache HTML {cache_file}: {e}")

# ===================== FUNÇÕES DE BUSCA E SCRAPING =====================
def build_queries(nome_empresa, cnpj=None, foco_contato="TI"):
    queries = []
    nome_empresa_q = f'"{nome_empresa}"'
    queries.append(f'{nome_empresa_q} CNPJ Razão Social Endereço')
    queries.append(f'{nome_empresa_q} site oficial contato')
    queries.append(f'{nome_empresa_q} linkedin página empresa')
    queries.append(f'{nome_empresa_q} contato {foco_contato} email telefone')
    if cnpj:
        cnpj_q = f'"{formatar_cnpj(cnpj)}"'
        queries.append(f'{cnpj_q} contato {foco_contato}')
        queries.append(f'{cnpj_q} quadro societário administradores')
    queries.append(f'site:linkedin.com/in {nome_empresa_q} {foco_contato}')
    cargos_foco = CARGOS_RELEVANTES.get(foco_contato, [foco_contato])
    for cargo in cargos_foco[:3]:
        queries.append(f'{nome_empresa_q} "{cargo}" nome email linkedin')
        if cnpj:
             queries.append(f'{cnpj_q} "{cargo}" nome email linkedin')
    queries = list(dict.fromkeys(queries))
    return queries

def search_searx(query, logger):
    cache_key = hashlib.md5(query.encode()).hexdigest()
    cached_result = get_from_cache(SEARX_CACHE, cache_key)
    if cached_result:
        logger.info(f"Cache HIT para SearX query: {query[:50]}...")
        return cached_result
    logger.info(f"Buscando no SearX: {query[:100]}...")
    try:
        response = requests.get(
            SEARX_URL,
            params={'q': query, 'format': 'json', 'engines': 'google,bing,duckduckgo', 'language': 'pt-BR', 'safesearch': '0'},
            headers={'User-Agent': USER_AGENT, 'Accept': 'application/json'},
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        data = response.json()
        urls = []
        for result in data.get('results', [])[:MAX_SEARCH_RESULTS]:
            url = result.get('url', '')
            if url and isinstance(url, str) and url.startswith('http'):
                 domain = extrair_dominio(url)
                 if domain and not any(blacklisted in domain for blacklisted in SITE_BLACKLIST):
                    urls.append(url)
        logger.info(f"SearX encontrou {len(urls)} URLs válidas para: {query[:50]}...")
        save_to_cache(SEARX_CACHE, cache_key, urls)
        return urls
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro de rede ao buscar no SearX ({query[:50]}...): {e}")
    except json.JSONDecodeError as e:
        logger.error(f"Erro ao decodificar JSON do SearX ({query[:50]}...): {e} - Resposta: {response.text[:200]}")
    except Exception as e:
        logger.error(f"Erro inesperado ao buscar no SearX ({query[:50]}...): {e}")
    return []

def make_driver():
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-notifications')
    options.add_argument(f'--user-agent={USER_AGENT}')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    try:
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(SELENIUM_TIMEOUT)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        return driver
    except WebDriverException as e:
        print(f"Erro ao inicializar o WebDriver: {e}. Verifique se o ChromeDriver está instalado e no PATH.")
        return None
    except Exception as e:
        print(f"Erro inesperado ao criar driver: {e}")
        return None

def download_html_selenium(url, logger, driver):
    cached_html = get_html_from_cache(url)
    if cached_html:
        logger.info(f"Cache HIT para HTML: {url}")
        return cached_html
    if not driver:
        logger.error("Driver do Selenium não está disponível para baixar HTML.")
        return None
    logger.info(f"Baixando HTML com Selenium: {url}")
    try:
        driver.get(url)
        time.sleep(3) # Espera básica, pode precisar de ajuste
        html = driver.page_source
        if not html or len(html) < 500:
             logger.warning(f"HTML suspeito (muito pequeno) obtido de {url}")
        if len(html) > 5 * 1024 * 1024:
            logger.warning(f"HTML de {url} muito grande ({len(html)/(1024*1024):.1f}MB), truncando.")
            html = html[:5 * 1024 * 1024]
        save_html_to_cache(url, html)
        url_hash = hashlib.md5(url.encode()).hexdigest()
        debug_file = os.path.join(DEBUG_HTML_DIR, f"{url_hash}.html")
        with open(debug_file, 'w', encoding='utf-8', errors='ignore') as f:
            f.write(html)
        return html
    except WebDriverException as e:
        logger.error(f"Erro do Selenium ao baixar {url}: {e}")
        if "Timeout" in str(e):
            logger.warning(f"Timeout ao carregar {url}")
        elif "net::ERR" in str(e):
             logger.warning(f"Erro de rede ao carregar {url}: {e}")
    except Exception as e:
        logger.error(f"Erro inesperado ao baixar HTML com Selenium ({url}): {e}")
    return None

def extract_text_from_html(html):
    if not html:
        return ""
    try:
        soup = BeautifulSoup(html, 'lxml')
        for script_or_style in soup(["script", "style"]):
            script_or_style.decompose()
        text = soup.get_text(separator='\n', strip=True)
        text = "\n".join(line for line in text.splitlines() if line.strip())
        return text
    except Exception as e:
        print(f"Erro ao extrair texto com BeautifulSoup: {e}")
        return ""

def extract_candidates_from_text(text, logger):
    candidates = defaultdict(list)
    if not text:
        return candidates
    for key, pattern in PATTERNS.items():
        try:
            matches = pattern.findall(text)
            if matches:
                cleaned_matches = set()
                for match in matches:
                    if isinstance(match, tuple):
                        # Pega o primeiro grupo não vazio se houver grupos (útil para linkedin_profile)
                        match_str = next((m for m in match if m), None)
                        if not match_str: continue
                    else:
                        match_str = str(match).strip()

                    if key == 'cnpj':
                        formatted = formatar_cnpj(match_str)
                        if validar_cnpj(formatted):
                            cleaned_matches.add(formatted)
                    elif key == 'email':
                        if re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", match_str):
                             cleaned_matches.add(match_str.lower())
                    elif key == 'telefone' or key == 'celular':
                         formatted = formatar_telefone(match_str)
                         if validar_telefone(formatted):
                             cleaned_matches.add(formatted)
                    elif key == 'cep':
                         cep_limpo = re.sub(r'\D', '', match_str)
                         if len(cep_limpo) == 8:
                             cleaned_matches.add(f"{cep_limpo[:5]}-{cep_limpo[5:]}")
                    elif key == 'linkedin_profile':
                         # A validação mais rigorosa será feita depois
                         if '/in/' in match_str:
                              cleaned_matches.add(match_str)
                if cleaned_matches:
                    candidates[key].extend(list(cleaned_matches))
        except Exception as e:
            logger.error(f"Erro ao aplicar regex para '{key}': {e}")
    for key in candidates:
        candidates[key] = list(set(candidates[key]))
    return candidates

# ===================== FUNÇÕES DE API EXTERNA =====================
def query_brasilapi_cnpj(cnpj, logger):
    cnpj_limpo = re.sub(r'\D', '', str(cnpj))
    if not validar_cnpj(cnpj_limpo):
        logger.warning(f"Tentando consultar CNPJ inválido na BrasilAPI: {cnpj}")
        return None
    cache_key = f"brasilapi_cnpj_{cnpj_limpo}"
    cached_data = get_from_cache(API_CACHE, cache_key)
    if cached_data:
        logger.info(f"Cache HIT para BrasilAPI CNPJ: {cnpj}")
        if cached_data.get('error') == 'not_found':
            return None
        return cached_data
    logger.info(f"Consultando BrasilAPI para CNPJ: {cnpj}")
    url = BRASILAPI_CNPJ_URL.format(cnpj=cnpj_limpo)
    try:
        response = requests.get(url, headers={'User-Agent': USER_AGENT}, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        cep_formatado = None
        if data.get('cep'):
            cep_limpo_api = re.sub(r'\D', '', str(data['cep']))
            if len(cep_limpo_api) == 8:
                cep_formatado = f"{cep_limpo_api[:5]}-{cep_limpo_api[5:]}"
        cleaned_data = {
            'razao_social': data.get('razao_social'),
            'nome_fantasia': data.get('nome_fantasia'),
            'cnpj': formatar_cnpj(data.get('cnpj')),
            'logradouro': data.get('logradouro'),
            'numero': data.get('numero'),
            'complemento': data.get('complemento'),
            'bairro': data.get('bairro'),
            'municipio': data.get('municipio'),
            'uf': data.get('uf'),
            'cep': cep_formatado,
            'ddd_telefone_1': data.get('ddd_telefone_1'),
            'porte': data.get('porte')
        }
        save_to_cache(API_CACHE, cache_key, cleaned_data)
        return cleaned_data
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logger.warning(f"CNPJ {cnpj} não encontrado na BrasilAPI (404).")
            save_to_cache(API_CACHE, cache_key, {'error': 'not_found'})
        else:
            logger.error(f"Erro HTTP ao consultar BrasilAPI CNPJ {cnpj}: {e}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro de rede ao consultar BrasilAPI CNPJ {cnpj}: {e}")
    except Exception as e:
        logger.error(f"Erro inesperado ao consultar BrasilAPI CNPJ {cnpj}: {e}")
    return None

# ===================== FUNÇÕES DE IA (OLLAMA) - PROMPTS REFINADOS =====================
def call_ollama(prompt, logger):
    logger.info(f"Chamando Ollama (Modelo: {OLLAMA_MODEL}). Prompt: {prompt[:150]}... (Total: {len(prompt)} chars)")
    try:
        response = requests.post(
            OLLAMA_URL,
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False, "format": "json"},
            headers={'Content-Type': 'application/json'},
            timeout=OLLAMA_TIMEOUT
        )
        response.raise_for_status()
        response_data = response.json()
        json_response_str = response_data.get('response')
        if not json_response_str:
             logger.error("Ollama retornou uma resposta vazia ou sem o campo 'response'.")
             return None
        try:
            final_json = json.loads(json_response_str)
            logger.info(f"Ollama respondeu com JSON válido: {str(final_json)[:150]}...")
            # Validar se o JSON retornado não está vazio ou só com nulls
            if not final_json or all(v is None for v in final_json.values()):
                logger.info("Ollama retornou JSON vazio ou apenas com valores nulos.")
                return None # Considerar como não encontrado
            return final_json
        except json.JSONDecodeError as e:
            logger.error(f"Erro ao decodificar o JSON interno da resposta do Ollama: {e}")
            logger.error(f"String recebida do Ollama: {json_response_str[:500]}...")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro de rede ao chamar Ollama: {e}")
    except Exception as e:
        logger.error(f"Erro inesperado ao chamar Ollama: {e}")
    return None

def prompt_extrair_dados_empresa_v3(nome_empresa_input, texto_pagina):
    """[V3] Prompt RIGOROSO para extrair dados básicos da empresa, validando associação."""
    max_len = 8000
    texto_limitado = texto_pagina[:max_len]
    if len(texto_pagina) > max_len:
        texto_limitado += "... (texto truncado)"

    prompt = f"""
**Tarefa CRÍTICA:** Analisar o texto fornecido e extrair **APENAS** a Razão Social e o CNPJ que **INEQUIVOCAMENTE** pertencem à empresa '{nome_empresa_input}'. Extrair também o Porte, se mencionado diretamente.

**Empresa Alvo:** {nome_empresa_input}

**Texto para Análise:**
--- TEXTO ---
{texto_limitado}
--- FIM DO TEXTO ---

**Instruções RIGOROSAS:**
1.  **Razão Social:** Identifique a Razão Social. **SOMENTE retorne o valor se o texto indicar CLARAMENTE que essa Razão Social pertence à empresa '{nome_empresa_input}'.** Se houver múltiplas razões sociais ou a associação for duvidosa, retorne `null`.
2.  **CNPJ:** Identifique o CNPJ (formato XX.XXX.XXX/XXXX-XX). **SOMENTE retorne o valor se o texto associar CLARAMENTE este CNPJ à empresa '{nome_empresa_input}' ou à Razão Social validada no passo 1.** Se houver múltiplos CNPJs ou a associação for incerta, retorne `null`.
3.  **Porte:** Identifique menções diretas ao porte ou faixa de funcionários (ex: 'mais de 500 funcionários', 'porte médio'). Retorne a descrição encontrada. Se não houver menção clara, retorne `null`.
4.  **Formato de Saída:** Retorne **ESTRITAMENTE** um JSON com as chaves: "razao_social", "cnpj", "porte_descricao".
5.  **NÃO FAÇA SUPOSIÇÕES!** Se a informação não estiver explicitamente associada à empresa '{nome_empresa_input}' no texto, retorne `null` para a chave correspondente.

**Exemplo de Saída VÁLIDA:**
{{"razao_social": "AD SHOPPING AGENCIA DE DESENVOLVIMENTO DE SHOPPING CENTERS LTDA", "cnpj": "65.040.727/0001-48", "porte_descricao": "501-1000 funcionários"}}

**Exemplo de Saída INVÁLIDA (Associação Duvidosa):**
{{"razao_social": null, "cnpj": null, "porte_descricao": null}}

**Sua Resposta (APENAS JSON):**
"""
    return prompt

def prompt_identificar_contato_v3(nome_empresa_input, foco_contato, texto_pagina):
    """[V3] Prompt RIGOROSO para identificar o contato alvo, validando associação e relevância."""
    max_len = 8000
    texto_limitado = texto_pagina[:max_len]
    if len(texto_pagina) > max_len:
        texto_limitado += "... (texto truncado)"

    cargos_exemplo = CARGOS_RELEVANTES.get(foco_contato, [foco_contato])

    prompt = f"""
**Tarefa CRÍTICA:** Analisar o texto e identificar **UM ÚNICO** contato que:
    a) Trabalhe **ATUALMENTE** na empresa '{nome_empresa_input}'.
    b) Atue na área de **'{foco_contato}'** (cargos comuns: {', '.join(cargos_exemplo)}).
    c) Possua informações de contato **DIRETO e PROFISSIONAL** (E-mail corporativo, Celular direto, LinkedIn específico).

**Empresa Alvo:** {nome_empresa_input}
**Área de Foco:** {foco_contato}

**Texto para Análise:**
--- TEXTO ---
{texto_limitado}
--- FIM DO TEXTO ---

**Instruções RIGOROSAS:**
1.  **Identificação:** Procure por nomes de pessoas mencionados no texto.
2.  **Associação com Empresa:** Verifique se o texto **CONFIRMA** que a pessoa trabalha **NA EMPRESA '{nome_empresa_input}'**. Não considere ex-funcionários ou menções vagas.
3.  **Associação com Área:** Verifique se o texto associa a pessoa à área de **'{foco_contato}'** através do cargo ou descrição de função.
4.  **Extração de Dados (SOMENTE SE OS CRITÉRIOS ACIMA FOREM ATENDIDOS):**
    *   `nome_completo`: Nome completo da pessoa.
    *   `cargo`: Cargo exato mencionado no texto.
    *   `email`: **APENAS** se for um e-mail que pareça corporativo (domínio da empresa ou similar) e direto da pessoa. **NÃO retorne e-mails genéricos (contato@, etc.) ou pessoais (gmail, hotmail, etc.)**. Se não houver e-mail válido, retorne `null`.
    *   `celular`: **APENAS** se for um número de celular (formato (XX) 9XXXX-XXXX) explicitamente associado à pessoa no contexto profissional. Se não houver, retorne `null`.
    *   `linkedin_url`: **APENAS** se for uma URL de perfil do LinkedIn (`linkedin.com/in/...`) claramente associada à pessoa e que **NÃO SEJA GENÉRICA** (ex: `/in/daniel/` é inválido). Se não houver URL válida, retorne `null`.
5.  **Prioridade:** Se encontrar múltiplos contatos válidos, escolha o que tiver informações mais completas e cargo mais relevante para a área.
6.  **Formato de Saída:** Retorne **ESTRITAMENTE** um JSON com as chaves: "nome_completo", "cargo", "email", "celular", "linkedin_url".
7.  **CASO NENHUM CONTATO ATENDA A TODOS OS CRITÉRIOS (Nome + Associação Empresa + Associação Área + Dados Válidos), RETORNE `null` PARA TODAS AS CHAVES.**

**Exemplo de Saída VÁLIDA:**
{{"nome_completo": "Carlos Silva", "cargo": "Coordenador de TI na AD Shopping", "email": "carlos.silva@adshopping.com.br", "celular": null, "linkedin_url": "https://www.linkedin.com/in/carlossilvati"}}

**Exemplo de Saída INVÁLIDA (Associação Duvidosa ou Dados Insuficientes):**
{{"nome_completo": null, "cargo": null, "email": null, "celular": null, "linkedin_url": null}}

**Sua Resposta (APENAS JSON):**
"""
    return prompt

# ===================== LÓGICA PRINCIPAL DE PROCESSAMENTO =====================
def processar_empresa(empresa_info, logger, lock, shared_cache):
    nome_empresa = empresa_info.get('Empresa', '').strip()
    if not nome_empresa:
        logger.warning("Nome da empresa vazio, pulando.")
        return None

    logger.info(f"--- Iniciando processamento para: {nome_empresa} ---")
    start_time = time.time()

    dados_encontrados = defaultdict(lambda: None)
    dados_encontrados['Empresa'] = nome_empresa
    candidatos_agregados = defaultdict(list)
    urls_processadas = set()
    driver = None
    dominio_principal_empresa = None # Para validar emails
    cnpj_confirmado_api = None # CNPJ validado pela BrasilAPI
    razao_social_confirmada_api = None # Razão social da BrasilAPI

    try:
        # 1. Busca Inicial e por CNPJ
        queries_iniciais = build_queries(nome_empresa, foco_contato=FOCO_CONTATO)
        urls_iniciais = set()
        for query in queries_iniciais[:4]:
            urls_iniciais.update(search_searx(query, logger))

        # Tentar extrair CNPJ e domínio das primeiras páginas
        driver = make_driver()
        if not driver:
             logger.error("Falha ao criar driver, scraping limitado.")

        cnpj_encontrado_inicial = None
        for url in list(urls_iniciais)[:5]:
            if url in urls_processadas or not driver:
                continue
            html = download_html_selenium(url, logger, driver)
            urls_processadas.add(url)
            if html:
                # Tentar extrair domínio principal da URL do site oficial
                if not dominio_principal_empresa and nome_empresa.lower().replace(' ','') in url.lower():
                    dominio_principal_empresa = extrair_dominio(url)
                    logger.info(f"Domínio principal inferido: {dominio_principal_empresa} de {url}")

                texto = extract_text_from_html(html)
                cands_pagina = extract_candidates_from_text(texto, logger)
                for key, values in cands_pagina.items():
                    candidatos_agregados[key].extend(values)

                if not cnpj_encontrado_inicial and cands_pagina.get('cnpj'):
                    for cnpj_cand in cands_pagina['cnpj']:
                        if validar_cnpj(cnpj_cand):
                            cnpj_encontrado_inicial = formatar_cnpj(cnpj_cand)
                            logger.info(f"CNPJ preliminar encontrado (regex): {cnpj_encontrado_inicial} em {url}")
                            dados_encontrados['CNPJ'] = cnpj_encontrado_inicial # Temporário
                            break
            if cnpj_encontrado_inicial:
                 break

        # 2. Enriquecer e VALIDAR com BrasilAPI
        cnpj_para_validar = dados_encontrados['CNPJ'] # Usa o CNPJ encontrado por regex
        if cnpj_para_validar:
            dados_cnpj_api = query_brasilapi_cnpj(cnpj_para_validar, logger)
            if dados_cnpj_api:
                logger.info(f"Dados da BrasilAPI obtidos para {cnpj_para_validar}")
                # Validar se a Razão Social/Nome Fantasia da API bate com o nome da empresa buscada
                nome_api = dados_cnpj_api.get('razao_social', '') or dados_cnpj_api.get('nome_fantasia', '')
                if nome_api and normalizar_texto(nome_empresa) in normalizar_texto(nome_api):
                    logger.info(f"CNPJ {cnpj_para_validar} CONFIRMADO pela BrasilAPI para {nome_empresa}")
                    cnpj_confirmado_api = dados_cnpj_api.get('cnpj')
                    razao_social_confirmada_api = dados_cnpj_api.get('razao_social')
                    dados_encontrados['CNPJ'] = cnpj_confirmado_api
                    dados_encontrados['Razão Social'] = razao_social_confirmada_api
                    dados_encontrados['Porte'] = dados_encontrados.get('Porte') or dados_cnpj_api.get('porte') # Porte da API é mais confiável
                    dados_encontrados['Cidade'] = dados_encontrados.get('Cidade') or dados_cnpj_api.get('municipio')
                    dados_encontrados['Estado'] = dados_encontrados.get('Estado') or dados_cnpj_api.get('uf')
                    dados_encontrados['CEP'] = dados_encontrados.get('CEP') or dados_cnpj_api.get('cep')
                else:
                    logger.warning(f"CNPJ {cnpj_para_validar} encontrado, mas Razão Social/Nome Fantasia da API ('{nome_api}') não parece corresponder a '{nome_empresa}'. Descartando CNPJ.")
                    dados_encontrados['CNPJ'] = None # Descarta CNPJ se a validação falhar
            else:
                 logger.warning(f"Não foi possível obter ou validar dados da BrasilAPI para {cnpj_para_validar}")
                 dados_encontrados['CNPJ'] = None # Descarta CNPJ se API falhar

        # 3. Novas buscas com CNPJ (se confirmado) e foco no contato
        queries_contato = build_queries(nome_empresa, cnpj=dados_encontrados['CNPJ'], foco_contato=FOCO_CONTATO)
        urls_contato = set()
        for query in queries_contato:
             if query not in queries_iniciais[:4]:
                 urls_contato.update(search_searx(query, logger))

        # 4. Processar URLs de contato (priorizar LinkedIn)
        urls_para_processar = list(urls_contato.union(urls_iniciais))
        urls_para_processar.sort(key=lambda u: 'linkedin.com/in' in u, reverse=True)

        contato_final_encontrado = False
        for url in urls_para_processar:
            if url in urls_processadas or not driver:
                continue
            if contato_final_encontrado:
                 logger.info("Contato alvo já encontrado e validado, pulando URLs restantes.")
                 break

            html = download_html_selenium(url, logger, driver)
            urls_processadas.add(url)
            if html:
                texto = extract_text_from_html(html)
                if not texto:
                     continue

                # Tentar extrair/validar dados da empresa com Ollama (se ainda não tiver)
                if not dados_encontrados['Razão Social'] or not dados_encontrados['CNPJ']:
                    prompt_empresa = prompt_extrair_dados_empresa_v3(nome_empresa, texto)
                    resultado_ia_empresa = call_ollama(prompt_empresa, logger)
                    if resultado_ia_empresa:
                        # Validar Razão Social da IA
                        rs_ia = resultado_ia_empresa.get('razao_social')
                        if rs_ia and not dados_encontrados['Razão Social']:
                            # Validar se o nome da empresa input está contido na razão social da IA
                            if normalizar_texto(nome_empresa) in normalizar_texto(rs_ia):
                                dados_encontrados['Razão Social'] = rs_ia
                                logger.info(f"IA validou Razão Social: {rs_ia}")
                            else:
                                logger.warning(f"IA retornou Razão Social '{rs_ia}' que não parece corresponder a '{nome_empresa}'. Descartando.")

                        # Validar CNPJ da IA
                        cnpj_ia = resultado_ia_empresa.get('cnpj')
                        if cnpj_ia and not dados_encontrados['CNPJ'] and validar_cnpj(cnpj_ia):
                            # Tentar validar com BrasilAPI
                            dados_cnpj_ia_api = query_brasilapi_cnpj(cnpj_ia, logger)
                            if dados_cnpj_ia_api:
                                nome_api_ia = dados_cnpj_ia_api.get('razao_social', '') or dados_cnpj_ia_api.get('nome_fantasia', '')
                                if nome_api_ia and normalizar_texto(nome_empresa) in normalizar_texto(nome_api_ia):
                                    logger.info(f"IA encontrou CNPJ {cnpj_ia} e foi CONFIRMADO pela BrasilAPI para {nome_empresa}")
                                    dados_encontrados['CNPJ'] = dados_cnpj_ia_api.get('cnpj')
                                    # Atualizar outros dados da API se necessário
                                    if not dados_encontrados['Razão Social']:
                                        dados_encontrados['Razão Social'] = dados_cnpj_ia_api.get('razao_social')
                                    if not dados_encontrados['Porte']:
                                        dados_encontrados['Porte'] = dados_cnpj_ia_api.get('porte')
                                    # ... outros campos da API
                                else:
                                    logger.warning(f"IA encontrou CNPJ {cnpj_ia}, mas Razão Social/Nome Fantasia da API ('{nome_api_ia}') não bate com '{nome_empresa}'. Descartando.")
                            else:
                                logger.warning(f"IA encontrou CNPJ {cnpj_ia}, mas não foi possível validar com BrasilAPI. Descartando.")
                        elif cnpj_ia: # Se CNPJ da IA for inválido ou já tivermos um
                             logger.warning(f"IA retornou CNPJ '{cnpj_ia}' inválido ou já possuíamos um CNPJ. Descartando.")

                        # Porte (menos crítico, aceitar se IA retornar)
                        porte_ia = resultado_ia_empresa.get('porte_descricao')
                        if porte_ia and not dados_encontrados['Porte']:
                             dados_encontrados['Porte'] = porte_ia
                             logger.info(f"IA extraiu Porte: {porte_ia}")

                # Tentar identificar o contato alvo com Ollama (PROMPT V3)
                prompt_contato = prompt_identificar_contato_v3(nome_empresa, FOCO_CONTATO, texto)
                resultado_ia_contato = call_ollama(prompt_contato, logger)

                # === VALIDAÇÃO PÓS-IA RIGOROSA ===
                if resultado_ia_contato and resultado_ia_contato.get('nome_completo'):
                    logger.info(f"IA identificou contato potencial: {resultado_ia_contato['nome_completo']} ({resultado_ia_contato.get('cargo', 'N/A')}) em {url}")

                    nome_completo = resultado_ia_contato.get('nome_completo')
                    cargo = resultado_ia_contato.get('cargo')
                    email = resultado_ia_contato.get('email')
                    celular = resultado_ia_contato.get('celular')
                    linkedin_url = resultado_ia_contato.get('linkedin_url')

                    # Flags de validação
                    contato_valido = True

                    # 1. Nome e Cargo são obrigatórios e não podem ser genéricos
                    if not nome_completo or not cargo or len(nome_completo.split()) < 2:
                        logger.warning("IA retornou contato sem nome completo ou cargo. Descartando.")
                        contato_valido = False

                    # 2. Validar Email (se existir)
                    email_validado = None
                    if email:
                        if validar_email(email, dominio_principal_empresa):
                            email_validado = email
                            logger.info(f"Email validado: {email_validado}")
                        else:
                            logger.warning(f"Email '{email}' retornado pela IA foi descartado (blacklist ou domínio não corresponde a '{dominio_principal_empresa}').")
                            # Não invalidar o contato inteiro só por causa do email, mas não usar o email

                    # 3. Validar Celular (se existir)
                    celular_validado = None
                    if celular:
                        celular_fmt = formatar_telefone(celular)
                        if validar_telefone(celular_fmt) and '9' in celular_fmt[4:7]:
                            celular_validado = celular_fmt
                            logger.info(f"Celular validado: {celular_validado}")
                        else:
                            logger.warning(f"Celular '{celular}' retornado pela IA foi descartado (formato inválido).")

                    # 4. Validar LinkedIn URL (se existir)
                    linkedin_validado = None
                    if linkedin_url:
                        if validar_linkedin_profile(linkedin_url):
                            linkedin_validado = linkedin_url
                            logger.info(f"LinkedIn validado: {linkedin_validado}")
                        else:
                            logger.warning(f"LinkedIn URL '{linkedin_url}' retornado pela IA foi descartado (formato inválido ou genérico).")

                    # Se o contato passou nas validações essenciais (nome, cargo)
                    if contato_valido:
                        partes_nome = nome_completo.split()
                        dados_encontrados['Nome'] = partes_nome[0]
                        dados_encontrados['Sobrenome'] = ' '.join(partes_nome[1:])
                        dados_encontrados['Cargo'] = cargo
                        dados_encontrados['E-mail'] = email_validado # Usa o validado (pode ser None)
                        dados_encontrados['Celular'] = celular_validado # Usa o validado (pode ser None)
                        dados_encontrados['LINKEDIN'] = linkedin_validado # Usa o validado (pode ser None)

                        contato_final_encontrado = True
                        logger.info(f"CONTATO ALVO VALIDADO: {nome_completo} - {cargo}")
                elif resultado_ia_contato: # Se a IA retornou algo, mas sem nome_completo (provavelmente nulls)
                     logger.info("IA não encontrou contato válido nesta página.")

        # 5. Agregação Final e Limpeza (SEM FALLBACKS NÃO VALIDADOS)
        # A lógica de fallback para candidatos_agregados foi removida conforme solicitado.
        # Apenas dados validados pela IA (ou API) são mantidos.

        # Preencher telefone geral da empresa APENAS se nenhum contato com celular foi encontrado
        # E se o telefone geral parecer confiável (ex: da API ou site oficial)
        if not dados_encontrados['Celular'] and not dados_encontrados['Telefone']:
            tel_api = dados_cnpj_api.get('ddd_telefone_1') if dados_cnpj_api else None
            if tel_api:
                tel_fmt = formatar_telefone(tel_api)
                if validar_telefone(tel_fmt):
                    dados_encontrados['Telefone'] = tel_fmt
                    logger.info(f"Telefone geral preenchido com dado da API: {tel_fmt}")
            # Poderia tentar pegar de candidatos_agregados, mas com muito cuidado
            # elif candidatos_agregados.get('telefone'):
            #     # Lógica para escolher o telefone mais confiável dos candidatos
            #     pass

        # Limpar campos finais (remover termos genéricos)
        for key in list(dados_encontrados.keys()): # Usar list() para poder modificar o dict
            if isinstance(dados_encontrados[key], str):
                valor_original = dados_encontrados[key]
                for termo in TEXTOS_REMOVER:
                     # Usar regex para remover termo como palavra inteira, case-insensitive
                     dados_encontrados[key] = re.sub(rf'\b{re.escape(termo)}\b', '', dados_encontrados[key], flags=re.IGNORECASE)
                dados_encontrados[key] = re.sub(r'\s+', ' ', dados_encontrados[key]).strip()
                # Se o campo ficou vazio após a limpeza, remover ou setar para None?
                if not dados_encontrados[key] and valor_original: # Evitar remover campos que já eram None
                    dados_encontrados[key] = None # Setar para None se ficou vazio

    except Exception as e:
        logger.error(f"Erro GERAL no processamento de {nome_empresa}: {e}")
        logger.error(traceback.format_exc())
    finally:
        if driver:
            try:
                driver.quit()
            except Exception as e:
                 logger.warning(f"Erro ao fechar o driver: {e}")
        # Salvar caches em disco
        salvar_json_arquivo(API_CACHE, API_CACHE_FILE)
        salvar_json_arquivo(SEARX_CACHE, SEARX_CACHE_FILE)

    end_time = time.time()
    logger.info(f"--- Processamento de {nome_empresa} concluído em {end_time - start_time:.2f}s ---")

    # Retornar apenas os campos definidos no output, garantindo que todos existam
    output_final = {
        'Empresa': dados_encontrados.get('Empresa'),
        'Razão Social': dados_encontrados.get('Razão Social'),
        'CNPJ': dados_encontrados.get('CNPJ'),
        'Porte': dados_encontrados.get('Porte'),
        'Nome': dados_encontrados.get('Nome'),
        'Sobrenome': dados_encontrados.get('Sobrenome'),
        'Cargo': dados_encontrados.get('Cargo'),
        'Telefone': dados_encontrados.get('Telefone'),
        'Celular': dados_encontrados.get('Celular'),
        'E-mail': dados_encontrados.get('E-mail'),
        'Cidade': dados_encontrados.get('Cidade'),
        'Estado': dados_encontrados.get('Estado'),
        'CEP': dados_encontrados.get('CEP'),
        'LINKEDIN': dados_encontrados.get('LINKEDIN')
    }
    return output_final

# ===================== FUNÇÃO WORKER PARA MULTIPROCESSAMENTO =====================
def worker(chunk, output_queue, lock, shared_cache):
    process_id = os.getpid()
    logger = setup_logger(process_id)
    logger.info(f"Worker iniciado para processar {len(chunk)} empresas.")
    resultados_chunk = []
    for empresa_info in chunk:
        resultado = processar_empresa(empresa_info, logger, lock, shared_cache)
        if resultado:
            # Verificar se o resultado contém dados além do nome da empresa original
            if any(v for k, v in resultado.items() if k != 'Empresa'):
                resultados_chunk.append(resultado)
            else:
                logger.info(f"Nenhum dado adicional encontrado para {resultado['Empresa']}, descartando linha vazia.")
    output_queue.put(resultados_chunk)
    logger.info(f"Worker finalizado.")

# ===================== FUNÇÃO PRINCIPAL =====================
def main(input_file, output_file):
    print(f"Iniciando buscador de empresas v2.1 (Validação Rigorosa)")
    print(f"Foco do Contato: {FOCO_CONTATO}")
    print(f"Usando {NUM_PROCESSES} processos.")

    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            if 'Empresa' not in reader.fieldnames:
                 print(f"Erro: Arquivo de entrada '{input_file}' não contém a coluna 'Empresa'.")
                 sys.exit(1)
            empresas = list(reader)
        print(f"Total de {len(empresas)} empresas para processar.")
    except FileNotFoundError:
        print(f"Erro: Arquivo de entrada '{input_file}' não encontrado.")
        sys.exit(1)
    except Exception as e:
        print(f"Erro ao ler o arquivo de entrada '{input_file}': {e}")
        sys.exit(1)

    if not empresas:
        print("Nenhuma empresa encontrada no arquivo de entrada.")
        sys.exit(0)

    manager = Manager()
    output_queue = manager.Queue()
    lock = manager.Lock()
    shared_cache = manager.dict() # Não usado ativamente para API/SearX, mas mantido

    chunks = [empresas[i:i + CHUNK_SIZE] for i in range(0, len(empresas), CHUNK_SIZE)]
    start_total_time = time.time()

    with Pool(processes=NUM_PROCESSES) as pool:
        pool.starmap(worker, [(chunk, output_queue, lock, shared_cache) for chunk in chunks])

    resultados_finais = []
    while not output_queue.empty():
        resultados_finais.extend(output_queue.get())

    # Salvar caches locais (feito dentro de processar_empresa agora)
    # salvar_json_arquivo(API_CACHE, API_CACHE_FILE)
    # salvar_json_arquivo(SEARX_CACHE, SEARX_CACHE_FILE)

    if resultados_finais:
        fieldnames = ['Empresa', 'Razão Social', 'CNPJ', 'Porte', 'Nome', 'Sobrenome', 'Cargo', 'Telefone', 'Celular', 'E-mail', 'Cidade', 'Estado', 'CEP', 'LINKEDIN']
        try:
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(resultados_finais)
            print(f"Resultados salvos em: {output_file}")
        except Exception as e:
            print(f"Erro ao salvar o arquivo de saída '{output_file}': {e}")
    else:
        print("Nenhum resultado válido foi gerado após validação.")

    end_total_time = time.time()
    print(f"Processamento total concluído em {end_total_time - start_total_time:.2f} segundos.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python buscador_empresas.v2.py <arquivo_entrada.csv> <arquivo_saida.csv>")
        sys.exit(1)

    input_csv = sys.argv[1]
    output_csv = sys.argv[2]

    # Adicionar validação básica dos arquivos de entrada/saída?

    main(input_csv, output_csv)

