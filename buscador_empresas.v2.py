#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
buscador_empresas.v2.py

Versão 2.0 do buscador de empresas com:
- Estratégia de busca e fallbacks aprimorada (CNPJ, LinkedIn)
- Integração com BrasilAPI para dados de CNPJ
- Prompts de IA (Ollama) específicos para dados empresariais e contato alvo
- Foco configurável no tipo de contato (ex: TI, Comercial)
- Extração dos campos: Empresa, Razão Social, CNPJ, Porte, Nome, Sobrenome, Cargo, Telefone, Celular, E-mail, Cidade, Estado, CEP, LINKEDIN
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
# Foco do contato (ex: "TI", "Contabilidade", "RH", "Comercial", "Financeiro")
# Esta variável será usada nos prompts da IA e nas queries de busca
FOCO_CONTATO = "TI"  # <-- MODIFIQUE AQUI O FOCO DO CONTATO DESEJADO

# URLs de Serviços
SEARX_URL       = os.environ.get("SEARX_URL", "http://124.81.6.163:8092/search")
OLLAMA_URL      = os.environ.get("OLLAMA_URL", "http://124.81.6.163:11434/api/generate")
BRASILAPI_CNPJ_URL = "https://brasilapi.com.br/api/cnpj/v1/{cnpj}"
# VIACEP_URL      = "https://viacep.com.br/ws/{uf}/{cidade}/{rua}/json/" # Manter se for usar busca de CEP por endereço
# BRASILAPI_CEP_URL = "https://brasilapi.com.br/api/cep/v2/{cep}" # Manter se for usar busca de endereço por CEP

# Configurações Gerais
USER_AGENT  = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/114.0.0.0 Safari/537.36"
)
MAX_SEARCH_RESULTS = 10 # Reduzir para focar em qualidade
REQUEST_TIMEOUT = 30
SELENIUM_TIMEOUT = 45
OLLAMA_TIMEOUT = 120 # Aumentar timeout para Ollama
OLLAMA_MODEL = "llama3" # Modelo a ser usado no Ollama

# Configurações de Paralelismo
NUM_PROCESSES = max(1, multiprocessing.cpu_count() // 2) # Usar metade dos cores para evitar sobrecarga
CHUNK_SIZE = 5

# Caminhos (ajustar se necessário)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data_empresas') # Diretório específico para dados de empresas
TIPOS_EMPRESA_FILE = os.path.join(DATA_DIR, 'tipos_empresa.txt')
TEXTOS_REMOVER_FILE = os.path.join(DATA_DIR, 'textos_remover.txt')
EMAIL_BLACKLIST_FILE = os.path.join(DATA_DIR, 'email_blacklist.txt')
SITE_BLACKLIST_FILE = os.path.join(DATA_DIR, 'site_blacklist.txt')
CARGOS_RELEVANTES_FILE = os.path.join(DATA_DIR, 'cargos_relevantes.json') # Usar JSON para mapear FOCO_CONTATO -> Cargos
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
    """Carrega uma lista de um arquivo de texto, criando um padrão se não existir."""
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
    """Carrega um JSON de um arquivo, criando um padrão se não existir."""
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
    """Salva dados em um arquivo JSON."""
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
    'telefone': re.compile(r"\b\(?\d{2}\)?\s?\d{4,5}-?\d{4}\b"), # Inclui fixo e móvel inicial
    'celular': re.compile(r"\b\(?\d{2}\)?\s?9\d{4}-?\d{4}\b"), # Específico para celular
    'email': re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
    'cep': re.compile(r"\b\d{5}-?\d{3}\b"),
    'linkedin_profile': re.compile(r"https://[a-z]{2,3}\.linkedin\.com/in/[a-zA-Z0-9_-]+"),
    'linkedin_company': re.compile(r"https://[a-z]{2,3}\.linkedin\.com/company/[a-zA-Z0-9_-]+")
}

# ===================== LOGGING =====================
def setup_logger(process_id):
    logger = logging.getLogger(f"process_{process_id}")
    if logger.hasHandlers(): # Evitar adicionar handlers múltiplos vezes
        return logger
    logger.setLevel(logging.INFO)
    log_file = os.path.join(LOG_DIR, f'buscador_empresas_p{process_id}.log')
    file_handler = logging.FileHandler(log_file, 'a', 'utf-8') # Usar append mode
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
        # Remover acentos e converter para minúsculas
        texto = unicodedata.normalize('NFKD', str(texto)).encode('ASCII', 'ignore').decode('ASCII')
        texto = texto.lower()
        # Remover caracteres especiais exceto @ . -
        texto = re.sub(r'[^\w\s@.-]', ' ', texto)
        # Substituir múltiplos espaços por um único
        texto = re.sub(r'\s+', ' ', texto).strip()
        return texto
    except Exception as e:
        # Fallback simples em caso de erro de normalização
        return str(texto).lower().strip()

def formatar_cnpj(cnpj):
    if not cnpj:
        return ""
    cnpj_limpo = re.sub(r'\D', '', str(cnpj))
    if len(cnpj_limpo) == 14:
        return f"{cnpj_limpo[:2]}.{cnpj_limpo[2:5]}.{cnpj_limpo[5:8]}/{cnpj_limpo[8:12]}-{cnpj_limpo[12:]}"
    return cnpj # Retorna original se não tiver 14 dígitos

def validar_cnpj(cnpj):
    cnpj_limpo = re.sub(r'\D', '', str(cnpj))
    if len(cnpj_limpo) != 14 or len(set(cnpj_limpo)) == 1:
        return False
    # Cálculo do dígito verificador (simplificado, pode não ser 100% preciso mas ajuda)
    try:
        pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        pesos2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        soma1 = sum(int(d) * p for d, p in zip(cnpj_limpo[:12], pesos1))
        dv1 = (11 - (soma1 % 11)) % 10
        soma2 = sum(int(d) * p for d, p in zip(cnpj_limpo[:13], pesos2))
        dv2 = (11 - (soma2 % 11)) % 10
        return int(cnpj_limpo[12]) == dv1 and int(cnpj_limpo[13]) == dv2
    except:
        return False # Em caso de erro no cálculo

def validar_email(email):
    if not email or not isinstance(email, str):
        return False
    email_lower = email.lower()
    if any(domain in email_lower for domain in EMAIL_BLACKLIST):
        return False
    # Regex um pouco mais permissivo mas ainda útil
    if not re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", email):
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
    return telefone # Retorna original se não encaixar nos padrões

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
            # Remove www.
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain.lower()
    except Exception:
        return None

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
    """Constrói queries de busca conforme a estratégia."""
    queries = []
    nome_empresa_q = f'"{nome_empresa}"' # Usar aspas para nome exato

    # 1. Busca Inicial (Nome da Empresa)
    queries.append(f'{nome_empresa_q} CNPJ Razão Social Endereço')
    queries.append(f'{nome_empresa_q} site oficial contato')
    queries.append(f'{nome_empresa_q} linkedin página empresa')
    queries.append(f'{nome_empresa_q} contato {foco_contato} email telefone')

    # 2. Busca Aprofundada com CNPJ (se encontrado)
    if cnpj:
        cnpj_q = f'"{formatar_cnpj(cnpj)}"' # Formatar e usar aspas
        queries.append(f'{cnpj_q} contato {foco_contato}')
        # Buscar pela razão social também pode ser útil aqui, mas precisa dela primeiro
        # queries.append(f'"[Razão Social]" {foco_contato} email telefone linkedin')
        queries.append(f'{cnpj_q} quadro societário administradores')

    # 3. Busca Focada no Contato (LinkedIn e Geral)
    queries.append(f'site:linkedin.com/in {nome_empresa_q} {foco_contato}')
    # Adicionar variações de cargos relevantes
    cargos_foco = CARGOS_RELEVANTES.get(foco_contato, [foco_contato]) # Pega lista de cargos ou usa o foco direto
    for cargo in cargos_foco[:3]: # Limitar para não gerar muitas queries
        queries.append(f'{nome_empresa_q} "{cargo}" nome email linkedin')
        if cnpj:
             queries.append(f'{cnpj_q} "{cargo}" nome email linkedin')

    # Remover duplicatas
    queries = list(dict.fromkeys(queries))
    return queries

def search_searx(query, logger):
    """Busca no SearXNG, usando cache."""
    cache_key = hashlib.md5(query.encode()).hexdigest()
    cached_result = get_from_cache(SEARX_CACHE, cache_key)
    if cached_result:
        logger.info(f"Cache HIT para SearX query: {query[:50]}...")
        return cached_result

    logger.info(f"Buscando no SearX: {query[:100]}...")
    try:
        response = requests.get(
            SEARX_URL,
            params={
                'q': query,
                'format': 'json',
                'engines': 'google,bing,duckduckgo', # Motores podem ser ajustados
                'language': 'pt-BR',
                'safesearch': '0',
            },
            headers={'User-Agent': USER_AGENT, 'Accept': 'application/json'},
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status() # Levanta erro para status >= 400

        data = response.json()
        urls = []
        for result in data.get('results', [])[:MAX_SEARCH_RESULTS]:
            url = result.get('url', '')
            # Filtros adicionais de URL
            if url and isinstance(url, str) and url.startswith('http'):
                 domain = extrair_dominio(url)
                 if domain and not any(blacklisted in domain for blacklisted in SITE_BLACKLIST):
                    urls.append(url)

        logger.info(f"SearX encontrou {len(urls)} URLs válidas para: {query[:50]}...")
        save_to_cache(SEARX_CACHE, cache_key, urls) # Salva no cache
        # Salvar cache em disco periodicamente pode ser útil
        # if len(SEARX_CACHE) % 50 == 0: salvar_json_arquivo(SEARX_CACHE, SEARX_CACHE_FILE)
        return urls

    except requests.exceptions.RequestException as e:
        logger.error(f"Erro de rede ao buscar no SearX ({query[:50]}...): {e}")
    except json.JSONDecodeError as e:
        logger.error(f"Erro ao decodificar JSON do SearX ({query[:50]}...): {e} - Resposta: {response.text[:200]}")
    except Exception as e:
        logger.error(f"Erro inesperado ao buscar no SearX ({query[:50]}...): {e}")
    return []

def make_driver():
    """Cria uma instância do driver do Chrome com otimizações."""
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
    # Argumentos para tentar evitar detecção
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    # Desabilitar imagens pode acelerar, mas quebrar alguns sites
    # prefs = {'profile.managed_default_content_settings.images': 2}
    # options.add_experimental_option('prefs', prefs)

    try:
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(SELENIUM_TIMEOUT)
        # Script para tentar esconder o webdriver
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        return driver
    except WebDriverException as e:
        print(f"Erro ao inicializar o WebDriver: {e}. Verifique se o ChromeDriver está instalado e no PATH.")
        return None
    except Exception as e:
        print(f"Erro inesperado ao criar driver: {e}")
        return None

def download_html_selenium(url, logger, driver):
    """Baixa HTML usando Selenium, com cache e tratamento de erro."""
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
        # Esperar um pouco para JS carregar (ajustar conforme necessário)
        time.sleep(3)
        html = driver.page_source

        # Verifica se o HTML é muito pequeno ou vazio (pode indicar erro/bloqueio)
        if not html or len(html) < 500:
             logger.warning(f"HTML suspeito (muito pequeno) obtido de {url}")
             # Tentar novamente ou retornar None?
             # return None

        # Limitar tamanho para evitar problemas de memória
        if len(html) > 5 * 1024 * 1024: # Limite de 5MB
            logger.warning(f"HTML de {url} muito grande ({len(html)/(1024*1024):.1f}MB), truncando.")
            html = html[:5 * 1024 * 1024]

        save_html_to_cache(url, html)
        # Salvar cópia para debug
        url_hash = hashlib.md5(url.encode()).hexdigest()
        debug_file = os.path.join(DEBUG_HTML_DIR, f"{url_hash}.html")
        with open(debug_file, 'w', encoding='utf-8', errors='ignore') as f:
            f.write(html)

        return html

    except WebDriverException as e:
        logger.error(f"Erro do Selenium ao baixar {url}: {e}")
        # Verificar se é erro de timeout
        if "Timeout" in str(e):
            logger.warning(f"Timeout ao carregar {url}")
        # Verificar se é erro de conexão/resolução
        elif "net::ERR" in str(e):
             logger.warning(f"Erro de rede ao carregar {url}: {e}")
        else:
             # Outro erro do WebDriver
             pass
    except Exception as e:
        logger.error(f"Erro inesperado ao baixar HTML com Selenium ({url}): {e}")

    return None

def extract_text_from_html(html):
    """Extrai texto limpo do HTML usando BeautifulSoup."""
    if not html:
        return ""
    try:
        soup = BeautifulSoup(html, 'lxml') # Usar lxml para performance
        # Remover tags de script e style
        for script_or_style in soup(["script", "style"]):
            script_or_style.decompose()
        # Obter texto e limpar espaços
        text = soup.get_text(separator='\n', strip=True)
        # Remover linhas em branco excessivas
        text = "\n".join(line for line in text.splitlines() if line.strip())
        return text
    except Exception as e:
        print(f"Erro ao extrair texto com BeautifulSoup: {e}")
        # Fallback para regex simples se BS falhar?
        return ""

def extract_candidates_from_text(text, logger):
    """Extrai candidatos (CNPJ, email, telefone, etc.) do texto usando regex."""
    candidates = defaultdict(list)
    if not text:
        return candidates

    # Usar finditer para obter correspondências com contexto (se necessário no futuro)
    for key, pattern in PATTERNS.items():
        try:
            matches = pattern.findall(text)
            if matches:
                # Limpar e deduplicar
                cleaned_matches = set()
                for match in matches:
                    if isinstance(match, tuple):
                        match = next(m for m in match if m) # Pega o primeiro grupo não vazio se houver grupos
                    match_str = str(match).strip()
                    if key == 'cnpj':
                        match_str = formatar_cnpj(match_str)
                        if validar_cnpj(match_str):
                            cleaned_matches.add(match_str)
                    elif key == 'email':
                        if validar_email(match_str):
                             cleaned_matches.add(match_str.lower())
                    elif key == 'telefone' or key == 'celular':
                         formatted = formatar_telefone(match_str)
                         if validar_telefone(formatted):
                             cleaned_matches.add(formatted)
                    elif key == 'cep':
                         cep_limpo = re.sub(r'\D', '', match_str)
                         if len(cep_limpo) == 8:
                             cleaned_matches.add(f"{cep_limpo[:5]}-{cep_limpo[5:]}")
                    elif key.startswith('linkedin'):
                         cleaned_matches.add(match_str)
                    # Adicionar outros campos se necessário

                if cleaned_matches:
                    candidates[key].extend(list(cleaned_matches))
                    # logger.debug(f"Candidatos para '{key}': {list(cleaned_matches)}")

        except Exception as e:
            logger.error(f"Erro ao aplicar regex para '{key}': {e}")

    # Deduplicar listas finais
    for key in candidates:
        candidates[key] = list(set(candidates[key]))

    return candidates

# ===================== FUNÇÕES DE API EXTERNA =====================
def query_brasilapi_cnpj(cnpj, logger):
    """Consulta dados de CNPJ na BrasilAPI, usando cache."""
    cnpj_limpo = re.sub(r'\D', '', str(cnpj))
    if not validar_cnpj(cnpj_limpo):
        logger.warning(f"Tentando consultar CNPJ inválido na BrasilAPI: {cnpj}")
        return None

    cache_key = f"brasilapi_cnpj_{cnpj_limpo}"
    cached_data = get_from_cache(API_CACHE, cache_key)
    if cached_data:
        logger.info(f"Cache HIT para BrasilAPI CNPJ: {cnpj}")
        return cached_data

    logger.info(f"Consultando BrasilAPI para CNPJ: {cnpj}")
    url = BRASILAPI_CNPJ_URL.format(cnpj=cnpj_limpo)
    try:
        response = requests.get(url, headers={'User-Agent': USER_AGENT}, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        # Simplificar/Limpar dados antes de salvar no cache
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
            'cep': formatar_cep(data.get('cep')),
            'ddd_telefone_1': data.get('ddd_telefone_1'),
            'porte': data.get('porte') # Porte pode ser útil
        }
        save_to_cache(API_CACHE, cache_key, cleaned_data)
        # Salvar cache em disco periodicamente
        # if len(API_CACHE) % 20 == 0: salvar_json_arquivo(API_CACHE, API_CACHE_FILE)
        return cleaned_data
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logger.warning(f"CNPJ {cnpj} não encontrado na BrasilAPI (404).")
            # Salvar 'not_found' no cache para evitar re-tentativas?
            save_to_cache(API_CACHE, cache_key, {'error': 'not_found'})
        else:
            logger.error(f"Erro HTTP ao consultar BrasilAPI CNPJ {cnpj}: {e}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro de rede ao consultar BrasilAPI CNPJ {cnpj}: {e}")
    except Exception as e:
        logger.error(f"Erro inesperado ao consultar BrasilAPI CNPJ {cnpj}: {e}")

    return None

# ===================== FUNÇÕES DE IA (OLLAMA) =====================
def call_ollama(prompt, logger):
    """Chama a API do Ollama com o prompt fornecido."""
    logger.info(f"Chamando Ollama (Modelo: {OLLAMA_MODEL}). Prompt: {prompt[:150]}... (Total: {len(prompt)} chars)")
    try:
        response = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False, # Não usar stream para obter resposta completa
                "format": "json" # Solicitar resposta em JSON
            },
            headers={'Content-Type': 'application/json'},
            timeout=OLLAMA_TIMEOUT
        )
        response.raise_for_status()

        # Ollama retorna JSON onde a resposta está dentro de um campo 'response' que é *outro* JSON string
        response_data = response.json()
        json_response_str = response_data.get('response')

        if not json_response_str:
             logger.error("Ollama retornou uma resposta vazia ou sem o campo 'response'.")
             return None

        # Tentar parsear o JSON string dentro do campo 'response'
        try:
            final_json = json.loads(json_response_str)
            logger.info(f"Ollama respondeu com JSON válido: {str(final_json)[:150]}...")
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

def prompt_extrair_dados_empresa(nome_empresa, texto_pagina):
    """Cria o prompt para extrair dados básicos da empresa."""
    # Limitar tamanho do texto para não sobrecarregar Ollama
    max_len = 8000 # Ajustar conforme necessário
    texto_limitado = texto_pagina[:max_len]
    if len(texto_pagina) > max_len:
        texto_limitado += "... (texto truncado)"

    prompt = f"""
Tarefa: Analisar o texto fornecido sobre a empresa '{nome_empresa}' e extrair/validar as seguintes informações: Razão Social, CNPJ e Porte (descrição textual da faixa de funcionários, se encontrada).
Texto para Análise:
--- TEXTO ---
{texto_limitado}
--- FIM DO TEXTO ---
Instruções:
1. Identifique a Razão Social mais provável mencionada no texto.
2. Identifique o CNPJ (formato XX.XXX.XXX/XXXX-XX) mais provável associado à empresa. Valide o formato.
3. Identifique qualquer menção ao porte da empresa ou faixa de funcionários (ex: 'mais de 500 funcionários', '100-200 colaboradores').
4. Retorne os dados encontrados em formato JSON com as chaves: "razao_social", "cnpj", "porte_descricao".
5. Se alguma informação não for encontrada, retorne null para a chave correspondente.
Exemplo de Saída: {{"razao_social": "Exemplo Soluções Tecnológicas Ltda", "cnpj": "12.345.678/0001-99", "porte_descricao": "51-200 funcionários"}}
Sua Resposta (APENAS JSON):
"""
    return prompt

def prompt_identificar_contato(nome_empresa, foco_contato, texto_pagina):
    """Cria o prompt para identificar o contato alvo."""
    max_len = 8000
    texto_limitado = texto_pagina[:max_len]
    if len(texto_pagina) > max_len:
        texto_limitado += "... (texto truncado)"

    cargos_exemplo = CARGOS_RELEVANTES.get(foco_contato, [foco_contato])

    prompt = f"""
Tarefa: Analisar o texto fornecido sobre a empresa '{nome_empresa}' e identificar UM contato relevante que trabalhe na área de '{foco_contato}'. Extrair os detalhes deste contato.
Texto para Análise:
--- TEXTO ---
{texto_limitado}
--- FIM DO TEXTO ---
Instruções:
1. Procure por nomes de pessoas associados a cargos ou descrições relacionadas à área de '{foco_contato}'. Cargos comuns nesta área incluem: {', '.join(cargos_exemplo)}.
2. Verifique se há indicação clara de que a pessoa trabalha na empresa '{nome_empresa}'.
3. Para o contato mais relevante encontrado, extraia: Nome Completo, Cargo, E-mail direto (se disponível e parecer pessoal/profissional direto, NÃO genérico como contato@ ou faleconosco@), Celular direto (formato (XX) 9XXXX-XXXX, se disponível) e URL do perfil LinkedIn (se disponível).
4. Priorize contatos onde a associação com a empresa e a área '{foco_contato}' seja clara e explícita.
5. Retorne os dados encontrados em formato JSON estritamente com as chaves: "nome_completo", "cargo", "email", "celular", "linkedin_url".
6. Se nenhum contato adequado for encontrado que atenda a TODOS os critérios (nome, associação com empresa E área de foco), retorne null para todas as chaves.
Exemplo de Saída (Contato Encontrado): {{"nome_completo": "Maria Souza", "cargo": "Gerente de TI", "email": "maria.souza@exemploempresa.com.br", "celular": "(11) 98765-4321", "linkedin_url": "https://linkedin.com/in/mariasouzati"}}
Exemplo de Saída (Não Encontrado): {{"nome_completo": null, "cargo": null, "email": null, "celular": null, "linkedin_url": null}}
Sua Resposta (APENAS JSON):
"""
    return prompt

# ===================== LÓGICA PRINCIPAL DE PROCESSAMENTO =====================
def processar_empresa(empresa_info, logger, lock, shared_cache):
    """Processa uma única empresa para encontrar dados e contato."""
    nome_empresa = empresa_info.get('Empresa', '').strip()
    if not nome_empresa:
        logger.warning("Nome da empresa vazio, pulando.")
        return None

    logger.info(f"--- Iniciando processamento para: {nome_empresa} ---")
    start_time = time.time()

    # Estrutura para armazenar dados encontrados
    dados_encontrados = defaultdict(lambda: None) # Inicia com None por padrão
    dados_encontrados['Empresa'] = nome_empresa
    candidatos_agregados = defaultdict(list)
    urls_processadas = set()
    driver = None # Inicializar driver como None

    try:
        # 1. Busca Inicial e por CNPJ
        queries_iniciais = build_queries(nome_empresa, foco_contato=FOCO_CONTATO)
        urls_iniciais = set()
        for query in queries_iniciais[:4]: # Limitar queries iniciais
            urls_iniciais.update(search_searx(query, logger))

        # Tentar extrair CNPJ das primeiras páginas
        driver = make_driver()
        if not driver:
             logger.error("Falha ao criar driver, scraping limitado.")
             # Continuar sem Selenium? Ou retornar erro?

        cnpj_encontrado = None
        for url in list(urls_iniciais)[:5]: # Analisar as 5 primeiras URLs
            if url in urls_processadas or not driver:
                continue
            html = download_html_selenium(url, logger, driver)
            urls_processadas.add(url)
            if html:
                texto = extract_text_from_html(html)
                cands_pagina = extract_candidates_from_text(texto, logger)
                for key, values in cands_pagina.items():
                    candidatos_agregados[key].extend(values)
                # Priorizar o primeiro CNPJ válido encontrado
                if not cnpj_encontrado and cands_pagina.get('cnpj'):
                    for cnpj_cand in cands_pagina['cnpj']:
                        if validar_cnpj(cnpj_cand):
                            cnpj_encontrado = formatar_cnpj(cnpj_cand)
                            logger.info(f"CNPJ preliminar encontrado: {cnpj_encontrado} em {url}")
                            dados_encontrados['CNPJ'] = cnpj_encontrado
                            break # Pega o primeiro válido
            if cnpj_encontrado:
                 break # Para de procurar CNPJ se já achou um

        # 2. Enriquecer com BrasilAPI se CNPJ foi encontrado
        if cnpj_encontrado:
            dados_cnpj_api = query_brasilapi_cnpj(cnpj_encontrado, logger)
            if dados_cnpj_api and 'error' not in dados_cnpj_api:
                logger.info(f"Dados da BrasilAPI obtidos para {cnpj_encontrado}")
                dados_encontrados['Razão Social'] = dados_encontrados['Razão Social'] or dados_cnpj_api.get('razao_social')
                dados_encontrados['Porte'] = dados_encontrados['Porte'] or dados_cnpj_api.get('porte')
                dados_encontrados['Cidade'] = dados_encontrados['Cidade'] or dados_cnpj_api.get('municipio')
                dados_encontrados['Estado'] = dados_encontrados['Estado'] or dados_cnpj_api.get('uf')
                dados_encontrados['CEP'] = dados_encontrados['CEP'] or dados_cnpj_api.get('cep')
                # Poderia pegar telefone da API também, mas priorizar o contato alvo
                # dados_encontrados['Telefone'] = dados_encontrados['Telefone'] or dados_cnpj_api.get('ddd_telefone_1')
            else:
                 logger.warning(f"Não foi possível obter dados da BrasilAPI para {cnpj_encontrado}")

        # 3. Novas buscas com CNPJ e foco no contato
        queries_contato = build_queries(nome_empresa, cnpj=cnpj_encontrado, foco_contato=FOCO_CONTATO)
        urls_contato = set()
        # Executar queries restantes (excluindo as já feitas)
        for query in queries_contato:
             if query not in queries_iniciais[:4]: # Evitar re-executar queries iniciais
                 urls_contato.update(search_searx(query, logger))

        # 4. Processar URLs de contato (priorizar LinkedIn)
        urls_para_processar = list(urls_contato.union(urls_iniciais)) # Combinar todas as URLs encontradas
        urls_para_processar.sort(key=lambda u: 'linkedin.com/in' in u, reverse=True) # Priorizar perfis LinkedIn

        contato_final_encontrado = False
        for url in urls_para_processar:
            if url in urls_processadas or not driver:
                continue
            if contato_final_encontrado: # Otimização: parar se já achou o contato alvo
                 logger.info("Contato alvo já encontrado, pulando URLs restantes.")
                 break

            html = download_html_selenium(url, logger, driver)
            urls_processadas.add(url)
            if html:
                texto = extract_text_from_html(html)
                if not texto:
                     continue

                # Extrair candidatos gerais da página
                cands_pagina = extract_candidates_from_text(texto, logger)
                for key, values in cands_pagina.items():
                    candidatos_agregados[key].extend(values)

                # Tentar extrair dados da empresa com Ollama (se ainda não tiver)
                if not dados_encontrados['Razão Social'] or not dados_encontrados['CNPJ']:
                    prompt_empresa = prompt_extrair_dados_empresa(nome_empresa, texto)
                    resultado_ia_empresa = call_ollama(prompt_empresa, logger)
                    if resultado_ia_empresa:
                        if not dados_encontrados['Razão Social'] and resultado_ia_empresa.get('razao_social'):
                            dados_encontrados['Razão Social'] = resultado_ia_empresa['razao_social']
                            logger.info(f"IA extraiu Razão Social: {dados_encontrados['Razão Social']}")
                        if not dados_encontrados['CNPJ'] and resultado_ia_empresa.get('cnpj') and validar_cnpj(resultado_ia_empresa['cnpj']):
                            dados_encontrados['CNPJ'] = formatar_cnpj(resultado_ia_empresa['cnpj'])
                            logger.info(f"IA extraiu CNPJ: {dados_encontrados['CNPJ']}")
                            # Se a IA achou CNPJ, tentar enriquecer com BrasilAPI novamente
                            if not dados_cnpj_api:
                                dados_cnpj_api = query_brasilapi_cnpj(dados_encontrados['CNPJ'], logger)
                                if dados_cnpj_api and 'error' not in dados_cnpj_api:
                                     # Atualizar dados da empresa com base na API
                                     pass # Lógica de merge/atualização
                        if not dados_encontrados['Porte'] and resultado_ia_empresa.get('porte_descricao'):
                             dados_encontrados['Porte'] = resultado_ia_empresa['porte_descricao']
                             logger.info(f"IA extraiu Porte: {dados_encontrados['Porte']}")

                # Tentar identificar o contato alvo com Ollama
                prompt_contato = prompt_identificar_contato(nome_empresa, FOCO_CONTATO, texto)
                resultado_ia_contato = call_ollama(prompt_contato, logger)

                if resultado_ia_contato and resultado_ia_contato.get('nome_completo'):
                    logger.info(f"IA identificou contato potencial: {resultado_ia_contato['nome_completo']} ({resultado_ia_contato.get('cargo', 'N/A')}) em {url}")
                    # Validar se o contato é relevante e preencher dados
                    nome_completo = resultado_ia_contato.get('nome_completo')
                    cargo = resultado_ia_contato.get('cargo')
                    email = resultado_ia_contato.get('email')
                    celular = resultado_ia_contato.get('celular')
                    linkedin_url = resultado_ia_contato.get('linkedin_url')

                    # Validações básicas
                    if nome_completo and cargo:
                        # Separar nome e sobrenome (heurística simples)
                        partes_nome = nome_completo.split()
                        dados_encontrados['Nome'] = partes_nome[0]
                        dados_encontrados['Sobrenome'] = ' '.join(partes_nome[1:]) if len(partes_nome) > 1 else ''
                        dados_encontrados['Cargo'] = cargo
                        if validar_email(email):
                            dados_encontrados['E-mail'] = email
                        if celular:
                             celular_fmt = formatar_telefone(celular)
                             if validar_telefone(celular_fmt) and '9' in celular_fmt[4:7]: # Checar se parece celular
                                 dados_encontrados['Celular'] = celular_fmt
                        if linkedin_url and PATTERNS['linkedin_profile'].match(linkedin_url):
                             dados_encontrados['LINKEDIN'] = linkedin_url

                        # Marcar que o contato alvo foi encontrado para otimizar
                        contato_final_encontrado = True
                        logger.info(f"CONTATO ALVO ENCONTRADO e validado pela IA: {nome_completo} - {cargo}")
                        # break # Descomentar se quiser parar após o primeiro contato da IA

        # 5. Agregação Final e Limpeza (Fora do loop de URLs)
        # Se a IA não achou, tentar pegar dos candidatos agregados (menos confiável)
        if not contato_final_encontrado:
             logger.info("IA não identificou contato alvo. Tentando agregação de candidatos...")
             # Lógica para escolher o melhor candidato de `candidatos_agregados`
             # Ex: Priorizar email com domínio da empresa, celular válido, etc.
             # Esta parte precisa ser bem definida baseada na confiabilidade desejada
             pass

        # Preencher telefone geral da empresa se não achou celular do contato
        if not dados_encontrados['Celular'] and candidatos_agregados.get('telefone'):
            # Escolher o telefone mais frequente ou o primeiro válido?
            for tel in candidatos_agregados['telefone']:
                 tel_fmt = formatar_telefone(tel)
                 if validar_telefone(tel_fmt):
                     dados_encontrados['Telefone'] = tel_fmt
                     logger.info(f"Telefone geral da empresa preenchido: {tel_fmt}")
                     break

        # Limpar campos finais
        for key in dados_encontrados:
            if isinstance(dados_encontrados[key], str):
                # Remover textos genéricos definidos em TEXTOS_REMOVER
                for termo in TEXTOS_REMOVER:
                     if termo.lower() in dados_encontrados[key].lower():
                          dados_encontrados[key] = dados_encontrados[key].replace(termo, '').strip()
                dados_encontrados[key] = re.sub(r'\s+', ' ', dados_encontrados[key]).strip()

    except Exception as e:
        logger.error(f"Erro GERAL no processamento de {nome_empresa}: {e}")
        logger.error(traceback.format_exc())
    finally:
        if driver:
            try:
                driver.quit()
            except Exception as e:
                 logger.warning(f"Erro ao fechar o driver: {e}")
        # Salvar caches em disco ao final do processamento da empresa?
        # Pode ser melhor salvar no final do script principal

    end_time = time.time()
    logger.info(f"--- Processamento de {nome_empresa} concluído em {end_time - start_time:.2f}s ---")

    # Retornar apenas os campos definidos no output
    output_final = {
        'Empresa': dados_encontrados.get('Empresa'),
        'Razão Social': dados_encontrados.get('Razão Social'),
        'CNPJ': dados_encontrados.get('CNPJ'),
        'Porte': dados_encontrados.get('Porte'),
        'Nome': dados_encontrados.get('Nome'),
        'Sobrenome': dados_encontrados.get('Sobrenome'),
        'Cargo': dados_encontrados.get('Cargo'),
        'Telefone': dados_encontrados.get('Telefone'), # Telefone geral
        'Celular': dados_encontrados.get('Celular'),   # Celular do contato
        'E-mail': dados_encontrados.get('E-mail'),     # Email do contato
        'Cidade': dados_encontrados.get('Cidade'),
        'Estado': dados_encontrados.get('Estado'),
        'CEP': dados_encontrados.get('CEP'),
        'LINKEDIN': dados_encontrados.get('LINKEDIN') # LinkedIn do contato
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
            resultados_chunk.append(resultado)
    output_queue.put(resultados_chunk)
    logger.info(f"Worker finalizado.")

# ===================== FUNÇÃO PRINCIPAL =====================
def main(input_file, output_file):
    print(f"Iniciando buscador de empresas v2.0")
    print(f"Foco do Contato: {FOCO_CONTATO}")
    print(f"Usando {NUM_PROCESSES} processos.")

    # Ler arquivo de entrada
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            # Garantir que a coluna 'Empresa' existe
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

    # Configurar multiprocessamento
    manager = Manager()
    output_queue = manager.Queue()
    lock = manager.Lock() # Lock para acesso a recursos compartilhados se necessário
    shared_cache = manager.dict() # Cache compartilhado entre processos (usar com cuidado)
    # Inicializar cache compartilhado (se necessário)
    # shared_cache.update(API_CACHE)
    # shared_cache.update(SEARX_CACHE)

    # Dividir em chunks
    chunks = [empresas[i:i + CHUNK_SIZE] for i in range(0, len(empresas), CHUNK_SIZE)]

    start_total_time = time.time()

    # Processar em paralelo
    with Pool(processes=NUM_PROCESSES) as pool:
        pool.starmap(worker, [(chunk, output_queue, lock, shared_cache) for chunk in chunks])

    # Coletar resultados
    resultados_finais = []
    while not output_queue.empty():
        resultados_finais.extend(output_queue.get())

    # Salvar caches atualizados (se usar cache compartilhado)
    # API_CACHE.update(shared_cache.get('api', {}))
    # SEARX_CACHE.update(shared_cache.get('searx', {}))
    # salvar_json_arquivo(API_CACHE, API_CACHE_FILE)
    # salvar_json_arquivo(SEARX_CACHE, SEARX_CACHE_FILE)
    # Salvar caches locais (se não usar compartilhado)
    salvar_json_arquivo(API_CACHE, API_CACHE_FILE)
    salvar_json_arquivo(SEARX_CACHE, SEARX_CACHE_FILE)

    # Salvar resultados no CSV de saída
    if resultados_finais:
        fieldnames = ['Empresa', 'Razão Social', 'CNPJ', 'Porte', 'Nome', 'Sobrenome', 'Cargo', 'Telefone', 'Celular', 'E-mail', 'Cidade', 'Estado', 'CEP', 'LINKEDIN']
        try:
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore') # Ignora chaves extras
                writer.writeheader()
                writer.writerows(resultados_finais)
            print(f"Resultados salvos em: {output_file}")
        except Exception as e:
            print(f"Erro ao salvar o arquivo de saída '{output_file}': {e}")
    else:
        print("Nenhum resultado foi gerado.")

    end_total_time = time.time()
    print(f"Processamento total concluído em {end_total_time - start_total_time:.2f} segundos.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python buscador_empresas.v2.py <arquivo_entrada.csv> <arquivo_saida.csv>")
        sys.exit(1)

    input_csv = sys.argv[1]
    output_csv = sys.argv[2]

    main(input_csv, output_csv)

