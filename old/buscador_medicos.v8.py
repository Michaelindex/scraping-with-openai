#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
buscador_medicos.v8.py

Versão 8.0 do buscador de médicos com:
- Restauração da lógica de extração de endereços da v6 para melhor qualidade
- Manutenção do sistema de busca de CEP com cascata de fallbacks da v7:
  1. ViaCEP API (método principal)
  2. BrasilAPI (primeiro fallback)
  3. Web Scraping do Google (segundo fallback)
  4. Site dos Correios (terceiro fallback)
  5. CEP geral da cidade (último recurso)
- Sistema de cache para CEPs já encontrados
- Normalização de endereços para melhorar taxa de sucesso
- Processamento paralelo para maior velocidade
- Sistema de treinamento da IA para melhor precisão
- Estrutura de dados otimizada
- Validação inteligente de resultados

Aprimoramentos:
- Prioriza telefones celulares (começando com DDD +9)
- Filtra e-mails inválidos (strings com 'subject=')
- Remove complementos sem sentido (e.g., 'Salarial')
- Especialista de descoberta de cidades via busca na web
- Processamento paralelo para maior velocidade
- Sistema de treinamento da IA para melhor precisão
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
from selenium.common.exceptions import StaleElementReferenceException
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
VIACEP_URL  = "https://viacep.com.br/ws/{uf}/{cidade}/{rua}/json/"
BRASILAPI_URL = "https://brasilapi.com.br/api/cep/v2/{cep}"
CORREIOS_URL = "https://buscacepinter.correios.com.br/app/endereco/index.php"
USER_AGENT  = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/114.0.0.0 Safari/537.36"
)
MAX_RESULTS = 15

# Configurações de paralelismo
NUM_PROCESSES = max(1, multiprocessing.cpu_count() - 1)
CHUNK_SIZE = 10

# Caminhos dos arquivos
DATA_DIR = 'data'
ESPECIALIDADES_FILE = os.path.join(DATA_DIR, 'especialidades.txt')
TEXTOS_REMOVER_FILE = os.path.join(DATA_DIR, 'textos_remover.txt')
EXEMPLOS_FILE = os.path.join(DATA_DIR, 'exemplos_treinamento.txt')
EMAIL_BLACKLIST_FILE = os.path.join(DATA_DIR, 'email_blacklist.txt')
SITE_BLACKLIST_FILE = os.path.join(DATA_DIR, 'site_blacklist.txt')
LOG_DIR = os.path.join(DATA_DIR, 'logmulti')
DEBUG_HTML_DIR = os.path.join(DATA_DIR, 'debug_html_v8') # Atualizado para v8
CACHE_DIR = os.path.join(DATA_DIR, 'cache')
CEP_CACHE_FILE = os.path.join(CACHE_DIR, 'cep_cache.json')

# Criar diretórios necessários
for dir_path in [DATA_DIR, DEBUG_HTML_DIR, LOG_DIR, CACHE_DIR]:
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

# Padrões regex (restaurado da v6 para endereço)
PATTERNS = {
    'address': re.compile(r"(?:Av\.|Rua|Travessa|Estrada)[^,\n]{5,100},?\s*\d{1,5}"),
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
    file_handler = logging.FileHandler(os.path.join(LOG_DIR, f'buscador_medicos_v8_p{process_id}.log'), 'w', 'utf-8') # Atualizado para v8
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
        f.write("Endereço para correspondência\nEndereço para atendimento\nEndereço para consulta")
    TEXTOS_REMOVER = carregar_lista_arquivo(TEXTOS_REMOVER_FILE)

if not ESPECIALIDADES:
    with open(ESPECIALIDADES_FILE, 'w', encoding='utf-8') as f:
        f.write("Clínico Geral\nPediatra\nGinecologista\nCardiologista\nDermatologista")
    ESPECIALIDADES = carregar_lista_arquivo(ESPECIALIDADES_FILE)

if not EMAIL_BLACKLIST:
    with open(EMAIL_BLACKLIST_FILE, 'w', encoding='utf-8') as f:
        f.write("@pixeon.com\n@boaconsulta.com")
    EMAIL_BLACKLIST = carregar_lista_arquivo(EMAIL_BLACKLIST_FILE)

if not SITE_BLACKLIST:
    with open(SITE_BLACKLIST_FILE, 'w', encoding='utf-8') as f:
        f.write("google.com\nbing.com\nyahoo.com")
    SITE_BLACKLIST = carregar_lista_arquivo(SITE_BLACKLIST_FILE)

# Sistema de cache para CEPs
def carregar_cache_cep():
    """Carrega o cache de CEPs do arquivo"""
    try:
        if os.path.exists(CEP_CACHE_FILE):
            with open(CEP_CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}
    except Exception as e:
        print(f"Erro ao carregar cache de CEP: {e}")
        return {}

def salvar_cache_cep(cache):
    """Salva o cache de CEPs no arquivo"""
    try:
        with open(CEP_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Erro ao salvar cache de CEP: {e}")

# Inicializa o cache de CEPs
CEP_CACHE = carregar_cache_cep()

def normalizar_texto(texto):
    """Remove acentos e converte para minúsculas"""
    if not texto:
        return ""
    # Remove acentos
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
    # Converte para minúsculas
    texto = texto.lower()
    # Remove caracteres especiais
    texto = re.sub(r'[^\w\s]', ' ', texto)
    # Substitui múltiplos espaços por um único
    texto = re.sub(r'\s+', ' ', texto).strip()
    return texto

# Função restaurada da v6
def normalizar_endereco(endereco):
    """Normaliza o endereço para busca de CEP"""
    if not endereco:
        return ""
    
    endereco = normalizar_texto(endereco)
    
    # Padroniza abreviações comuns
    abreviacoes = {
        'r ': 'rua ',
        'r. ': 'rua ',
        'av ': 'avenida ',
        'av. ': 'avenida ',
        'trav ': 'travessa ',
        'trav. ': 'travessa ',
        'pç ': 'praca ',
        'pç. ': 'praca ',
        'pc ': 'praca ',
        'pc. ': 'praca ',
        'al ': 'alameda ',
        'al. ': 'alameda ',
        'est ': 'estrada ',
        'est. ': 'estrada '
    }
    
    for abrev, completo in abreviacoes.items():
        if endereco.startswith(abrev):
            endereco = completo + endereco[len(abrev):]
    
    return endereco

# Função restaurada da v6
def normalizar_cidade(cidade):
    """Normaliza o nome da cidade para busca de CEP"""
    if not cidade:
        return ""
    
    cidade = normalizar_texto(cidade)
    
    # Remove prefixos comuns
    prefixos = ['cidade de ', 'municipio de ', 'distrito de ']
    for prefixo in prefixos:
        if cidade.startswith(prefixo):
            cidade = cidade[len(prefixo):]
    
    return cidade

def gerar_chave_cache(rua, cidade, uf):
    """Gera uma chave única para o cache de CEP"""
    # Normaliza os componentes
    rua_norm = normalizar_endereco(rua)
    cidade_norm = normalizar_cidade(cidade)
    uf_norm = uf.upper() if uf else ""
    
    # Gera a chave
    return f"{rua_norm}|{cidade_norm}|{uf_norm}"

def formatar_cep(cep):
    """Formata o CEP para o padrão XXXXX-XXX"""
    if not cep:
        return ""
    
    # Remove caracteres não numéricos
    cep = re.sub(r'\D', '', cep)
    
    # Verifica se tem 8 dígitos
    if len(cep) != 8:
        return ""
    
    # Formata como XXXXX-XXX
    return f"{cep[:5]}-{cep[5:]}"

def make_driver():
    """Cria uma instância do driver do Chrome"""
    options = Options()
    options.add_argument('--headless=new') # Atualizado para novo headless
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-notifications')
    options.add_argument(f'--user-agent={USER_AGENT}')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-features=NetworkService')
    options.add_argument('--disable-features=VizDisplayCompositor')
    options.add_argument('--disable-web-security')
    options.add_argument('--disable-features=IsolateOrigins,site-per-process')
    options.add_argument('--disable-site-isolation-trials')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Desativa imagens e JavaScript para melhorar performance
    prefs = {
        'profile.default_content_setting_values': {
            'images': 2,  # 2 = block
            'javascript': 1,  # 1 = allow
            'notifications': 2,  # 2 = block
            'plugins': 2,  # 2 = block
        }
    }
    options.add_experimental_option('prefs', prefs)
    
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(30)
    
    # Executa JavaScript para ocultar que é automatizado
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

def build_query(medico, logger):
    """Constrói a query de busca para o médico"""
    nome = f"{medico.get('Firstname', '')} {medico.get('LastName', '')}".strip()
    crm = medico.get('CRM', '')
    uf = medico.get('UF', '')
    
    query = f"{nome} CRM {crm} {uf} telefone e-mail endereço"
    return query

def search_searx(query, logger):
    """Busca no SearX"""
    try:
        response = requests.get(
            SEARX_URL,
            params={
                'q': query,
                'format': 'json',
                'engines': 'google,bing,duckduckgo',
                'language': 'pt-BR',
                'time_range': '',
                'safesearch': '0',
                'categories': 'general'
            },
            headers={'User-Agent': USER_AGENT},
            timeout=30
        )
        
        if response.status_code != 200:
            logger.warning(f"SearX retornou status code {response.status_code}")
            return []
        
        data = response.json()
        urls = []
        
        for result in data.get('results', [])[:MAX_RESULTS]:
            url = result.get('url', '')
            if url and not any(blacklisted in url.lower() for blacklisted in SITE_BLACKLIST):
                urls.append(url)
        
        logger.info(f"SearX results: {len(urls)} URLs")
        return urls
    
    except Exception as e:
        logger.error(f"Erro ao buscar no SearX: {e}")
        return []

def search_google(query, driver, logger):
    """Busca no Google"""
    try:
        driver.get(f"https://www.google.com/search?q={urllib.parse.quote(query)}")
        time.sleep(2)
        
        # Extrai o texto da página para análise posterior
        page_text = driver.page_source
        
        # Extrai URLs
        urls = []
        elements = driver.find_elements(By.CSS_SELECTOR, 'a[href^="http"]')
        
        for element in elements:
            try:
                url = element.get_attribute('href')
                if url and not any(blacklisted in url.lower() for blacklisted in SITE_BLACKLIST):
                    # Filtra URLs do Google
                    if not url.startswith('https://www.google.com/') and not url.startswith('https://accounts.google.com/'):
                        urls.append(url)
            except StaleElementReferenceException:
                continue
        
        # Remove duplicatas e limita
        urls = list(dict.fromkeys(urls))[:MAX_RESULTS]
        logger.info(f"Google results: {len(urls)} URLs")
        
        return urls, page_text
    
    except Exception as e:
        logger.error(f"Erro ao buscar no Google: {e}")
        return [], ""

def search_bing(query, driver, logger):
    """Busca no Bing"""
    try:
        driver.get(f"https://www.bing.com/search?q={urllib.parse.quote(query)}")
        time.sleep(2)
        
        # Extrai o texto da página para análise posterior
        page_text = driver.page_source
        
        # Extrai URLs
        urls = []
        elements = driver.find_elements(By.CSS_SELECTOR, 'a[href^="http"]')
        
        for element in elements:
            try:
                url = element.get_attribute('href')
                if url and not any(blacklisted in url.lower() for blacklisted in SITE_BLACKLIST):
                    # Filtra URLs do Bing
                    if not url.startswith('https://www.bing.com/') and not url.startswith('https://login.live.com/'):
                        urls.append(url)
            except StaleElementReferenceException:
                continue
        
        # Remove duplicatas e limita
        urls = list(dict.fromkeys(urls))[:MAX_RESULTS]
        logger.info(f"Bing results: {len(urls)} URLs")
        
        return urls, page_text
    
    except Exception as e:
        logger.error(f"Erro ao buscar no Bing: {e}")
        return [], ""

def download_html(url, logger, driver):
    """Baixa o HTML da URL"""
    try:
        # Ignora URLs de arquivos não-HTML
        if any(ext in url.lower() for ext in ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.csv', '.txt']):
            logger.info(f"Ignorando URL de arquivo não-HTML: {url}")
            return None
        
        # Gera um hash da URL para identificação única
        url_hash = hashlib.md5(url.encode()).hexdigest()
        
        # Tenta carregar a página
        driver.get(url)
        time.sleep(2)
        
        # Limita o tamanho da página para evitar problemas de memória
        html = driver.page_source
        if len(html) > 3 * 1024 * 1024:  # 3MB
            logger.warning(f"Página muito grande ({len(html)/1024/1024:.2f}MB), truncando")
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

# Função restaurada da v6
def limpar_endereco(endereco):
    """Limpa o endereço removendo textos indesejados"""
    for texto in TEXTOS_REMOVER:
        endereco = endereco.replace(texto, '')
    return endereco.strip()

# Função restaurada da v6
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
    
    return True

# Função restaurada da v6
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

# Função restaurada da v6
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
    
    # Verifica se não contém caracteres especiais ou espaços
    if re.search(r'[<>()[\]\\,;:\s"]', email):
        return False
    
    # Verifica se não é uma resposta de IA ou texto explicativo
    if any(termo in email.lower() for termo in ['não posso', 'não é possível', 'ajudar', 'exemplo']):
        return False
    
    return True

# Função restaurada da v6
def validar_cep(cep):
    """Valida se o CEP parece válido"""
    if not cep:
        return False
    
    # Remove caracteres não numéricos
    digits = re.sub(r"\D", "", cep)
    
    # Verifica se tem 8 dígitos
    if len(digits) != 8:
        return False
    
    # Verifica se não é um CEP inválido (00000000)
    if digits == "00000000":
        return False
    
    return True

# Função restaurada da v6
def normalize_phone(raw):
    """Normaliza telefones para formato padrão"""
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 11:
        return f"({digits[:2]}) {digits[2:7]}-{digits[7:]}"
    if len(digits) == 10:
        return f"({digits[:2]}) {digits[2:6]}-{digits[6:]}"
    return raw

# Função restaurada da v6
def extract_candidates(html, url, logger):
    """Extrai candidatos de informações do HTML"""
    if not html:
        return {
            'address': [],
            'phone': [],
            'email': [],
            'complement': [],
            'cep': []
        }
    
    try:
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text(' ')
        
        # Extrai usando regex
        addrs = PATTERNS['address'].findall(text)
        phones= PATTERNS['phone'].findall(text)
        emails= PATTERNS['email'].findall(text)
        comps = PATTERNS['complement'].findall(text)
        ceps  = PATTERNS['cep'].findall(text)
        
        # Extrai links tel: e mailto:
        for a in soup.select("a[href^='tel:']"):
            num = a['href'].split(':',1)[1]
            norm = normalize_phone(num)
            if norm not in phones: phones.append(norm)
        
        for a in soup.select("a[href^='mailto:']"):
            mail = a['href'].split(':',1)[1]
            if 'subject=' in mail: continue
            if mail not in emails: emails.append(mail)
        
        # Limpa e valida os resultados
        addrs = [limpar_endereco(addr) for addr in addrs if validar_endereco(addr)]
        emails = [email for email in emails if validar_email(email)]
        comps = [c for c in comps if len(c.strip())>3 and 'salari' not in c.lower()]
        ceps = [formatar_cep(cep) for cep in ceps if validar_cep(cep)]
        
        # Deduplica
        def dedupe(lst):
            seen, out = set(), []
            for x in lst:
                if x not in seen:
                    seen.add(x); out.append(x)
            return out
        
        cands = {
            'address': dedupe(addrs),
            'phone':   dedupe(phones),
            'email':   dedupe(emails),
            'complement': dedupe(comps),
            'cep':     dedupe(ceps)
        }
        
        for k,v in cands.items():
            logger.info(f"Candidates {k}: {len(v)} items")
        
        return cands
    
    except Exception as e:
        logger.error(f"Erro ao extrair candidatos: {e}")
        return {
            'address': [],
            'phone': [],
            'email': [],
            'complement': [],
            'cep': []
        }

# Função restaurada da v6
def aggregate_and_rank(all_c, logger):
    """Agrega e ranqueia os candidatos"""
    ranked = {}
    for k,lst in all_c.items():
        ranked[k] = [item for item,_ in Counter(lst).most_common()]
        logger.info(f"Ranked {k}: {len(ranked[k])} items")
    return ranked

# Função restaurada da v6
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

# Funções de busca de CEP (mantidas da v7)
def buscar_dados_via_viacep(rua, cidade, uf, logger):
    """Busca dados de endereço via ViaCEP API"""
    if not rua or not cidade or not uf:
        logger.warning("Dados insuficientes para busca no ViaCEP")
        return None
    
    # Verifica no cache primeiro
    chave_cache = gerar_chave_cache(rua, cidade, uf)
    if chave_cache in CEP_CACHE:
        logger.info(f"Dados encontrados no cache: {CEP_CACHE[chave_cache]}")
        return CEP_CACHE[chave_cache]
    
    try:
        # Normaliza os parâmetros
        rua_norm = normalizar_endereco(rua)
        cidade_norm = normalizar_cidade(cidade)
        uf_norm = uf.upper()
        
        # Codifica os parâmetros para URL
        rua_encoded = urllib.parse.quote(rua_norm)
        cidade_encoded = urllib.parse.quote(cidade_norm)
        
        # Constrói a URL
        url = VIACEP_URL.format(uf=uf_norm, cidade=cidade_encoded, rua=rua_encoded)
        logger.info(f"Buscando no ViaCEP: {url}")
        
        # Faz a requisição
        response = requests.get(url, timeout=10)
        
        # Verifica se a resposta foi bem-sucedida
        if response.status_code == 200:
            data = response.json()
            
            # Verifica se é uma lista ou um objeto único
            if isinstance(data, list) and data:
                # Pega o primeiro resultado
                result = data[0]
                logger.info(f"Dados encontrados no ViaCEP: {result}")
                
                # Salva no cache
                CEP_CACHE[chave_cache] = result
                salvar_cache_cep(CEP_CACHE)
                
                return result
            elif isinstance(data, dict) and not data.get('erro'):
                logger.info(f"Dados encontrados no ViaCEP: {data}")
                
                # Salva no cache
                CEP_CACHE[chave_cache] = data
                salvar_cache_cep(CEP_CACHE)
                
                return data
        
        logger.warning(f"ViaCEP retornou status {response.status_code} ou nenhum resultado")
        return None
    
    except Exception as e:
        logger.error(f"Erro ao buscar no ViaCEP: {e}")
        return None

def buscar_cep_via_brasilapi(rua, cidade, uf, logger):
    """Busca CEP via BrasilAPI"""
    if not rua or not cidade or not uf:
        logger.warning("Dados insuficientes para busca na BrasilAPI")
        return None
    
    # Verifica no cache primeiro
    chave_cache = gerar_chave_cache(rua, cidade, uf)
    if chave_cache in CEP_CACHE:
        logger.info(f"Dados encontrados no cache: {CEP_CACHE[chave_cache]}")
        return CEP_CACHE[chave_cache]
    
    try:
        # Constrói a query de busca
        query = f"{rua}, {cidade}, {uf}"
        logger.info(f"Buscando na BrasilAPI: {query}")
        
        # Faz a busca no Google primeiro para encontrar o CEP
        response = requests.get(
            "https://www.google.com/search",
            params={"q": f"CEP {query}"},
            headers={"User-Agent": USER_AGENT},
            timeout=10
        )
        
        # Extrai CEPs do resultado
        ceps = PATTERNS['cep'].findall(response.text)
        
        if not ceps:
            logger.warning("Nenhum CEP encontrado na busca do Google")
            return None
        
        # Pega o primeiro CEP encontrado
        cep = formatar_cep(ceps[0])
        if not cep:
            logger.warning("CEP encontrado é inválido")
            return None
        
        # Consulta a BrasilAPI com o CEP encontrado
        cep_limpo = cep.replace("-", "")
        url = BRASILAPI_URL.format(cep=cep_limpo)
        logger.info(f"Consultando BrasilAPI: {url}")
        
        api_response = requests.get(url, timeout=10)
        
        if api_response.status_code == 200:
            data = api_response.json()
            logger.info(f"Dados encontrados na BrasilAPI: {data}")
            
            # Mapeia os campos para o formato do ViaCEP
            result = {
                "cep": data.get("cep", ""),
                "logradouro": data.get("street", ""),
                "bairro": data.get("neighborhood", ""),
                "localidade": data.get("city", ""),
                "uf": data.get("state", ""),
                "complemento": ""
            }
            
            # Salva no cache
            CEP_CACHE[chave_cache] = result
            salvar_cache_cep(CEP_CACHE)
            
            return result
        
        logger.warning(f"BrasilAPI retornou status {api_response.status_code}")
        return None
    
    except Exception as e:
        logger.error(f"Erro ao buscar na BrasilAPI: {e}")
        return None

def buscar_cep_por_endereco(rua, cidade, driver, logger):
    """Busca CEP baseado na rua e cidade já encontradas"""
    if not rua or not cidade:
        logger.warning("Rua ou cidade não disponíveis para busca de CEP")
        return ""
    
    # Formata a query de busca
    query = f"CEP da {rua}, {cidade}"
    logger.info(f"Buscando CEP: {query}")
    
    try:
        # Realiza a busca no Google
        driver.get(f"https://www.google.com/search?q={urllib.parse.quote(query)}")
        time.sleep(1)
        
        # Extrai o texto da página de resultados
        page_text = driver.page_source
        soup = BeautifulSoup(page_text, 'html.parser')
        text = soup.get_text(' ')
        
        # Procura por padrões de CEP no texto
        ceps = re.findall(r'\d{5}-\d{3}|\d{8}', text)
        
        if ceps:
            cep = formatar_cep(ceps[0])
            logger.info(f"CEP encontrado: {cep}")
            return cep
        
        # Tenta uma segunda busca com formato alternativo
        query = f"{rua}, {cidade} CEP"
        logger.info(f"Segunda tentativa: {query}")
        driver.get(f"https://www.google.com/search?q={urllib.parse.quote(query)}")
        time.sleep(1)
        
        page_text = driver.page_source
        soup = BeautifulSoup(page_text, 'html.parser')
        text = soup.get_text(' ')
        ceps = re.findall(r'\d{5}-\d{3}|\d{8}', text)
        
        if ceps:
            cep = formatar_cep(ceps[0])
            logger.info(f"CEP encontrado na segunda tentativa: {cep}")
            return cep
        
        logger.warning("CEP não encontrado")
        return ""
    
    except Exception as e:
        logger.error(f"Erro ao buscar CEP por endereço: {e}")
        return ""

def buscar_cep_via_correios(rua, cidade, uf, driver, logger):
    """Busca CEP no site dos Correios"""
    if not rua or not cidade:
        logger.warning("Dados insuficientes para busca nos Correios")
        return None
    
    # Verifica no cache primeiro
    chave_cache = gerar_chave_cache(rua, cidade, uf)
    if chave_cache in CEP_CACHE:
        logger.info(f"Dados encontrados no cache: {CEP_CACHE[chave_cache]}")
        return CEP_CACHE[chave_cache]
    
    try:
        # Acessa o site dos Correios
        logger.info(f"Buscando nos Correios: {rua}, {cidade}, {uf}")
        driver.get(CORREIOS_URL)
        time.sleep(2)
        
        # Preenche o formulário
        endereco_input = driver.find_element(By.ID, "endereco")
        endereco_input.clear()
        endereco_input.send_keys(f"{rua}, {cidade}, {uf}")
        
        # Clica no botão de busca
        buscar_button = driver.find_element(By.ID, "btn_pesquisar")
        buscar_button.click()
        time.sleep(3)
        
        # Extrai os resultados
        page_text = driver.page_source
        soup = BeautifulSoup(page_text, 'html.parser')
        
        # Procura pela tabela de resultados
        tabela = soup.find('table', {'class': 'tmptabela'})
        if not tabela:
            logger.warning("Tabela de resultados não encontrada")
            return None
        
        # Extrai os dados da primeira linha
        linhas = tabela.find_all('tr')
        if len(linhas) <= 1:  # Cabeçalho + pelo menos uma linha de dados
            logger.warning("Nenhum resultado encontrado na tabela")
            return None
        
        # Pega a primeira linha de dados (após o cabeçalho)
        colunas = linhas[1].find_all('td')
        if len(colunas) < 4:
            logger.warning("Formato da tabela não reconhecido")
            return None
        
        # Extrai os dados
        logradouro = colunas[0].text.strip()
        bairro = colunas[1].text.strip()
        localidade_uf = colunas[2].text.strip()
        cep = colunas[3].text.strip()
        
        # Separa localidade e UF
        localidade_uf_parts = localidade_uf.split('/')
        localidade = localidade_uf_parts[0].strip() if localidade_uf_parts else ""
        uf_encontrado = localidade_uf_parts[1].strip() if len(localidade_uf_parts) > 1 else uf
        
        # Cria o resultado no formato do ViaCEP
        result = {
            "cep": formatar_cep(cep),
            "logradouro": logradouro,
            "bairro": bairro,
            "localidade": localidade,
            "uf": uf_encontrado,
            "complemento": ""
        }
        
        logger.info(f"Dados encontrados nos Correios: {result}")
        
        # Salva no cache
        CEP_CACHE[chave_cache] = result
        salvar_cache_cep(CEP_CACHE)
        
        return result
    
    except Exception as e:
        logger.error(f"Erro ao buscar nos Correios: {e}")
        return None

def obter_cep_geral_cidade(cidade, uf, logger):
    """Obtém o CEP geral da cidade como último recurso"""
    if not cidade or not uf:
        logger.warning("Cidade ou UF não disponíveis para busca de CEP geral")
        return None
    
    # Verifica no cache primeiro
    chave_cache = gerar_chave_cache("", cidade, uf)
    if chave_cache in CEP_CACHE:
        logger.info(f"CEP geral encontrado no cache: {CEP_CACHE[chave_cache]}")
        return CEP_CACHE[chave_cache]
    
    # Formata a query de busca
    query = f"CEP geral de {cidade} {uf}"
    logger.info(f"Buscando CEP geral da cidade: {query}")
    
    try:
        # Faz a busca no Google
        response = requests.get(
            "https://www.google.com/search",
            params={"q": query},
            headers={"User-Agent": USER_AGENT},
            timeout=10
        )
        
        # Extrai CEPs do resultado
        ceps = PATTERNS['cep'].findall(response.text)
        
        if ceps:
            cep = formatar_cep(ceps[0])
            logger.info(f"CEP geral encontrado: {cep}")
            
            # Cria um resultado simplificado
            result = {
                "cep": cep,
                "logradouro": "",
                "bairro": "",
                "localidade": cidade,
                "uf": uf,
                "complemento": ""
            }
            
            # Salva no cache
            CEP_CACHE[chave_cache] = result
            salvar_cache_cep(CEP_CACHE)
            
            return result
        
        logger.warning("CEP geral não encontrado")
        return None
    except Exception as e:
        logger.error(f"Erro ao buscar CEP geral: {e}")
        return None

def buscar_cep_com_cascata(rua, cidade, uf, driver, logger):
    """Busca CEP usando sistema de cascata de fallbacks"""
    if not rua or not cidade:
        logger.warning("Dados insuficientes para busca de CEP")
        return None
    
    logger.info(f"Iniciando busca de CEP em cascata para: {rua}, {cidade}, {uf}")
    
    # 1. Tenta ViaCEP (método principal)
    logger.info("Método 1: ViaCEP API")
    viacep_data = buscar_dados_via_viacep(rua, cidade, uf, logger)
    if viacep_data and viacep_data.get('cep'):
        logger.info(f"CEP encontrado via ViaCEP: {viacep_data['cep']}")
        return viacep_data
    
    # 2. Tenta BrasilAPI (primeiro fallback)
    logger.info("Método 2: BrasilAPI")
    brasilapi_data = buscar_cep_via_brasilapi(rua, cidade, uf, logger)
    if brasilapi_data and brasilapi_data.get('cep'):
        logger.info(f"CEP encontrado via BrasilAPI: {brasilapi_data['cep']}")
        return brasilapi_data
    
    # 3. Tenta Web Scraping do Google (segundo fallback)
    logger.info("Método 3: Web Scraping do Google")
    google_cep = buscar_cep_por_endereco(rua, cidade, driver, logger)
    if google_cep:
        logger.info(f"CEP encontrado via Google: {google_cep}")
        return {
            "cep": google_cep,
            "logradouro": rua,
            "bairro": "",
            "localidade": cidade,
            "uf": uf,
            "complemento": ""
        }
    
    # 4. Tenta Site dos Correios (terceiro fallback)
    logger.info("Método 4: Site dos Correios")
    correios_data = buscar_cep_via_correios(rua, cidade, uf, driver, logger)
    if correios_data and correios_data.get('cep'):
        logger.info(f"CEP encontrado via Correios: {correios_data['cep']}")
        return correios_data
    
    # 5. Tenta CEP geral da cidade (último recurso)
    logger.info("Método 5: CEP geral da cidade")
    cep_geral = obter_cep_geral_cidade(cidade, uf, logger)
    if cep_geral and cep_geral.get('cep'):
        logger.info(f"CEP geral encontrado: {cep_geral['cep']}")
        return cep_geral
    
    logger.warning("Nenhum CEP encontrado após tentar todos os métodos")
    return None

def log_memory_usage(logger, prefix=""):
    """Registra o uso de memória atual"""
    process = psutil.Process()
    memory_info = process.memory_info()
    logger.info(f"{prefix} Uso de memória: {memory_info.rss / 1024 / 1024:.2f} MB")

def log_execution_time(logger, start_time, operation_name):
    """Registra o tempo de execução de uma operação"""
    elapsed = time.time() - start_time
    logger.info(f"Tempo de execução de {operation_name}: {elapsed:.2f} segundos")

def process_medico(m, driver, logger):
    """Processa um médico"""
    start_time = time.time()
    log_memory_usage(logger, "Início do processamento")
    
    try:
        # Constrói e executa a busca
        query = build_query(m, logger)
        logger.info(f"Iniciando busca para: {query}")
        
        urls_searx = search_searx(query, logger)
        log_memory_usage(logger, "Após SearX")
        
        urls_google, google_text = search_google(query, driver, logger)
        log_memory_usage(logger, "Após Google")
        
        urls_bing, bing_text = search_bing(query, driver, logger)
        log_memory_usage(logger, "Após Bing")
        
        # Combina URLs únicas
        all_urls = list(set(urls_searx + urls_google + urls_bing))
        logger.info(f"Total de URLs únicas: {len(all_urls)}")
        
        # Coleta candidatos
        all_candidates_raw = []
        for i, url in enumerate(all_urls):
            logger.info(f"Processando URL {i+1}/{len(all_urls)}: {url}")
            html = download_html(url, logger, driver)
            if html:
                candidates = extract_candidates(html, url, logger) # Usando extract_candidates da v6
                all_candidates_raw.append(candidates)
            log_memory_usage(logger, f"Após processar URL {i+1}")
        
        # Agrega e ranqueia (usando aggregate_and_rank da v6)
        if all_candidates_raw:
            logger.info("Iniciando agregação e ranqueamento")
            aggregated = {}
            for k in ['address', 'phone', 'email', 'complement', 'cep']:
                aggregated[k] = []
                for c in all_candidates_raw:
                    aggregated[k].extend(c.get(k, []))
            
            ranked = aggregate_and_rank(aggregated, logger)
            log_memory_usage(logger, "Após agregação")
            
            # Cria resultado mantendo todas as colunas originais
            result = m.copy()
            
            # Mapeia os campos encontrados para os campos corretos (usando validação da v6)
            if ranked.get('address'):
                logger.info(f"Validando endereço: {ranked['address'][:3]}")
                endereco_completo = ranked['address'][0] # Pega o primeiro candidato ranqueado
                if validar_endereco(endereco_completo): # Usa validar_endereco da v6
                    logger.info(f"Endereço validado: {endereco_completo}")
                    result['Endereco Completo A1'] = endereco_completo
                    
                    # Extrai o número do endereço (usando extrair_numero_endereco da v6)
                    endereco_sem_numero, numero = extrair_numero_endereco(endereco_completo)
                    result['Address A1'] = endereco_sem_numero
                    result['Numero A1'] = numero
            
            if ranked.get('phone'):
                logger.info(f"Validando telefone: {ranked['phone'][:3]}")
                for phone in ranked['phone'][:3]:
                    if validar_telefone(phone): # Usa validar_telefone da v6
                        result['Phone A1'] = phone
                        logger.info(f"Telefone validado: {result['Phone A1']}")
                        break
            
            if ranked.get('email'):
                logger.info(f"Validando email: {ranked['email'][:3]}")
                for email in ranked['email'][:3]:
                    if validar_email(email): # Usa validar_email da v6
                        result['E-mail A1'] = email
                        logger.info(f"Email validado: {result['E-mail A1']}")
                        break
            
            if ranked.get('complement'):
                logger.info(f"Validando complemento: {ranked['complement'][:3]}")
                result['Complement A1'] = ranked['complement'][0] # Pega o primeiro
                logger.info(f"Complemento validado: {result['Complement A1']}")
            
            # Descobre a cidade (mantém lógica da v7)
            if result.get('Address A1') and not result.get('City A1'):
                logger.info("Descobrindo cidade")
                result['City A1'] = descobrir_cidade(
                    result['Address A1'],
                    m['UF'],
                    driver,
                    logger
                )
                logger.info(f"Cidade descoberta: {result['City A1']}")
            
            # Busca CEP e dados de endereço usando sistema de cascata (mantém lógica da v7)
            if result.get('Address A1') and result.get('City A1'):
                logger.info("Buscando CEP com sistema de cascata")
                cep_data = buscar_cep_com_cascata(
                    result['Address A1'],
                    result['City A1'],
                    m['UF'],
                    driver,
                    logger
                )
                
                if cep_data:
                    logger.info(f"Dados de CEP encontrados: {cep_data}")
                    # Preenche os campos com os dados encontrados
                    if 'cep' in cep_data and cep_data['cep']:
                        result['postal code A1'] = cep_data['cep']
                    
                    if 'bairro' in cep_data and cep_data['bairro'] and not result.get('Bairro A1'):
                        result['Bairro A1'] = cep_data['bairro']
                    
                    if 'complemento' in cep_data and cep_data['complemento'] and not result.get('Complement A1'):
                        result['Complement A1'] = cep_data['complemento']
                    
                    if 'localidade' in cep_data and cep_data['localidade']:
                        result['City A1'] = cep_data['localidade']
                    
                    if 'uf' in cep_data and cep_data['uf']:
                        result['State A1'] = cep_data['uf']
            
            log_execution_time(logger, start_time, "processamento completo do médico")
            return result
        else:
            logger.warning("Nenhum candidato encontrado para o médico")
            return m.copy()  # Retorna o médico original sem alterações
        
    except Exception as e:
        logger.error(f"Erro ao processar médico {m.get('Firstname', '')} {m.get('LastName', '')}: {e}")
        logger.error(traceback.format_exc())
        log_memory_usage(logger, "Após erro")
        return m.copy()  # Retorna o médico original sem alterações

def process_batch(batch_id, medicos_batch, output_file, fieldnames, progress_dict, lock):
    """Processa um lote de médicos"""
    logger = setup_logger(batch_id)
    driver = None
    batch_start_time = time.time()
    
    try:
        results = []
        for i, medico in enumerate(medicos_batch):
            try:
                medico_start_time = time.time()
                logger.info(f"\n{'='*50}\nProcessando médico {i+1}/{len(medicos_batch)}")
                logger.info(f"Nome: {medico.get('Firstname', '')} {medico.get('LastName', '')}")
                
                # Reinicia o driver a cada 10 médicos para evitar vazamento de memória
                if i % 10 == 0 or driver is None:
                    if driver:
                        try:
                            driver.quit()
                        except:
                            pass
                    logger.info("Reiniciando driver do Chrome")
                    driver = make_driver()
                    log_memory_usage(logger, "Após reiniciar driver")
                
                result = process_medico(medico, driver, logger)
                if result:
                    results.append(result)
                    logger.info(f"Resultado processado: {result}")
                
                # Salva resultados a cada 5 médicos para evitar perda de dados
                if len(results) >= 5:
                    logger.info("Salvando lote de resultados")
                    with lock:
                        with open(output_file, 'a', newline='', encoding='utf-8') as f:
                            writer = csv.DictWriter(f, fieldnames=fieldnames)
                            writer.writerows(results)
                    results = []
                    log_memory_usage(logger, "Após salvar resultados")
                
                # Atualiza progresso
                with lock:
                    progress_dict['processed'] += 1
                    progress = (progress_dict['processed'] / progress_dict['total']) * 100
                    logger.info(f"Progresso: {progress:.1f}%")
                
                # Força coleta de lixo
                if i % 5 == 0:
                    logger.info("Executando coleta de lixo")
                    import gc
                    gc.collect()
                    log_memory_usage(logger, "Após coleta de lixo")
                
                log_execution_time(logger, medico_start_time, f"processamento do médico {i+1}")
                
            except Exception as e:
                logger.error(f"Erro ao processar médico {medico.get('Firstname', '')} {medico.get('LastName', '')}: {e}")
                logger.error(traceback.format_exc())
                continue
        
        # Salva resultados finais do lote
        if results:
            logger.info("Salvando resultados finais do lote")
            with lock:
                with open(output_file, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writerows(results)
        
        log_execution_time(logger, batch_start_time, "processamento do lote completo")
        
    except Exception as e:
        logger.error(f"Erro no lote {batch_id}: {e}")
        logger.error(traceback.format_exc())
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass

def run_parallel(inp, outp, num_processes=None):
    """Executa o processamento em paralelo"""
    if num_processes is None:
        num_processes = NUM_PROCESSES
    
    # Lê os médicos
    with open(inp, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        medicos = list(reader)
    
    # Define a estrutura de saída desejada
    fieldnames = [
        'Hash', 'CRM', 'UF', 'Firstname', 'LastName', 'Medical specialty',
        'Endereco Completo A1', 'Address A1', 'Numero A1', 'Complement A1',
        'Bairro A1', 'postal code A1', 'City A1', 'State A1',
        'Phone A1', 'Phone A2', 'Cell phone A1', 'Cell phone A2',
        'E-mail A1', 'E-mail A2', 'OPT-IN', 'STATUS', 'LOTE'
    ]
    
    # Prepara o arquivo de saída
    with open(outp, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
    
    # Divide em lotes menores para melhor gerenciamento de memória
    batch_size = max(1, min(5, math.ceil(len(medicos) / num_processes)))
    batches = [medicos[i:i + batch_size] for i in range(0, len(medicos), batch_size)]
    
    # Configura o progresso
    manager = Manager()
    progress_dict = manager.dict()
    progress_dict['processed'] = 0
    progress_dict['total'] = len(medicos)
    lock = manager.Lock()
    
    # Processa em paralelo com timeout
    with Pool(num_processes) as pool:
        args = [(i, batch, outp, fieldnames, progress_dict, lock) for i, batch in enumerate(batches)]
        try:
            pool.starmap(process_batch, args, chunksize=1)
        except Exception as e:
            print(f"Erro no processamento paralelo: {e}")
            traceback.print_exc()
            pool.terminate()
            pool.join()
            raise

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Uso: python buscador_medicos.v8.py input.csv output.csv") # Atualizado para v8
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    run_parallel(input_file, output_file)
