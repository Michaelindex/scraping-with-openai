#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
buscador_medicos.v6.py

Versão 6.0 do buscador de médicos com:
- Processamento paralelo para maior velocidade
- Sistema de treinamento da IA para melhor precisão
- Estrutura de dados otimizada
- Validação inteligente de resultados
- Busca de CEP com sistema de cascata de fallbacks:
  1. ViaCEP API (método principal)
  2. BrasilAPI (primeiro fallback)
  3. Web Scraping do Google (segundo fallback)
  4. Site dos Correios (terceiro fallback)
  5. CEP geral da cidade (último recurso)
- Sistema de cache para CEPs já encontrados
- Normalização de endereços para melhorar taxa de sucesso

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
DEBUG_HTML_DIR = os.path.join(DATA_DIR, 'debug_html_v6')
CACHE_DIR = os.path.join(DATA_DIR, 'cache')
CEP_CACHE_FILE = os.path.join(CACHE_DIR, 'cep_cache.json')

# Criar diretórios necessários
for dir_path in [DATA_DIR, DEBUG_HTML_DIR, LOG_DIR, CACHE_DIR]:
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

# Padrões regex
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
    file_handler = logging.FileHandler(os.path.join(LOG_DIR, f'buscador_medicos_v6_p{process_id}.log'), 'w', 'utf-8')
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

def normalizar_endereco(endereco):
    """Normaliza o endereço para busca de CEP"""
    if not endereco:
        return ""
    
    # Normaliza o texto (remove acentos, converte para minúsculas)
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

def normalizar_cidade(cidade):
    """Normaliza o nome da cidade para busca de CEP"""
    if not cidade:
        return ""
    
    # Normaliza o texto (remove acentos, converte para minúsculas)
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

def is_blacklisted_site(url):
    """Verifica se o site está na blacklist"""
    return any(domain in url.lower() for domain in SITE_BLACKLIST)

def normalize_phone(raw):
    """Normaliza telefones para formato padrão"""
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 11:
        return f"({digits[:2]}) {digits[2:7]}-{digits[7:]}"
    if len(digits) == 10:
        return f"({digits[:2]}) {digits[2:6]}-{digits[6:]}"
    return raw

def build_query(m, logger):
    """Constrói a query de busca para o médico"""
    q = f"{m['Firstname']} {m['LastName']} CRM {m['CRM']} {m['UF']} telefone e-mail endereço"
    logger.info(f"Query: {q}")
    return q

def search_searx(query, logger):
    """Busca usando SearX"""
    try:
        resp = requests.get(SEARX_URL, params={'format':'json','q':query}, timeout=10).json()
        urls = [r['url'] for r in resp.get('results', [])][:MAX_RESULTS]
        logger.info(f"SearX results: {len(urls)} URLs")
        return urls
    except Exception as e:
        logger.error(f"SearX error: {e}")
        return []

def make_driver():
    """Cria e configura o driver do Chrome"""
    opts = Options()
    opts.add_argument('--headless=new')
    opts.add_argument(f'user-agent={USER_AGENT}')
    opts.add_argument('--disable-gpu')
    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    opts.add_experimental_option('excludeSwitches',['enable-logging'])
    
    # Configurações para reduzir uso de memória
    opts.add_argument('--js-flags=--expose-gc')
    opts.add_argument('--disable-extensions')
    opts.add_argument('--disable-popup-blocking')
    opts.add_argument('--blink-settings=imagesEnabled=false')
    opts.add_argument('--disable-javascript')  # Desabilita JavaScript quando possível
    opts.add_argument('--disk-cache-size=1')   # Minimiza cache em disco
    opts.add_argument('--media-cache-size=1')  # Minimiza cache de mídia
    opts.add_argument('--aggressive-cache-discard')  # Descarta cache agressivamente
    
    return webdriver.Chrome(options=opts)

def search_google(query, driver, logger):
    """Busca usando Google"""
    driver.get(f"https://www.google.com/search?q={requests.utils.quote(query)}")
    time.sleep(1)
    urls = []
    page_text = driver.page_source
    
    try:
        maps_elements = driver.find_elements(By.CSS_SELECTOR, 'a[href*="maps.google"]')
        for a in maps_elements:
            try:
                href = a.get_attribute('href')
                if href and href not in urls:
                    urls.append(href)
            except StaleElementReferenceException:
                continue
    except Exception as e:
        logger.warning(f"Erro ao buscar elementos do Google Maps: {e}")
    
    for a in driver.find_elements(By.CSS_SELECTOR,'div.yuRUbf a'):
        try:
            href = a.get_attribute('href')
        except StaleElementReferenceException:
            continue
        if href and not is_blacklisted_site(href):
            urls.append(href)
        if len(urls) >= MAX_RESULTS: break
    
    logger.info(f"Google results: {len(urls)} URLs")
    return urls, page_text

def search_bing(query, driver, logger):
    """Busca usando Bing"""
    driver.get(f"https://www.bing.com/search?q={requests.utils.quote(query)}")
    time.sleep(1)
    urls = []
    page_text = driver.page_source
    
    for a in driver.find_elements(By.CSS_SELECTOR,'li.b_algo h2 a'):
        try:
            href = a.get_attribute('href')
        except StaleElementReferenceException:
            continue
        if href and not is_blacklisted_site(href):
            urls.append(href)
        if len(urls) >= MAX_RESULTS: break
    
    logger.info(f"Bing results: {len(urls)} URLs")
    return urls, page_text

def save_debug_html(url, html, logger):
    """Salva HTML para debug"""
    if not html:
        return
    
    # Cria um nome de arquivo baseado no hash da URL
    url_hash = hashlib.md5(url.encode()).hexdigest()
    filename = os.path.join(DEBUG_HTML_DIR, f"{url_hash}.html")
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html)
        logger.info(f"HTML salvo para debug: {filename}")
    except Exception as e:
        logger.error(f"Erro ao salvar HTML para debug: {e}")

def download_html(url, logger, driver=None):
    """Download do HTML da página"""
    try:
        if driver:
            driver.get(url)
            time.sleep(1)
            html = driver.page_source
        else:
            r = requests.get(url, timeout=10, headers={'User-Agent': USER_AGENT})
            if r.status_code == 200:
                html = r.text
            else:
                return ''
        
        # Salva para debug
        save_debug_html(url, html, logger)
        return html
    except Exception as e:
        logger.warning(f"Download fail {url}: {e}")
        return ''

def limpar_endereco(endereco):
    """Limpa o endereço removendo textos indesejados"""
    for texto in TEXTOS_REMOVER:
        endereco = endereco.replace(texto, '')
    return endereco.strip()

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
    
    # Verifica se não contém caracteres especiais ou espaços
    if re.search(r'[<>()[\]\\,;:\s"]', email):
        return False
    
    # Verifica se não é uma resposta de IA ou texto explicativo
    if any(termo in email.lower() for termo in ['não posso', 'não é possível', 'ajudar', 'exemplo']):
        return False
    
    return True

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

def formatar_cep(cep):
    """Formata o CEP para o padrão XXXXX-XXX"""
    if not cep:
        return ""
    
    # Remove caracteres não numéricos
    digits = re.sub(r"\D", "", cep)
    
    # Verifica se tem 8 dígitos
    if len(digits) != 8:
        return cep
    
    # Formata como XXXXX-XXX
    return f"{digits[:5]}-{digits[5:]}"

def extract_candidates(html, url, logger):
    """Extrai candidatos de informações do HTML"""
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

def aggregate_and_rank(all_c, logger):
    """Agrega e ranqueia os candidatos"""
    ranked = {}
    for k,lst in all_c.items():
        ranked[k] = [item for item,_ in Counter(lst).most_common()]
        logger.info(f"Ranked {k}: {len(ranked[k])} items")
    return ranked

def carregar_exemplos():
    """Carrega exemplos de treinamento"""
    try:
        with open(EXEMPLOS_FILE, 'r', encoding='utf-8') as f:
            return [linha.strip() for linha in f if linha.strip()]
    except Exception as e:
        print(f"Erro ao carregar exemplos: {e}")
        return []

def criar_prompt_validacao(field, cands, m, exemplos):
    """Cria prompt para validação usando exemplos"""
    prompt = f"Dado o médico {m['Firstname']} {m['LastName']} (CRM {m['CRM']} {m['UF']}), "
    
    # Adiciona instruções específicas para cada campo
    if field == 'phone':
        prompt += "selecione APENAS um número de telefone no formato (XX) XXXX-XXXX ou (XX) XXXXX-XXXX. "
        prompt += "O número deve ser um telefone fixo ou celular válido do Brasil. "
        prompt += "Se não houver um número válido, retorne vazio (''). "
    elif field == 'email':
        prompt += "selecione APENAS um endereço de e-mail válido no formato usuario@dominio.com. "
        prompt += "Se não houver um e-mail válido, retorne vazio (''). "
    elif field == 'address':
        prompt += "selecione APENAS um endereço completo com rua e número. "
        prompt += "O endereço deve estar no formato 'Rua/Av/Travessa Nome da Rua, Número'. "
        prompt += "Se não houver um endereço válido, retorne vazio (''). "
    elif field == 'complement':
        prompt += "selecione APENAS um complemento de endereço válido como 'Sala X', 'Apto Y', 'Conjunto Z'. "
        prompt += "Se não houver um complemento válido, retorne vazio (''). "
    
    # Adiciona os candidatos
    prompt += f"Candidatos: {', '.join(cands)}. "
    
    # Adiciona exemplos de treinamento relevantes
    relevant_examples = [ex for ex in exemplos if field.lower() in ex.lower()][:5]
    if relevant_examples:
        prompt += f"Exemplos: {' | '.join(relevant_examples)}. "
    
    # Adiciona instruções finais
    prompt += "Retorne APENAS o valor selecionado, sem explicações ou texto adicional."
    
    return prompt

def validate(field, cands, m, logger):
    """Valida candidatos usando IA"""
    if not cands:
        return ""
    
    # Carrega exemplos de treinamento
    exemplos = carregar_exemplos()
    
    # Cria o prompt
    prompt = criar_prompt_validacao(field, cands, m, exemplos)
    logger.info(f"Prompt: {prompt}")
    
    # Chama a API do Ollama
    try:
        resp = requests.post(
            OLLAMA_URL,
            json={"model": "llama3.1:8b", "prompt": prompt},
            timeout=30
        ).json()
        
        result = resp.get('response', '').strip()
        logger.info(f"IA response: {result}")
        
        # Validações específicas por campo
        if field == 'phone' and not validar_telefone(result):
            logger.warning(f"Telefone inválido: {result}")
            return ""
        
        if field == 'email' and not validar_email(result):
            logger.warning(f"Email inválido: {result}")
            return ""
        
        if field == 'address' and not validar_endereco(result):
            logger.warning(f"Endereço inválido: {result}")
            return ""
        
        return result
    except Exception as e:
        logger.error(f"Erro na validação: {e}")
        return ""

def descobrir_cidade(endereco, uf, driver, logger):
    """Descobre a cidade baseado no endereço"""
    if not endereco or not uf:
        return ""
    
    # Busca no Google
    query = f"cidade {endereco} {uf}"
    logger.info(f"Buscando cidade: {query}")
    
    try:
        driver.get(f"https://www.google.com/search?q={requests.utils.quote(query)}")
        time.sleep(1)
        
        # Extrai o texto da página
        page_text = driver.page_source
        soup = BeautifulSoup(page_text, 'html.parser')
        text = soup.get_text(' ')
        
        # Lista de cidades do estado
        cidades_uf = []
        
        # Procura por padrões de cidade
        # Padrão 1: "em [Cidade], [UF]"
        matches = re.findall(r'em ([A-Z][a-zÀ-ú]+(?:\s[A-Z][a-zÀ-ú]+)*),\s*' + uf, text)
        cidades_uf.extend(matches)
        
        # Padrão 2: "[Cidade] - [UF]"
        matches = re.findall(r'([A-Z][a-zÀ-ú]+(?:\s[A-Z][a-zÀ-ú]+)*)\s*-\s*' + uf, text)
        cidades_uf.extend(matches)
        
        # Padrão 3: "cidade de [Cidade]"
        matches = re.findall(r'cidade\s+de\s+([A-Z][a-zÀ-ú]+(?:\s[A-Z][a-zÀ-ú]+)*)', text)
        cidades_uf.extend(matches)
        
        # Conta a frequência de cada cidade
        cidade_counter = Counter(cidades_uf)
        
        # Pega a cidade mais frequente
        if cidade_counter:
            cidade = cidade_counter.most_common(1)[0][0]
            logger.info(f"Cidade encontrada: {cidade}")
            return cidade
        
        logger.warning("Cidade não encontrada")
        return ""
    except Exception as e:
        logger.error(f"Erro ao descobrir cidade: {e}")
        return ""

def buscar_dados_via_viacep(rua, cidade, uf, logger):
    """Busca dados via ViaCEP API"""
    if not rua or not cidade or not uf:
        logger.warning("Dados insuficientes para busca no ViaCEP")
        return None
    
    # Verifica no cache primeiro
    chave_cache = gerar_chave_cache(rua, cidade, uf)
    if chave_cache in CEP_CACHE:
        logger.info(f"CEP encontrado no cache: {CEP_CACHE[chave_cache]}")
        return CEP_CACHE[chave_cache]
    
    # Normaliza os parâmetros
    rua_norm = normalizar_endereco(rua)
    cidade_norm = normalizar_cidade(cidade)
    
    # Codifica os parâmetros para URL
    rua_encoded = urllib.parse.quote(rua_norm)
    cidade_encoded = urllib.parse.quote(cidade_norm)
    uf_encoded = urllib.parse.quote(uf)
    
    # Constrói a URL
    url = VIACEP_URL.format(uf=uf_encoded, cidade=cidade_encoded, rua=rua_encoded)
    logger.info(f"Buscando no ViaCEP: {url}")
    
    try:
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
        
        logger.warning(f"ViaCEP não retornou dados válidos: {response.text}")
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
        logger.info(f"CEP encontrado no cache: {CEP_CACHE[chave_cache]}")
        return CEP_CACHE[chave_cache]
    
    # Normaliza os parâmetros
    rua_norm = normalizar_endereco(rua)
    cidade_norm = normalizar_cidade(cidade)
    
    # Constrói a query para buscar o CEP
    query = f"{rua_norm} {cidade_norm} {uf}"
    logger.info(f"Buscando na BrasilAPI: {query}")
    
    try:
        # Faz a busca no Google primeiro para encontrar o CEP
        response = requests.get(
            "https://www.google.com/search",
            params={"q": f"CEP {query}"},
            headers={"User-Agent": USER_AGENT},
            timeout=10
        )
        
        # Extrai CEPs do resultado do Google
        ceps = PATTERNS['cep'].findall(response.text)
        
        if ceps:
            # Pega o primeiro CEP encontrado
            cep = re.sub(r"\D", "", ceps[0])
            
            # Busca detalhes do CEP na BrasilAPI
            url = BRASILAPI_URL.format(cep=cep)
            logger.info(f"Consultando detalhes do CEP na BrasilAPI: {url}")
            
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Dados encontrados na BrasilAPI: {data}")
                
                # Converte para o formato do ViaCEP para compatibilidade
                result = {
                    "cep": data.get("cep", ""),
                    "logradouro": data.get("street", ""),
                    "complemento": data.get("complement", ""),
                    "bairro": data.get("neighborhood", ""),
                    "localidade": data.get("city", ""),
                    "uf": data.get("state", "")
                }
                
                # Salva no cache
                CEP_CACHE[chave_cache] = result
                salvar_cache_cep(CEP_CACHE)
                
                return result
        
        logger.warning("BrasilAPI não retornou dados válidos")
        return None
    except Exception as e:
        logger.error(f"Erro ao buscar na BrasilAPI: {e}")
        return None

def buscar_cep_por_endereco(rua, cidade, driver, logger):
    """Busca CEP baseado na rua e cidade via Google"""
    if not rua or not cidade:
        logger.warning("Rua ou cidade não disponíveis para busca de CEP")
        return ""
    
    # Verifica no cache primeiro
    chave_cache = gerar_chave_cache(rua, cidade, "")
    if chave_cache in CEP_CACHE and "cep" in CEP_CACHE[chave_cache]:
        logger.info(f"CEP encontrado no cache: {CEP_CACHE[chave_cache]['cep']}")
        return CEP_CACHE[chave_cache]["cep"]
    
    # Formata a query de busca
    query = f"CEP da {rua}, {cidade}"
    logger.info(f"Buscando CEP via Google: {query}")
    
    # Realiza a busca no Google
    driver.get(f"https://www.google.com/search?q={requests.utils.quote(query)}")
    time.sleep(1)
    
    # Extrai o texto da página de resultados
    page_text = driver.page_source
    soup = BeautifulSoup(page_text, 'html.parser')
    text = soup.get_text(' ')
    
    # Procura por padrões de CEP no texto
    ceps = PATTERNS['cep'].findall(text)
    
    if ceps:
        cep = formatar_cep(ceps[0])
        logger.info(f"CEP encontrado via Google: {cep}")
        
        # Salva no cache
        CEP_CACHE[chave_cache] = {"cep": cep}
        salvar_cache_cep(CEP_CACHE)
        
        return cep
    
    # Tenta uma segunda busca com formato alternativo
    query = f"{rua}, {cidade} CEP"
    logger.info(f"Segunda tentativa via Google: {query}")
    driver.get(f"https://www.google.com/search?q={requests.utils.quote(query)}")
    time.sleep(1)
    
    page_text = driver.page_source
    soup = BeautifulSoup(page_text, 'html.parser')
    text = soup.get_text(' ')
    ceps = PATTERNS['cep'].findall(text)
    
    if ceps:
        cep = formatar_cep(ceps[0])
        logger.info(f"CEP encontrado na segunda tentativa via Google: {cep}")
        
        # Salva no cache
        CEP_CACHE[chave_cache] = {"cep": cep}
        salvar_cache_cep(CEP_CACHE)
        
        return cep
    
    logger.warning("CEP não encontrado via Google")
    return ""

def buscar_cep_via_correios(rua, cidade, uf, driver, logger):
    """Busca CEP no site dos Correios"""
    if not rua or not cidade:
        logger.warning("Rua ou cidade não disponíveis para busca de CEP nos Correios")
        return None
    
    # Verifica no cache primeiro
    chave_cache = gerar_chave_cache(rua, cidade, uf)
    if chave_cache in CEP_CACHE:
        logger.info(f"CEP encontrado no cache: {CEP_CACHE[chave_cache]}")
        return CEP_CACHE[chave_cache]
    
    logger.info(f"Buscando CEP nos Correios: {rua}, {cidade}, {uf}")
    
    try:
        # Acessa o site dos Correios
        driver.get(CORREIOS_URL)
        time.sleep(2)
        
        # Preenche o formulário
        try:
            # Seleciona o tipo de busca por endereço
            driver.find_element(By.ID, "endereco").click()
            time.sleep(1)
            
            # Preenche o campo de endereço
            endereco_input = driver.find_element(By.ID, "endereco")
            endereco_input.clear()
            endereco_input.send_keys(rua)
            
            # Preenche o campo de cidade
            cidade_input = driver.find_element(By.ID, "cidade")
            cidade_input.clear()
            cidade_input.send_keys(cidade)
            
            # Seleciona o estado
            uf_select = driver.find_element(By.ID, "uf")
            uf_select.send_keys(uf)
            
            # Clica no botão de busca
            driver.find_element(By.ID, "btn_pesquisar").click()
            time.sleep(3)
            
            # Extrai os resultados
            page_text = driver.page_source
            soup = BeautifulSoup(page_text, 'html.parser')
            
            # Procura pela tabela de resultados
            tabela = soup.select_one("table.tmptabela")
            if tabela:
                # Extrai as linhas da tabela
                linhas = tabela.select("tr")
                
                if len(linhas) > 1:  # Primeira linha é o cabeçalho
                    # Extrai os dados da primeira linha de resultado
                    colunas = linhas[1].select("td")
                    
                    if len(colunas) >= 4:
                        logradouro = colunas[0].get_text(strip=True)
                        bairro = colunas[1].get_text(strip=True)
                        localidade = colunas[2].get_text(strip=True)
                        cep = colunas[3].get_text(strip=True)
                        
                        # Formata o CEP
                        cep = formatar_cep(cep)
                        
                        # Cria o resultado no formato do ViaCEP
                        result = {
                            "cep": cep,
                            "logradouro": logradouro,
                            "bairro": bairro,
                            "localidade": localidade.split('/')[0].strip(),
                            "uf": localidade.split('/')[-1].strip() if '/' in localidade else uf,
                            "complemento": ""
                        }
                        
                        logger.info(f"CEP encontrado nos Correios: {result}")
                        
                        # Salva no cache
                        CEP_CACHE[chave_cache] = result
                        salvar_cache_cep(CEP_CACHE)
                        
                        return result
            
            # Procura por CEPs na página
            ceps = PATTERNS['cep'].findall(page_text)
            if ceps:
                cep = formatar_cep(ceps[0])
                logger.info(f"CEP encontrado nos Correios (alternativo): {cep}")
                
                # Cria um resultado simplificado
                result = {
                    "cep": cep,
                    "logradouro": rua,
                    "bairro": "",
                    "localidade": cidade,
                    "uf": uf,
                    "complemento": ""
                }
                
                # Salva no cache
                CEP_CACHE[chave_cache] = result
                salvar_cache_cep(CEP_CACHE)
                
                return result
        except Exception as e:
            logger.error(f"Erro ao interagir com o site dos Correios: {e}")
        
        logger.warning("CEP não encontrado nos Correios")
        return None
    except Exception as e:
        logger.error(f"Erro ao buscar CEP nos Correios: {e}")
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
        all_candidates = []
        for i, url in enumerate(all_urls):
            logger.info(f"Processando URL {i+1}/{len(all_urls)}: {url}")
            html = download_html(url, logger, driver)
            if html:
                candidates = extract_candidates(html, url, logger)
                all_candidates.append(candidates)
            log_memory_usage(logger, f"Após processar URL {i+1}")
        
        # Agrega e ranqueia
        if all_candidates:
            logger.info("Iniciando agregação e ranqueamento")
            aggregated = {}
            for k in all_candidates[0].keys():
                aggregated[k] = []
                for c in all_candidates:
                    aggregated[k].extend(c.get(k, []))
            
            ranked = aggregate_and_rank(aggregated, logger)
            log_memory_usage(logger, "Após agregação")
            
            # Cria resultado mantendo todas as colunas originais
            result = m.copy()
            
            # Mapeia os campos encontrados para os campos corretos
            if ranked.get('address'):
                logger.info("Validando endereço")
                endereco_completo = validate('address', ranked['address'][:3], m, logger)
                if endereco_completo:
                    # Extrai o número do endereço
                    endereco_sem_numero, numero = extrair_numero_endereco(endereco_completo)
                    result['Address A1'] = endereco_sem_numero
                    result['Numero A1'] = numero
            
            if ranked.get('phone'):
                logger.info("Validando telefone")
                result['Phone A1'] = validate('phone', ranked['phone'][:3], m, logger)
            
            if ranked.get('email'):
                logger.info("Validando email")
                result['E-mail A1'] = validate('email', ranked['email'][:3], m, logger)
            
            if ranked.get('complement'):
                logger.info("Validando complemento")
                result['Complement A1'] = validate('complement', ranked['complement'][:3], m, logger)
            
            # Descobre a cidade
            if result.get('Address A1') and not result.get('City A1'):
                logger.info("Descobrindo cidade")
                result['City A1'] = descobrir_cidade(
                    result['Address A1'],
                    m['UF'],
                    driver,
                    logger
                )
            
            # Busca CEP e dados de endereço usando sistema de cascata
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
        
    except Exception as e:
        logger.error(f"Erro ao processar médico {m['Firstname']} {m['LastName']}: {e}")
        logger.error(traceback.format_exc())
        log_memory_usage(logger, "Após erro")
    
    return None

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
                if i % 10 == 0:
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
                
                # Salva resultados a cada 5 médicos para evitar perda de dados
                if len(results) >= 5:
                    logger.info("Salvando lote de resultados")
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
    batch_size = min(50, math.ceil(len(medicos) / num_processes))
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
            pool.terminate()
            pool.join()
            raise

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Uso: python buscador_medicos.v6.py input.csv output.csv")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    run_parallel(input_file, output_file)
