#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
buscador_empresas.v1.py

Versão 1.0 do buscador de empresas com:
- Foco configurável no tipo de contato (ex: TI, Contabilidade, etc)
- Busca e extração de dados corporativos: razão social, CNPJ, faixa de funcionários, contato, cargo, telefones, email, cidade, estado, CEP
- Uso de CNPJ para novas queries assim que encontrado
- Sistema de cache e paralelismo
- Limpeza e validação de dados
- Prompts e exemplos específicos para empresas
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

# ===================== CONFIGURAÇÕES =====================
# Foco do contato (ex: "TI", "Contabilidade", "RH", "Comercial", "Financeiro")
FOCO_CONTATO = "TI"  # <-- MODIFIQUE AQUI O FOCO DO CONTATO

SEARX_URL   = "http://124.81.6.163:8092/search"
VIACEP_URL  = "https://viacep.com.br/ws/{uf}/{cidade}/{rua}/json/"
BRASILAPI_URL = "https://brasilapi.com.br/api/cep/v2/{cep}"
CORREIOS_URL = "https://buscacepinter.correios.com.br/app/endereco/index.php"
USER_AGENT  = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/114.0.0.0 Safari/537.36"
)
MAX_RESULTS = 15
NUM_PROCESSES = max(1, multiprocessing.cpu_count() - 1)
CHUNK_SIZE = 10

DATA_DIR = 'data'
TIPOS_EMPRESA_FILE = os.path.join(DATA_DIR, 'tipos_empresa.txt')
TEXTOS_REMOVER_FILE = os.path.join(DATA_DIR, 'textos_remover.txt')
EXEMPLOS_FILE = os.path.join(DATA_DIR, 'exemplos_treinamento.txt')
EMAIL_BLACKLIST_FILE = os.path.join(DATA_DIR, 'email_blacklist.txt')
SITE_BLACKLIST_FILE = os.path.join(DATA_DIR, 'site_blacklist.txt')
CARGOS_FILE = os.path.join(DATA_DIR, 'cargos.txt')
FAIXAS_FUNCIONARIOS_FILE = os.path.join(DATA_DIR, 'faixas_funcionarios.txt')
LOG_DIR = os.path.join(DATA_DIR, 'logmulti')
DEBUG_HTML_DIR = os.path.join(DATA_DIR, 'debug_html_empresas')
CACHE_DIR = os.path.join(DATA_DIR, 'cache')
CNPJ_CACHE_FILE = os.path.join(CACHE_DIR, 'cnpj_cache.json')
MANUAL_CNPJ_FILE = os.path.join(DATA_DIR, 'manual_cnpjs.json')

for dir_path in [DATA_DIR, DEBUG_HTML_DIR, LOG_DIR, CACHE_DIR]:
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

# ===================== FUNÇÕES DE UTILIDADE =====================
def carregar_lista_arquivo(nome_arquivo):
    try:
        with open(nome_arquivo, 'r', encoding='utf-8') as f:
            return [linha.strip() for linha in f if linha.strip()]
    except Exception as e:
        print(f"Erro ao carregar arquivo {nome_arquivo}: {e}")
        return []

TIPOS_EMPRESA = carregar_lista_arquivo(TIPOS_EMPRESA_FILE)
TEXTOS_REMOVER = carregar_lista_arquivo(TEXTOS_REMOVER_FILE)
EMAIL_BLACKLIST = carregar_lista_arquivo(EMAIL_BLACKLIST_FILE)
SITE_BLACKLIST = carregar_lista_arquivo(SITE_BLACKLIST_FILE)
CARGOS = carregar_lista_arquivo(CARGOS_FILE)
FAIXAS_FUNCIONARIOS = carregar_lista_arquivo(FAIXAS_FUNCIONARIOS_FILE)

# Se os arquivos não existirem, cria com valores padrão
if not TIPOS_EMPRESA:
    with open(TIPOS_EMPRESA_FILE, 'w', encoding='utf-8') as f:
        f.write("Ltda\nS.A.\nEireli\nME\nEPP\nSociedade Simples\nMEI\nCooperativa\nConsórcio\nAssociação\nFundação\n")
    TIPOS_EMPRESA = carregar_lista_arquivo(TIPOS_EMPRESA_FILE)
if not TEXTOS_REMOVER:
    with open(TEXTOS_REMOVER_FILE, 'w', encoding='utf-8') as f:
        f.write("Matriz\nFilial\nSede\nEscritório\nUnidade\nDepartamento\nSetor\nÁrea\nCNPJ\nRazão Social\nInscrição Estadual\nInscrição Municipal\nPorte\nCapital Social\nNatureza Jurídica\nData de Abertura\nQuadro Societário\nSócios\nAdministrador\n")
    TEXTOS_REMOVER = carregar_lista_arquivo(TEXTOS_REMOVER_FILE)
if not EMAIL_BLACKLIST:
    with open(EMAIL_BLACKLIST_FILE, 'w', encoding='utf-8') as f:
        f.write("@gmail.com\n@yahoo.com\n@hotmail.com\n@outlook.com\n@bol.com.br\n@example.com\n@dominio.com\n@empresa.com\n@teste.com\n")
    EMAIL_BLACKLIST = carregar_lista_arquivo(EMAIL_BLACKLIST_FILE)
if not SITE_BLACKLIST:
    with open(SITE_BLACKLIST_FILE, 'w', encoding='utf-8') as f:
        f.write("google.com\nbing.com\nyahoo.com\nfacebook.com\nlinkedin.com\ninstagram.com\ntwitter.com\nyoutube.com\nwikipedia.org\npdf\n.doc\n.docx\n.xls\n.xlsx\n.ppt\n.pptx\n.csv\n.txt\n")
    SITE_BLACKLIST = carregar_lista_arquivo(SITE_BLACKLIST_FILE)
if not CARGOS:
    with open(CARGOS_FILE, 'w', encoding='utf-8') as f:
        f.write("Analista de TI\nGerente de TI\nDiretor de TI\nCoordenador de TI\nSupervisor de TI\nEspecialista de TI\nAdministrador de Redes\nDesenvolvedor\nEngenheiro de Software\nAnalista de Sistemas\nSuporte Técnico\nAnalista de Infraestrutura\nAnalista de Segurança da Informação\nCIO\nCTO\nAnalista Contábil\nGerente Financeiro\nDiretor Financeiro\nContador\nAnalista de RH\nGerente de RH\nDiretor de RH\nAnalista Comercial\nGerente Comercial\nDiretor Comercial\nAnalista de Marketing\nGerente de Marketing\nDiretor de Marketing\nAnalista Administrativo\nGerente Administrativo\nDiretor Administrativo\n")
    CARGOS = carregar_lista_arquivo(CARGOS_FILE)
if not FAIXAS_FUNCIONARIOS:
    with open(FAIXAS_FUNCIONARIOS_FILE, 'w', encoding='utf-8') as f:
        f.write("1-10\n11-50\n51-200\n201-500\n501-1000\n1001-5000\n5001-10000\n10001+\n")
    FAIXAS_FUNCIONARIOS = carregar_lista_arquivo(FAIXAS_FUNCIONARIOS_FILE)

# Cache de CNPJ
CNPJ_CACHE = {}
if os.path.exists(CNPJ_CACHE_FILE):
    try:
        with open(CNPJ_CACHE_FILE, 'r', encoding='utf-8') as f:
            CNPJ_CACHE = json.load(f)
    except:
        CNPJ_CACHE = {}

# CNPJs manuais
MANUAL_CNPJS = {}
if os.path.exists(MANUAL_CNPJ_FILE):
    try:
        with open(MANUAL_CNPJ_FILE, 'r', encoding='utf-8') as f:
            MANUAL_CNPJS = json.load(f)
    except:
        MANUAL_CNPJS = {}

# ===================== PADRÕES REGEX =====================
PATTERNS = {
    'cnpj': re.compile(r"\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}"),
    'telefone': re.compile(r"\(\d{2}\)\s?\d{4,5}-\d{4}"),
    'celular': re.compile(r"\(\d{2}\)\s?9\d{4}-\d{4}"),
    'email': re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"),
    'faixa_funcionarios': re.compile(r"\d+[\s-]?a[\s-]?\d+\s*(funcionários|colaboradores|empregados)", re.IGNORECASE),
    'cep': re.compile(r"\d{5}-\d{3}|\d{8}"),
}

# ===================== LOGGING =====================
def setup_logger(process_id):
    logger = logging.getLogger(f"process_{process_id}")
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(os.path.join(LOG_DIR, f'buscador_empresas_v1_p{process_id}.log'), 'w', 'utf-8')
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
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
    texto = texto.lower()
    texto = re.sub(r'[^\w\s]', ' ', texto)
    texto = re.sub(r'\s+', ' ', texto).strip()
    return texto

def validar_cnpj(cnpj):
    if not cnpj:
        return False
    cnpj = re.sub(r'\D', '', cnpj)
    if len(cnpj) != 14:
        return False
    if cnpj == cnpj[0] * 14:
        return False
    return True

def validar_email(email):
    if not email:
        return False
    if any(domain in email.lower() for domain in EMAIL_BLACKLIST):
        return False
    if not re.match(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$', email):
        return False
    if re.search(r'[<>()[\]\\,;:\s"]', email):
        return False
    return True

def validar_telefone(telefone):
    if not telefone:
        return False
    digits = re.sub(r"\D", "", telefone)
    if len(digits) < 10 or len(digits) > 11:
        return False
    ddd = int(digits[:2])
    if ddd < 11 or ddd > 99:
        return False
    return True

def validar_faixa_funcionarios(faixa):
    if not faixa:
        return False
    return any(faixa in f for f in FAIXAS_FUNCIONARIOS)

def validar_cargo(cargo):
    if not cargo:
        return False
    return any(c.lower() in cargo.lower() for c in CARGOS)

# ===================== FUNÇÕES DE BUSCA E EXTRAÇÃO =====================
def build_query(empresa, cnpj=None):
    nome = empresa.get('Empresa', '')
    queries = [
        f"{nome} contato {FOCO_CONTATO} telefone email",
        f"{nome} {FOCO_CONTATO} responsável telefone email",
        f"{nome} funcionários quantidade",
        f"{nome} endereço cidade estado cep",
    ]
    if cnpj:
        queries.append(f"{cnpj} contato {FOCO_CONTATO} telefone email")
        queries.append(f"{cnpj} endereço cidade estado cep")
    return queries

def search_searx(query, logger):
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
    options.add_argument('--disable-features=NetworkService')
    options.add_argument('--disable-features=VizDisplayCompositor')
    options.add_argument('--disable-web-security')
    options.add_argument('--disable-features=IsolateOrigins,site-per-process')
    options.add_argument('--disable-site-isolation-trials')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    prefs = {
        'profile.default_content_setting_values': {
            'images': 2,
            'javascript': 1,
            'notifications': 2,
            'plugins': 2,
        }
    }
    options.add_experimental_option('prefs', prefs)
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(30)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def download_html(url, logger, driver):
    try:
        if any(ext in url.lower() for ext in ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.csv', '.txt']):
            logger.info(f"Ignorando URL de arquivo não-HTML: {url}")
            return None
        url_hash = hashlib.md5(url.encode()).hexdigest()
        driver.get(url)
        time.sleep(2)
        html = driver.page_source
        if len(html) > 3 * 1024 * 1024:
            logger.warning(f"Página muito grande ({len(html)/1024/1024:.2f}MB), truncando")
            html = html[:3 * 1024 * 1024]
        debug_file = os.path.join(DEBUG_HTML_DIR, f"{url_hash}.html")
        with open(debug_file, 'w', encoding='utf-8', errors='ignore') as f:
            f.write(html)
        logger.info(f"HTML salvo para debug: {debug_file}")
        return html
    except Exception as e:
        logger.error(f"Erro ao baixar HTML de {url}: {e}")
        return None

def extract_candidates(html, url, logger):
    if not html:
        return {
            'razao_social': [],
            'cnpj': [],
            'faixa_funcionarios': [],
            'contato_nome': [],
            'contato_sobrenome': [],
            'cargo': [],
            'telefone': [],
            'celular': [],
            'email': [],
            'cidade': [],
            'estado': [],
            'cep': []
        }
    try:
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text(' ')
        cnpjs = PATTERNS['cnpj'].findall(text)
        telefones = PATTERNS['telefone'].findall(text)
        celulares = PATTERNS['celular'].findall(text)
        emails = PATTERNS['email'].findall(text)
        faixas = PATTERNS['faixa_funcionarios'].findall(text)
        ceps = PATTERNS['cep'].findall(text)
        # Razão social: heurística - linhas com "Ltda", "S.A.", etc
        razoes = [linha.strip() for linha in text.split('\n') if any(tp in linha for tp in TIPOS_EMPRESA)]
        # Cargo: linhas com cargos conhecidos
        cargos = [linha.strip() for linha in text.split('\n') if any(cg in linha for cg in CARGOS)]
        # Nome do contato: heurística - nomes próximos a cargos
        contato_nomes = []
        contato_sobrenomes = []
        for linha in text.split('\n'):
            for cg in CARGOS:
                if cg in linha:
                    partes = linha.split()
                    idx = partes.index(cg.split()[0]) if cg.split()[0] in partes else -1
                    if idx > 0:
                        contato_nomes.append(partes[idx-1])
                        if idx > 1:
                            contato_sobrenomes.append(partes[idx-2])
        # Estado: siglas de 2 letras
        estados = re.findall(r'\b[A-Z]{2}\b', text)
        cidades = []  # Heurística: linhas com "cidade" ou "município"
        for linha in text.split('\n'):
            if 'cidade' in linha.lower() or 'município' in linha.lower():
                cidades.append(linha.strip())
        def dedupe(lst):
            seen, out = set(), []
            for x in lst:
                if x not in seen:
                    seen.add(x); out.append(x)
            return out
        cands = {
            'razao_social': dedupe(razoes),
            'cnpj': dedupe(cnpjs),
            'faixa_funcionarios': dedupe(faixas),
            'contato_nome': dedupe(contato_nomes),
            'contato_sobrenome': dedupe(contato_sobrenomes),
            'cargo': dedupe(cargos),
            'telefone': dedupe(telefones),
            'celular': dedupe(celulares),
            'email': dedupe(emails),
            'cidade': dedupe(cidades),
            'estado': dedupe(estados),
            'cep': dedupe(ceps)
        }
        for k,v in cands.items():
            logger.info(f"Candidates {k}: {len(v)} items")
        return cands
    except Exception as e:
        logger.error(f"Erro ao extrair candidatos: {e}")
        return {
            'razao_social': [],
            'cnpj': [],
            'faixa_funcionarios': [],
            'contato_nome': [],
            'contato_sobrenome': [],
            'cargo': [],
            'telefone': [],
            'celular': [],
            'email': [],
            'cidade': [],
            'estado': [],
            'cep': []
        }

def aggregate_and_rank(all_c, logger):
    ranked = {}
    for k,lst in all_c.items():
        ranked[k] = [item for item,_ in Counter(lst).most_common()]
        logger.info(f"Ranked {k}: {len(ranked[k])} items")
    return ranked

# ===================== PROCESSAMENTO DE EMPRESA =====================
def process_empresa(emp, driver, logger):
    start_time = time.time()
    try:
        queries = build_query(emp)
        all_urls = set()
        for query in queries:
            urls = search_searx(query, logger)
            all_urls.update(urls)
        logger.info(f"Total de URLs únicas: {len(all_urls)}")
        all_candidates_raw = []
        cnpj_encontrado = None
        for i, url in enumerate(all_urls):
            logger.info(f"Processando URL {i+1}/{len(all_urls)}: {url}")
            html = download_html(url, logger, driver)
            if html:
                candidates = extract_candidates(html, url, logger)
                all_candidates_raw.append(candidates)
                if not cnpj_encontrado and candidates['cnpj']:
                    cnpj_encontrado = candidates['cnpj'][0]
        # Se encontrou CNPJ, faz queries extras
        if cnpj_encontrado:
            emp['CNPJ'] = cnpj_encontrado
            queries_cnpj = build_query(emp, cnpj=cnpj_encontrado)
            for query in queries_cnpj:
                urls = search_searx(query, logger)
                all_urls.update(urls)
            for url in all_urls:
                html = download_html(url, logger, driver)
                if html:
                    candidates = extract_candidates(html, url, logger)
                    all_candidates_raw.append(candidates)
        # Agrega e ranqueia
        if all_candidates_raw:
            aggregated = {k: [] for k in all_candidates_raw[0].keys()}
            for c in all_candidates_raw:
                for k in aggregated:
                    aggregated[k].extend(c.get(k, []))
            ranked = aggregate_and_rank(aggregated, logger)
            result = emp.copy()
            if ranked.get('razao_social'):
                result['Razão Social'] = ranked['razao_social'][0]
            if ranked.get('cnpj'):
                result['CNPJ'] = ranked['cnpj'][0]
            if ranked.get('faixa_funcionarios'):
                result['Porte'] = ranked['faixa_funcionarios'][0]
            if ranked.get('contato_nome'):
                result['Nome'] = ranked['contato_nome'][0]
            if ranked.get('contato_sobrenome'):
                result['sobrenome'] = ranked['contato_sobrenome'][0]
            if ranked.get('cargo'):
                result['Cargo'] = ranked['cargo'][0]
            if ranked.get('telefone'):
                result['Telefone'] = ranked['telefone'][0]
            if ranked.get('celular'):
                result['Celular'] = ranked['celular'][0]
            if ranked.get('email'):
                result['E-mail'] = ranked['email'][0]
            if ranked.get('cidade'):
                result['Cidade'] = ranked['cidade'][0]
            if ranked.get('estado'):
                result['estado'] = ranked['estado'][0]
            if ranked.get('cep'):
                result['CEP'] = ranked['cep'][0]
            return result
        else:
            logger.warning("Nenhum candidato encontrado para a empresa")
            return emp.copy()
    except Exception as e:
        logger.error(f"Erro ao processar empresa {emp.get('Empresa', '')}: {e}")
        logger.error(traceback.format_exc())
        return emp.copy()

# ===================== PROCESSAMENTO EM LOTE =====================
def process_batch(batch_id, empresas_batch, output_file, fieldnames, progress_dict, lock):
    logger = setup_logger(batch_id)
    driver = None
    try:
        results = []
        for i, empresa in enumerate(empresas_batch):
            try:
                if i % 5 == 0 or driver is None:
                    if driver:
                        try:
                            driver.quit()
                        except:
                            pass
                    logger.info("Reiniciando driver do Chrome")
                    driver = make_driver()
                result = process_empresa(empresa, driver, logger)
                if result:
                    results.append(result)
                if len(results) >= 3:
                    with lock:
                        with open(output_file, 'a', newline='', encoding='utf-8') as f:
                            writer = csv.DictWriter(f, fieldnames=fieldnames)
                            writer.writerows(results)
                    results = []
                with lock:
                    progress_dict['processed'] += 1
                    progress = (progress_dict['processed'] / progress_dict['total']) * 100
                    logger.info(f"Progresso: {progress:.1f}%")
            except Exception as e:
                logger.error(f"Erro ao processar empresa {empresa.get('Empresa', '')}: {e}")
                logger.error(traceback.format_exc())
                continue
        if results:
            with lock:
                with open(output_file, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writerows(results)
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
    if num_processes is None:
        num_processes = NUM_PROCESSES
    with open(inp, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        empresas = list(reader)
    fieldnames = [
        'Empresa', 'Razão Social', 'CNPJ', 'Porte', 'Nome', 'sobrenome', 'Cargo',
        'Telefone', 'Celular', 'E-mail', 'Cidade', 'estado', 'CEP', 'LINKEDIN'
    ]
    with open(outp, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
    batch_size = max(1, min(3, math.ceil(len(empresas) / num_processes)))
    batches = [empresas[i:i + batch_size] for i in range(0, len(empresas), batch_size)]
    manager = Manager()
    progress_dict = manager.dict()
    progress_dict['processed'] = 0
    progress_dict['total'] = len(empresas)
    lock = manager.Lock()
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
        print("Uso: python buscador_empresas.v1.py empresas.csv empresas-output.csv")
        sys.exit(1)
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    run_parallel(input_file, output_file) 