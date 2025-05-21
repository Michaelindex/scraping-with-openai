#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
buscador_medicos.v5.py

Versão 5.0 do buscador de médicos com:
- Processamento paralelo para maior velocidade
- Sistema de treinamento da IA para melhor precisão
- Estrutura de dados otimizada
- Validação inteligente de resultados
- Busca de CEP e dados de endereço via ViaCEP API

Aprimoramentos:
- Prioriza telefones celulares (começando com DDD +9)
- Filtra e-mails inválidos (strings com 'subject=')
- Remove complementos sem sentido (e.g., 'Salarial')
- Especialista de descoberta de cidades via busca na web
- Processamento paralelo para maior velocidade
- Sistema de treinamento da IA para melhor precisão
- Busca de CEP e dados de endereço via ViaCEP API
- Fallback para busca web quando ViaCEP não retorna resultados
"""
import sys
import csv
import re
import requests
import logging
import time
import os
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

# Configurações
SEARX_URL   = "http://124.81.6.163:8092/search"
OLLAMA_URL  = "http://124.81.6.163:11434/api/generate"
VIACEP_URL  = "https://viacep.com.br/ws/{uf}/{cidade}/{rua}/json/"
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
DEBUG_HTML_DIR = os.path.join(DATA_DIR, 'debug_html_v5')

# Criar diretórios necessários
for dir_path in [DATA_DIR, DEBUG_HTML_DIR, LOG_DIR]:
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

# Padrões regex
PATTERNS = {
    'address': re.compile(r"(?:Av\.|Rua|Travessa|Estrada)[^,\n]{5,100},?\s*\d{1,5}"),
    'phone':   re.compile(r"\(\d{2}\)\s?\d{4,5}-\d{4}"),
    'email':   re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"),
    'complement': re.compile(r"(?:Sala|Bloco|Apt\.?|Conjunto)[^,\n]{1,50}")
}

# Configuração de logging para multiprocessamento
def setup_logger(process_id):
    logger = logging.getLogger(f"process_{process_id}")
    logger.setLevel(logging.INFO)
    
    # Cria um handler para arquivo
    file_handler = logging.FileHandler(os.path.join(LOG_DIR, f'buscador_medicos_v5_p{process_id}.log'), 'w', 'utf-8')
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
        logger.error(f"Erro ao carregar arquivo {nome_arquivo}: {e}")
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

def extract_candidates(html, url, logger):
    """Extrai candidatos de informações do HTML"""
    soup = BeautifulSoup(html, 'html.parser')
    text = soup.get_text(' ')
    
    # Extrai usando regex
    addrs = PATTERNS['address'].findall(text)
    phones= PATTERNS['phone'].findall(text)
    emails= PATTERNS['email'].findall(text)
    comps = PATTERNS['complement'].findall(text)
    
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
        'complement': dedupe(comps)
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
        logger.error(f"Erro ao carregar exemplos: {e}")
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
        prompt += "selecione APENAS um complemento de endereço (sala, apto, etc). "
        prompt += "Se não houver um complemento válido, retorne vazio (''). "
    
    if exemplos:
        prompt += "\nAqui estão alguns exemplos de respostas corretas:\n"
        for ex in exemplos[:3]:
            prompt += f"- {ex}\n"
        prompt += "\n"
    
    prompt += f"Destes candidatos para {field}: {cands}\n"
    prompt += "Responda APENAS com o item mais provável de estar correto, sem explicações. "
    prompt += "Se nenhum item for válido, responda com uma string vazia ('')."
    
    return prompt

def validate(field, cands, m, logger):
    """Valida os candidatos usando IA"""
    if not cands:
        return ''
    
    exemplos = carregar_exemplos()
    prompt = criar_prompt_validacao(field, cands, m, exemplos)
    
    try:
        r = requests.post(OLLAMA_URL, json={'model':'llama3.1:8b','prompt':prompt,'stream':False}, timeout=15)
        if r.status_code == 200:
            resposta = r.json().get('response','').strip()
            
            # Validação adicional baseada no campo
            if field == 'phone' and not validar_telefone(resposta):
                logger.warning(f"Telefone inválido retornado pela IA: {resposta}")
                return ''
            elif field == 'email' and not validar_email(resposta):
                logger.warning(f"Email inválido retornado pela IA: {resposta}")
                return ''
            
            # Verifica se a resposta não é uma explicação ou texto inadequado
            if any(termo in resposta.lower() for termo in ['não posso', 'não é possível', 'ajudar', 'exemplo']):
                logger.warning(f"Resposta inadequada da IA: {resposta}")
                return ''
            
            return resposta
    except Exception as e:
        logger.error(f"Ollama error: {e}")
    
    return ''

def extrair_cidade_via_ia(textos, endereco, uf, logger):
    """Extrai cidade usando IA"""
    prompt = f"Dado o endereço '{endereco}' no estado {uf}, "
    prompt += "responda APENAS com o nome da cidade, sem pontuação ou explicações."
    
    try:
        r = requests.post(OLLAMA_URL, json={'model':'llama3.1:8b','prompt':prompt,'stream':False}, timeout=15)
        if r.status_code == 200:
            return r.json().get('response','').strip()
    except Exception as e:
        logger.error(f"Erro ao extrair cidade via IA: {e}")
    
    return ''

def descobrir_cidade(endereco, uf, driver, logger):
    """Descobre a cidade usando múltiplas estratégias"""
    # Tenta via IA
    cidade = extrair_cidade_via_ia([], endereco, uf, logger)
    if cidade:
        return cidade
    
    return ''

def buscar_dados_via_viacep(rua, cidade, uf, logger):
    """Busca dados de endereço via ViaCEP API"""
    if not rua or not cidade or not uf:
        logger.warning("Rua, cidade ou UF não disponíveis para busca no ViaCEP")
        return None
    
    # Formata a URL para a API do ViaCEP
    rua_encoded = urllib.parse.quote(rua)
    cidade_encoded = urllib.parse.quote(cidade)
    uf_encoded = urllib.parse.quote(uf)
    
    url = VIACEP_URL.format(uf=uf_encoded, cidade=cidade_encoded, rua=rua_encoded)
    logger.info(f"Buscando dados no ViaCEP: {url}")
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            
            # Verifica se é uma lista de resultados ou um único resultado
            if isinstance(data, list) and len(data) > 0:
                # Pega o primeiro resultado
                result = data[0]
                logger.info(f"Dados encontrados no ViaCEP: {result}")
                return result
            elif isinstance(data, dict) and 'erro' not in data:
                logger.info(f"Dados encontrados no ViaCEP: {data}")
                return data
            else:
                logger.warning("Nenhum resultado encontrado no ViaCEP")
        else:
            logger.warning(f"Erro ao buscar no ViaCEP: {response.status_code}")
    except Exception as e:
        logger.error(f"Erro ao acessar ViaCEP: {e}")
    
    return None

def buscar_cep_por_endereco(rua, cidade, driver, logger):
    """Busca CEP baseado na rua e cidade já encontradas (fallback para quando ViaCEP falha)"""
    if not rua or not cidade:
        logger.warning("Rua ou cidade não disponíveis para busca de CEP")
        return ""
    
    # Formata a query de busca
    query = f"CEP da {rua}, {cidade}"
    logger.info(f"Buscando CEP via web: {query}")
    
    # Realiza a busca no Google
    driver.get(f"https://www.google.com/search?q={requests.utils.quote(query)}")
    time.sleep(1)
    
    # Extrai o texto da página de resultados
    page_text = driver.page_source
    soup = BeautifulSoup(page_text, 'html.parser')
    text = soup.get_text(' ')
    
    # Procura por padrões de CEP no texto
    ceps = re.findall(r'\d{5}-\d{3}', text)
    
    if ceps:
        logger.info(f"CEP encontrado via web: {ceps[0]}")
        return ceps[0]
    
    # Tenta uma segunda busca com formato alternativo
    query = f"{rua}, {cidade} CEP"
    logger.info(f"Segunda tentativa via web: {query}")
    driver.get(f"https://www.google.com/search?q={requests.utils.quote(query)}")
    time.sleep(1)
    
    page_text = driver.page_source
    soup = BeautifulSoup(page_text, 'html.parser')
    text = soup.get_text(' ')
    ceps = re.findall(r'\d{5}-\d{3}', text)
    
    if ceps:
        logger.info(f"CEP encontrado na segunda tentativa via web: {ceps[0]}")
        return ceps[0]
    
    logger.warning("CEP não encontrado via web")
    return ""

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
            
            # Busca dados via ViaCEP ao final do processo
            if result.get('Address A1') and result.get('City A1'):
                logger.info("Buscando dados via ViaCEP")
                viacep_data = buscar_dados_via_viacep(
                    result['Address A1'],
                    result['City A1'],
                    m['UF'],
                    logger
                )
                
                if viacep_data:
                    # Preenche os campos com os dados do ViaCEP
                    if 'cep' in viacep_data and viacep_data['cep']:
                        result['postal code A1'] = viacep_data['cep']
                    
                    if 'bairro' in viacep_data and viacep_data['bairro']:
                        result['Bairro A1'] = viacep_data['bairro']
                    
                    if 'complemento' in viacep_data and viacep_data['complemento'] and not result.get('Complement A1'):
                        result['Complement A1'] = viacep_data['complemento']
                    
                    if 'localidade' in viacep_data and viacep_data['localidade']:
                        result['City A1'] = viacep_data['localidade']
                    
                    if 'uf' in viacep_data and viacep_data['uf']:
                        result['State A1'] = viacep_data['uf']
                else:
                    # Fallback: busca CEP via web se ViaCEP falhar
                    logger.info("Fallback: buscando CEP via web")
                    result['postal code A1'] = buscar_cep_por_endereco(
                        result['Address A1'],
                        result['City A1'],
                        driver,
                        logger
                    )
            
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
        print("Uso: python buscador_medicos.v5.py input.csv output.csv")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    run_parallel(input_file, output_file)
