#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
medicos-ai.v2.py

Aprimoramentos:
- Processamento paralelo (3 processos)
- Chrome em modo headless
- Melhor filtro de URLs
- Limite de 3MB por página
- Priorização de URLs com nome do médico
"""

import sys
import csv
import re
import requests
import logging
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
import time
from collections import Counter
import os
import hashlib
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
import math
from typing import List, Dict, Any
import json
from selenium.common.exceptions import TimeoutException

# Configurações
SEARX_URL   = "http://124.81.6.163:8092/search"
OLLAMA_URL  = "http://124.81.6.163:11434/api/generate"
USER_AGENT  = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/114.0.0.0 Safari/537.36"
)
MAX_RESULTS = 15
MAX_PAGE_SIZE = 3 * 1024 * 1024  # 3MB
NUM_PROCESSES = 3  # Número de processos paralelos

# Caminhos dos arquivos
DATA_DIR = 'data'
MULTISCRAPS_DIR = 'multiscraps'
ESPECIALIDADES_FILE = os.path.join(DATA_DIR, 'especialidades.txt')
TEXTOS_REMOVER_FILE = os.path.join(DATA_DIR, 'textos_remover.txt')
EXEMPLOS_FILE = os.path.join(DATA_DIR, 'exemplos_treinamento.txt')
EMAIL_BLACKLIST_FILE = os.path.join(DATA_DIR, 'email_blacklist.txt')
SITE_BLACKLIST_FILE = os.path.join(DATA_DIR, 'site_blacklist.txt')
LOG_FILE = os.path.join(DATA_DIR, 'buscador_medicos.log')
DEBUG_HTML_DIR = os.path.join(DATA_DIR, 'debug_html')

# Criar diretórios se não existirem
for directory in [DATA_DIR, MULTISCRAPS_DIR, DEBUG_HTML_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, 'w', 'utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger()

def carregar_lista_arquivo(nome_arquivo: str) -> List[str]:
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

# Carrega whitelist e blacklist
def load_domain_lists() -> tuple:
    whitelist = set()
    blacklist = set()
    
    # Carrega whitelist
    with open('data/whitelist.txt', 'r', encoding='utf-8') as f:
        for line in f:
            if 'http' in line or 'www' in line:
                domain = re.search(r'https?://(?:www\.)?([^/\s]+)', line)
                if domain:
                    whitelist.add(domain.group(1))
    
    # Carrega blacklist
    with open('data/blacklist.txt', 'r', encoding='utf-8') as f:
        for line in f:
            if 'http' in line or 'www' in line:
                domain = re.search(r'https?://(?:www\.)?([^/\s]+)', line)
                if domain:
                    blacklist.add(domain.group(1))
    
    return whitelist, blacklist

WHITELIST, BLACKLIST = load_domain_lists()

def is_valid_url(url: str) -> bool:
    """Verifica se a URL é válida e não é um arquivo"""
    blocked_extensions = ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.txt', '.trf', '.csv']
    return not any(url.lower().endswith(ext) for ext in blocked_extensions)

def is_site_too_large(html: str) -> bool:
    """Verifica se o site é muito grande"""
    return len(html) > MAX_PAGE_SIZE

def is_relevant_url(url: str, nome_medico: str) -> bool:
    """Verifica se a URL é relevante"""
    # Se estiver na whitelist, é relevante
    domain = re.search(r'https?://(?:www\.)?([^/\s]+)', url)
    if domain and domain.group(1) in WHITELIST:
        return True
        
    # Se estiver na blacklist, não é relevante
    if domain and domain.group(1) in BLACKLIST:
        return False
    
    # Verifica se contém o nome do médico
    nomes = nome_medico.split()
    for nome in nomes:
        if nome.lower() in url.lower():
            return True
            
    return False

def deduplicate_phones(phones: List[str]) -> List[str]:
    """Remove números de telefone duplicados"""
    seen = set()
    unique_phones = []
    for phone in phones:
        clean_phone = re.sub(r'\D', '', phone)
        if clean_phone not in seen:
            seen.add(clean_phone)
            unique_phones.append(phone)
    return unique_phones

def build_query(m: Dict[str, str]) -> str:
    """Constrói a query de busca"""
    return f"{m['Firstname']} {m['LastName']} {m['CRM']}"

def make_driver() -> webdriver.Chrome:
    """Cria e configura o driver do Chrome"""
    opts = Options()
    opts.add_argument('--headless=new')  # Modo headless
    opts.add_argument(f'user-agent={USER_AGENT}')
    opts.add_argument('--disable-gpu')  # Desabilita GPU
    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    opts.add_argument('--disable-software-rasterizer')  # Desabilita rasterizador de software
    opts.add_argument('--disable-webgl')  # Desabilita WebGL
    opts.add_argument('--disable-webgl2')  # Desabilita WebGL2
    opts.add_argument('--disable-3d-apis')  # Desabilita APIs 3D
    opts.add_argument('--disable-extensions')  # Desabilita extensões
    opts.add_argument('--disable-notifications')  # Desabilita notificações
    opts.add_argument('--disable-infobars')  # Desabilita barras de informação
    opts.add_argument('--disable-logging')  # Desabilita logging
    opts.add_argument('--log-level=3')  # Define nível de log mínimo
    opts.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
    opts.add_experimental_option('useAutomationExtension', False)
    
    # Configurações de performance
    prefs = {
        'profile.default_content_setting_values': {
            'notifications': 2,
            'images': 2,  # Desabilita carregamento de imagens
            'javascript': 1,  # Mantém JavaScript habilitado
            'plugins': 2,  # Desabilita plugins
            'popups': 2,  # Bloqueia popups
            'geolocation': 2,  # Desabilita geolocalização
            'auto_select_certificate': 2,  # Desabilita seleção automática de certificados
            'fullscreen': 2,  # Desabilita fullscreen
            'mouselock': 2,  # Desabilita mouselock
            'mixed_script': 2,  # Bloqueia scripts mistos
            'media_stream': 2,  # Desabilita stream de mídia
            'media_stream_mic': 2,  # Desabilita microfone
            'media_stream_camera': 2,  # Desabilita câmera
            'protocol_handlers': 2,  # Desabilita handlers de protocolo
            'ppapi_broker': 2,  # Desabilita broker PPAPI
            'automatic_downloads': 2,  # Desabilita downloads automáticos
            'midi_sysex': 2,  # Desabilita MIDI
            'push_messaging': 2,  # Desabilita push messaging
            'ssl_cert_decisions': 2,  # Desabilita decisões de certificado SSL
            'metro_switch_to_desktop': 2,  # Desabilita switch para desktop
            'protected_media_identifier': 2,  # Desabilita identificador de mídia protegida
            'app_banner': 2,  # Desabilita banners de app
            'site_engagement': 2,  # Desabilita engajamento de site
            'durable_storage': 2  # Desabilita armazenamento durável
        },
        'profile.managed_default_content_settings': {
            'javascript': 1  # Mantém JavaScript habilitado
        },
        'profile.cookie_controls_mode': 2,  # Bloqueia cookies de terceiros
        'profile.password_manager_enabled': False,  # Desabilita gerenciador de senhas
        'profile.default_content_settings.popups': 0,  # Bloqueia popups
        'profile.managed_default_content_settings.images': 2,  # Desabilita imagens
        'profile.managed_default_content_settings.javascript': 1,  # Mantém JavaScript
        'profile.managed_default_content_settings.plugins': 2,  # Desabilita plugins
        'profile.managed_default_content_settings.popups': 2,  # Bloqueia popups
        'profile.managed_default_content_settings.geolocation': 2,  # Desabilita geolocalização
        'profile.managed_default_content_settings.media_stream': 2,  # Desabilita stream de mídia
        'profile.managed_default_content_settings.mixed_script': 2,  # Bloqueia scripts mistos
        'profile.managed_default_content_settings.notifications': 2,  # Desabilita notificações
        'profile.managed_default_content_settings.auto_select_certificate': 2,  # Desabilita seleção automática de certificados
        'profile.managed_default_content_settings.fullscreen': 2,  # Desabilita fullscreen
        'profile.managed_default_content_settings.mouselock': 2,  # Desabilita mouselock
        'profile.managed_default_content_settings.media_stream_mic': 2,  # Desabilita microfone
        'profile.managed_default_content_settings.media_stream_camera': 2,  # Desabilita câmera
        'profile.managed_default_content_settings.protocol_handlers': 2,  # Desabilita handlers de protocolo
        'profile.managed_default_content_settings.ppapi_broker': 2,  # Desabilita broker PPAPI
        'profile.managed_default_content_settings.automatic_downloads': 2,  # Desabilita downloads automáticos
        'profile.managed_default_content_settings.midi_sysex': 2,  # Desabilita MIDI
        'profile.managed_default_content_settings.push_messaging': 2,  # Desabilita push messaging
        'profile.managed_default_content_settings.ssl_cert_decisions': 2,  # Desabilita decisões de certificado SSL
        'profile.managed_default_content_settings.metro_switch_to_desktop': 2,  # Desabilita switch para desktop
        'profile.managed_default_content_settings.protected_media_identifier': 2,  # Desabilita identificador de mídia protegida
        'profile.managed_default_content_settings.app_banner': 2,  # Desabilita banners de app
        'profile.managed_default_content_settings.site_engagement': 2,  # Desabilita engajamento de site
        'profile.managed_default_content_settings.durable_storage': 2  # Desabilita armazenamento durável
    }
    opts.add_experimental_option('prefs', prefs)
    
    driver = webdriver.Chrome(options=opts)
    logger.info("Driver Chrome iniciado em modo headless com otimizações")
    return driver

def search_searx(q: str) -> List[str]:
    """Busca no SearXNG"""
    logger.info(f"Query: {q}")
    try:
        url = f"{SEARX_URL}?q={q}&category_general=1&language=auto&time_range=&safesearch=0&theme=simple"
        logger.info(f"URL de busca SearXNG: {url}")
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            urls = []
            for result in soup.find_all('article', class_='result result-default category-general'):
                link = result.find('a', class_='url')
                if link and link.get('href'):
                    url = link['href']
                    if is_valid_url(url) and is_relevant_url(url, q):
                        urls.append(url)
            logger.info(f"SearX found {len(urls)} URLs")
            return urls
    except Exception as e:
        logger.error(f"Erro ao buscar no SearXNG: {e}")
    return []

def search_bing(q: str, driver: webdriver.Chrome) -> tuple:
    """Busca no Bing"""
    logger.info(f"Query: {q}")
    try:
        url = f"https://www.bing.com/search?q={q}"
        logger.info(f"URL de busca Bing: {url}")
        driver.get(url)
        time.sleep(2)
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        urls = []
        for result in soup.find_all('li', class_='b_algo'):
            link = result.find('a')
            if link and link.get('href'):
                url = link['href']
                if is_valid_url(url) and is_relevant_url(url, q):
                    urls.append(url)
        logger.info(f"Bing found {len(urls)} URLs")
        return urls, html
    except Exception as e:
        logger.error(f"Erro ao buscar no Bing: {e}")
    return [], ''

def download_html(url: str, driver: webdriver.Chrome, nome_medico: str) -> str:
    """Baixa o HTML de uma URL"""
    logger.info(f"Tentando baixar HTML de: {url}")
    try:
        # Verifica se a URL é válida
        if not is_valid_url(url):
            logger.info(f"URL inválida (arquivo): {url}")
            return None
            
        # Verifica se o domínio está na blacklist
        domain = re.search(r'https?://(?:www\.)?([^/\s]+)', url)
        if domain and domain.group(1) in BLACKLIST:
            logger.info(f"Site {url} está na blacklist")
            return None
            
        # Se estiver na whitelist, não verifica relevância
        is_whitelisted = domain and domain.group(1) in WHITELIST
        
        # Se não estiver na whitelist, verifica relevância
        if not is_whitelisted and not is_relevant_url(url, nome_medico):
            logger.info(f"Site {url} não é relevante")
            return None
            
        logger.info("Usando Selenium para baixar HTML")
        
        # Configura timeout de 5 minutos
        driver.set_page_load_timeout(300)  # 5 minutos em segundos
        
        try:
            driver.get(url)
            # Espera a página carregar completamente
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )
            time.sleep(2)  # Espera um pouco mais para garantir
            
            # Tenta encontrar o conteúdo principal
            main_content = None
            for selector in ['main', 'article', '.content', '#content', '.main', '#main', '.container', '#container', '.wrapper', '#wrapper']:
                try:
                    element = driver.find_element(By.CSS_SELECTOR, selector)
                    if element:
                        main_content = element.get_attribute('outerHTML')
                        break
                except:
                    continue
            
            # Se não encontrou conteúdo principal, pega todo o HTML
            html = main_content if main_content else driver.page_source
            
            # Verifica se o HTML é muito pequeno
            if len(html) < 1000:  # Aumentei o limite mínimo
                logger.warning(f"HTML muito pequeno ({len(html)} bytes), tentando novamente...")
                time.sleep(5)  # Espera mais um pouco
                html = driver.page_source
                
                # Se ainda estiver pequeno, tenta clicar em "Ver mais" ou similar
                try:
                    for button_text in ['Ver mais', 'Leia mais', 'Mostrar mais', 'Carregar mais']:
                        try:
                            button = driver.find_element(By.XPATH, f"//button[contains(text(), '{button_text}')]")
                            button.click()
                            time.sleep(2)
                        except:
                            continue
                except:
                    pass
                
                html = driver.page_source
            
            # Verifica se o site é muito grande
            if is_site_too_large(html):
                logger.info(f"Site {url} é muito grande, pulando")
                return None
                
            logger.info(f"HTML baixado via Selenium, tamanho: {len(html)}")
            
            # Salva o HTML para debug
            debug_file = os.path.join('data', 'debug_html', f"{hashlib.md5(url.encode()).hexdigest()}.html")
            os.makedirs(os.path.dirname(debug_file), exist_ok=True)
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(html)
            logger.info(f"HTML salvo em: {debug_file}")
            
            return html
            
        except TimeoutException:
            logger.warning(f"Timeout após 5 minutos ao tentar acessar: {url}")
            return None
            
    except Exception as e:
        logger.error(f"Erro ao baixar HTML: {e}")
        return None

def aggregate_and_rank(candidates: Dict[str, List[str]]) -> Dict[str, List[str]]:
    """Agrega e ranqueia os candidatos encontrados"""
    ranked = {}
    for key, values in candidates.items():
        # Remove valores vazios e duplicatas
        values = [v.strip() for v in values if v.strip()]
        values = list(dict.fromkeys(values))
        
        # Conta frequência de cada valor
        counter = Counter(values)
        
        # Ordena por frequência (mais frequente primeiro)
        ranked[key] = [v for v, _ in counter.most_common()]
    
    return ranked

def extract_candidates(html: str, url: str) -> Dict[str, List[str]]:
    """Extrai candidatos usando regex e IA com exemplos de treinamento e instruções rígidas para evitar textos longos ou irrelevantes."""
    soup = BeautifulSoup(html, 'html.parser')
    # Remove elementos de UI
    for element in soup.find_all(['script', 'style', 'nav', 'header', 'footer', 'button', 'a', 'form', 'input', 'select', 'option', 'iframe', 'meta', 'link']):
        element.decompose()
    # Remove elementos com classes de UI
    ui_classes = ['menu', 'nav', 'header', 'footer', 'button', 'form', 'search', 'login', 'signup', 'cookie', 'banner', 'popup', 'modal', 'social', 'share', 'comment', 'ad', 'advertisement']
    for element in soup.find_all(class_=lambda x: x and any(word in str(x).lower() for word in ui_classes)):
        element.decompose()
    # Remove elementos com IDs de UI
    for element in soup.find_all(id=lambda x: x and any(word in str(x).lower() for word in ui_classes)):
        element.decompose()
    # Extrai texto limpo
    text = ' '.join(soup.stripped_strings)
    logger.info(f"Texto extraído para análise: {text[:200]}...")
    # Regex primeiro
    addrs, ceps, phones, emails, comps, specialties = [], [], [], [], [], []
    addr_patterns = [
        r'(?:Rua|Avenida|Av\.|Alameda|Al\.|Travessa|Tv\.|Praça|Pç\.)\s+[^,]+(?:,\s*\d+)?(?:,\s*[^,]+)?',
        r'(?:R\.|Av\.|Al\.|Tv\.|Pç\.)\s+[^,]+(?:,\s*\d+)?(?:,\s*[^,]+)?',
        r'[^,]+(?:,\s*\d+)(?:,\s*[^,]+)?'
    ]
    for pattern in addr_patterns:
        matches = re.finditer(pattern, text, re.IGNORECASE)
        for match in matches:
            addr = match.group().strip()
            if 10 < len(addr) < 80 and not any(word in addr.lower() for word in ['compre', 'encontre', 'obtenha', 'baixe', 'clique', 'agende', 'online', 'especialista', 'recomendado', 'profissional', 'informou', 'formação', 'acadêmica', 'trata', 'dados', 'base', 'fontes', 'públicas', 'indicou', 'horários', 'pacientes', 'opinião', 'contato', 'diretamente', 'recomendamos', 'respondeu', 'pergunta', 'deseja', 'fazer', 'pergunta', 'especialistas', 'tratados', 'intuito', 'atender', 'interesse', 'público', 'aceita', 'métodos', 'pagamento', 'disponibilidade', 'honesta', 'real', 'crm', 'facebook', 'doctoralia', 'portador']):
                addrs.append(addr)
    cep_pattern = r'\b\d{5}-?\d{3}\b'
    ceps = re.findall(cep_pattern, text)
    phone_pattern = r'(?:\+55\s*)?(?:\(?\d{2}\)?\s*)?(?:9\s*)?\d{4,5}[-\s]?\d{4}'
    phones = re.findall(phone_pattern, text)
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    emails = re.findall(email_pattern, text)
    comp_pattern = r'(?:sala|andar|bloco|apto|apartamento|loja|conjunto)\s+\d+'
    comps = re.findall(comp_pattern, text.lower())
    for esp in ESPECIALIDADES:
        if esp.lower() in text.lower():
            specialties.append(esp)
    # Se não encontrou dados suficientes, usa a IA
    if not (addrs or ceps or phones or emails):
        exemplos_endereco = "Rua das Flores, 123\nAvenida Paulista, 1000\nRua Conselheiro Furtado, 500\nRua General Cornelio de Barros, 5\nRua Frei Caneca, 1282\nRua Carutapera, Quadra 37B\nRua Frei Edgar, 138"
        exemplos_endereco_inv = 'r. Pedro Alberto Lemos Fioratti, CRM 23556\nr. Francisco Gomes da Silva, portador do CRM 1377\nr. André Pinheiro... - Instituto Logos de Psicologia e Saúde | Facebook\nal. Claudio Blum atende em: Avenida Treze de Maio, 33\nr. Fabricio Foppa, CRM 13036\n"Compre vitaminas e suplementos"\n"Agende online com um dos especialistas recomendados"'
        prompt_addr = f"Extraia apenas o endereço completo do médico, sem nomes, CRM, textos longos, frases genéricas, links, textos de UI, etc.\nNUNCA retorne textos longos, nomes de médico, CRM, textos de UI, frases genéricas, links, etc.\nExemplos válidos:\n{exemplos_endereco}\nExemplos inválidos (IGNORAR):\n{exemplos_endereco_inv}\nTexto para análise:\n{text}\nRetorne apenas o endereço, sem explicações, com no máximo 80 caracteres."
        # Repita lógica semelhante para os outros campos (cep, phone, email, complement, specialty) usando exemplos do arquivo de treinamento
        # ...
        # Consulta a IA (exemplo para endereço)
        try:
            response = requests.post(
                OLLAMA_URL,
                json={
                    "model": "llama2",
                    "prompt": prompt_addr,
                    "stream": False
                },
                timeout=30
            )
            if response.status_code == 200:
                result = response.json().get('response', '').strip()
                if 10 < len(result) < 80 and not any(word in result.lower() for word in ['crm', 'facebook', 'doctoralia', 'portador']):
                    addrs.append(result)
        except Exception as e:
            logger.error(f"Erro na IA para endereço: {e}")
    def dedupe(lst):
        seen = set()
        return [x for x in lst if not (x in seen or seen.add(x))]
    emails = [email for email in dedupe(emails) if not any(bl in email.lower() for bl in EMAIL_BLACKLIST)]
    cands = {
        'address': dedupe(addrs),
        'cep': dedupe(ceps),
        'phone': dedupe(phones),
        'email': dedupe(emails),
        'complement': [c for c in dedupe(comps) if len(c.strip()) > 3 and 'salari' not in c.lower()],
        'specialty': dedupe(specialties)
    }
    for k,v in cands.items(): 
        logger.info(f"Candidates {k}: {v}")
        if k == 'address' and not v:
            logger.warning("No valid addresses found in the text!")
        if k == 'specialty' and not v:
            logger.warning("No specialties found in the text!")
    return cands

def process_medico(m: Dict[str, str], driver: webdriver.Chrome) -> Dict[str, str]:
    """Processa um médico"""
    logger.info(f"----- Processing CRM {m['CRM']} -----")
    q = build_query(m)
    urls = []
    
    # Limita a 3 URLs do SearX
    urls_searx = search_searx(q)[:3]
    urls.extend(urls_searx)
    logger.info(f"URLs do SearX (limitado a 3): {urls_searx}")
    
    # Limita a 3 URLs do Bing
    urls_bing, _ = search_bing(q, driver)
    urls_bing = urls_bing[:3]
    urls.extend(urls_bing)
    logger.info(f"URLs do Bing (limitado a 3): {urls_bing}")

    # Filtra URLs duplicadas
    seen, uf = [], []
    for u in urls:
        if u not in seen:
            seen.append(u); uf.append(u)
    logger.info(f"URLs únicas após filtro: {uf}")

    # Extrai e agrega dados
    all_c = {k: [] for k in ['address','cep','phone','email','complement','specialty']}
    
    for u in uf:
        html = download_html(u, driver, q)
        if not html: continue
        c = extract_candidates(html, u)
        for k in all_c: all_c[k].extend(c.get(k, []))
    
    ranked = aggregate_and_rank(all_c)
    
    # Processa endereço e CEP
    address = ranked.get('address', [''])[0] if ranked.get('address') else ''
    cep = ranked.get('cep', [''])[0] if ranked.get('cep') else ''
    
    # Processa telefones
    phones = ranked.get('phone', [])
    phones = deduplicate_phones(phones)
    phone = phones[0] if phones else ''
    phone2 = phones[1] if len(phones) > 1 else ''
    
    # Processa emails
    emails = ranked.get('email', [])
    emails = [e for e in emails if not any(bl in e.lower() for bl in EMAIL_BLACKLIST)]
    email = emails[0] if emails else ''
    email2 = emails[1] if len(emails) > 1 else ''
    
    # Processa complemento
    complement = ranked.get('complement', [''])[0] if ranked.get('complement') else ''
    
    # Processa especialidade
    specialty = ranked.get('specialty', [''])[0] if ranked.get('specialty') else ''
    
    # Processa endereço via ViaCEP
    if cep:
        cep_data = descobrir_cidade(cep, address)
        if cep_data:
            address = cep_data.get('logradouro', '')
            complement = cep_data.get('complemento', '')
            cep = cep_data.get('cep', '')
            city = cep_data.get('cidade', '')
            state = cep_data.get('estado', '')
            neighborhood = cep_data.get('bairro', '')
        else:
            city = ''
            state = ''
            neighborhood = ''
    else:
        city = ''
        state = ''
        neighborhood = ''
    
    # Prepara dados finais
    dados_final = {
        'Endereco Completo A1': f"{address}, {complement}, {neighborhood}, {city} - {state}, {cep}".strip(' ,-'),
        'Address A1': address,
        'Numero A1': '',
        'Complement A1': complement,
        'Bairro A1': neighborhood,
        'postal code A1': cep,
        'City A1': city,
        'State A1': state,
        'Phone A1': phone,
        'Phone A2': phone2,
        'Cell phone A1': phone if '9' in phone else '',
        'Cell phone A2': phone2 if '9' in phone2 else '',
        'E-mail A1': email,
        'E-mail A2': email2,
        'Medical specialty': specialty
    }
    
    # Remove campos vazios
    dados_final = {k: v for k, v in dados_final.items() if v}
    
    # Remove duplicatas
    for k in dados_final:
        if isinstance(dados_final[k], str):
            dados_final[k] = dados_final[k].strip()
    
    return {**m, **dados_final}

def process_chunk(chunk: List[Dict[str, str]], output_file: str, fieldnames: List[str]) -> None:
    """Processa um chunk de médicos"""
    driver = make_driver()
    try:
        with open(output_file, 'a', newline='', encoding='utf-8') as outf:
            writer = csv.DictWriter(outf, fieldnames=fieldnames, delimiter=',')
            for row in chunk:
                res = process_medico(row, driver)
                out_row = {
                    k: (res.get(k, '') if not row.get(k, '').strip() else row[k])
                    for k in fieldnames
                }
                writer.writerow(out_row)
    finally:
        driver.quit()

def run(inp: str, outp: str) -> None:
    """Executa o processamento em paralelo"""
    # Lê o arquivo de entrada
    with open(inp, newline='', encoding='utf-8') as inf:
        reader = csv.DictReader(inf, delimiter=',')
        fieldnames = reader.fieldnames + ['Address A1','Complement A1','postal code A1','City A1','State A1',
                                        'Phone A1','Phone A2','Cell phone A1','Cell phone A2','E-mail A1','E-mail A2']
        
        # Divide os médicos em chunks
        medicos = list(reader)
        chunk_size = math.ceil(len(medicos) / NUM_PROCESSES)
        chunks = [medicos[i:i + chunk_size] for i in range(0, len(medicos), chunk_size)]
        
        # Cria arquivos temporários para cada processo na pasta multiscraps
        temp_files = [os.path.join(MULTISCRAPS_DIR, f"temp_{i}.csv") for i in range(NUM_PROCESSES)]
        
        # Escreve o cabeçalho em cada arquivo temporário
        for temp_file in temp_files:
            with open(temp_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=',')
                writer.writeheader()
        
        # Processa os chunks em paralelo
        with ProcessPoolExecutor(max_workers=NUM_PROCESSES) as executor:
            futures = [executor.submit(process_chunk, chunk, temp_file, fieldnames) 
                      for chunk, temp_file in zip(chunks, temp_files)]
            
            # Espera todos os processos terminarem
            for future in futures:
                future.result()
        
        # Combina os arquivos temporários
        with open(outp, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter=',')
            writer.writeheader()
            
            for temp_file in temp_files:
                with open(temp_file, 'r', newline='', encoding='utf-8') as infile:
                    reader = csv.DictReader(infile, delimiter=',')
                    for row in reader:
                        writer.writerow(row)
                # Não remove mais os arquivos temporários
                logger.info(f"Arquivo temporário mantido em: {temp_file}")
    
    logger.info(f"Processing complete. Output: {outp}")

def descobrir_cidade(cep: str, endereco: str) -> Dict[str, str]:
    """Consulta o ViaCEP para obter dados do endereço"""
    try:
        # Remove caracteres não numéricos do CEP
        cep = re.sub(r'\D', '', cep)
        if len(cep) != 8:
            return None
            
        # Consulta o ViaCEP
        url = f'https://viacep.com.br/ws/{cep}/json/'
        response = requests.get(url, timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            if 'erro' not in data:
                return {
                    'logradouro': data.get('logradouro', ''),
                    'complemento': data.get('complemento', ''),
                    'bairro': data.get('bairro', ''),
                    'cidade': data.get('localidade', ''),
                    'estado': data.get('uf', ''),
                    'cep': data.get('cep', '')
                }
    except Exception as e:
        logger.error(f"Erro ao consultar ViaCEP: {e}")
    return None

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: python medicos-ai.v2.py medicos_input.csv medicos_output.csv')
    else:
        run(sys.argv[1], sys.argv[2]) 