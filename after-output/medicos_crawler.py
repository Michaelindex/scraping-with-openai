#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Crawler/Scraping para Profissionais da Saúde

Este script realiza a extração de dados de médicos, com foco em especialidades e e-mails,
utilizando uma abordagem híbrida com múltiplos fallbacks.

Requisitos:
- Python 3.6+
- Bibliotecas: requests, beautifulsoup4, selenium, playwright, pandas, tqdm

Uso:
    python medicos_crawler.py [arquivo_entrada] [arquivo_saida]

Exemplo:
    python medicos_crawler.py medicos.txt resultados.csv
"""

import os
import sys
import time
import json
import random
import re
import csv
import logging
import argparse
import concurrent.futures
import traceback
from datetime import datetime
from urllib.parse import quote_plus, urlparse
from typing import Dict, List, Tuple, Set, Any, Optional, Union
import threading

# Bibliotecas de terceiros - instalação necessária
try:
    import requests
    from requests.adapters import HTTPAdapter
    from requests.packages.urllib3.util.retry import Retry
    from bs4 import BeautifulSoup
    import pandas as pd
    from tqdm import tqdm
    
    # Importações condicionais para Selenium
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options as ChromeOptions
        from selenium.webdriver.chrome.service import Service as ChromeService
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.common.exceptions import (
            TimeoutException, NoSuchElementException, 
            WebDriverException, StaleElementReferenceException
        )
        SELENIUM_AVAILABLE = True
    except ImportError:
        SELENIUM_AVAILABLE = False
        print("Aviso: Selenium não está disponível. Alguns recursos serão limitados.")
    
    # Importações condicionais para Playwright
    try:
        from playwright.sync_api import sync_playwright
        PLAYWRIGHT_AVAILABLE = True
    except ImportError:
        PLAYWRIGHT_AVAILABLE = False
        print("Aviso: Playwright não está disponível. Alguns recursos serão limitados.")
        
except ImportError as e:
    print(f"Erro: Biblioteca necessária não encontrada: {e}")
    print("Por favor, instale as dependências com: pip install requests beautifulsoup4 selenium playwright pandas tqdm")
    sys.exit(1)

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("medicos_crawler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("MedicosCrawler")

# Constantes e configurações
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59"
]

# URLs e endpoints
SEARX_URL = "http://124.81.6.163:8092/search"
OLLAMA_URL = "http://124.81.6.163:11434/api/generate"

# Mapeamento de UFs para nomes completos dos estados
UF_TO_ESTADO = {
    'AC': 'Acre',
    'AL': 'Alagoas',
    'AP': 'Amapá',
    'AM': 'Amazonas',
    'BA': 'Bahia',
    'CE': 'Ceará',
    'DF': 'Distrito Federal',
    'ES': 'Espírito Santo',
    'GO': 'Goiás',
    'MA': 'Maranhão',
    'MT': 'Mato Grosso',
    'MS': 'Mato Grosso do Sul',
    'MG': 'Minas Gerais',
    'PA': 'Pará',
    'PB': 'Paraíba',
    'PR': 'Paraná',
    'PE': 'Pernambuco',
    'PI': 'Piauí',
    'RJ': 'Rio de Janeiro',
    'RN': 'Rio Grande do Norte',
    'RS': 'Rio Grande do Sul',
    'RO': 'Rondônia',
    'RR': 'Roraima',
    'SC': 'Santa Catarina',
    'SP': 'São Paulo',
    'SE': 'Sergipe',
    'TO': 'Tocantins'
}

# Lista de especialidades médicas para validação
ESPECIALIDADES_MEDICAS = [
    "acupuntura", "alergia e imunologia", "anestesiologia", "angiologia", "cancerologia",
    "cardiologia", "cirurgia cardiovascular", "cirurgia da mão", "cirurgia de cabeça e pescoço",
    "cirurgia do aparelho digestivo", "cirurgia geral", "cirurgia pediátrica",
    "cirurgia plástica", "cirurgia torácica", "cirurgia vascular", "clínica médica",
    "coloproctologia", "dermatologia", "endocrinologia", "endoscopia", "gastroenterologia",
    "genética médica", "geriatria", "ginecologia", "hematologia", "homeopatia", "infectologia",
    "mastologia", "medicina de emergência", "medicina de família", "medicina do trabalho",
    "medicina de tráfego", "medicina esportiva", "medicina física e reabilitação",
    "medicina intensiva", "medicina legal", "medicina nuclear", "medicina preventiva",
    "nefrologia", "neurocirurgia", "neurologia", "nutrologia", "obstetrícia", "oftalmologia",
    "oncologia", "ortopedia", "otorrinolaringologia", "patologia", "pediatria", "pneumologia",
    "psiquiatria", "radiologia", "radioterapia", "reumatologia", "urologia"
]

# Padrões de expressões regulares
EMAIL_PATTERN = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
PHONE_PATTERN = r'(?:\+55|0)?(?:\s|\()?(\d{2})(?:\s|\))?(?:\s|\-)?(9?\d{4})(?:\s|\-)?(\d{4})'

# Classe principal do crawler
class MedicosCrawler:
    """Classe principal para extração de dados de médicos."""
    
    def __init__(self, config=None):
        """
        Inicializa o crawler com configurações personalizáveis.
        
        Args:
            config (dict, optional): Configurações personalizadas.
        """
        self.config = {
            'max_retries': 3,
            'timeout': 30,
            'delay_min': 1,
            'delay_max': 3,
            'max_workers': min(4, os.cpu_count()),
            'batch_size': 5,
            'use_selenium': SELENIUM_AVAILABLE,
            'use_playwright': PLAYWRIGHT_AVAILABLE,
            'use_searx': True,
            'use_ollama': True,
            'debug': False
        }
        
        if config:
            self.config.update(config)
            
        self.session = self._create_session()
        self.driver = None
        self.playwright = None
        self.browser = None
        self.whitelist_urls = {}
        self.results_cache = {}
        
        # Carregar whitelist de URLs por estado
        self._load_whitelist()
        
    def _load_whitelist(self):
        """Carrega a whitelist de URLs por estado a partir do arquivo CSV."""
        try:
            # Tenta carregar do arquivo padrão
            whitelist_file = "Iniciar pesquisa - Iniciar pesquisa.csv"
            if not os.path.exists(whitelist_file):
                logger.warning(f"Arquivo de whitelist não encontrado: {whitelist_file}")
                return
                
            df = pd.read_csv(whitelist_file)
            for _, row in df.iterrows():
                estado_uf = row['Estado (UF)'].split('(')[1].split(')')[0]
                urls = [url.strip() for url in row['URLs dos Portais Recomendados'].split(',')]
                self.whitelist_urls[estado_uf] = urls
                
            logger.info(f"Whitelist carregada com sucesso: {len(self.whitelist_urls)} estados")
        except Exception as e:
            logger.error(f"Erro ao carregar whitelist: {e}")
    
    def _create_session(self):
        """Cria uma sessão HTTP com retry e timeout configurados."""
        session = requests.Session()
        retry_strategy = Retry(
            total=self.config['max_retries'],
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Configurar headers padrão
        session.headers.update({
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        
        return session
    
    def _get_selenium_driver(self):
        """Inicializa e retorna um driver do Selenium."""
        if not SELENIUM_AVAILABLE:
            logger.error("Selenium não está disponível")
            return None
            
        try:
            options = ChromeOptions()
            options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")
            options.add_argument(f"--user-agent={random.choice(USER_AGENTS)}")
            
            # Configurações adicionais para melhorar estabilidade
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-popup-blocking")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--disable-infobars")
            options.add_argument("--disable-notifications")
            options.add_argument("--disable-web-security")
            options.add_argument("--ignore-certificate-errors")
            options.add_argument("--enable-unsafe-swiftshader")
            
            # Desabilitar imagens para melhorar performance
            prefs = {
                "profile.managed_default_content_settings.images": 2,
                "profile.default_content_setting_values.notifications": 2,
                "profile.managed_default_content_settings.javascript": 1
            }
            options.add_experimental_option("prefs", prefs)
            
            # Configurar timeouts
            service = ChromeService()
            driver = webdriver.Chrome(service=service, options=options)
            driver.set_page_load_timeout(30)
            driver.set_script_timeout(30)
            
            return driver
        except Exception as e:
            logger.error(f"Erro ao inicializar Selenium: {e}")
            return None
    
    def _get_playwright_browser(self):
        """Inicializa e retorna um browser do Playwright."""
        if not PLAYWRIGHT_AVAILABLE:
            logger.error("Playwright não está disponível")
            return None
            
        try:
            # Criar um novo contexto para cada thread
            if not hasattr(self, '_playwright_contexts'):
                self._playwright_contexts = {}
                
            thread_id = threading.get_ident()
            if thread_id not in self._playwright_contexts:
                playwright = sync_playwright().start()
                browser = playwright.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-dev-shm-usage"]
                )
                self._playwright_contexts[thread_id] = {
                    'playwright': playwright,
                    'browser': browser
                }
                
            return self._playwright_contexts[thread_id]['browser']
        except Exception as e:
            logger.error(f"Erro ao inicializar Playwright: {e}")
            return None
    
    def _random_delay(self):
        """Adiciona um delay aleatório entre requisições."""
        delay = random.uniform(self.config['delay_min'], self.config['delay_max'])
        time.sleep(delay)
    
    def _extract_emails(self, text):
        """Extrai endereços de e-mail de um texto."""
        if not text:
            return []
        return re.findall(EMAIL_PATTERN, text)
    
    def _extract_phones(self, text):
        """Extrai números de telefone de um texto."""
        if not text:
            return []
        return re.findall(PHONE_PATTERN, text)
    
    def _normalize_specialty(self, specialty):
        """Normaliza uma especialidade médica para formato padrão."""
        if not specialty or pd.isna(specialty):
            return None
            
        # Converter para string se for float
        if isinstance(specialty, float):
            return None
            
        specialty = str(specialty).lower().strip()
        
        # Verificar correspondência exata
        for esp in ESPECIALIDADES_MEDICAS:
            if esp in specialty:
                return esp
                
        # Usar IA para classificar se disponível
        if self.config['use_ollama']:
            return self._classify_specialty_with_ollama(specialty)
            
        return specialty
    
    def _classify_specialty_with_ollama(self, text):
        """Usa o Ollama para classificar uma especialidade médica."""
        if not self.config['use_ollama']:
            return text
            
        try:
            prompt = f"""
            Identifique a especialidade médica no seguinte texto e retorne APENAS o nome da especialidade, 
            sem explicações adicionais. Se não houver especialidade clara, retorne "não identificada".
            
            Texto: "{text}"
            
            Lista de especialidades válidas:
            {', '.join(ESPECIALIDADES_MEDICAS)}
            """
            
            data = {
                "model": "llama3.1:8b",
                "prompt": prompt,
                "stream": False
            }
            
            response = requests.post(OLLAMA_URL, json=data, timeout=10)
            
            if response.status_code == 200:
                result = response.json()
                if 'response' in result:
                    specialty = result['response'].strip().lower()
                    
                    # Verificar se a resposta é uma especialidade válida
                    for esp in ESPECIALIDADES_MEDICAS:
                        if esp in specialty:
                            return esp
            
            return text
        except Exception as e:
            logger.error(f"Erro ao classificar especialidade com Ollama: {e}")
            return text
    
    def _extract_data_with_ollama(self, text, medico_info):
        """
        Usa o Ollama para extrair dados estruturados de um texto.
        
        Args:
            text (str): Texto a ser analisado
            medico_info (dict): Informações do médico para contexto
            
        Returns:
            dict: Dados extraídos (especialidade, email, telefone)
        """
        if not self.config['use_ollama'] or not text:
            return {}
            
        try:
            nome_completo = f"{medico_info.get('Firstname', '')} {medico_info.get('LastName', '')}".strip()
            crm = medico_info.get('CRM', '')
            uf = medico_info.get('UF', '')
            
            prompt = f"""
            Analise o texto abaixo sobre o médico {nome_completo} (CRM {crm}/{uf}) e extraia APENAS as seguintes informações:
            1. Especialidade médica
            2. Email de contato
            3. Telefone de contato
            
            Texto: "{text}"
            
            Responda APENAS em formato JSON com as chaves "especialidade", "email" e "telefone".
            Se alguma informação não for encontrada, deixe o valor como null.
            """
            
            data = {
                "model": "llama3.1:8b",
                "prompt": prompt,
                "stream": False
            }
            
            response = requests.post(OLLAMA_URL, json=data, timeout=15)
            
            if response.status_code == 200:
                result = response.json()
                if 'response' in result:
                    # Tentar extrair JSON da resposta
                    try:
                        # Encontrar padrão JSON na resposta
                        json_match = re.search(r'({.*})', result['response'], re.DOTALL)
                        if json_match:
                            json_str = json_match.group(1)
                            extracted_data = json.loads(json_str)
                            
                            # Normalizar especialidade
                            if extracted_data.get('especialidade'):
                                extracted_data['especialidade'] = self._normalize_specialty(extracted_data['especialidade'])
                                
                            return extracted_data
                    except json.JSONDecodeError:
                        logger.warning("Não foi possível decodificar JSON da resposta do Ollama")
            
            return {}
        except Exception as e:
            logger.error(f"Erro ao extrair dados com Ollama: {e}")
            return {}
    
    def _search_with_searx(self, query, num_results=5):
        """
        Realiza uma busca usando o SearXNG.
        
        Args:
            query (str): Consulta de busca
            num_results (int): Número de resultados a retornar
            
        Returns:
            list: Lista de resultados (dicts com title, url, snippet)
        """
        if not self.config['use_searx']:
            return []
            
        try:
            params = {
                'q': query,
                'format': 'json',
                'engines': 'google,bing,duckduckgo',
                'language': 'pt-BR',
                'categories': 'general',
                'time_range': '',
                'safesearch': 0,
                'max_results': num_results
            }
            
            headers = {
                'User-Agent': random.choice(USER_AGENTS),
                'Accept': 'application/json'
            }
            
            response = requests.get(SEARX_URL, params=params, headers=headers, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                if 'results' in data:
                    return data['results']
            
            return []
        except Exception as e:
            logger.error(f"Erro na busca com SearXNG: {e}")
            return []
    
    def _fetch_with_requests(self, url):
        """
        Busca uma página usando requests.
        
        Args:
            url (str): URL para buscar
            
        Returns:
            str: Conteúdo HTML da página ou None em caso de erro
        """
        try:
            # Atualizar User-Agent aleatoriamente
            self.session.headers.update({'User-Agent': random.choice(USER_AGENTS)})
            
            response = self.session.get(url, timeout=self.config['timeout'])
            
            if response.status_code == 200:
                return response.text
            else:
                logger.warning(f"Status code não-200 para {url}: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Erro ao buscar {url} com requests: {e}")
            return None
    
    def _fetch_with_selenium(self, url):
        """
        Busca uma página usando Selenium.
        
        Args:
            url (str): URL para buscar
            
        Returns:
            str: Conteúdo HTML da página ou None em caso de erro
        """
        if not self.config['use_selenium']:
            return None
            
        driver = self._get_selenium_driver()
        if not driver:
            return None
            
        try:
            driver.get(url)
            
            # Esperar pelo carregamento da página
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Scroll para carregar conteúdo dinâmico
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
            time.sleep(1)
            
            return driver.page_source
        except Exception as e:
            logger.error(f"Erro ao buscar {url} com Selenium: {e}")
            return None
    
    def _fetch_with_playwright(self, url):
        """
        Busca uma página usando Playwright.
        
        Args:
            url (str): URL para buscar
            
        Returns:
            str: Conteúdo HTML da página ou None em caso de erro
        """
        if not self.config['use_playwright']:
            return None
            
        browser = self._get_playwright_browser()
        if not browser:
            return None
            
        try:
            page = browser.new_page(user_agent=random.choice(USER_AGENTS))
            page.goto(url, timeout=30000)
            
            # Esperar pelo carregamento completo
            page.wait_for_load_state("networkidle")
            
            # Scroll para carregar conteúdo dinâmico
            page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
            page.wait_for_timeout(1000)
            
            content = page.content()
            page.close()
            
            return content
        except Exception as e:
            logger.error(f"Erro ao buscar {url} com Playwright: {e}")
            return None
    
    def _fetch_page(self, url, use_js=False):
        """
        Busca uma página web usando a estratégia mais adequada.
        
        Args:
            url (str): URL para buscar
            use_js (bool): Se True, usa Selenium/Playwright para renderizar JavaScript
            
        Returns:
            str: Conteúdo HTML da página ou None em caso de erro
        """
        if not url:
            return None
            
        # Verificar cache
        cache_key = f"page_{url}"
        if cache_key in self.results_cache:
            return self.results_cache[cache_key]
        
        content = None
        
        # Estratégia 1: Requests (para páginas sem JavaScript)
        if not use_js:
            content = self._fetch_with_requests(url)
            
        # Estratégia 2: Selenium (para páginas com JavaScript)
        if (content is None or use_js) and self.config['use_selenium']:
            content = self._fetch_with_selenium(url)
            
        # Estratégia 3: Playwright (fallback para Selenium)
        if (content is None or use_js) and self.config['use_playwright']:
            content = self._fetch_with_playwright(url)
        
        # Armazenar em cache
        if content:
            self.results_cache[cache_key] = content
            
        return content
    
    def _extract_from_html(self, html, medico_info):
        """
        Extrai dados de um HTML usando BeautifulSoup.
        
        Args:
            html (str): Conteúdo HTML
            medico_info (dict): Informações do médico para contexto
            
        Returns:
            dict: Dados extraídos (especialidade, email, telefone)
        """
        if not html:
            return {}
            
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Extrair texto completo para análise
            text = soup.get_text(" ", strip=True)
            
            # Extrair emails do HTML
            emails = self._extract_emails(html)
            
            # Extrair telefones do HTML
            phones = self._extract_phones(html)
            
            # Usar Ollama para extrair dados estruturados
            ollama_data = self._extract_data_with_ollama(text, medico_info)
            
            # Combinar resultados
            result = {
                'especialidade': ollama_data.get('especialidade'),
                'email': ollama_data.get('email') or (emails[0] if emails else None),
                'telefone': ollama_data.get('telefone') or (phones[0] if phones else None)
            }
            
            return result
        except Exception as e:
            logger.error(f"Erro ao extrair dados do HTML: {e}")
            return {}
    
    def _search_crm_portal(self, medico_info):
        """
        Busca informações no portal do CRM do estado.
        
        Args:
            medico_info (dict): Informações do médico
            
        Returns:
            dict: Dados extraídos (especialidade, email, telefone)
        """
        uf = medico_info.get('UF')
        crm = medico_info.get('CRM')
        
        if not uf or not crm:
            return {}
            
        # Mapeamento de URLs dos CRMs por estado
        crm_urls = {
            'SP': f"https://www.cremesp.org.br/?siteAcao=ConsultaMedicos&numcrm={crm}",
            'RJ': f"https://www.cremerj.org.br/consulta/",
            'MG': f"https://www.crmmg.org.br/busca-medicos.php?crm={crm}",
            'BA': f"https://www.cremeb.org.br/index.php/buscar-medicos/",
            'PR': f"https://www.crmpr.org.br/busca-medico/",
            'RS': f"https://www.cremers.org.br/index.php?indice=15",
            'SC': f"https://www.portalmedico.org.br/busca-medico/",
            # Adicionar outros estados conforme necessário
        }
        
        # URL padrão do CFM para estados não mapeados
        default_url = f"https://portal.cfm.org.br/busca-medicos/"
        
        # Obter URL do CRM para o estado
        url = crm_urls.get(uf, default_url)
        
        try:
            # Buscar página do CRM (geralmente requer JavaScript)
            html = self._fetch_page(url, use_js=True)
            
            if not html:
                return {}
                
            # Extrair dados do HTML
            return self._extract_from_html(html, medico_info)
        except Exception as e:
            logger.error(f"Erro ao buscar no portal do CRM {uf}: {e}")
            return {}
    
    def _search_whitelist_sites(self, medico_info):
        """
        Busca informações nos sites da whitelist para o estado.
        
        Args:
            medico_info (dict): Informações do médico
            
        Returns:
            dict: Dados extraídos (especialidade, email, telefone)
        """
        uf = medico_info.get('UF')
        nome = f"{medico_info.get('Firstname', '')} {medico_info.get('LastName', '')}".strip()
        crm = medico_info.get('CRM')
        
        if not uf or not nome:
            return {}
            
        # Obter URLs da whitelist para o estado
        urls = self.whitelist_urls.get(uf, [])
        
        if not urls:
            return {}
            
        results = []
        
        # Buscar em cada URL da whitelist
        for base_url in urls[:2]:  # Limitar a 2 sites por médico para performance
            try:
                # Para Doctoralia
                if "doctoralia.com.br" in base_url:
                    search_url = f"{base_url}busca?q={quote_plus(nome)}&loc={quote_plus(UF_TO_ESTADO.get(uf, uf))}"
                    html = self._fetch_page(search_url, use_js=True)
                    
                    if html:
                        data = self._extract_from_html(html, medico_info)
                        if data.get('especialidade') or data.get('email'):
                            results.append(data)
                
                # Para BoaConsulta
                elif "boaconsulta.com" in base_url:
                    search_url = f"{base_url}busca?q={quote_plus(nome)}&specialtyId=&locationName={quote_plus(UF_TO_ESTADO.get(uf, uf))}"
                    html = self._fetch_page(search_url, use_js=True)
                    
                    if html:
                        data = self._extract_from_html(html, medico_info)
                        if data.get('especialidade') or data.get('email'):
                            results.append(data)
                            
                # Para outros sites da whitelist
                else:
                    search_url = f"{base_url}?q={quote_plus(nome)}&crm={crm}&uf={uf}"
                    html = self._fetch_page(search_url, use_js=True)
                    
                    if html:
                        data = self._extract_from_html(html, medico_info)
                        if data.get('especialidade') or data.get('email'):
                            results.append(data)
                            
            except Exception as e:
                logger.error(f"Erro ao buscar em {base_url}: {e}")
                continue
                
        # Combinar resultados
        if not results:
            return {}
            
        combined = {}
        
        # Priorizar especialidade e email
        for result in results:
            if result.get('especialidade') and not combined.get('especialidade'):
                combined['especialidade'] = result['especialidade']
                
            if result.get('email') and not combined.get('email'):
                combined['email'] = result['email']
                
            if result.get('telefone') and not combined.get('telefone'):
                combined['telefone'] = result['telefone']
                
            # Se já temos especialidade e email, podemos parar
            if combined.get('especialidade') and combined.get('email'):
                break
                
        return combined
    
    def _search_with_searxng(self, medico_info):
        """
        Busca informações usando o SearXNG.
        
        Args:
            medico_info (dict): Informações do médico
            
        Returns:
            dict: Dados extraídos (especialidade, email, telefone)
        """
        if not self.config['use_searx']:
            return {}
            
        nome = f"{medico_info.get('Firstname', '')} {medico_info.get('LastName', '')}".strip()
        crm = medico_info.get('CRM')
        uf = medico_info.get('UF')
        
        if not nome or not crm or not uf:
            return {}
            
        # Construir query de busca
        query = f"médico {nome} CRM {crm} {uf} especialidade"
        
        # Realizar busca
        search_results = self._search_with_searx(query, num_results=3)
        
        if not search_results:
            return {}
            
        results = []
        
        # Analisar cada resultado de busca
        for result in search_results:
            try:
                url = result.get('url')
                if not url:
                    continue
                    
                # Verificar se é um site confiável
                domain = urlparse(url).netloc
                if any(blocked in domain for blocked in ['facebook.com', 'instagram.com', 'twitter.com', 'youtube.com']):
                    continue
                    
                # Buscar página
                html = self._fetch_page(url, use_js=True)
                
                if html:
                    data = self._extract_from_html(html, medico_info)
                    if data.get('especialidade') or data.get('email'):
                        results.append(data)
                        
            except Exception as e:
                logger.error(f"Erro ao processar resultado de busca: {e}")
                continue
                
        # Combinar resultados
        if not results:
            return {}
            
        combined = {}
        
        # Priorizar especialidade e email
        for result in results:
            if result.get('especialidade') and not combined.get('especialidade'):
                combined['especialidade'] = result['especialidade']
                
            if result.get('email') and not combined.get('email'):
                combined['email'] = result['email']
                
            if result.get('telefone') and not combined.get('telefone'):
                combined['telefone'] = result['telefone']
                
            # Se já temos especialidade e email, podemos parar
            if combined.get('especialidade') and combined.get('email'):
                break
                
        return combined
    
    def process_medico(self, medico_info):
        """
        Processa um médico, buscando informações em múltiplas fontes.
        
        Args:
            medico_info (dict): Informações do médico
            
        Returns:
            dict: Dados extraídos e enriquecidos
        """
        nome = f"{medico_info.get('Firstname', '')} {medico_info.get('LastName', '')}".strip()
        crm = medico_info.get('CRM')
        uf = medico_info.get('UF')
        
        logger.info(f"Processando médico: {nome} (CRM {crm}/{uf})")
        
        # Verificar cache
        cache_key = f"medico_{crm}_{uf}"
        if cache_key in self.results_cache:
            return self.results_cache[cache_key]
        
        # Inicializar resultado com dados básicos
        result = {
            'nome': nome,
            'crm': crm,
            'uf': uf,
            'especialidade': None,
            'email': None,
            'telefone': None,
            'fonte': None
        }
        
        # Verificar se já temos email no arquivo de entrada
        if medico_info.get('E-mail A1'):
            result['email'] = medico_info['E-mail A1']
            
        # Verificar se já temos especialidade no arquivo de entrada
        if medico_info.get('Medical specialty'):
            result['especialidade'] = self._normalize_specialty(medico_info['Medical specialty'])
        
        # Se já temos especialidade e email, retornar imediatamente
        if result['especialidade'] and result['email']:
            logger.info(f"Dados já disponíveis para {nome} (CRM {crm}/{uf})")
            self.results_cache[cache_key] = result
            return result
            
        # Adicionar delay para evitar sobrecarga
        self._random_delay()
        
        # Estratégia 1: Portal do CRM
        if not (result['especialidade'] and result['email']):
            try:
                crm_data = self._search_crm_portal(medico_info)
                
                if crm_data.get('especialidade'):
                    result['especialidade'] = crm_data['especialidade']
                    result['fonte'] = 'CRM'
                    
                if crm_data.get('email'):
                    result['email'] = crm_data['email']
                    result['fonte'] = 'CRM'
                    
                if crm_data.get('telefone') and not result.get('telefone'):
                    result['telefone'] = crm_data['telefone']
            except Exception as e:
                logger.error(f"Erro na estratégia CRM para {nome}: {e}")
        
        # Estratégia 2: Sites da whitelist
        if not (result['especialidade'] and result['email']):
            try:
                whitelist_data = self._search_whitelist_sites(medico_info)
                
                if whitelist_data.get('especialidade') and not result['especialidade']:
                    result['especialidade'] = whitelist_data['especialidade']
                    result['fonte'] = 'Whitelist'
                    
                if whitelist_data.get('email') and not result['email']:
                    result['email'] = whitelist_data['email']
                    result['fonte'] = 'Whitelist'
                    
                if whitelist_data.get('telefone') and not result.get('telefone'):
                    result['telefone'] = whitelist_data['telefone']
            except Exception as e:
                logger.error(f"Erro na estratégia Whitelist para {nome}: {e}")
        
        # Estratégia 3: SearXNG
        if not (result['especialidade'] and result['email']) and self.config['use_searx']:
            try:
                searx_data = self._search_with_searxng(medico_info)
                
                if searx_data.get('especialidade') and not result['especialidade']:
                    result['especialidade'] = searx_data['especialidade']
                    result['fonte'] = 'SearXNG'
                    
                if searx_data.get('email') and not result['email']:
                    result['email'] = searx_data['email']
                    result['fonte'] = 'SearXNG'
                    
                if searx_data.get('telefone') and not result.get('telefone'):
                    result['telefone'] = searx_data['telefone']
            except Exception as e:
                logger.error(f"Erro na estratégia SearXNG para {nome}: {e}")
        
        # Armazenar em cache
        self.results_cache[cache_key] = result
        
        logger.info(f"Concluído: {nome} - Especialidade: {result['especialidade']} - Email: {result['email']}")
        return result
    
    def process_batch(self, batch):
        """
        Processa um lote de médicos em paralelo.
        
        Args:
            batch (list): Lista de dicionários com informações dos médicos
            
        Returns:
            list: Resultados processados
        """
        results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.config['max_workers']) as executor:
            future_to_medico = {executor.submit(self.process_medico, medico): medico for medico in batch}
            
            for future in tqdm(concurrent.futures.as_completed(future_to_medico), total=len(batch), desc="Processando lote"):
                medico = future_to_medico[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"Erro ao processar médico {medico.get('CRM')}/{medico.get('UF')}: {e}")
                    logger.error(traceback.format_exc())
                    
        return results
    
    def process_file(self, input_file, output_file):
        """
        Processa um arquivo de médicos.
        
        Args:
            input_file (str): Caminho para o arquivo de entrada
            output_file (str): Caminho para o arquivo de saída
            
        Returns:
            int: Número de médicos processados
        """
        try:
            # Verificar formato do arquivo
            if input_file.endswith('.csv'):
                df = pd.read_csv(input_file)
                medicos = df.to_dict('records')
            elif input_file.endswith('.txt'):
                # Assumir formato específico do arquivo fornecido
                medicos = []
                with open(input_file, 'r', encoding='utf-8') as f:
                    header = f.readline().strip().split(',')
                    for line in f:
                        values = line.strip().split(',')
                        if len(values) >= len(header):
                            medico = {header[i]: values[i] for i in range(len(header))}
                            medicos.append(medico)
            else:
                logger.error(f"Formato de arquivo não suportado: {input_file}")
                return 0
                
            logger.info(f"Carregados {len(medicos)} médicos do arquivo {input_file}")
            
            # Processar em lotes
            all_results = []
            batch_size = self.config['batch_size']
            
            for i in range(0, len(medicos), batch_size):
                batch = medicos[i:i+batch_size]
                logger.info(f"Processando lote {i//batch_size + 1}/{(len(medicos) + batch_size - 1)//batch_size}")
                
                batch_results = self.process_batch(batch)
                all_results.extend(batch_results)
                
                # Salvar resultados parciais
                self._save_results(all_results, output_file)
                
                logger.info(f"Resultados parciais salvos em {output_file}")
                
            logger.info(f"Processamento concluído. {len(all_results)} médicos processados.")
            return len(all_results)
            
        except Exception as e:
            logger.error(f"Erro ao processar arquivo: {e}")
            logger.error(traceback.format_exc())
            return 0
    
    def _save_results(self, results, output_file):
        """
        Salva os resultados em um arquivo CSV.
        
        Args:
            results (list): Lista de resultados
            output_file (str): Caminho para o arquivo de saída
        """
        try:
            df = pd.DataFrame(results)
            df.to_csv(output_file, index=False, encoding='utf-8')
        except Exception as e:
            logger.error(f"Erro ao salvar resultados: {e}")
    
    def cleanup(self):
        """Libera recursos utilizados pelo crawler."""
        try:
            if self.driver:
                self.driver.quit()
                self.driver = None
                
            # Limpar todos os contextos do Playwright
            if hasattr(self, '_playwright_contexts'):
                for context in self._playwright_contexts.values():
                    if context['browser']:
                        context['browser'].close()
                    if context['playwright']:
                        context['playwright'].stop()
                self._playwright_contexts = {}
                
            logger.info("Recursos liberados com sucesso")
        except Exception as e:
            logger.error(f"Erro ao liberar recursos: {e}")

def main():
    """Função principal."""
    parser = argparse.ArgumentParser(description='Crawler/Scraping para Profissionais da Saúde')
    parser.add_argument('input_file', nargs='?', default='medicoporestado.txt', help='Arquivo de entrada com dados dos médicos')
    parser.add_argument('output_file', nargs='?', default='resultados_medicos.csv', help='Arquivo de saída para os resultados')
    parser.add_argument('--batch-size', type=int, default=10, help='Tamanho do lote para processamento')
    parser.add_argument('--max-workers', type=int, default=None, help='Número máximo de workers para processamento paralelo')
    parser.add_argument('--no-selenium', action='store_true', help='Desabilitar uso do Selenium')
    parser.add_argument('--no-playwright', action='store_true', help='Desabilitar uso do Playwright')
    parser.add_argument('--no-searx', action='store_true', help='Desabilitar uso do SearXNG')
    parser.add_argument('--no-ollama', action='store_true', help='Desabilitar uso do Ollama')
    parser.add_argument('--debug', action='store_true', help='Ativar modo de debug')
    
    args = parser.parse_args()
    
    # Configurar crawler
    config = {
        'batch_size': args.batch_size,
        'use_selenium': not args.no_selenium and SELENIUM_AVAILABLE,
        'use_playwright': not args.no_playwright and PLAYWRIGHT_AVAILABLE,
        'use_searx': not args.no_searx,
        'use_ollama': not args.no_ollama,
        'debug': args.debug
    }
    
    if args.max_workers:
        config['max_workers'] = args.max_workers
    
    # Inicializar e executar crawler
    try:
        print(f"Iniciando crawler para {args.input_file}...")
        print(f"Resultados serão salvos em {args.output_file}")
        
        crawler = MedicosCrawler(config)
        
        start_time = time.time()
        num_processed = crawler.process_file(args.input_file, args.output_file)
        end_time = time.time()
        
        print(f"Processamento concluído em {end_time - start_time:.2f} segundos")
        print(f"{num_processed} médicos processados")
        print(f"Resultados salvos em {args.output_file}")
        
    except KeyboardInterrupt:
        print("\nProcessamento interrompido pelo usuário")
    except Exception as e:
        print(f"Erro durante a execução: {e}")
        traceback.print_exc()
    finally:
        if 'crawler' in locals():
            crawler.cleanup()

if __name__ == "__main__":
    main()
