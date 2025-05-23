#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Crawler/Scraping para Profissionais da Saúde

Este script realiza a extração de dados de médicos, com foco em especialidades e e-mails,
utilizando uma abordagem híbrida com múltiplos fallbacks e validação por IA.

Requisitos:
- Python 3.6+
- Bibliotecas: requests, beautifulsoup4, selenium, pandas, tqdm, lxml (opcional, recomendado)

Uso:
    python medicos_crawler.py [arquivo_entrada.csv] [arquivo_saida.csv] [opções]

Exemplo:
    python medicos_crawler.py input.csv output.csv --workers 8 --debug
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
from collections import Counter

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

except ImportError as e:
    print(f"Erro: Biblioteca necessária não encontrada: {e}")
    print("Por favor, instale as dependências com: pip install requests beautifulsoup4 selenium pandas tqdm lxml")
    sys.exit(1)

# Configuração de logging
# O logger principal é configurado no __main__ para refletir o nível de debug

# Constantes e configurações
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
]

# URLs e endpoints (devem ser configuráveis ou descobertos)
SEARX_URL = os.environ.get("SEARX_URL", "http://127.0.0.1:8080/search")
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://127.0.0.1:11434/api/generate")

# Mapeamento de UFs para nomes completos dos estados
UF_TO_ESTADO = {
    'AC': 'Acre', 'AL': 'Alagoas', 'AP': 'Amapá', 'AM': 'Amazonas', 'BA': 'Bahia',
    'CE': 'Ceará', 'DF': 'Distrito Federal', 'ES': 'Espírito Santo', 'GO': 'Goiás',
    'MA': 'Maranhão', 'MT': 'Mato Grosso', 'MS': 'Mato Grosso do Sul', 'MG': 'Minas Gerais',
    'PA': 'Pará', 'PB': 'Paraíba', 'PR': 'Paraná', 'PE': 'Pernambuco', 'PI': 'Piauí',
    'RJ': 'Rio de Janeiro', 'RN': 'Rio Grande do Norte', 'RS': 'Rio Grande do Sul',
    'RO': 'Rondônia', 'RR': 'Roraima', 'SC': 'Santa Catarina', 'SP': 'São Paulo',
    'SE': 'Sergipe', 'TO': 'Tocantins'
}

# Lista OFICIAL de especialidades médicas para validação RIGOROSA
ESPECIALIDADES_MEDICAS_OFICIAL = set([
    "acupuntura", "alergia e imunologia", "anestesiologia", "angiologia", "cancerologia", "oncologia clínica",
    "cardiologia", "cirurgia cardiovascular", "cirurgia da mão", "cirurgia de cabeça e pescoço",
    "cirurgia do aparelho digestivo", "cirurgia geral", "cirurgia pediátrica",
    "cirurgia plástica", "cirurgia torácica", "cirurgia vascular", "clínica médica",
    "coloproctologia", "dermatologia", "endocrinologia e metabologia", "endoscopia", "gastroenterologia",
    "genética médica", "geriatria", "ginecologia e obstetrícia", "hematologia e hemoterapia", "homeopatia", "infectologia",
    "mastologia", "medicina de emergência", "medicina de família e comunidade", "medicina do trabalho",
    "medicina de tráfego", "medicina esportiva", "medicina física e reabilitação",
    "medicina intensiva", "medicina legal e perícia médica", "medicina nuclear", "medicina preventiva e social",
    "nefrologia", "neurocirurgia", "neurologia", "nutrologia", "obstetrícia", "oftalmologia",
    "oncologia",
    "ortopedia e traumatologia", "otorrinolaringologia", "patologia", "patologia clínica/medicina laboratorial",
    "pediatria", "pneumologia", "psiquiatria", "radiologia e diagnóstico por imagem", "radioterapia", "reumatologia", "urologia"
])

# Padrões de expressões regulares
EMAIL_PATTERN = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
PHONE_PATTERN = r'(?:\(?([1-9]{2})\)?\s?|([1-9]{2})\s?)(9\d{4})[-.\s]?(\d{4})\b' # Melhorado para DDD

# Palavras-chave para identificar e-mails genéricos/institucionais (lowercase)
GENERIC_EMAIL_KEYWORDS = [
    'contato', 'faleconosco', 'atendimento', 'suporte', 'info', 'geral',
    'secretaria', 'administrativo', 'financeiro', 'comercial', 'marketing',
    'trabalheconosco', 'rh', 'ouvidoria', 'imprensa', 'comunicacao',
    'sac', 'privacidade', 'diretoria', 'presidencia', 'agendamento'
]
GENERIC_DOMAINS = ['example.com', 'domain.com', 'email.com', 'site.com', 'mail.com']

# Classe principal do crawler
class MedicosCrawler:
    """Classe principal para extração de dados de médicos."""

    def __init__(self, config=None):
        """
        Inicializa o crawler com configurações personalizáveis.
        Args:
            config (dict, optional): Configurações personalizadas.
        """
        self.logger = logging.getLogger(f"MedicosCrawler_{id(self)}") # Logger de instância
        self.logger.info("Inicializando MedicosCrawler...")

        # Configurações padrão
        self.config = {
            'max_retries': 3,
            'timeout': 30,
            'delay_min': 1,
            'delay_max': 3,
            'max_workers': min(4, os.cpu_count()),
            'batch_size': 5,
            'use_selenium': SELENIUM_AVAILABLE,
            'use_searx': True,
            'use_ollama': True,
            'debug': False,
            'chromedriver_path': None,
            'ollama_model': "llama3.1:8b",
            'ollama_email_threshold': 0.3,
            'ollama_timeout': 25,
            'max_urls_per_medico': 15 # Limite de URLs a processar por médico
        }
        if config:
            self.config.update(config)

        self.session = self._create_session()
        self.whitelist_urls = {}
        self.results_cache = {}
        self.selenium_drivers = threading.local() # Armazena um driver por thread
        self._load_whitelist()
        
        # Métricas de sucesso
        self.metrics = {
            'total_processed': 0,
            'success_email': Counter(), # Conta fontes que encontraram o email final
            'success_specialty': Counter(), # Conta fontes que encontraram a especialidade final
            'success_phone': Counter(), # Conta fontes que encontraram o telefone final
            'errors': Counter() # Conta tipos de erro
        }
        self.metrics_lock = threading.Lock() # Lock para atualizar métricas

    def _load_whitelist(self):
        """Carrega a whitelist de URLs por estado a partir do arquivo CSV."""
        try:
            whitelist_file = "whitelist_portais.csv"
            if not os.path.exists(whitelist_file):
                script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in locals() else '.'
                whitelist_file = os.path.join(script_dir, whitelist_file)
                if not os.path.exists(whitelist_file):
                     self.logger.warning(f"Arquivo de whitelist não encontrado: {whitelist_file}")
                     return

            df = pd.read_csv(whitelist_file)
            for _, row in df.iterrows():
                try:
                    estado_uf = row['Estado (UF)'].split('(')[1].split(')')[0]
                    urls = [url.strip() for url in row['URLs dos Portais Recomendados'].split(',') if url.strip()]
                    if estado_uf and urls:
                         self.whitelist_urls[estado_uf] = urls
                except (IndexError, KeyError, AttributeError, TypeError) as e:
                    self.logger.warning(f"Erro ao processar linha da whitelist: {row} - {e}")
                    continue
            self.logger.info(f"Whitelist carregada com sucesso: {len(self.whitelist_urls)} estados")
        except FileNotFoundError:
             self.logger.warning(f"Arquivo de whitelist não encontrado: {whitelist_file}")
        except Exception as e:
            self.logger.error(f"Erro crítico ao carregar whitelist: {e}", exc_info=self.config['debug'])

    def _create_session(self):
        """Cria uma sessão HTTP com retry e timeout configurados."""
        session = requests.Session()
        retry_strategy = Retry(
            total=self.config['max_retries'], backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive', 'Upgrade-Insecure-Requests': '1'
        })
        return session

    def _get_selenium_driver(self):
        """Obtém ou inicializa um driver Selenium para a thread atual."""
        driver = getattr(self.selenium_drivers, 'driver', None)
        if driver is not None:
            try:
                _ = driver.window_handles
                return driver
            except WebDriverException:
                self.logger.warning("Driver Selenium da thread estava inativo. Recriando.")
                self._close_selenium_driver_thread()
        
        if not self.config['use_selenium'] or not SELENIUM_AVAILABLE:
            self.logger.error("Selenium não está habilitado ou disponível.")
            return None

        self.logger.info(f"Inicializando driver Selenium para thread {threading.get_ident()}...")
        try:
            options = ChromeOptions()
            options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")
            options.add_argument(f"--user-agent={random.choice(USER_AGENTS)}")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-popup-blocking")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--disable-infobars")
            options.add_argument("--disable-notifications")
            options.add_argument("--ignore-certificate-errors")
            prefs = {
                "profile.managed_default_content_settings.images": 2,
                "profile.default_content_setting_values.notifications": 2,
                "profile.managed_default_content_settings.javascript": 1
            }
            options.add_experimental_option("prefs", prefs)

            service_args = []
            if self.config.get('chromedriver_path'):
                 service = ChromeService(executable_path=self.config['chromedriver_path'])
                 self.logger.info(f"Usando chromedriver em: {self.config['chromedriver_path']}")
            else:
                 try:
                     from webdriver_manager.chrome import ChromeDriverManager
                     driver_path = ChromeDriverManager().install()
                     service = ChromeService(executable_path=driver_path)
                     self.logger.info(f"Usando chromedriver gerenciado por webdriver-manager em: {driver_path}")
                 except ImportError:
                     self.logger.warning("webdriver-manager não encontrado. Tentando encontrar chromedriver no PATH.")
                     service = ChromeService()

            new_driver = webdriver.Chrome(service=service, options=options)
            new_driver.set_page_load_timeout(self.config['timeout'])
            new_driver.set_script_timeout(self.config['timeout'])
            self.selenium_drivers.driver = new_driver
            self.logger.info(f"Driver Selenium inicializado com sucesso para thread {threading.get_ident()}.")
            return new_driver
        except WebDriverException as e:
            self.logger.error(f"Erro WebDriver ao inicializar Selenium: {e}", exc_info=self.config['debug'])
            self.logger.error("Verifique se o ChromeDriver está instalado e no PATH ou especifique 'chromedriver_path' na config.")
            self.selenium_drivers.driver = None
            # Registrar erro na métrica
            with self.metrics_lock:
                 self.metrics['errors']['selenium_init'] += 1
            return None
        except Exception as e:
            self.logger.error(f"Erro inesperado ao inicializar Selenium: {e}", exc_info=self.config['debug'])
            self.selenium_drivers.driver = None
            with self.metrics_lock:
                 self.metrics['errors']['selenium_init_unexpected'] += 1
            return None

    def _close_selenium_driver_thread(self):
        """Fecha o driver Selenium da thread atual, se existir."""
        driver = getattr(self.selenium_drivers, 'driver', None)
        if driver:
            self.logger.info(f"Fechando driver Selenium da thread {threading.get_ident()}...")
            try:
                driver.quit()
            except Exception as e:
                self.logger.error(f"Erro ao fechar driver Selenium da thread: {e}", exc_info=self.config['debug'])
            finally:
                self.selenium_drivers.driver = None

    def close_all_drivers(self):
         """Fecha o driver Selenium da thread principal (chamado no final)."""
         self._close_selenium_driver_thread()

    def _random_delay(self):
        """Adiciona um delay aleatório entre requisições."""
        delay = random.uniform(self.config['delay_min'], self.config['delay_max'])
        time.sleep(delay)

    def _is_generic_email(self, email: str) -> bool:
        """Verifica se um e-mail parece genérico ou institucional por palavras-chave e domínios."""
        if not email: return True
        email_lower = email.lower()
        local_part, domain = email_lower.split('@', 1)
        if domain in GENERIC_DOMAINS: return True
        if any(local_part.startswith(keyword) for keyword in GENERIC_EMAIL_KEYWORDS): return True
        if len(local_part) <= 3 and local_part.isdigit(): return True
        return False

    def _extract_emails(self, text: str) -> List[str]:
        """Extrai e-mails e aplica filtro inicial de genéricos."""
        if not text: return []
        potential_emails = set(re.findall(EMAIL_PATTERN, text))
        valid_emails = [email for email in potential_emails if not self._is_generic_email(email)]
        if len(potential_emails) > len(valid_emails):
             self.logger.debug(f"{len(potential_emails) - len(valid_emails)} e-mails filtrados por regra genérica.")
        return valid_emails

    def _extract_phones(self, text: str) -> List[str]:
        """Extrai números de telefone (formato brasileiro)."""
        if not text: return []
        phones = re.findall(PHONE_PATTERN, text)
        formatted_phones = set()
        for p in phones:
            ddd = p[0] or p[1]
            numero = f"{p[2]}{p[3]}"
            if ddd and numero:
                 formatted_phones.add(f"({ddd}) {numero[:5]}-{numero[5:]}" if len(numero) == 9 else f"({ddd}) {numero[:4]}-{numero[4:]}")
        return list(formatted_phones)

    def _normalize_specialty(self, specialty_text: str) -> Optional[str]:
        """Normaliza e valida RIGOROSAMENTE uma especialidade médica contra a lista oficial."""
        if not specialty_text or pd.isna(specialty_text): return None
        normalized = str(specialty_text).lower().strip()
        replacements = {'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u', 'ç': 'c', 'â': 'a', 'ê': 'e', 'ô': 'o'}
        for k, v in replacements.items(): normalized = normalized.replace(k, v)
        normalized = re.sub(r'\s*-\s*', ' e ', normalized)
        normalized = re.sub(r'[()/]', '', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        if normalized in ESPECIALIDADES_MEDICAS_OFICIAL: return normalized
        variations = {
            "clinica medica": "clínica médica", "ginecologia obstetricia": "ginecologia e obstetrícia",
            "gineco e obstetricia": "ginecologia e obstetrícia", "hematologia hemoterapia": "hematologia e hemoterapia",
            "medicina familia e comunidade": "medicina de família e comunidade", "medicina legal pericia medica": "medicina legal e perícia médica",
            "medicina preventiva social": "medicina preventiva e social", "ortopedia traumatologia": "ortopedia e traumatologia",
            "patologia clinica medicina laboratorial": "patologia clínica/medicina laboratorial", "radiologia diagnostico por imagem": "radiologia e diagnóstico por imagem",
            "cancerologia clinica": "oncologia clínica", "endocrinologia metabologia": "endocrinologia e metabologia",
            "oftalmo": "oftalmologia", "otorrino": "otorrinolaringologia", "pediatra": "pediatria"
        }
        official_name = variations.get(normalized)
        if official_name and official_name in ESPECIALIDADES_MEDICAS_OFICIAL:
             self.logger.debug(f"Normalizando variação '{specialty_text}' para '{official_name}'")
             return official_name
        self.logger.debug(f"Especialidade não validada contra lista oficial: '{specialty_text}' (Normalizado: '{normalized}')")
        return None

    def _call_ollama_api(self, prompt: str, format_json: bool = False) -> Optional[Dict]:
        """Função centralizada para chamar a API Ollama com tratamento de erro e timing."""
        if not self.config['use_ollama']: return None
        start_time = time.time()
        try:
            data = {
                "model": self.config['ollama_model'],
                "prompt": prompt,
                "stream": False
            }
            if format_json: data["format"] = "json"
            
            self.logger.debug(f"Enviando prompt ({'JSON' if format_json else 'TEXT'}) para Ollama ({self.config['ollama_model']}): {prompt[:150]}...")
            response = self.session.post(OLLAMA_URL, json=data, timeout=self.config['ollama_timeout'])
            response.raise_for_status()
            result = response.json()
            duration = time.time() - start_time
            self.logger.debug(f"Resposta JSON crua do Ollama recebida em {duration:.2f}s: {json.dumps(result, ensure_ascii=False)}")
            return result
        except requests.exceptions.Timeout:
             duration = time.time() - start_time
             self.logger.error(f"Timeout ({self.config['ollama_timeout']}s) ao contatar Ollama ({OLLAMA_URL}) após {duration:.2f}s.")
             with self.metrics_lock: self.metrics['errors']['ollama_timeout'] += 1
             return None
        except requests.exceptions.RequestException as req_e:
            duration = time.time() - start_time
            self.logger.error(f"Erro de rede ({req_e}) ao contatar Ollama ({OLLAMA_URL}) após {duration:.2f}s.")
            with self.metrics_lock: self.metrics['errors']['ollama_network'] += 1
            return None
        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"Erro geral ({e}) ao chamar Ollama após {duration:.2f}s.", exc_info=self.config['debug'])
            with self.metrics_lock: self.metrics['errors']['ollama_unexpected'] += 1
            return None

    def _classify_specialties_with_ollama(self, text_snippet: str) -> List[str]:
        """Usa o Ollama para identificar especialidades em um trecho de texto, com validação rigorosa."""
        prompt = f"""
        Tarefa: Identificar TODAS as especialidades médicas mencionadas no texto fornecido.
        Contexto: O texto pode conter descrições de um profissional de saúde, clínica ou hospital.
        Importante: Um médico PODE ter MAIS DE UMA especialidade ou área de atuação.
        Texto para Análise:
        --- TEXTO ---
        {text_snippet[:2000]} # Limitar tamanho do input
        --- FIM DO TEXTO ---
        Lista Oficial de Especialidades Válidas (Brasil):
        {json.dumps(list(ESPECIALIDADES_MEDICAS_OFICIAL), ensure_ascii=False, indent=2)}
        Instruções de Saída:
        1. Analise o texto e identifique CADA menção a uma especialidade médica.
        2. Compare CADA especialidade identificada com a Lista Oficial de Especialidades Válidas.
        3. Retorne APENAS uma lista JSON contendo os nomes EXATOS das especialidades VÁLIDAS encontradas.
        4. Se uma especialidade mencionada não estiver na lista oficial, IGNORE-A.
        5. Se NENHUMA especialidade válida for encontrada, retorne uma lista JSON vazia [].
        6. NÃO inclua explicações, apenas a lista JSON.
        Exemplos de Saída Esperada:
        - Texto: "Dr. Silva é cardiologista e clínico geral." -> ["cardiologia", "clínica médica"]
        - Texto: "Atua em ginecologia e obstetrícia." -> ["ginecologia e obstetrícia"]
        - Texto: "Especialista em cirurgia plástica e dermatologia estética." -> ["cirurgia plástica", "dermatologia"]
        - Texto: "Médico com foco em bem-estar." -> []
        - Texto: "Clínica de Otorrino e Oftalmo." -> ["otorrinolaringologia", "oftalmologia"]
        Sua Resposta (APENAS a lista JSON):
        """
        result = self._call_ollama_api(prompt, format_json=True)
        if not result or 'response' not in result:
            self.logger.warning("Falha ao obter resposta válida do Ollama para classificação de especialidades.")
            return []
        try:
            potential_specialties = json.loads(result['response'])
            if isinstance(potential_specialties, list):
                validated_specialties = set()
                for spec in potential_specialties:
                    if isinstance(spec, str):
                        normalized_spec = self._normalize_specialty(spec)
                        if normalized_spec:
                            validated_specialties.add(normalized_spec)
                        else:
                             self.logger.debug(f"Ollama retornou especialidade '{spec}' que não foi validada.")
                if validated_specialties:
                     self.logger.info(f"Ollama identificou e validou especialidades: {list(validated_specialties)}")
                     return list(validated_specialties)
                else:
                     self.logger.info(f"Ollama retornou lista, mas nenhuma especialidade foi validada: {potential_specialties}")
                     return []
            else:
                self.logger.warning(f"Ollama não retornou uma lista JSON válida para especialidades: {result['response']}")
                return []
        except json.JSONDecodeError as json_err:
            self.logger.error(f"Erro ao decodificar JSON da resposta do Ollama (especialidades): {json_err} - Resposta: {result['response']}")
            with self.metrics_lock: self.metrics['errors']['ollama_json_decode_spec'] += 1
            # Tentar extrair manualmente
            manual_list = re.findall(r'"(.*?)"', result['response'])
            validated_specialties = {spec for spec in map(self._normalize_specialty, manual_list) if spec}
            if validated_specialties:
                self.logger.info(f"Especialidades extraídas manualmente e validadas: {list(validated_specialties)}")
                return list(validated_specialties)
            return []
        except Exception as inner_e:
             self.logger.error(f"Erro inesperado ao processar resposta do Ollama (especialidades): {inner_e} - Resposta: {result['response']}", exc_info=self.config['debug'])
             with self.metrics_lock: self.metrics['errors']['ollama_processing_spec'] += 1
             return []

    def _validate_email_with_ollama(self, email: str, text_context: str, medico_info: Dict) -> float:
        """Usa o Ollama para validar a relevância de um e-mail, retornando um score (0.0 a 1.0)."""
        if self._is_generic_email(email):
             self.logger.debug(f"E-mail {email} pré-filtrado como genérico antes da chamada Ollama.")
             return 0.0
        if not self.config['use_ollama']: return 0.5 # Retorna score neutro se IA desligada
        
        nome_completo = f"{medico_info.get('firstname', '')} {medico_info.get('lastname', '')}".strip()
        crm = medico_info.get('crm', '')
        uf = medico_info.get('uf', '')
        prompt = f"""
        Tarefa: Avaliar a relevância do e-mail encontrado para o médico especificado.
        Médico: {nome_completo} (CRM {crm}/{uf})
        E-mail Encontrado: {email}
        Contexto onde o e-mail foi encontrado (trecho):
        --- CONTEXTO ---
        {text_context[:1000]}...
        --- FIM DO CONTEXTO ---
        Instruções:
        1. Analise o e-mail ({email}) e o contexto.
        2. Considere se o e-mail parece pertencer DIRETAMENTE ao médico {nome_completo} (pessoal ou profissional direto).
        3. Avalie a probabilidade de NÃO ser um e-mail genérico (contato@, faleconosco@), institucional (hospital@, clinica@, secretaria@), de terceiros ou de outra pessoa/departamento.
        4. Atribua um score de relevância de 0.0 a 1.0.
        Critérios de Score:
        - 1.0: Altamente provável ser o e-mail pessoal/profissional direto do médico (ex: nome.sobrenome@dominio.com.br, dr.nome@dominio.com).
        - 0.7: Provável (ex: nome@dominio-pessoal.com, especialidade@clinica-do-medico.com).
        - 0.5: Possível, mas incerto (ex: nome.silva@gmail.com - nome comum, domínio genérico, mas nome bate).
        - 0.2: Improvável (ex: contato@clinicagrande.com, atendimento@hospitalxyz.org).
        - 0.0: Muito improvável / Genérico / Institucional (ex: faleconosco@site.com, newsletter@provedor.com, rh@empresa.com).
        Responda APENAS com o número do score (ex: 0.8).
        """
        result = self._call_ollama_api(prompt)
        if not result or 'response' not in result:
             self.logger.warning(f"Falha ao obter resposta válida do Ollama para validação do e-mail {email}.")
             return 0.0 # Score baixo em caso de falha na API
        try:
            score_str = result['response'].strip()
            score_match = re.search(r'[0-9]+\.?[0-9]*', score_str)
            if score_match:
                score = float(score_match.group(0))
                score = max(0.0, min(1.0, score))
                self.logger.info(f"Ollama avaliou e-mail '{email}' com score: {score:.2f}")
                return score
            else:
                 self.logger.warning(f"Não foi possível extrair um score numérico da resposta do Ollama (email): {score_str}")
                 return 0.0
        except (ValueError, TypeError) as conv_err:
            self.logger.error(f"Erro ao converter resposta do Ollama para score de e-mail: {conv_err} - Resposta: {result['response']}")
            with self.metrics_lock: self.metrics['errors']['ollama_score_conversion'] += 1
            return 0.0
        except Exception as inner_e:
             self.logger.error(f"Erro inesperado ao processar resposta do Ollama (validação email): {inner_e} - Resposta: {result['response']}", exc_info=self.config['debug'])
             with self.metrics_lock: self.metrics['errors']['ollama_processing_email'] += 1
             return 0.0

    def _fetch_url(self, url: str, use_selenium: bool = False) -> Optional[str]:
        """Busca o conteúdo de uma URL com timing e fallback."""
        self.logger.debug(f"Tentando buscar URL: {url} (Selenium: {use_selenium})")
        start_time = time.time()
        self._random_delay()
        html_content = None
        fetch_method = "Requests"
        try:
            if use_selenium and self.config['use_selenium']:
                driver = self._get_selenium_driver()
                if driver:
                    fetch_method = "Selenium"
                    driver.get(url)
                    WebDriverWait(driver, 10).until(lambda d: d.execute_script('return document.readyState') == 'complete')
                    time.sleep(random.uniform(0.5, 1.5))
                    html_content = driver.page_source
                else:
                    self.logger.warning(f"Selenium solicitado para {url[:80]}..., mas driver não disponível. Usando Requests.")
                    fetch_method = "Requests (Fallback)"
            
            if html_content is None: # Se Selenium não usado ou falhou
                html_content = self._fetch_with_requests(url, fetch_method)
            
            duration = time.time() - start_time
            if html_content:
                 self.logger.info(f"Conteúdo obtido com {fetch_method} de: {url[:80]}... em {duration:.2f}s")
            else:
                 self.logger.warning(f"Falha ao obter conteúdo de {url[:80]}... com {fetch_method} em {duration:.2f}s")
                 with self.metrics_lock: self.metrics['errors'][f'fetch_{fetch_method.lower().split()[0]}'] += 1
            return html_content
        except (TimeoutException, WebDriverException) as sel_e:
             duration = time.time() - start_time
             self.logger.warning(f"Erro do {fetch_method} ao buscar {url[:80]}... ({sel_e}) em {duration:.2f}s. Tentando Requests.")
             with self.metrics_lock: self.metrics['errors'][f'fetch_{fetch_method.lower()}_error'] += 1
             # Tentar com Requests como fallback explícito
             html_content = self._fetch_with_requests(url, "Requests (Fallback)")
             duration = time.time() - start_time
             if html_content:
                  self.logger.info(f"Conteúdo obtido com Requests (Fallback) de: {url[:80]}... em {duration:.2f}s")
             else:
                  self.logger.warning(f"Falha ao obter conteúdo de {url[:80]}... também com Requests (Fallback) em {duration:.2f}s")
             return html_content
        except Exception as e:
             duration = time.time() - start_time
             self.logger.error(f"Erro inesperado do {fetch_method} ao buscar {url[:80]}... ({e}) em {duration:.2f}s", exc_info=self.config['debug'])
             with self.metrics_lock: self.metrics['errors'][f'fetch_{fetch_method.lower()}_unexpected'] += 1
             return None

    def _fetch_with_requests(self, url: str, method_name: str = "Requests") -> Optional[str]:
        """Busca o conteúdo de uma URL usando a biblioteca requests."""
        start_time = time.time()
        try:
            response = self.session.get(url, timeout=self.config['timeout'])
            response.raise_for_status()
            response.encoding = response.apparent_encoding # Tenta detectar encoding
            content = response.text
            duration = time.time() - start_time
            self.logger.debug(f"Conteúdo obtido com {method_name} de: {url[:80]}... (Status: {response.status_code}) em {duration:.2f}s")
            return content
        except requests.exceptions.Timeout:
            duration = time.time() - start_time
            self.logger.warning(f"Timeout ({self.config['timeout']}s) ao buscar {url[:80]}... com {method_name} após {duration:.2f}s")
            return None
        except requests.exceptions.RequestException as e:
            duration = time.time() - start_time
            status_code = f" (Status: {e.response.status_code})" if e.response is not None else ""
            self.logger.warning(f"Erro do {method_name} ao buscar {url[:80]}...: {e}{status_code} em {duration:.2f}s")
            return None
        except Exception as e:
             duration = time.time() - start_time
             self.logger.error(f"Erro inesperado no _fetch_with_requests para {url[:80]}... ({e}) em {duration:.2f}s", exc_info=self.config['debug'])
             return None

    def _parse_html(self, html_content: str, url: str) -> Optional[BeautifulSoup]:
        """Analisa o conteúdo HTML usando BeautifulSoup, preferindo lxml."""
        if not html_content: return None
        start_time = time.time()
        parser = 'html.parser'
        try:
            import lxml
            parser = 'lxml'
        except ImportError: pass
        try:
            soup = BeautifulSoup(html_content, parser)
            duration = time.time() - start_time
            self.logger.debug(f"HTML de {url[:60]}... parseado com {parser} em {duration:.3f}s")
            return soup
        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"Erro ao parsear HTML de {url[:60]}... com {parser} ({e}) em {duration:.3f}s", exc_info=self.config['debug'])
            with self.metrics_lock: self.metrics['errors'][f'parse_{parser}'] += 1
            if parser == 'lxml': # Tentar fallback para html.parser
                try:
                    soup = BeautifulSoup(html_content, 'html.parser')
                    duration = time.time() - start_time
                    self.logger.info(f"Parse HTML de {url[:60]}... refeito com html.parser em {duration:.3f}s")
                    return soup
                except Exception as e2:
                     duration = time.time() - start_time
                     self.logger.error(f"Erro ao parsear HTML de {url[:60]}... também com html.parser ({e2}) em {duration:.3f}s", exc_info=self.config['debug'])
                     with self.metrics_lock: self.metrics['errors']['parse_html.parser'] += 1
            return None

    def _search_with_searx(self, query: str, max_results: int = 5) -> List[Dict[str, str]]:
        """Realiza uma busca no SearXNG e retorna os resultados válidos."""
        if not self.config['use_searx']: return []
        results = []
        start_time = time.time()
        try:
            params = {'q': query, 'format': 'json', 'language': 'pt-BR'}
            self.logger.info(f"Buscando no SearX ({SEARX_URL}): '{query}'")
            response = self.session.get(SEARX_URL, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            duration = time.time() - start_time
            self.logger.debug(f"Resposta SearX recebida em {duration:.2f}s. Resultados: {len(data.get('results', []))}")

            if 'results' in data:
                count = 0
                for item in data['results']:
                    url = item.get('url')
                    parsed_url = urlparse(url)
                    # Filtros adicionais (ex: evitar PDFs, domínios de busca)
                    if url and parsed_url.scheme in ['http', 'https'] and \
                       not any(b in parsed_url.netloc for b in ['google.com', 'bing.com', 'duckduckgo.com']) and \
                       not url.lower().endswith(('.pdf', '.doc', '.docx', '.xls', '.xlsx')):
                        results.append({
                            'title': item.get('title', ''),
                            'url': url,
                            'snippet': item.get('content', '')
                        })
                        count += 1
                        if count >= max_results: break
                self.logger.info(f"SearX retornou {len(results)} resultados válidos para '{query}'")
            else:
                self.logger.warning(f"Nenhum resultado encontrado no SearX para '{query}'")
        except requests.exceptions.RequestException as e:
            duration = time.time() - start_time
            self.logger.error(f"Erro de rede ao buscar no SearX ({SEARX_URL}) para '{query}' após {duration:.2f}s: {e}")
            with self.metrics_lock: self.metrics['errors']['searx_network'] += 1
        except json.JSONDecodeError as e:
             duration = time.time() - start_time
             self.logger.error(f"Erro ao decodificar JSON do SearX para '{query}' após {duration:.2f}s: {e} - Resposta: {response.text[:200]}...")
             with self.metrics_lock: self.metrics['errors']['searx_json_decode'] += 1
        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"Erro inesperado ao buscar no SearX para '{query}' após {duration:.2f}s: {e}", exc_info=self.config['debug'])
            with self.metrics_lock: self.metrics['errors']['searx_unexpected'] += 1
        return results

    def _extract_data_from_soup(self, soup: BeautifulSoup, medico_info: Dict, source_url: str) -> Dict:
        """Extrai e valida dados (e-mails, telefones, especialidades) de um objeto BeautifulSoup."""
        extracted_data = {'emails': [], 'phones': set(), 'specialties': set()}
        if not soup: return extracted_data
        start_time = time.time()

        body = soup.body
        text_content = ""
        if body:
            for tag in body(['script', 'style', 'head', 'nav', 'footer', 'header', 'aside']): tag.decompose()
            text_content = body.get_text(separator=' ', strip=True)
        if not text_content: text_content = soup.get_text(separator=' ', strip=True)
        if not text_content:
             self.logger.warning(f"Não foi possível extrair texto significativo de: {source_url[:80]}...")
             return extracted_data

        extracted_data['phones'] = set(self._extract_phones(text_content))
        potential_emails = self._extract_emails(text_content)
        validated_emails_with_scores = []
        if self.config['use_ollama'] and potential_emails:
            self.logger.info(f"Validando {len(potential_emails)} e-mails potenciais de {source_url[:60]}... com Ollama.")
            for email in potential_emails:
                score = self._validate_email_with_ollama(email, text_content[:1500], medico_info)
                if score >= self.config['ollama_email_threshold']:
                    validated_emails_with_scores.append((email, score))
                else:
                     self.logger.debug(f"E-mail '{email}' descartado (Score: {score:.2f} < {self.config['ollama_email_threshold']})")
            validated_emails_with_scores.sort(key=lambda x: x[1], reverse=True)
            extracted_data['emails'] = validated_emails_with_scores
        elif potential_emails:
             extracted_data['emails'] = [(email, 0.5) for email in potential_emails] # Score 0.5 se IA desligada
             self.logger.info(f"Ollama desabilitado. {len(potential_emails)} e-mails extraídos por regex terão score 0.5.")

        if self.config['use_ollama']:
            ollama_specialties = self._classify_specialties_with_ollama(text_content[:2000])
            extracted_data['specialties'] = set(ollama_specialties)
        else:
            possible_specialties = set()
            for esp in ESPECIALIDADES_MEDICAS_OFICIAL:
                try:
                    pattern = r'\b' + re.escape(esp) + r'\b'
                    if re.search(pattern, text_content, re.IGNORECASE):
                        possible_specialties.add(esp)
                except re.error as re_err:
                     self.logger.error(f"Erro de regex ao buscar especialidade '{esp}': {re_err}")
            extracted_data['specialties'] = possible_specialties
            if possible_specialties:
                self.logger.info(f"Especialidades encontradas por regex fallback: {possible_specialties}")
        
        duration = time.time() - start_time
        self.logger.debug(f"Extração de dados do soup de {source_url[:60]}... concluída em {duration:.2f}s. Encontrados: Emails: {len(extracted_data['emails'])}, Fones: {len(extracted_data['phones'])}, Especs: {len(extracted_data['specialties'])}")
        return extracted_data

    def _process_medico(self, medico_info: Dict) -> Dict:
        """Processa um único registro de médico, buscando, extraindo e selecionando dados."""
        process_start_time = time.time()
        nome_completo = f"{medico_info.get('firstname', '')} {medico_info.get('lastname', '')}".strip()
        crm = medico_info.get('crm', '')
        uf = medico_info.get('uf', '')
        cache_key = f"{nome_completo}_{crm}_{uf}"

        # Não usar cache por enquanto para garantir reprocessamento com novas lógicas
        # if cache_key in self.results_cache:
        #     self.logger.info(f"Retornando dados do cache para: {cache_key}")
        #     return self.results_cache[cache_key]

        self.logger.info(f"Processando: {nome_completo} (CRM: {crm}/{uf})")
        found_data = {
            'emails': {}, 'phones': {}, 'specialties': {}
        }
        processed_urls = set()
        
        original_specialty_normalized = self._normalize_specialty(medico_info.get('especialidade'))
        if original_specialty_normalized:
             found_data['specialties'][original_specialty_normalized] = {'source': "Input Normalizado"}

        # --- Estratégia de Busca e Extração em Cascata --- 
        search_sources = []
        # 1. Whitelist
        urls_whitelist = list(self.whitelist_urls.get(uf, []))
        if urls_whitelist:
             self.logger.info(f"Adicionando {len(urls_whitelist)} URLs da whitelist para {uf}")
             for portal_url in urls_whitelist:
                 domain = urlparse(portal_url).netloc
                 if not domain: continue
                 search_query = f'site:{domain} "{nome_completo}" "CRM {crm}"'
                 search_sources.append({'type': 'whitelist', 'query': search_query, 'max_results': 1})
        # 2. Busca Progressiva no SearXNG
        queries = [
            f'"{nome_completo}" "CRM {crm}" {UF_TO_ESTADO.get(uf, uf)} contato email especialidade',
            f'médico "{nome_completo}" {UF_TO_ESTADO.get(uf, uf)} especialidade contato',
            f'"{nome_completo}" crm {crm} {uf}', 
            f'"{nome_completo}" linkedin perfil profissional',
            f'"{nome_completo}" doctoralia',
            f'"{nome_completo}" catalogo medico {uf}',
            f'"{nome_completo}" {original_specialty_normalized if original_specialty_normalized else "médico"} {UF_TO_ESTADO.get(uf, uf)}'
        ]
        for query in queries:
             search_sources.append({'type': 'general', 'query': query, 'max_results': 3})

        # Executar buscas e processar resultados
        urls_processed_count = 0
        for source_info in search_sources:
            if urls_processed_count >= self.config['max_urls_per_medico']: 
                 self.logger.info(f"Limite de URLs ({self.config['max_urls_per_medico']}) atingido para {nome_completo}.")
                 break
            search_type = source_info['type']
            query = source_info['query']
            max_r = source_info['max_results']
            searx_results = self._search_with_searx(query, max_results=max_r)
            for result in searx_results:
                if urls_processed_count >= self.config['max_urls_per_medico']: break
                url = result['url']
                if url not in processed_urls:
                    self.logger.debug(f"Processando URL ({search_type}, {urls_processed_count+1}/{self.config['max_urls_per_medico']}): {url[:80]}...")
                    use_sel = any(d in url for d in ['linkedin.com', 'doctoralia.com.br']) or search_type == 'whitelist'
                    html = self._fetch_url(url, use_selenium=use_sel)
                    processed_urls.add(url)
                    urls_processed_count += 1
                    if html:
                        soup = self._parse_html(html, url)
                        if soup:
                            extracted = self._extract_data_from_soup(soup, medico_info, url)
                            source_tag = f"{search_type.capitalize()} SearX: {urlparse(url).netloc}"
                            # Agregar dados encontrados, atualizando se score for maior
                            for email, score in extracted['emails']:
                                if email not in found_data['emails'] or score > found_data['emails'][email]['score']:
                                     found_data['emails'][email] = {'score': score, 'source': source_tag}
                                     self.logger.debug(f"Email '{email}' adicionado/atualizado de {source_tag} com score {score:.2f}")
                            for phone in extracted['phones']:
                                 if phone not in found_data['phones']:
                                      found_data['phones'][phone] = {'source': source_tag}
                                      self.logger.debug(f"Telefone '{phone}' adicionado de {source_tag}")
                            for spec in extracted['specialties']:
                                 if spec not in found_data['specialties']:
                                      found_data['specialties'][spec] = {'source': source_tag}
                                      self.logger.debug(f"Especialidade '{spec}' adicionada de {source_tag}")
                else:
                     self.logger.debug(f"URL ignorada (já processada): {url[:80]}...")

        # 3. Processamento Final e Seleção dos Melhores Dados
        self.logger.info(f"Dados brutos agregados para {nome_completo}: Emails: {len(found_data['emails'])}, Fones: {len(found_data['phones'])}, Especs: {len(found_data['specialties'])}")
        self.logger.debug(f"Detalhes encontrados: {found_data}")
        final_data = {
            'especialidade_final': None, 'email_final': None, 'telefone_final': None,
            'fonte_especialidade': None, 'fonte_email': None, 'fonte_telefone': None,
            'score_email': None
        }
        # Selecionar Especialidade(s)
        if found_data['specialties']:
            sorted_specialties = sorted(list(found_data['specialties'].keys()))
            final_data['especialidade_final'] = ", ".join(sorted_specialties)
            first_spec_info = found_data['specialties'].get(sorted_specialties[0])
            final_data['fonte_especialidade'] = first_spec_info['source'] if first_spec_info else "Múltiplas Fontes"
            self.logger.info(f"Especialidade(s) final(is) selecionada(s): {final_data['especialidade_final']}")
            # Atualizar métrica de sucesso
            with self.metrics_lock:
                 source_origin = final_data['fonte_especialidade'].split(':')[0] # Ex: 'Input Normalizado', 'Whitelist SearX', 'General SearX'
                 self.metrics['success_specialty'][source_origin] += 1
        else:
            self.logger.warning(f"Nenhuma especialidade VÁLIDA encontrada para {nome_completo}")
            final_data['fonte_especialidade'] = "Não encontrada"
        # Selecionar Melhor E-mail
        if found_data['emails']:
            sorted_emails = sorted(found_data['emails'].items(), key=lambda item: item[1]['score'], reverse=True)
            best_email_info = sorted_emails[0]
            best_email, best_score, source = best_email_info[0], best_email_info[1]['score'], best_email_info[1]['source']
            final_data['email_final'] = best_email
            final_data['score_email'] = f"{best_score:.2f}"
            final_data['fonte_email'] = f"{source} (Score: {best_score:.2f})"
            self.logger.info(f"Melhor email selecionado: {best_email} (Score: {best_score:.2f} / Fonte: {source})")
            with self.metrics_lock:
                 source_origin = source.split(':')[0]
                 self.metrics['success_email'][source_origin] += 1
            if len(sorted_emails) > 1:
                 other_emails_log = [f"{e} (Score: {s['score']:.2f})" for e, s in sorted_emails[1:]]
                 self.logger.debug(f"Outros emails considerados: {', '.join(other_emails_log)}")
        else:
             self.logger.warning(f"Nenhum e-mail válido (acima do threshold {self.config['ollama_email_threshold']}) encontrado para {nome_completo}")
             final_data['fonte_email'] = "Não encontrado"
             final_data['score_email'] = "N/A"
        # Selecionar Telefone
        if found_data['phones']:
            first_phone = list(found_data['phones'].keys())[0]
            source = found_data['phones'][first_phone]['source']
            final_data['telefone_final'] = first_phone
            final_data['fonte_telefone'] = source
            self.logger.info(f"Telefone final selecionado: {final_data['telefone_final']} (Fonte: {source})")
            with self.metrics_lock:
                 source_origin = source.split(':')[0]
                 self.metrics['success_phone'][source_origin] += 1
        else:
             final_data['fonte_telefone'] = "Não encontrado"

        # Adicionar ao cache (desativado)
        # self.results_cache[cache_key] = final_data
        self._close_selenium_driver_thread() # Fecha driver da thread
        process_duration = time.time() - process_start_time
        self.logger.info(f"Processamento de {nome_completo} concluído em {process_duration:.2f}s.")
        return final_data

    def run(self, input_file: str, output_file: str):
        """Executa o processo de crawling a partir de um arquivo de entrada CSV."""
        self.logger.info(f"Iniciando crawling. Entrada: {input_file}, Saída: {output_file}")
        run_start_time = time.time()
        try:
            if not input_file.lower().endswith('.csv'):
                 self.logger.error(f"Formato de arquivo de entrada não suportado: {input_file}. Use CSV.")
                 return
            try:
                df_input = pd.read_csv(input_file, dtype=str).fillna('')
                df_input.columns = [col.lower().strip().replace(' ', '_').replace('(', '').replace(')', '') for col in df_input.columns]
                column_mapping = {
                    'firstname': ['firstname', 'primeiro_nome', 'nome'], 'lastname': ['lastname', 'ultimo_nome', 'sobrenome'],
                    'crm': ['crm', 'registro'], 'uf': ['uf', 'estado'], 'especialidade': ['especialidade', 'area_atuacao']
                }
                final_columns = {}
                missing_mandatory = []
                for target_col, possible_names in column_mapping.items():
                    found = False
                    for name in possible_names:
                        if name in df_input.columns:
                            final_columns[name] = target_col; found = True; break
                    if not found and target_col in ['firstname', 'crm', 'uf']:
                         missing_mandatory.append(f"{target_col} (tentativas: {possible_names})")
                if missing_mandatory:
                     raise ValueError(f"Colunas obrigatórias não encontradas no CSV: {', '.join(missing_mandatory)}")
                df_input.rename(columns=final_columns, inplace=True)
                for col in ['lastname', 'especialidade']: # Adicionar opcionais se faltarem
                    if col not in df_input.columns: df_input[col] = ''
                medicos_list = df_input.to_dict('records')
            except Exception as csv_err:
                 self.logger.error(f"Erro ao ler ou processar CSV de entrada '{input_file}': {csv_err}", exc_info=self.config['debug'])
                 return

            total_medicos = len(medicos_list)
            self.logger.info(f"{total_medicos} registros de médicos para processar.")
            results = []
            processed_count = 0
            error_count = 0

            with concurrent.futures.ThreadPoolExecutor(max_workers=self.config['max_workers']) as executor:
                future_to_medico = {executor.submit(self._process_medico, medico): medico for medico in medicos_list}
                
                for future in tqdm(concurrent.futures.as_completed(future_to_medico), total=total_medicos, desc="Processando Médicos"):
                    medico_original = future_to_medico[future]
                    try:
                        result_data = future.result()
                        combined_result = {**medico_original, **result_data}
                        results.append(combined_result)
                        processed_count += 1
                    except Exception as exc:
                        error_count += 1
                        self.logger.error(f"Erro CRÍTICO ao processar {medico_original.get('firstname')}: {exc}", exc_info=self.config['debug'])
                        with self.metrics_lock: self.metrics['errors']['process_medico_fatal'] += 1
                        results.append({**medico_original, 'especialidade_final': 'ERRO PROCESSAMENTO', 'email_final': 'ERRO', 'telefone_final': 'ERRO', 'fonte_especialidade': str(exc), 'fonte_email': 'ERRO', 'fonte_telefone': 'ERRO', 'score_email': 'ERRO'})
            
            # Atualizar métrica total processada
            with self.metrics_lock: self.metrics['total_processed'] = processed_count

            # Salvar resultados em CSV
            if results:
                df_output = pd.DataFrame(results)
                output_columns = [
                    'firstname', 'lastname', 'crm', 'uf', 'especialidade', # Input
                    'especialidade_final', 'email_final', 'score_email', 'telefone_final', # Output
                    'fonte_especialidade', 'fonte_email', 'fonte_telefone' # Fontes
                ]
                for col in output_columns:
                     if col not in df_output.columns: df_output[col] = pd.NA
                df_output = df_output[output_columns]
                df_output.to_csv(output_file, index=False, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
                self.logger.info(f"Resultados salvos em: {output_file}")
            else:
                self.logger.warning("Nenhum resultado foi gerado.")

            run_duration = time.time() - run_start_time
            self.logger.info(f"Processo concluído em {run_duration:.2f} segundos.")
            self.logger.info(f"--- MÉTRICAS FINAIS ---")
            self.logger.info(f"Total de médicos no input: {total_medicos}")
            self.logger.info(f"Processados com sucesso (sem erro fatal): {processed_count}")
            self.logger.info(f"Erros fatais no processamento: {error_count}")
            
            # Calcular e logar taxas de sucesso
            if processed_count > 0:
                 email_found_count = sum(self.metrics['success_email'].values())
                 spec_found_count = sum(self.metrics['success_specialty'].values())
                 phone_found_count = sum(self.metrics['success_phone'].values())
                 self.logger.info(f"Taxa de sucesso - Email: {email_found_count}/{processed_count} ({email_found_count/processed_count:.1%})")
                 self.logger.info(f"Taxa de sucesso - Especialidade: {spec_found_count}/{processed_count} ({spec_found_count/processed_count:.1%})")
                 self.logger.info(f"Taxa de sucesso - Telefone: {phone_found_count}/{processed_count} ({phone_found_count/processed_count:.1%})")
                 self.logger.info(f"Fontes de sucesso - Email: {dict(self.metrics['success_email'])}")
                 self.logger.info(f"Fontes de sucesso - Especialidade: {dict(self.metrics['success_specialty'])}")
                 self.logger.info(f"Fontes de sucesso - Telefone: {dict(self.metrics['success_phone'])}")
            self.logger.info(f"Contagem de Erros por tipo: {dict(self.metrics['errors'])}")
            self.logger.info(f"-----------------------")

        except FileNotFoundError:
            self.logger.error(f"Arquivo de entrada não encontrado: {input_file}")
        except pd.errors.EmptyDataError:
             self.logger.error(f"Arquivo de entrada vazio ou inválido: {input_file}")
        except ValueError as ve:
             self.logger.error(f"Erro de configuração ou dados inválidos: {ve}")
        except Exception as e:
            self.logger.critical(f"Erro GERAL CATASTRÓFICO na execução do crawler: {e}", exc_info=True)
        finally:
            self.close_all_drivers()
            self.logger.info("Crawler finalizado.")

# Função principal para execução via linha de comando
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Crawler para dados de profissionais da saúde v2.1 (com logging aprimorado).")
    parser.add_argument("input_file", help="Arquivo de entrada CSV com dados dos médicos.")
    parser.add_argument("output_file", help="Arquivo de saída CSV para salvar os resultados.")
    parser.add_argument("--workers", type=int, default=min(4, os.cpu_count()), help="Número máximo de threads paralelas.")
    parser.add_argument("--timeout", type=int, default=30, help="Timeout para requisições HTTP e Selenium (segundos).")
    parser.add_argument("--ollama-timeout", type=int, default=25, help="Timeout para chamadas à API Ollama (segundos).")
    parser.add_argument("--max-urls", type=int, default=15, help="Máximo de URLs a serem processadas por médico.")
    parser.add_argument("--no-selenium", action="store_true", help="Desabilitar o uso do Selenium.")
    parser.add_argument("--no-searx", action="store_true", help="Desabilitar o uso do SearXNG.")
    parser.add_argument("--no-ollama", action="store_true", help="Desabilitar o uso do Ollama para IA.")
    parser.add_argument("--debug", action="store_true", help="Habilitar logging de debug.")
    parser.add_argument("--chromedriver-path", type=str, help="Caminho para o executável do ChromeDriver (opcional).")
    parser.add_argument("--ollama-model", type=str, default="llama3.1:8b", help="Modelo Ollama a ser utilizado.")
    parser.add_argument("--email-threshold", type=float, default=0.3, help="Score mínimo de relevância Ollama para aceitar um e-mail (0.0 a 1.0).")
    parser.add_argument("--searx-url", type=str, default=SEARX_URL, help="URL da instância SearXNG.")
    parser.add_argument("--ollama-url", type=str, default=OLLAMA_URL, help="URL da API Ollama.")

    args = parser.parse_args()

    # Atualizar URLs globais
    SEARX_URL = args.searx_url
    OLLAMA_URL = args.ollama_url

    # Configuração do logger principal
    log_level = logging.DEBUG if args.debug else logging.INFO
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(threadName)s - %(message)s'
    log_file = "medicos_crawler.log"
    logging.basicConfig(level=log_level, format=log_format,
                        handlers=[logging.FileHandler(log_file, mode='w'),
                                  logging.StreamHandler()])
    logging.info(f"Nível de logging definido para: {logging.getLevelName(log_level)}")
    logging.info(f"Log detalhado sendo salvo em: {os.path.abspath(log_file)}")
    if not args.debug:
         logging.getLogger("urllib3").setLevel(logging.WARNING)
         logging.getLogger("selenium").setLevel(logging.WARNING)
         logging.getLogger("webdriver_manager").setLevel(logging.WARNING)

    # Configuração do crawler
    crawler_config = {
        'max_workers': args.workers,
        'timeout': args.timeout,
        'use_selenium': not args.no_selenium and SELENIUM_AVAILABLE,
        'use_searx': not args.no_searx,
        'use_ollama': not args.no_ollama,
        'debug': args.debug,
        'chromedriver_path': args.chromedriver_path,
        'ollama_model': args.ollama_model,
        'ollama_email_threshold': args.email_threshold,
        'ollama_timeout': args.ollama_timeout,
        'max_urls_per_medico': args.max_urls
    }
    logging.info(f"Configurações do Crawler: {crawler_config}")

    # Instanciar e executar
    crawler = MedicosCrawler(config=crawler_config)
    crawler.run(args.input_file, args.output_file)

