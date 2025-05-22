#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
buscador_medicos.py

Aprimoramentos:
- Prioriza telefones celulares (começando com DDD +9)
- Filtra e-mails inválidos (strings com 'subject=')
- Remove complementos sem sentido (e.g., 'Salarial')
- Especialista de descoberta de cidades via CEP e busca na web
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

# Configurações
SEARX_URL   = "http://124.81.6.163:8092/search"  # Atualizado para usar IP diretamente
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
LOG_FILE = os.path.join(DATA_DIR, 'buscador_medicos.log')
DEBUG_HTML_DIR = os.path.join(DATA_DIR, 'debug_html')

# Criar diretório data se não existir
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

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
        f.write("""google.com
microsoft.com
bing.com
yahoo.com
hotmail.com
outlook.com
live.com
msn.com
aol.com
icloud.com
apple.com
amazon.com
facebook.com
twitter.com
instagram.com
linkedin.com
youtube.com
wikipedia.org
wordpress.com
blogspot.com
medium.com
github.com
gitlab.com
bitbucket.org
dropbox.com
drive.google.com
docs.google.com
maps.google.com
translate.google.com
calendar.google.com
mail.google.com
accounts.google.com
support.google.com
cloud.google.com
play.google.com
chrome.google.com
firebase.google.com
analytics.google.com
ads.google.com
business.google.com
myaccount.google.com
pay.google.com
photos.google.com
meet.google.com
hangouts.google.com
chat.google.com
keep.google.com
sites.google.com
groups.google.com
classroom.google.com
admin.google.com
security.google.com
support.microsoft.com
office.com
microsoftonline.com
sharepoint.com
teams.microsoft.com
onedrive.com
azure.com
windows.com
office365.com
microsoft365.com
skype.com
stackoverflow.com
stackexchange.com
reddit.com
quora.com
dev.to
hackernews.com
producthunt.com
behance.net
dribbble.com
flickr.com
pinterest.com
tumblr.com
substack.com
wix.com
squarespace.com
weebly.com
shopify.com
ebay.com
alibaba.com
walmart.com
target.com
bestbuy.com
newegg.com
etsy.com
aliexpress.com
wish.com
shopee.com
mercadolivre.com
americanas.com
submarino.com
magazineluiza.com
casasbahia.com
extra.com
pontofrio.com
shoptime.com
netshoes.com
centauro.com
dafiti.com
kanui.com
zattini.com
riachuelo.com
c&a.com
renner.com
marisa.com
lupo.com
havaianas.com
nike.com
adidas.com
puma.com
reebok.com
underarmour.com
newbalance.com
asics.com
mizuno.com
brooks.com
saucony.com
skechers.com
converse.com
vans.com
timberland.com
dr.martens.com
clarks.com
ecco.com
geox.com
crocs.com
birkenstock.com
ipanema.com
melissa.com
grendene.com
arezzo.com
schutz.com
anacapri.com
dumond.com
carlos.com
carmim.com
dakota.com
democrata.com
ferracini.com
flormar.com
forum.com
greggo.com
klin.com
lacoste.com
lepostiche.com
malwee.com
mormaii.com
oakley.com
olympikus.com
penalty.com
pernambucanas.com
trackandfield.com
tramontina.com""")
    SITE_BLACKLIST = carregar_lista_arquivo(SITE_BLACKLIST_FILE)

def is_blacklisted_site(url):
    """Verifica se o site está na blacklist"""
    for domain in SITE_BLACKLIST:
        if domain in url.lower():
            logger.info(f"Site {url} está na blacklist")
            return True
    return False

# Padrões regex
PATTERNS = {
    'address': re.compile(r"(?:Av\.?enida|Rua|Travessa|Estrada|Alameda|Avenida)[^,\n]{5,100}(?:,?\s*(?:Num|Nº|Número)?\s*\d{1,5})?(?:\s*,\s*[^,\n]{1,50})?(?:\s*\([^)]+\))?", re.IGNORECASE),
    'cep':     re.compile(r"\d{5}-\d{3}"),
    'phone':   re.compile(r"\(\d{2}\)\s?\d{4,5}-\d{4}"),
    'email':   re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"),
    'complement': re.compile(r"(?:Sala|Bloco|Apt\.?|Conjunto)[^,\n]{1,50}"),
    'specialty': re.compile(r"(?:" + "|".join(ESPECIALIDADES) + r")(?:\s+e\s+(?:" + "|".join(ESPECIALIDADES) + r"))?", re.IGNORECASE)
}

# Normaliza telefones para formato padrão
def normalize_phone(raw):
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 11:
        return f"({digits[:2]}) {digits[2:7]}-{digits[7:]}"
    if len(digits) == 10:
        return f"({digits[:2]}) {digits[2:6]}-{digits[6:]}"
    return raw

# 1. Query Planning
def build_query(m):
    q = f"{m['Firstname']} {m['LastName']} CRM {m['CRM']} {m['UF']} telefone e-mail endereço"
    logger.info(f"Query: {q}")
    return q

# 2. Retrieval functions (SearXNG, Google, Bing)
def search_searx(query):
    try:
        params = {
            'q': query,
            'format': 'json',
            'engines': 'google,bing',
            'language': 'pt-BR',
            'results': MAX_RESULTS
        }
        headers = {'User-Agent': USER_AGENT}
        
        r = requests.get(SEARX_URL, params=params, headers=headers, timeout=10)
        if r.status_code == 200:
            data = r.json()
            urls = [r['url'] for r in data.get('results', [])]
            logger.info(f"SearX found {len(urls)} URLs")
            return urls
    except Exception as e:
        logger.error(f"SearX error: {e}")
    return []

def search_google(query, driver):
    urls = []
    page_text = ""
    try:
        driver.get(f"https://www.google.com/search?q={query}")
        time.sleep(2)  # Espera carregar
        
        # Captura URLs normais
        for a in driver.find_elements(By.CSS_SELECTOR, "a[href^='http']"):
            href = a.get_attribute('href')
            if href and 'google.com' not in href:
                urls.append(href)
        
        # Captura texto da página para análise
        page_text = driver.find_element(By.TAG_NAME, "body").text
        
        logger.info(f"Google found {len(urls)} URLs")
        return urls, page_text
    except Exception as e:
        logger.error(f"Google error: {e}")
        return [], ""

def search_bing(query, driver):
    urls = []
    page_text = ""
    try:
        driver.get(f"https://www.bing.com/search?q={query}")
        time.sleep(2)  # Espera carregar
        
        # Captura URLs dos resultados
        for a in driver.find_elements(By.CSS_SELECTOR, "a[href^='http']"):
            href = a.get_attribute('href')
            if href and 'bing.com' not in href:
                urls.append(href)
        
        # Captura texto da página para análise
        page_text = driver.find_element(By.TAG_NAME, "body").text
        
        logger.info(f"Bing found {len(urls)} URLs")
        return urls, page_text
    except Exception as e:
        logger.error(f"Bing error: {e}")
        return [], ""

# 3. Download & extract candidates
def save_debug_html(url, html):
    """Salva o HTML para debug, extraindo apenas o conteúdo relevante"""
    try:
        if not os.path.exists(DEBUG_HTML_DIR):
            os.makedirs(DEBUG_HTML_DIR)
            
        # Cria um nome de arquivo baseado na URL
        filename = hashlib.md5(url.encode()).hexdigest() + '.html'
        filepath = os.path.join(DEBUG_HTML_DIR, filename)
        
        # Extrai apenas o conteúdo relevante
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove scripts, styles e outros elementos não relevantes
        for element in soup.find_all(['script', 'style', 'meta', 'link', 'noscript', 'iframe']):
            element.decompose()
            
        # Procura por elementos que podem conter o conteúdo principal
        main_content = None
        
        # Tenta encontrar elementos comuns que contêm o conteúdo principal
        for tag in ['main', 'article', 'div[role="main"]', '.content', '#content', '.main-content', '#main-content']:
            main_content = soup.select_one(tag)
            if main_content:
                break
                
        # Se não encontrou um elemento principal, usa o body
        if not main_content:
            main_content = soup.body
            
        # Se ainda não encontrou, usa o html inteiro
        if not main_content:
            main_content = soup
            
        # Extrai o texto e limpa
        content = main_content.get_text(separator='\n', strip=True)
        
        # Adiciona a URL original como comentário
        html_content = f"<!-- URL original: {url} -->\n{content}"
        
        # Salva o conteúdo limpo
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)
            
        logger.info(f"HTML salvo em: {filepath}")
        return filepath
    except Exception as e:
        logger.error(f"Erro ao salvar HTML: {e}")
        return None

def download_html(url, driver=None):
    try:
        logger.info(f"Tentando baixar HTML de: {url}")
        
        if driver:
            logger.info("Usando Selenium para baixar HTML")
            driver.get(url)
            # Espera a página carregar
            time.sleep(5)  # Espera 5 segundos para garantir que a página carregue
            html = driver.page_source
            logger.info(f"HTML baixado via Selenium, tamanho: {len(html)}")
        else:
            logger.info("Usando Requests para baixar HTML")
            r = requests.get(url, timeout=10)
            ct = r.headers.get('Content-Type','')
            logger.info(f"Response status: {r.status_code}, content-type: {ct}")
            
            if r.status_code == 200 and 'html' in ct.lower():
                html = r.text
            else:
                logger.warning(f"Download failed for {url}: status={r.status_code}, content-type={ct}")
                return ''
        
        # Salva o HTML para debug
        save_debug_html(url, html)
        logger.info(f"Download successful for {url}")
        return html
    except Exception as e:
        logger.error(f"Download fail {url}: {e}")
    return ''

def limpar_endereco(endereco):
    """Limpa e formata o endereço extraído"""
    if not endereco:
        return ''
    
    # Remove textos indesejados
    endereco_limpo = endereco
    for texto in TEXTOS_REMOVER:
        endereco_limpo = endereco_limpo.replace(texto, '')
    
    # Remove espaços múltiplos
    endereco_limpo = re.sub(r'\s+', ' ', endereco_limpo)
    
    # Remove espaços antes de pontuação
    endereco_limpo = re.sub(r'\s+([,\.])', r'\1', endereco_limpo)
    
    # Remove espaços no início e fim
    endereco_limpo = endereco_limpo.strip()
    
    # Se o endereço terminar com vírgula, remove
    endereco_limpo = endereco_limpo.rstrip(',')
    
    logger.info(f"Endereço original: {endereco}")
    logger.info(f"Endereço limpo: {endereco_limpo}")
    
    return endereco_limpo

def validar_endereco(endereco):
    """Valida se o endereço tem pelo menos uma rua/avenida e um número"""
    if not endereco:
        return False
    
    # Verifica se tem uma rua/avenida
    tem_rua = bool(re.search(r'(?:Av\.?enida|Rua|Travessa|Estrada|Alameda|Avenida)', endereco, re.IGNORECASE))
    
    # Verifica se tem um número
    tem_numero = bool(re.search(r'(?:,|,?\s+)(?:Num|Nº|Número)?\s*\d{1,5}', endereco))
    
    # Log para debug
    logger.info(f"Validação de endereço: {endereco}")
    logger.info(f"Tem rua: {tem_rua}, Tem número: {tem_numero}")
    
    return tem_rua and tem_numero

def validar_email(email):
    """Valida se o email não está na blacklist"""
    if not email:
        return False
    
    # Verifica se o email está na blacklist
    for dominio in EMAIL_BLACKLIST:
        if dominio in email.lower():
            logger.info(f"Email {email} está na blacklist")
            return False
    
    return True

def extract_candidates(html, url=None):
    soup = BeautifulSoup(html, 'html.parser')
    soup_text = soup.get_text(' ')
    addrs = []
    comps = []

    if url and 'boaconsulta.com' in url:
        # Encontrar o bloco de consultórios
        consultorios = None
        for tag in soup.find_all(['div', 'section']):
            if tag.get_text().strip().startswith('Selecione o consultório'):
                consultorios = tag
                break
        if consultorios:
            # Extrair endereços dentro do bloco de consultórios
            for addr_tag in consultorios.find_all(class_='speakable-locations-address'):
                addr = addr_tag.get_text(' ', strip=True)
                addrs.append(addr)
                # Procura complementos próximos ao endereço
                parent = addr_tag.parent
                if parent:
                    # Procura complementos no mesmo elemento pai
                    for comp_tag in parent.find_all(string=re.compile(r'(?:Sala|Bloco|Apt\.?|Conjunto)', re.I)):
                        comp = comp_tag.strip()
                        if comp and 'salari' not in comp.lower():
                            comps.append(comp)
            # Fallback: pega linhas que parecem endereço dentro do bloco
            if not addrs:
                for line in consultorios.stripped_strings:
                    if re.search(r'\d{1,5}.*(Rua|Avenida|Travessa|Estrada|Alameda|Largo|Praça|Rodovia)', line, re.I):
                        addrs.append(line)
        logger.info(f"Endereços extraídos do bloco de consultórios do BoaConsulta: {addrs}")
        logger.info(f"Complementos extraídos do bloco de consultórios do BoaConsulta: {comps}")
    else:
        # Comportamento padrão para outros sites
        address_elements = soup.find_all(['p', 'div', 'span'], string=re.compile(r'(?:Endereço|Local|Atendimento)', re.I))
        for elem in address_elements:
            addr_text = elem.get_text(' ')
            addrs.extend(PATTERNS['address'].findall(addr_text))
            # Procura complementos no mesmo elemento
            comps.extend(PATTERNS['complement'].findall(addr_text))
        if not addrs:
            addrs = PATTERNS['address'].findall(soup_text)

    # Limpa e valida os endereços encontrados
    addrs = [limpar_endereco(addr) for addr in addrs]
    addrs = [addr for addr in addrs if validar_endereco(addr)]

    # Procura especialidades em elementos específicos
    specialty_elements = soup.find_all(['p', 'div', 'span', 'h1', 'h2', 'h3'], string=re.compile(r'(?:Especialidade|Área|Atuação)', re.I))
    specialties = []
    for elem in specialty_elements:
        spec_text = elem.get_text(' ')
        logger.info(f"Found specialty element: {spec_text}")
        specialties.extend(PATTERNS['specialty'].findall(spec_text))
    
    # Se não encontrou em elementos específicos, procura no texto todo
    if not specialties:
        specialties = PATTERNS['specialty'].findall(soup_text)
    
    ceps  = PATTERNS['cep'].findall(soup_text)
    phones= PATTERNS['phone'].findall(soup_text)
    emails= PATTERNS['email'].findall(soup_text)
    
    # tel: links
    for a in soup.select("a[href^='tel:']"):
        num = a['href'].split(':',1)[1]
        norm = normalize_phone(num)
        if norm not in phones: phones.append(norm)
    
    # mailto: links
    for a in soup.select("a[href^='mailto:']"):
        mail = a['href'].split(':',1)[1]
        if 'subject=' in mail: continue
        if mail not in emails: emails.append(mail)
    
    # dedupe
    def dedupe(lst):
        seen, out = set(), []
        for x in lst:
            if x not in seen:
                seen.add(x); out.append(x)
        return out
    
    # Filtra emails da blacklist
    emails = [email for email in dedupe(emails) if validar_email(email)]
    
    cands = {
        'address': dedupe(addrs),
        'cep':     dedupe(ceps),
        'phone':   dedupe(phones),
        'email':   dedupe(emails),
        'complement': [c for c in dedupe(comps) if len(c.strip())>3 and 'salari' not in c.lower()],
        'specialty': dedupe(specialties)
    }
    
    # Log detalhado dos candidatos encontrados
    for k,v in cands.items(): 
        logger.info(f"Candidates {k}: {v}")
        if k == 'address' and not v:
            logger.warning("No valid addresses found in the text!")
        if k == 'specialty' and not v:
            logger.warning("No specialties found in the text!")
    
    return cands

# 4. Aggregate & rank
def aggregate_and_rank(all_c):
    ranked = {}
    for k,lst in all_c.items():
        ranked[k] = [item for item,_ in Counter(lst).most_common()]
        logger.info(f"Ranked {k}: {ranked[k]}")
    return ranked

# 5. Validation via Ollama
def ask_ollama(prompt):
    try:
        r = requests.post(OLLAMA_URL, json={'model':'llama3.1:8b','prompt':prompt,'stream':False}, timeout=10)
        if r.status_code == 200: return r.json().get('response','').strip()
    except Exception as e:
        logger.error(f"Ollama error: {e}")
    return ''

def carregar_exemplos():
    """Carrega os exemplos de treinamento do arquivo"""
    exemplos = {}
    categoria_atual = None
    
    try:
        with open(EXEMPLOS_FILE, 'r', encoding='utf-8') as f:
            for linha in f:
                linha = linha.strip()
                if not linha:
                    continue
                    
                if linha.startswith('#'):
                    continue
                    
                if linha.endswith(':'):
                    categoria_atual = linha[:-1]
                    exemplos[categoria_atual] = []
                elif categoria_atual and linha.startswith('- '):
                    exemplos[categoria_atual].append(linha[2:])
        
        logger.info(f"Exemplos carregados: {list(exemplos.keys())}")
        return exemplos
    except Exception as e:
        logger.error(f"Erro ao carregar exemplos: {e}")
        return {}

# Carrega os exemplos de treinamento
EXEMPLOS = carregar_exemplos()

def criar_prompt_validacao(field, cands, m):
    """Cria um prompt específico para validação usando exemplos"""
    exemplos_field = EXEMPLOS.get(field.upper(), [])
    exemplos_text = "\n".join([f"- {ex}" for ex in exemplos_field[:5]])  # Usa até 5 exemplos
    
    prompt = f"""
    Analise os dados do médico {m['Firstname']} {m['LastName']} (CRM {m['CRM']} {m['UF']}).
    
    Exemplos de {field}s válidos:
    {exemplos_text}
    
    Dados encontrados:
    {cands}
    
    Qual é o {field} mais confiável? Responda apenas o valor.
    """
    
    return prompt

def validate(field, cands, m):
    if not cands: return ''
    
    prompt = criar_prompt_validacao(field, cands, m)
    resp = ask_ollama(prompt).lower()
    
    for c in cands:
        if resp in c.lower():
            logger.info(f"Validated {field}: {c}")
            return c
    
    logger.info(f"Fallback {field}: {cands[0]}")
    return cands[0]

# Função para consultar cidade via ViaCEP (Especialista de Cidade)
def obter_cidade_via_cep(cep):
    if not cep:
        return None
    
    cep_limpo = re.sub(r'\D', '', cep)
    if len(cep_limpo) != 8:
        logger.warning(f"CEP inválido: {cep}")
        return None
    
    try:
        url = f"https://viacep.com.br/ws/{cep_limpo}/json/"
        logger.info(f"Consultando ViaCEP: {url}")
        
        response = requests.get(url, timeout=5)
        if response.status_code != 200:
            logger.warning(f"Erro ao consultar ViaCEP: Status {response.status_code}")
            return None
        
        dados = response.json()
        if 'erro' in dados and dados['erro']:
            logger.warning(f"CEP não encontrado: {cep}")
            return None
        
        cidade = dados.get('localidade')
        if cidade:
            logger.info(f"Cidade encontrada via ViaCEP: {cidade}")
            return cidade
        else:
            logger.warning("ViaCEP não retornou cidade")
            return None
    
    except Exception as e:
        logger.error(f"Erro ao consultar ViaCEP: {e}")
        return None

# Função para consultar a IA e extrair a cidade (Especialista de Cidade)
def extrair_cidade_via_ia(textos, endereco, uf):
    if not textos or not endereco:
        return None
    
    # Prepara o prompt específico para a IA
    prompt = f"""
    Analise os textos abaixo e extraia APENAS o nome da cidade onde está localizado o endereço: "{endereco}" no estado {uf}.
    
    Responda SOMENTE com o nome da cidade, sem pontuação, sem explicações adicionais.
    Se não conseguir identificar a cidade com certeza, responda apenas "DESCONHECIDA".
    
    Textos para análise:
    {textos[:4000]}  # Limitando o tamanho para não sobrecarregar
    """
    
    try:
        r = requests.post(
            OLLAMA_URL, 
            json={
                'model': 'llama3.1:8b',
                'prompt': prompt,
                'stream': False
            }, 
            timeout=15
        )
        
        if r.status_code == 200:
            resposta = r.json().get('response', '').strip()
            
            # Limpa a resposta para garantir que seja apenas o nome da cidade
            resposta = re.sub(r'[^\w\sÀ-ÿ]', '', resposta).strip()
            
            if resposta.upper() == "DESCONHECIDA":
                logger.warning("IA não conseguiu identificar a cidade")
                return None
                
            logger.info(f"Cidade extraída via IA: {resposta}")
            return resposta
        else:
            logger.error(f"Erro ao consultar IA: Status {r.status_code}")
            return None
            
    except Exception as e:
        logger.error(f"Erro ao consultar IA: {e}")
        return None

# Função principal para descobrir a cidade (Especialista de Cidade)
def descobrir_cidade(endereco, cep, uf, driver):
    logger.info(f"Iniciando descoberta de cidade para endereço: {endereco}, CEP: {cep}, UF: {uf}")
    
    # Função para normalizar texto
    def normalizar_texto(texto):
        if not texto:
            return ''
        # Remove acentos, converte para minúsculo e remove caracteres especiais
        texto = texto.lower()
        texto = re.sub(r'[áàãâä]', 'a', texto)
        texto = re.sub(r'[éèêë]', 'e', texto)
        texto = re.sub(r'[íìîï]', 'i', texto)
        texto = re.sub(r'[óòõôö]', 'o', texto)
        texto = re.sub(r'[úùûü]', 'u', texto)
        texto = re.sub(r'[ç]', 'c', texto)
        texto = re.sub(r'[^a-z0-9\s]', '', texto)
        return texto.strip()
    
    # Etapa 1: Buscar no SearXNG
    if endereco:
        query = f"{endereco} {uf}"
        logger.info(f"Query de busca para SearXNG: {query}")
        try:
            url = f"{SEARX_URL}?q={query}&category_general=1&language=auto&time_range=&safesearch=0&theme=simple"
            logger.info(f"URL de busca SearXNG: {url}")
            driver.get(url)
            time.sleep(2)
            html = driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            
            # Pega TODAS as meta descrições da classe result result-default category-general
            contents = []
            for result in soup.find_all('article', class_='result result-default category-general'):
                content = result.find('p', class_='content')
                if content:
                    contents.append(content.get_text(' ', strip=True))
            
            logger.info(f"Meta descrições captadas: {contents}")
            
            # Tenta cada meta descrição em sequência
            for content in contents:
                # Procura o CEP na meta descrição
                cep_match = re.search(r'\d{5}-\d{3}', content)
                if cep_match:
                    cep = cep_match.group(0)
                    logger.info(f"CEP encontrado na meta descrição: {cep}")
                    
                    # Consulta o ViaCEP
                    cep_limpo = re.sub(r'\D', '', cep)
                    if len(cep_limpo) == 8:
                        try:
                            url = f"https://viacep.com.br/ws/{cep_limpo}/json/"
                            logger.info(f"Consultando ViaCEP: {url}")
                            response = requests.get(url, timeout=5)
                            if response.status_code == 200:
                                dados = response.json()
                                if 'erro' not in dados:
                                    # Normaliza os textos para comparação
                                    logradouro_viacep = normalizar_texto(dados.get('logradouro', ''))
                                    bairro_viacep = normalizar_texto(dados.get('bairro', ''))
                                    cidade_viacep = normalizar_texto(dados.get('localidade', ''))
                                    estado_viacep = normalizar_texto(dados.get('uf', ''))
                                    
                                    endereco_limpo = normalizar_texto(endereco)
                                    
                                    # Verifica se os dados batem
                                    logradouro_match = logradouro_viacep in endereco_limpo
                                    bairro_match = bairro_viacep in endereco_limpo
                                    cidade_match = cidade_viacep in endereco_limpo
                                    estado_match = estado_viacep in endereco_limpo
                                    
                                    logger.info(f"Comparação de endereços:")
                                    logger.info(f"Logradouro ViaCEP: {logradouro_viacep}")
                                    logger.info(f"Endereço nosso: {endereco_limpo}")
                                    logger.info(f"Matches: Logradouro={logradouro_match}, Bairro={bairro_match}, Cidade={cidade_match}, Estado={estado_match}")
                                    
                                    # Se pelo menos o logradouro e o bairro batem, consideramos válido
                                    if logradouro_match and bairro_match:
                                        logger.info(f"Endereço do ViaCEP bate com o nosso!")
                                        logger.info(f"Dados completos do ViaCEP: {dados}")
                                        return {
                                            'cidade': dados.get('localidade'),
                                            'bairro': dados.get('bairro'),
                                            'cep': dados.get('cep'),
                                            'estado': dados.get('uf'),
                                            'logradouro': dados.get('logradouro')
                                        }
                                    else:
                                        logger.info(f"Endereço do ViaCEP não bate com o nosso")
                                        # Continua para o próximo CEP
                                        continue
                        except Exception as e:
                            logger.error(f"Erro ao consultar ViaCEP: {e}")
                            # Continua para o próximo CEP
                            continue
            
            logger.warning("Nenhum CEP válido encontrado nas meta descrições")
                
        except Exception as e:
            logger.error(f"Erro ao buscar no SearXNG: {e}")
    
    logger.warning("Não foi possível encontrar a cidade")
    return None

def extrair_numero_endereco(endereco):
    """Extrai o número do endereço usando regex e IA"""
    if not endereco:
        return ''
    
    # Primeiro tenta com regex
    padroes = [
        r'(?:,|,?\s+)(?:Num|Nº|Número)?\s*(\d{1,5})(?=\s*,|\s+\()',  # Número após vírgula
        r'(?:,|,?\s+)(?:Num|Nº|Número)?\s*(\d{1,5})(?=\s*,|\s+$)',   # Número no final
        r'(?:,|,?\s+)(?:Num|Nº|Número)?\s*(\d{1,5})',                 # Número em qualquer lugar
    ]
    
    for padrao in padroes:
        match = re.search(padrao, endereco)
        if match:
            numero = match.group(1)
            logger.info(f"Número encontrado via regex: {numero}")
            return numero
    
    # Se não encontrou com regex, usa IA
    exemplos = EXEMPLOS.get('ENDEREÇO_NUMERO', [])
    exemplos_text = "\n".join([f"- {ex}" for ex in exemplos[:5]])
    
    prompt = f"""
    Analise o endereço abaixo e extraia APENAS o número do endereço (não confunda com números de complementos como 'Sala 45' ou 'Apto 101').

    Exemplos de endereços com números:
    {exemplos_text}

    Endereço para análise:
    {endereco}

    Responda APENAS com o número do endereço, sem pontuação ou texto adicional.
    Se não houver número claro, responda "NÃO_ENCONTRADO".
    """
    
    try:
        r = requests.post(
            OLLAMA_URL, 
            json={
                'model': 'llama3.1:8b',
                'prompt': prompt,
                'stream': False
            }, 
            timeout=15
        )
        
        if r.status_code == 200:
            resposta = r.json().get('response', '').strip()
            
            # Limpa a resposta
            numero = re.sub(r'[^\d]', '', resposta)
            
            if numero and numero != "NAO_ENCONTRADO":
                logger.info(f"Número encontrado via IA: {numero}")
                return numero
            else:
                logger.warning("IA não conseguiu identificar o número")
                return ''
        else:
            logger.error(f"Erro ao consultar IA: Status {r.status_code}")
            return ''
            
    except Exception as e:
        logger.error(f"Erro ao consultar IA: {e}")
        return ''

def process_medico(m, driver):
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

    # filter docs and unique
    seen, uf = [], []
    for u in urls:
        if any(ext in u.lower() for ext in ['.pdf','.doc','.xls']):
            continue
        if is_blacklisted_site(u):
            continue
        if u not in seen:
            seen.append(u); uf.append(u)
    logger.info(f"URLs únicas após filtro: {uf}")

    # extract & aggregate
    all_c = {k: [] for k in ['address','cep','phone','email','complement','specialty']}
    all_html_texts = []  # Para análise de cidade
    
    for u in uf:
        html = download_html(u, driver)  # Passando o driver para usar Selenium
        if not html: continue
        c = extract_candidates(html, u)
        
        # Extrair texto completo para busca de cidade
        soup = BeautifulSoup(html, 'html.parser')
        all_html_texts.append(soup.get_text(' '))
        
        for k in all_c: all_c[k].extend(c.get(k, []))
    
    ranked = aggregate_and_rank(all_c)

    # prioriza celulares
    phones = ranked['phone']
    cell1 = next((p for p in phones if re.match(r"\(\d{2}\)\s?9", p)), None)
    cell2 = next((p for p in phones if re.match(r"\(\d{2}\)\s?9", p) and p != cell1), None)
    phone1 = cell1 or (phones[0] if phones else '')
    phone2 = cell2 or (phones[1] if len(phones)>1 else '')
    
    # Especialista de Cidade: Descobrir a cidade
    endereco_original = ranked['address'][0] if ranked['address'] else ''
    cep = ranked['cep'][0] if ranked['cep'] else ''
    uf = m['UF']
    
    # Extrai o número do endereço original
    numero = extrair_numero_endereco(endereco_original)
    logger.info(f"Número extraído do endereço: {numero}")
    
    # Consulta o ViaCEP usando o endereço normalizado
    dados_endereco = descobrir_cidade(endereco_original, cep, uf, driver)
    if not dados_endereco:
        logger.warning(f"Não foi possível descobrir a cidade para CRM {m['CRM']}")
        cidade = bairro = cep = estado = ''  # Deixa vazio quando não encontrar a cidade
    else:
        cidade = dados_endereco['cidade']
        bairro = dados_endereco['bairro']
        cep = dados_endereco['cep']
        estado = dados_endereco['estado']

    # cria dicionário de dados novos
    novos_dados = {
        'Address A1': endereco_original,  # Mantém o endereço original
        'Numero A1': numero,  # Número extraído do endereço original
        'Bairro A1': bairro,  # Adiciona o bairro do ViaCEP
        'Complement A1': ranked['complement'][0] if ranked['complement'] else '',
        'postal code A1': cep,
        'City A1': cidade,
        'State A1': estado,
        'Phone A1': phone1,
        'Phone A2': phone2,
        'Cell phone A1': cell1 or '',
        'Cell phone A2': cell2 or '',
        'E-mail A1': validate('email', [e for e in ranked['email'] if 'subject=' not in e], m),
        'E-mail A2': (validate('email', [e for e in ranked['email'] if 'subject=' not in e][1:], m)
                      if len(ranked['email']) > 1 else ''),
        'Medical specialty': ranked['specialty'][0] if ranked['specialty'] else ''
    }

    # retorna apenas os dados novos que ainda estão vazios
    dados_final = {k: (novos_dados[k] if not m.get(k, '').strip() else m[k]) for k in novos_dados}
    return {**m, **dados_final}

# CSV output — também adaptado para manter dados existentes
def run(inp, outp):
    driver = make_driver()
    with open(inp, newline='', encoding='utf-8') as inf, open(outp, 'w', newline='', encoding='utf-8') as outf:
        reader = csv.DictReader(inf, delimiter=',')
        extras = ['Address A1','Complement A1','postal code A1','City A1','State A1',
                  'Phone A1','Phone A2','Cell phone A1','Cell phone A2','E-mail A1','E-mail A2']
        new_extras = [e for e in extras if e not in reader.fieldnames]
        fieldnames = reader.fieldnames + new_extras
        writer = csv.DictWriter(outf, fieldnames=fieldnames, delimiter=',')
        writer.writeheader()
        for row in reader:
            res = process_medico(row, driver)
            out_row = {
                k: (res.get(k, '') if not row.get(k, '').strip() else row[k])
                for k in fieldnames
            }
            writer.writerow(out_row)
    driver.quit()
    logger.info(f"Processing complete. Output: {outp}")

def make_driver():
    opts = Options()
    # Removendo o modo headless
    # opts.add_argument('--headless=new')
    opts.add_argument(f'user-agent={USER_AGENT}')
    opts.add_argument('--disable-gpu')
    opts.add_argument('--no-sandbox')
    opts.add_experimental_option('excludeSwitches',['enable-logging'])
    driver = webdriver.Chrome(options=opts)
    logger.info("Driver Chrome iniciado em modo visível")
    return driver

# Execução
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: python buscador_medicos.py medicos_input.csv medicos_output.csv')
    else:
        run(sys.argv[1], sys.argv[2])
