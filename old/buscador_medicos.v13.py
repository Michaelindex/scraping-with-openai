import os
import re
import json
import time
import csv
import urllib.parse
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging
from datetime import datetime

# Configurações
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
CEP_CACHE_FILE = "cep_cache.json"
MANUAL_CEP_FILE = "ceps_manuais.json"
CORREIOS_URL = "https://buscacepinter.correios.com.br/app/endereco/index.php"
SEARXNG_URL = "http://localhost:8080/search"
SEARXNG_TIMEOUT = 10
VIACEP_URL = "https://viacep.com.br/ws/{}/json/"
BRASILAPI_URL = "https://brasilapi.com.br/api/cep/v1/{}"
CEPABERTO_URL = "https://www.cepaberto.com/api/v3/cep?cep={}"
MAPBOX_URL = "https://api.mapbox.com/geocoding/v5/mapbox.places/{}.json?access_token={}&country=br"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search?format=json&q={}&countrycodes=br&limit=1"

# Padrões regex
PATTERNS = {
    'cep': re.compile(r'\d{5}-\d{3}|\d{8}'),
    'telefone': re.compile(r'\(?\d{2}\)?\s*\d{4,5}[-\s]?\d{4}'),
    'email': re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
}

# Cache de CEPs
CEP_CACHE = {}

def carregar_cache_cep():
    """Carrega o cache de CEPs do arquivo"""
    try:
        if os.path.exists(CEP_CACHE_FILE):
            with open(CEP_CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}
    except Exception as e:
        print(f"Erro ao carregar cache de CEPs: {e}")
        return {}

def salvar_cache_cep(cache):
    """Salva o cache de CEPs no arquivo"""
    try:
        with open(CEP_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Erro ao salvar cache de CEPs: {e}")

def carregar_ceps_manuais():
    """Carrega CEPs manuais do arquivo"""
    try:
        if os.path.exists(MANUAL_CEP_FILE):
            with open(MANUAL_CEP_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}
    except Exception as e:
        print(f"Erro ao carregar CEPs manuais: {e}")
        return {}

def formatar_cep(cep):
    """Formata o CEP para o padrão XXXXX-XXX"""
    if not cep:
        return ""
    cep = re.sub(r'\D', '', cep)
    if len(cep) == 8:
        return f"{cep[:5]}-{cep[5:]}"
    return cep

def gerar_chave_cache(rua, cidade, uf):
    """Gera uma chave única para o cache"""
    return f"{rua}_{cidade}_{uf}".upper().replace(" ", "_")

def buscar_cep_via_viacep(rua, cidade, uf, driver, logger):
    """Busca CEP usando a API do ViaCEP"""
    if not rua or not cidade or not uf:
        return None
    
    try:
        # Primeira tentativa: busca por endereço
        query = f"{rua}, {cidade}, {uf}"
        response = requests.get(
            VIACEP_URL.format(urllib.parse.quote(query)),
            headers={"User-Agent": USER_AGENT},
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if not data.get('erro'):
                logger.info(f"CEP encontrado via ViaCEP: {data['cep']}")
                return data
        
        # Segunda tentativa: busca por cidade
        query = f"{cidade}, {uf}"
        response = requests.get(
            VIACEP_URL.format(urllib.parse.quote(query)),
            headers={"User-Agent": USER_AGENT},
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if not data.get('erro'):
                logger.info(f"CEP geral encontrado via ViaCEP: {data['cep']}")
                return data
        
        return None
    except Exception as e:
        logger.error(f"Erro ao buscar CEP via ViaCEP: {e}")
        return None

def buscar_cep_via_brasilapi(rua, cidade, uf, driver, logger):
    """Busca CEP usando a API do BrasilAPI"""
    if not rua or not cidade or not uf:
        return None
    
    try:
        # Primeira tentativa: busca por endereço
        query = f"{rua}, {cidade}, {uf}"
        response = requests.get(
            BRASILAPI_URL.format(urllib.parse.quote(query)),
            headers={"User-Agent": USER_AGENT},
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get('cep'):
                logger.info(f"CEP encontrado via BrasilAPI: {data['cep']}")
                return data
        
        # Segunda tentativa: busca por cidade
        query = f"{cidade}, {uf}"
        response = requests.get(
            BRASILAPI_URL.format(urllib.parse.quote(query)),
            headers={"User-Agent": USER_AGENT},
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get('cep'):
                logger.info(f"CEP geral encontrado via BrasilAPI: {data['cep']}")
                return data
        
        return None
    except Exception as e:
        logger.error(f"Erro ao buscar CEP via BrasilAPI: {e}")
        return None

def buscar_cep_via_cepaberto(rua, cidade, uf, driver, logger):
    """Busca CEP usando a API do CEP Aberto"""
    if not rua or not cidade or not uf:
        return None
    
    try:
        # Primeira tentativa: busca por endereço
        query = f"{rua}, {cidade}, {uf}"
        response = requests.get(
            CEPABERTO_URL.format(urllib.parse.quote(query)),
            headers={"User-Agent": USER_AGENT},
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get('cep'):
                logger.info(f"CEP encontrado via CEP Aberto: {data['cep']}")
                return data
        
        # Segunda tentativa: busca por cidade
        query = f"{cidade}, {uf}"
        response = requests.get(
            CEPABERTO_URL.format(urllib.parse.quote(query)),
            headers={"User-Agent": USER_AGENT},
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get('cep'):
                logger.info(f"CEP geral encontrado via CEP Aberto: {data['cep']}")
                return data
        
        return None
    except Exception as e:
        logger.error(f"Erro ao buscar CEP via CEP Aberto: {e}")
        return None

def buscar_cep_via_mapbox(rua, cidade, uf, driver, logger):
    """Busca CEP usando a API do MapBox"""
    if not rua or not cidade or not uf:
        return None
    
    try:
        # Primeira tentativa: busca por endereço
        query = f"{rua}, {cidade}, {uf}"
        response = requests.get(
            MAPBOX_URL.format(urllib.parse.quote(query), os.getenv('MAPBOX_TOKEN', '')),
            headers={"User-Agent": USER_AGENT},
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get('features'):
                feature = data['features'][0]
                if feature.get('context'):
                    for context in feature['context']:
                        if context.get('id', '').startswith('postcode'):
                            cep = context.get('text', '').replace('-', '')
                            if len(cep) == 8:
                                logger.info(f"CEP encontrado via MapBox: {cep}")
                                return {
                                    "cep": formatar_cep(cep),
                                    "logradouro": rua,
                                    "bairro": "",
                                    "localidade": cidade,
                                    "uf": uf,
                                    "complemento": ""
                                }
        
        return None
    except Exception as e:
        logger.error(f"Erro ao buscar CEP via MapBox: {e}")
        return None

def buscar_cep_via_nominatim(rua, cidade, uf, driver, logger):
    """Busca CEP usando a API do Nominatim (OpenStreetMap)"""
    if not rua or not cidade or not uf:
        return None
    
    try:
        # Primeira tentativa: busca por endereço
        query = f"{rua}, {cidade}, {uf}"
        response = requests.get(
            NOMINATIM_URL.format(urllib.parse.quote(query)),
            headers={"User-Agent": USER_AGENT},
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                result = data[0]
                if result.get('display_name'):
                    # Procura por CEP no nome de exibição
                    cep_match = PATTERNS['cep'].search(result['display_name'])
                    if cep_match:
                        cep = cep_match.group(0)
                        logger.info(f"CEP encontrado via Nominatim: {cep}")
                        return {
                            "cep": formatar_cep(cep),
                            "logradouro": rua,
                            "bairro": "",
                            "localidade": cidade,
                            "uf": uf,
                            "complemento": ""
                        }
        
        return None
    except Exception as e:
        logger.error(f"Erro ao buscar CEP via Nominatim: {e}")
        return None

def buscar_cep_por_endereco(rua, cidade, driver, logger):
    """Busca CEP baseado na rua e cidade já encontradas"""
    if not rua or not cidade:
        logger.warning("Rua ou cidade não disponíveis para busca de CEP")
        return ""
    
    # Lista de variações de query para busca
    queries = [
        f"CEP da {rua}, {cidade}",
        f"{rua}, {cidade} CEP",
        f"CEP {rua} {cidade}",
        f"endereço {rua} {cidade} CEP",
        f"localização {rua} {cidade} CEP",
        f"código postal {rua} {cidade}",
        f"postal code {rua} {cidade}"
    ]
    
    for query in queries:
        try:
            logger.info(f"Buscando CEP: {query}")
            driver.get(f"https://www.google.com/search?q={urllib.parse.quote(query)}")
            time.sleep(1)
            
            page_text = driver.page_source
            soup = BeautifulSoup(page_text, 'html.parser')
            text = soup.get_text(' ')
            
            ceps = PATTERNS['cep'].findall(text)
            if ceps:
                cep = formatar_cep(ceps[0])
                logger.info(f"CEP encontrado: {cep}")
                return cep
            
        except Exception as e:
            logger.error(f"Erro na busca: {e}")
            continue
    
    logger.warning("CEP não encontrado em nenhuma tentativa")
    return ""

def buscar_cep_com_cascata(rua, cidade, uf, driver, logger, medico=None):
    """Busca CEP usando sistema de cascata com múltiplas fontes"""
    if not uf:
        logger.warning("UF não disponível para busca de CEP")
        return None
    
    # Se não tiver rua nem cidade, tenta buscar pelo nome do médico
    if not rua and not cidade and medico:
        nome_completo = f"{medico.get('Firstname', '')} {medico.get('LastName', '')}".strip()
        logger.info(f"Tentando buscar endereço pelo nome: {nome_completo}")
        
        # Tenta buscar no Google
        query = f"{nome_completo} médico {medico['UF']} endereço"
        try:
            driver.get(f"https://www.google.com/search?q={urllib.parse.quote(query)}")
            time.sleep(1)
            
            page_text = driver.page_source
            soup = BeautifulSoup(page_text, 'html.parser')
            text = soup.get_text(' ')
            
            # Procura por padrões de endereço
            enderecos = re.findall(r'[A-Za-z\s]+,\s*\d+[A-Za-z\s]*,[A-Za-z\s]+,[A-Za-z\s]+', text)
            if enderecos:
                endereco = enderecos[0]
                partes = endereco.split(',')
                if len(partes) >= 3:
                    rua = partes[0].strip()
                    cidade = partes[-2].strip()
                    logger.info(f"Endereço encontrado: {rua}, {cidade}")
        except Exception as e:
            logger.error(f"Erro ao buscar endereço: {e}")
    
    # Verifica no cache primeiro
    chave_cache = gerar_chave_cache(rua or "", cidade or "", uf)
    if chave_cache in CEP_CACHE:
        logger.info(f"Dados encontrados no cache: {CEP_CACHE[chave_cache]}")
        return CEP_CACHE[chave_cache]
    
    # Verifica CEPs manuais
    if medico:
        nome_chave = f"{medico['Firstname']}_{medico['LastName']}_{medico['UF']}".upper().replace(" ", "_")
        ceps_manuais = carregar_ceps_manuais()
        if nome_chave in ceps_manuais:
            logger.info(f"CEP encontrado em lista manual: {ceps_manuais[nome_chave]}")
            return ceps_manuais[nome_chave]
    
    # Tenta cada fonte em sequência
    sources = [
        (buscar_cep_via_viacep, "ViaCEP"),
        (buscar_cep_via_brasilapi, "BrasilAPI"),
        (buscar_cep_via_cepaberto, "CEP Aberto"),
        (buscar_cep_via_mapbox, "MapBox"),
        (buscar_cep_via_nominatim, "Nominatim"),
        (lambda r, c, u, d, l: buscar_cep_por_endereco(r, c, d, l), "Google Search")
    ]
    
    # Tenta diferentes variações do endereço
    variacoes = []
    if rua:
        variacoes.append((rua, cidade))
        # Remove números e caracteres especiais
        rua_limpa = re.sub(r'[^\w\s]', '', rua)
        if rua_limpa != rua:
            variacoes.append((rua_limpa, cidade))
        # Remove palavras comuns
        rua_sem_palavras = re.sub(r'\b(Rua|Avenida|Av\.|R\.|Alameda|Al\.|Travessa|Tv\.)\b', '', rua_limpa, flags=re.IGNORECASE)
        if rua_sem_palavras != rua_limpa:
            variacoes.append((rua_sem_palavras.strip(), cidade))
    else:
        variacoes.append(("", cidade))
    
    for r, c in variacoes:
        for source_func, source_name in sources:
            try:
                logger.info(f"Tentando buscar CEP via {source_name} com variação: {r}, {c}")
                result = source_func(r, c, uf, driver, logger)
                if result:
                    # Salva no cache
                    CEP_CACHE[chave_cache] = result
                    salvar_cache_cep(CEP_CACHE)
                    return result
            except Exception as e:
                logger.error(f"Erro ao buscar via {source_name}: {e}")
                continue
    
    # Último recurso: CEP geral da cidade
    if cidade:
        logger.info("Tentando obter CEP geral da cidade")
        result = obter_cep_geral_cidade(cidade, uf, logger)
        if result:
            CEP_CACHE[chave_cache] = result
            salvar_cache_cep(CEP_CACHE)
            return result
    
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
    
    # Tenta cada fonte em sequência
    sources = [
        (buscar_cep_via_viacep, "ViaCEP"),
        (buscar_cep_via_brasilapi, "BrasilAPI"),
        (buscar_cep_via_cepaberto, "CEP Aberto"),
        (buscar_cep_via_mapbox, "MapBox"),
        (buscar_cep_via_nominatim, "Nominatim")
    ]
    
    for source_func, source_name in sources:
        try:
            logger.info(f"Tentando buscar CEP geral via {source_name}")
            result = source_func("", cidade, uf, None, logger)
            if result:
                # Salva no cache
                CEP_CACHE[chave_cache] = result
                salvar_cache_cep(CEP_CACHE)
                return result
        except Exception as e:
            logger.error(f"Erro ao buscar CEP geral via {source_name}: {e}")
            continue
    
    return None

def process_medico(m, driver, logger):
    """Processa um médico"""
    start_time = time.time()
    logger.info(f"Processando médico: {m.get('Firstname', '')} {m.get('LastName', '')}")
    
    result = m.copy()
    
    # Extrai endereço e cidade do endereço completo se necessário
    endereco_completo = m.get('Endereco Completo A1', '')
    if endereco_completo and not m.get('Address A1'):
        # Tenta extrair o endereço do campo completo
        partes = endereco_completo.split(',')
        if len(partes) >= 2:
            result['Address A1'] = partes[0].strip()
            logger.info(f"Endereço extraído do campo completo: {result['Address A1']}")
    
    if endereco_completo and not m.get('City A1'):
        # Tenta extrair a cidade do campo completo
        partes = endereco_completo.split(',')
        if len(partes) >= 2:
            result['City A1'] = partes[-2].strip()
            logger.info(f"Cidade extraída do campo completo: {result['City A1']}")
    
    # Busca CEP e dados de endereço usando sistema de cascata
    if result.get('Address A1') or result.get('City A1'):
        logger.info("Buscando CEP com sistema de cascata")
        cep_data = buscar_cep_com_cascata(
            result.get('Address A1', ''),
            result.get('City A1', ''),
            m['UF'],
            driver,
            logger,
            m
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
    else:
        logger.warning("Dados insuficientes para busca de CEP")
    
    logger.info(f"Processamento concluído em {time.time() - start_time:.2f} segundos")
    return result

def main():
    # Configuração do logger
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('buscador_medicos.log'),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)
    
    # Carrega o cache
    global CEP_CACHE
    CEP_CACHE = carregar_cache_cep()
    
    # Configuração do driver
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument(f'user-agent={USER_AGENT}')
    
    driver = webdriver.Chrome(options=options)
    
    try:
        # Processa os médicos
        with open('medicos-input.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            results = []
            
            for m in reader:
                result = process_medico(m, driver, logger)
                results.append(result)
        
        # Salva os resultados
        with open('medicos-output.csv', 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)
        
        logger.info("Processamento concluído com sucesso")
    
    finally:
        driver.quit()

if __name__ == "__main__":
    main() 