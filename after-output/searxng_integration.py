"""
Módulo de integração com SearXNG para busca avançada de informações médicas
"""

import requests
import json
import logging
import random
import time
from typing import Dict, List, Any, Optional, Union
from urllib.parse import quote_plus

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SearXNGIntegration")

# URL do SearXNG
SEARX_URL = "http://124.81.6.163:8092/search"

# User agents para rotação
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59"
]

def search_medico(medico_info: Dict[str, Any], query_type: str = "general", num_results: int = 5) -> List[Dict[str, Any]]:
    """
    Realiza uma busca por informações de médico usando SearXNG.
    
    Args:
        medico_info (dict): Informações do médico
        query_type (str): Tipo de busca ('general', 'specialty', 'contact')
        num_results (int): Número de resultados a retornar
        
    Returns:
        list: Lista de resultados (dicts com title, url, snippet)
    """
    nome = f"{medico_info.get('Firstname', '')} {medico_info.get('LastName', '')}".strip()
    crm = medico_info.get('CRM', '')
    uf = medico_info.get('UF', '')
    
    if not nome or not crm or not uf:
        logger.warning("Informações insuficientes para busca")
        return []
    
    # Construir query baseada no tipo de busca
    if query_type == "specialty":
        query = f"médico {nome} CRM {crm} {uf} especialidade especialista"
    elif query_type == "contact":
        query = f"médico {nome} CRM {crm} {uf} contato email telefone"
    else:  # general
        query = f"médico {nome} CRM {crm} {uf}"
    
    try:
        # Adicionar delay para evitar sobrecarga
        time.sleep(random.uniform(1, 3))
        
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
                logger.info(f"Busca bem-sucedida: {len(data['results'])} resultados para {nome}")
                return data['results']
            else:
                logger.warning(f"Busca sem resultados para {nome}")
        else:
            logger.error(f"Erro na busca: status code {response.status_code}")
        
        return []
    except Exception as e:
        logger.error(f"Erro ao realizar busca: {e}")
        return []

def filter_medical_results(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Filtra resultados para priorizar sites médicos relevantes.
    
    Args:
        results (list): Lista de resultados da busca
        
    Returns:
        list: Lista filtrada de resultados
    """
    if not results:
        return []
    
    # Palavras-chave para priorização
    priority_keywords = [
        'medico', 'médico', 'doutor', 'dr', 'dra', 'crm', 'saude', 'saúde', 
        'clinica', 'clínica', 'hospital', 'consultório', 'especialista'
    ]
    
    # Domínios de baixa prioridade
    low_priority_domains = [
        'facebook.com', 'instagram.com', 'twitter.com', 'youtube.com',
        'linkedin.com', 'tiktok.com', 'pinterest.com'
    ]
    
    # Função para calcular pontuação de relevância
    def calculate_relevance(result):
        score = 0
        
        # Verificar título
        title = result.get('title', '').lower()
        for keyword in priority_keywords:
            if keyword in title:
                score += 2
        
        # Verificar snippet
        snippet = result.get('content', '').lower()
        for keyword in priority_keywords:
            if keyword in snippet:
                score += 1
        
        # Verificar domínio
        url = result.get('url', '').lower()
        for domain in low_priority_domains:
            if domain in url:
                score -= 5
        
        return score
    
    # Adicionar pontuação aos resultados
    scored_results = [(calculate_relevance(r), r) for r in results]
    
    # Ordenar por pontuação e filtrar resultados com pontuação negativa
    filtered_results = [r for score, r in sorted(scored_results, key=lambda x: x[0], reverse=True) if score >= 0]
    
    return filtered_results

def extract_domains_from_results(results: List[Dict[str, Any]]) -> List[str]:
    """
    Extrai domínios únicos dos resultados de busca.
    
    Args:
        results (list): Lista de resultados da busca
        
    Returns:
        list: Lista de domínios únicos
    """
    domains = set()
    
    for result in results:
        url = result.get('url', '')
        if url:
            # Extrair domínio da URL
            try:
                from urllib.parse import urlparse
                domain = urlparse(url).netloc
                if domain:
                    domains.add(domain)
            except:
                continue
    
    return list(domains)

def search_specialty_focused(medico_info: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Realiza uma busca focada em especialidade médica.
    
    Args:
        medico_info (dict): Informações do médico
        
    Returns:
        list: Lista de resultados filtrados
    """
    results = search_medico(medico_info, query_type="specialty", num_results=8)
    return filter_medical_results(results)

def search_contact_focused(medico_info: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Realiza uma busca focada em informações de contato.
    
    Args:
        medico_info (dict): Informações do médico
        
    Returns:
        list: Lista de resultados filtrados
    """
    results = search_medico(medico_info, query_type="contact", num_results=8)
    return filter_medical_results(results)

# Função para testar a integração
def test_searxng_integration():
    """Testa a integração com o SearXNG."""
    test_medico = {
        "Firstname": "João",
        "LastName": "Silva",
        "CRM": "12345",
        "UF": "SP"
    }
    
    print("Testando busca geral...")
    general_results = search_medico(test_medico)
    print(f"Resultados gerais: {len(general_results)}")
    
    print("\nTestando busca focada em especialidade...")
    specialty_results = search_specialty_focused(test_medico)
    print(f"Resultados de especialidade: {len(specialty_results)}")
    
    print("\nTestando busca focada em contato...")
    contact_results = search_contact_focused(test_medico)
    print(f"Resultados de contato: {len(contact_results)}")
    
    print("\nDomínios encontrados:")
    domains = extract_domains_from_results(general_results + specialty_results + contact_results)
    for domain in domains:
        print(f"- {domain}")
    
    return general_results, specialty_results, contact_results

if __name__ == "__main__":
    test_searxng_integration()
