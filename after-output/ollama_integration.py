"""
Módulo de integração com Ollama para classificação de especialidades médicas e extração de dados
"""

import requests
import json
import re
import logging
from typing import Dict, List, Any, Optional, Union

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("OllamaIntegration")

# URL do Ollama
OLLAMA_URL = "http://124.81.6.163:11434/api/generate"

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

# Exemplos de e-mails médicos para treinamento
EMAIL_EXAMPLES = [
    "dr.silva@clinica.com.br",
    "maria.santos@hospital.org.br",
    "contato@drcosta.med.br",
    "cardiologia@institutocardio.com",
    "atendimento@clinicamedica.com.br"
]

# Exemplos de telefones médicos para treinamento
PHONE_EXAMPLES = [
    "(11) 3456-7890",
    "11 98765-4321",
    "+55 21 3333-4444",
    "0800 123 4567",
    "11912345678"
]

def classify_specialty(text: str) -> Optional[str]:
    """
    Classifica uma especialidade médica usando o Ollama.
    
    Args:
        text (str): Texto contendo informações sobre especialidade
        
    Returns:
        Optional[str]: Especialidade normalizada ou None se não identificada
    """
    if not text:
        return None
        
    try:
        prompt = f"""
        Identifique a especialidade médica no seguinte texto e retorne APENAS o nome da especialidade, 
        sem explicações adicionais. Se não houver especialidade clara, retorne "não identificada".
        
        Texto: "{text}"
        
        Lista de especialidades válidas:
        {', '.join(ESPECIALIDADES_MEDICAS)}
        
        Exemplos:
        "Médico cardiologista com 10 anos de experiência" -> "cardiologia"
        "Especialista em doenças da pele" -> "dermatologia"
        "Atendimento em consultório particular" -> "não identificada"
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
        
        return None
    except Exception as e:
        logger.error(f"Erro ao classificar especialidade: {e}")
        return None

def extract_structured_data(text: str, medico_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extrai dados estruturados de um texto usando o Ollama.
    
    Args:
        text (str): Texto a ser analisado
        medico_info (dict): Informações do médico para contexto
        
    Returns:
        dict: Dados extraídos (especialidade, email, telefone)
    """
    if not text:
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
        
        Texto: "{text[:1500]}"  # Limitar tamanho para evitar tokens excessivos
        
        Exemplos de especialidades médicas válidas:
        {', '.join(ESPECIALIDADES_MEDICAS[:10])}... (entre outras)
        
        Exemplos de formatos de email médico:
        {', '.join(EMAIL_EXAMPLES)}
        
        Exemplos de formatos de telefone:
        {', '.join(PHONE_EXAMPLES)}
        
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
                            extracted_data['especialidade'] = normalize_specialty(extracted_data['especialidade'])
                            
                        return extracted_data
                except json.JSONDecodeError:
                    logger.warning("Não foi possível decodificar JSON da resposta do Ollama")
        
        return {}
    except Exception as e:
        logger.error(f"Erro ao extrair dados estruturados: {e}")
        return {}

def normalize_specialty(specialty: str) -> Optional[str]:
    """
    Normaliza uma especialidade médica para formato padrão.
    
    Args:
        specialty (str): Especialidade a ser normalizada
        
    Returns:
        Optional[str]: Especialidade normalizada ou None
    """
    if not specialty:
        return None
        
    specialty = specialty.lower().strip()
    
    # Verificar correspondência exata
    for esp in ESPECIALIDADES_MEDICAS:
        if esp in specialty:
            return esp
            
    return specialty

def validate_email(email: str) -> bool:
    """
    Valida se um email parece ser válido.
    
    Args:
        email (str): Email a ser validado
        
    Returns:
        bool: True se o email parece válido
    """
    if not email:
        return False
        
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(email_pattern, email))

def validate_phone(phone: str) -> bool:
    """
    Valida se um telefone parece ser válido.
    
    Args:
        phone (str): Telefone a ser validado
        
    Returns:
        bool: True se o telefone parece válido
    """
    if not phone:
        return False
        
    # Remover caracteres não numéricos
    digits = re.sub(r'\D', '', phone)
    
    # Verificar se tem entre 8 e 13 dígitos (considerando códigos de país)
    return 8 <= len(digits) <= 13

# Função para testar a integração
def test_ollama_integration():
    """Testa a integração com o Ollama."""
    test_text = """
    Dr. João Silva é um médico cardiologista com mais de 15 anos de experiência.
    Atende na Clínica Coração Saudável e pode ser contatado pelo email dr.joao@clinicacoracao.com.br
    ou pelo telefone (11) 3456-7890.
    """
    
    test_medico = {
        "Firstname": "João",
        "LastName": "Silva",
        "CRM": "12345",
        "UF": "SP"
    }
    
    print("Testando classificação de especialidade...")
    specialty = classify_specialty(test_text)
    print(f"Especialidade identificada: {specialty}")
    
    print("\nTestando extração de dados estruturados...")
    data = extract_structured_data(test_text, test_medico)
    print(f"Dados extraídos: {json.dumps(data, indent=2, ensure_ascii=False)}")
    
    return specialty, data

if __name__ == "__main__":
    test_ollama_integration()
