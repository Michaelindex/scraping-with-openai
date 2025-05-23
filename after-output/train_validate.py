"""
Script de treinamento e validação para identificação precisa de especialidades médicas e contatos
"""

import os
import json
import random
import logging
from typing import Dict, List, Any, Optional

# Importar módulos de integração
try:
    from ollama_integration import classify_specialty, extract_structured_data, normalize_specialty
    from searxng_integration import search_specialty_focused, search_contact_focused
except ImportError:
    print("Erro ao importar módulos de integração. Verifique se os arquivos estão no mesmo diretório.")
    exit(1)

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("TrainValidate")

# Dados de treinamento para especialidades médicas
ESPECIALIDADES_EXEMPLOS = [
    {"texto": "Cardiologista com foco em arritmias cardíacas", "esperado": "cardiologia"},
    {"texto": "Médico especialista em doenças da pele e unhas", "esperado": "dermatologia"},
    {"texto": "Neurologista infantil com experiência em epilepsia", "esperado": "neurologia"},
    {"texto": "Especialista em cirurgia do aparelho digestivo", "esperado": "cirurgia do aparelho digestivo"},
    {"texto": "Oftalmologista com foco em cirurgia refrativa", "esperado": "oftalmologia"},
    {"texto": "Médico do trabalho com experiência em ergonomia", "esperado": "medicina do trabalho"},
    {"texto": "Psiquiatra especializado em transtornos de ansiedade", "esperado": "psiquiatria"},
    {"texto": "Ortopedista com especialização em joelho", "esperado": "ortopedia"},
    {"texto": "Ginecologista e obstetra", "esperado": "ginecologia"},
    {"texto": "Pediatra com foco em desenvolvimento infantil", "esperado": "pediatria"}
]

# Dados de treinamento para extração de contatos
CONTATOS_EXEMPLOS = [
    {
        "texto": "Para agendar consultas, entre em contato pelo email dr.silva@clinica.com.br ou telefone (11) 3456-7890",
        "esperado": {"email": "dr.silva@clinica.com.br", "telefone": "(11) 3456-7890"}
    },
    {
        "texto": "Dra. Ana atende na clínica pelo telefone 11 98765-4321. Contato: ana@medicos.org",
        "esperado": {"email": "ana@medicos.org", "telefone": "11 98765-4321"}
    },
    {
        "texto": "Informações e agendamentos: contato@clinicasaude.com.br - Telefone: +55 21 2222-3333",
        "esperado": {"email": "contato@clinicasaude.com.br", "telefone": "+55 21 2222-3333"}
    },
    {
        "texto": "Para emergências, ligue para 0800 123 4567. Atendimento online: atendimento@emergencia.med.br",
        "esperado": {"email": "atendimento@emergencia.med.br", "telefone": "0800 123 4567"}
    },
    {
        "texto": "Consultório: Av. Paulista, 1000, sala 123. Contatos: (11) 3030-4040 / dra.costa@gmail.com",
        "esperado": {"email": "dra.costa@gmail.com", "telefone": "(11) 3030-4040"}
    }
]

# Exemplos de textos completos para extração estruturada
TEXTOS_COMPLETOS = [
    """
    Dr. Carlos Mendes - CRM 54321/SP
    Médico Cardiologista
    Especialista em arritmias e insuficiência cardíaca
    Atendimento: Hospital São Lucas e Clínica Cardio Saúde
    Contato: drcarlos@cardio.com.br
    Telefone: (11) 3333-4444
    """,
    
    """
    Dra. Mariana Costa
    Dermatologista - CRM 12345/RJ
    Especializada em tratamentos estéticos e dermatologia clínica
    Consultório: Av. Rio Branco, 123 - Sala 301
    Agende sua consulta: (21) 99876-5432
    Email profissional: mariana@clinicapele.com.br
    """,
    
    """
    PERFIL PROFISSIONAL
    Nome: Dr. Paulo Souza
    Registro: CRM 7890/MG
    Área de atuação: Neurologia
    Subespecialidade: Doenças neurodegenerativas
    Contatos para agendamento:
    - consultorio@neurocentro.com.br
    - (31) 3434-5656
    """,
    
    """
    Clínica Ortopédica Saúde
    Dr. Roberto Almeida - Ortopedista
    CRM: 45678/RS
    Especialista em joelho e quadril
    Horário de atendimento: Segunda a sexta, 8h às 18h
    Telefone: 51-3232-1010
    Email: roberto.ortopedia@clinica.com
    """,
    
    """
    Dra. Juliana Ferreira
    Ginecologista e Obstetra
    CRM 34567/PR
    Atendimento humanizado com foco em saúde da mulher
    Consultório: Rua das Flores, 500
    Contato: drajuliana@mulhersaude.com
    Telefone: (41) 98888-7777
    """
]

def validate_specialty_classification():
    """
    Valida a classificação de especialidades médicas.
    
    Returns:
        tuple: (acurácia, resultados detalhados)
    """
    logger.info("Iniciando validação de classificação de especialidades...")
    
    resultados = []
    acertos = 0
    
    for i, exemplo in enumerate(ESPECIALIDADES_EXEMPLOS):
        texto = exemplo["texto"]
        esperado = exemplo["esperado"]
        
        # Classificar especialidade
        classificado = classify_specialty(texto)
        
        # Verificar resultado
        acerto = classificado == esperado
        if acerto:
            acertos += 1
            
        resultados.append({
            "texto": texto,
            "esperado": esperado,
            "classificado": classificado,
            "acerto": acerto
        })
        
        logger.info(f"Exemplo {i+1}: {'✓' if acerto else '✗'} - Esperado: {esperado}, Classificado: {classificado}")
    
    # Calcular acurácia
    acuracia = acertos / len(ESPECIALIDADES_EXEMPLOS) if ESPECIALIDADES_EXEMPLOS else 0
    logger.info(f"Acurácia da classificação: {acuracia:.2%} ({acertos}/{len(ESPECIALIDADES_EXEMPLOS)})")
    
    return acuracia, resultados

def validate_contact_extraction():
    """
    Valida a extração de informações de contato.
    
    Returns:
        tuple: (acurácia, resultados detalhados)
    """
    logger.info("Iniciando validação de extração de contatos...")
    
    resultados = []
    acertos_email = 0
    acertos_telefone = 0
    
    for i, exemplo in enumerate(CONTATOS_EXEMPLOS):
        texto = exemplo["texto"]
        esperado = exemplo["esperado"]
        
        # Extrair dados estruturados
        medico_info = {"Firstname": "Teste", "LastName": "Médico", "CRM": "12345", "UF": "SP"}
        extraido = extract_structured_data(texto, medico_info)
        
        # Verificar resultados
        acerto_email = extraido.get("email") == esperado.get("email")
        acerto_telefone = extraido.get("telefone") == esperado.get("telefone")
        
        if acerto_email:
            acertos_email += 1
        if acerto_telefone:
            acertos_telefone += 1
            
        resultados.append({
            "texto": texto,
            "esperado": esperado,
            "extraido": extraido,
            "acerto_email": acerto_email,
            "acerto_telefone": acerto_telefone
        })
        
        logger.info(f"Exemplo {i+1}: Email {'✓' if acerto_email else '✗'}, Telefone {'✓' if acerto_telefone else '✗'}")
    
    # Calcular acurácia
    total = len(CONTATOS_EXEMPLOS) * 2  # Email e telefone para cada exemplo
    acertos_total = acertos_email + acertos_telefone
    acuracia = acertos_total / total if total else 0
    
    logger.info(f"Acurácia da extração: {acuracia:.2%} ({acertos_total}/{total})")
    logger.info(f"- Email: {acertos_email}/{len(CONTATOS_EXEMPLOS)}")
    logger.info(f"- Telefone: {acertos_telefone}/{len(CONTATOS_EXEMPLOS)}")
    
    return acuracia, resultados

def validate_structured_extraction():
    """
    Valida a extração estruturada de textos completos.
    
    Returns:
        list: Resultados da extração
    """
    logger.info("Iniciando validação de extração estruturada...")
    
    resultados = []
    
    for i, texto in enumerate(TEXTOS_COMPLETOS):
        # Extrair dados estruturados
        medico_info = {"Firstname": "Teste", "LastName": "Médico", "CRM": "12345", "UF": "SP"}
        extraido = extract_structured_data(texto, medico_info)
        
        resultados.append({
            "texto": texto,
            "extraido": extraido
        })
        
        logger.info(f"Exemplo {i+1}:")
        logger.info(f"- Especialidade: {extraido.get('especialidade')}")
        logger.info(f"- Email: {extraido.get('email')}")
        logger.info(f"- Telefone: {extraido.get('telefone')}")
    
    return resultados

def test_searxng_extraction():
    """
    Testa a extração de dados via SearXNG.
    
    Returns:
        dict: Resultados dos testes
    """
    logger.info("Iniciando teste de extração via SearXNG...")
    
    # Médicos de teste (usar dados reais para testes mais precisos)
    medicos_teste = [
        {"Firstname": "Antonio", "LastName": "Carlos Lopes", "CRM": "17833", "UF": "SP"},
        {"Firstname": "Drauzio", "LastName": "Varella", "CRM": "13753", "UF": "SP"}
    ]
    
    resultados = {}
    
    for i, medico in enumerate(medicos_teste):
        nome = f"{medico['Firstname']} {medico['LastName']}"
        logger.info(f"Testando médico {i+1}: {nome} (CRM {medico['CRM']}/{medico['UF']})")
        
        # Buscar especialidade
        specialty_results = search_specialty_focused(medico)
        
        # Buscar contato
        contact_results = search_contact_focused(medico)
        
        # Armazenar resultados
        resultados[nome] = {
            "medico": medico,
            "specialty_results": specialty_results[:2],  # Limitar para não sobrecarregar logs
            "contact_results": contact_results[:2],      # Limitar para não sobrecarregar logs
            "num_specialty_results": len(specialty_results),
            "num_contact_results": len(contact_results)
        }
        
        logger.info(f"- Resultados de especialidade: {len(specialty_results)}")
        logger.info(f"- Resultados de contato: {len(contact_results)}")
    
    return resultados

def main():
    """Função principal para execução dos testes de validação."""
    logger.info("Iniciando validação e treinamento de IA para extração de dados médicos...")
    
    # Criar diretório para resultados
    os.makedirs("resultados_validacao", exist_ok=True)
    
    # Validar classificação de especialidades
    acuracia_esp, resultados_esp = validate_specialty_classification()
    
    # Validar extração de contatos
    acuracia_cont, resultados_cont = validate_contact_extraction()
    
    # Validar extração estruturada
    resultados_estrut = validate_structured_extraction()
    
    # Testar extração via SearXNG
    resultados_searx = test_searxng_extraction()
    
    # Salvar resultados
    resultados = {
        "acuracia_especialidades": acuracia_esp,
        "acuracia_contatos": acuracia_cont,
        "resultados_especialidades": resultados_esp,
        "resultados_contatos": resultados_cont,
        "resultados_estruturados": resultados_estrut,
        "resultados_searxng": resultados_searx
    }
    
    with open("resultados_validacao/resultados.json", "w", encoding="utf-8") as f:
        json.dump(resultados, f, ensure_ascii=False, indent=2)
    
    logger.info("Validação concluída. Resultados salvos em resultados_validacao/resultados.json")
    
    # Resumo final
    print("\n" + "="*50)
    print("RESUMO DA VALIDAÇÃO")
    print("="*50)
    print(f"Acurácia na classificação de especialidades: {acuracia_esp:.2%}")
    print(f"Acurácia na extração de contatos: {acuracia_cont:.2%}")
    print(f"Testes de extração estruturada: {len(resultados_estrut)} exemplos processados")
    print(f"Testes de SearXNG: {len(resultados_searx)} médicos processados")
    print("="*50)

if __name__ == "__main__":
    main()
