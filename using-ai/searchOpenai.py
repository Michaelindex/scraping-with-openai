import csv
import json
from openai import OpenAI
import os
import time
from datetime import datetime

# Configuração da API
with open('key', 'r') as f:
    api_key = f.read().strip()
client = OpenAI(api_key=api_key)

def get_ai_response(address_info, retry_count=0):
    max_retries = 3
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Consultando IA para: {address_info['nome']}")
    
    prompt = f"""Você é um assistente especializado em encontrar CEPs e e-mails de contato de médicos(as).
    Por favor, faça uma busca detalhada para encontrar o CEP e e-mail do seguinte médico:

    Nome: {address_info['nome']}
    CRM: {address_info['crm']}
    Estado: {address_info['estado']}
    Endereço: {address_info['endereco']}

    Instruções específicas:
    1. Faça uma busca detalhada pelo CEP usando o endereço completo
    2. Verifique se o endereço existe na cidade/estado informado
    3. Se não encontrar o CEP exato, tente encontrar o CEP da região
    4. Para o e-mail, tente encontrar um e-mail profissional do médico
    5. Se não encontrar o e-mail exato, tente encontrar um e-mail institucional relacionado ao CRM/estado

    IMPORTANTE: Você DEVE retornar APENAS um JSON válido, sem nenhum texto adicional, no seguinte formato:
    {{
        "cep": "CEP encontrado (formato: 00000-000)",
        "email": "email encontrado"
    }}
    """
    
    try:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Enviando requisição para a API...")
        response = client.chat.completions.create(
            model="gpt-4o-mini-search-preview-2025-03-11",
            messages=[
                {"role": "system", "content": "Você é um assistente especializado em encontrar CEPs e e-mails de contato de médicos(as). Sua tarefa é encontrar informações precisas usando múltiplas fontes de dados. Você DEVE retornar APENAS um JSON válido, sem nenhum texto adicional."},
                {"role": "user", "content": prompt}
            ]
        )
        
        # Mostrar a resposta bruta para debug
        raw_response = response.choices[0].message.content
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Resposta bruta da API: {raw_response}")
        
        # Tentar limpar a resposta se necessário
        raw_response = raw_response.strip()
        if raw_response.startswith('```json'):
            raw_response = raw_response[7:]
        if raw_response.endswith('```'):
            raw_response = raw_response[:-3]
        raw_response = raw_response.strip()
        
        # Extrair a resposta JSON
        try:
            result = json.loads(raw_response)
        except json.JSONDecodeError as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ERRO ao decodificar JSON após limpeza: {e}")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Conteúdo que falhou: {raw_response}")
            raise
        
        # Validar o formato do CEP
        cep = result.get('cep', '')
        if cep and not cep.replace('-', '').isdigit():
            raise ValueError(f"CEP inválido retornado: {cep}")
            
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Resposta processada: CEP={result.get('cep', 'não encontrado')}, Email={result.get('email', 'não encontrado')}")
        return result
        
    except json.JSONDecodeError as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ERRO ao decodificar JSON: {e}")
        if retry_count < max_retries:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Tentando novamente... (Tentativa {retry_count + 1}/{max_retries})")
            time.sleep(2)  # Espera um pouco mais antes de tentar novamente
            return get_ai_response(address_info, retry_count + 1)
        return {"cep": "", "email": ""}
        
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ERRO ao processar: {e}")
        if retry_count < max_retries:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Tentando novamente... (Tentativa {retry_count + 1}/{max_retries})")
            time.sleep(2)
            return get_ai_response(address_info, retry_count + 1)
        return {"cep": "", "email": ""}

def process_csv():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Iniciando processamento do arquivo base.csv...")
    
    # Ler o arquivo CSV original
    with open('base.csv', 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        rows = list(reader)
    
    total_rows = len(rows)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Total de registros encontrados: {total_rows}")
    
    # Processar cada linha
    for index, row in enumerate(rows, 1):
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Processando registro {index}/{total_rows}")
        
        # Pular se já tiver CEP
        if row.get('Postal Code A1'):
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Registro já possui CEP, pulando...")
            continue
            
        # Preparar informações para a IA
        address_info = {
            'nome': f"{row['Firstname']} {row['LastName']}",
            'crm': row['CRM'],
            'estado': row['UF'],
            'endereco': f"{row['Address A1']}, {row['Numero A1']}"
        }
        
        # Obter resposta da IA
        result = get_ai_response(address_info)
        
        # Atualizar a linha com os novos dados
        row['Postal Code A1'] = result.get('cep', '')
        row['ai-Mail'] = result.get('email', '')
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Aguardando 2 segundos antes da próxima consulta...")
        time.sleep(2)  # Aumentei o tempo de espera para 2 segundos
    
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Salvando resultados em output.csv...")
    # Escrever o arquivo CSV atualizado
    fieldnames = list(rows[0].keys())
    with open('output.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Processo finalizado! Arquivo output.csv gerado com sucesso!")

if __name__ == "__main__":
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Iniciando script...")
    process_csv()
