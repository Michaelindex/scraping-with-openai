import requests
import json
import time
import os
from datetime import datetime

# --- Suas API Keys ---


# --- Endpoints ---
JINA_DEEPSEARCH_URL = "https://deepsearch.jina.ai/v1/chat/completions"
OPENAI_CHAT_URL = "https://api.openai.com/v1/chat/completions"

# --- Modelos ---
OPENAI_MODEL = "gpt-4o-mini-search-preview-2025-03-11" # Seu modelo específico
JINA_DEEPSEARCH_MODEL_INFO = "Jina DeepSearch (interno)"

# --- Preços por 1 milhão de tokens (aproximados, para referência) ---
OPENAI_PRICE_INPUT_PER_M_TOKENS = 0.15
OPENAI_PRICE_OUTPUT_PER_M_TOKENS = 0.60

JINA_PRICE_PER_BILLION_TOKENS = 50.00
JINA_PRICE_PER_M_TOKENS = JINA_PRICE_PER_BILLION_TOKENS / 1000
JINA_DEEPSEARCH_ESTIMATED_TOKENS_PER_REQUEST = 10000 # Estimativa de tokens consumidos por uma requisição do DeepSearch

# --- Dados da pessoa a ser consultada ---
MEDICAL_PROF_INFO = {
    "nome": "DEBORAH ANNA DUWE",
    "crm": "55252",
    "estado": "SP" # Assumindo o estado SP para CRM 55252 como base de busca
}

# --- Prompt para buscar todos os dados ---
def create_medical_prompt(info):
    return f"""Você é um assistente especializado em encontrar informações de contato e endereço de médicos, incluindo detalhes como especialidade, endereço completo, telefone e e-mail.
Por favor, faça uma busca detalhada e use todas as suas capacidades de pesquisa na web para encontrar os seguintes dados para o(a) médico(a):

Nome: {info['nome']}
CRM: {info['crm']}
Estado: {info['estado']}

Dados solicitados:
- Especialidade Médica
- Endereço Completo (incluindo rua, número, complemento, bairro, CEP, cidade, estado)
- Telefone
- Celular
- E-mail

IMPORTANTE: Você DEVE retornar APENAS um JSON válido, sem nenhum texto adicional ou explicações, no seguinte formato. Se um dado não for encontrado, use uma string vazia ("").

{{
    "Medical specialty": "",
    "Endereco Completo A1": "",
    "Address A1": "",
    "Numero A1": "",
    "Complement A1": "",
    "Bairro A1": "",
    "postal code A1": "",
    "City A1": "",
    "State A1": "",
    "Phone A1": "",
    "Cell phone A1": "",
    "E-mail A1": ""
}}
"""

# --- Função para extrair JSON da resposta ---
def extract_json_from_response(text):
    """
    Tenta extrair um objeto JSON de uma string, mesmo que contenha texto extra.
    Prioriza o conteúdo dentro de blocos de código JSON de Markdown.
    """
    try:
        # Tenta decodificar diretamente se for JSON puro
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Tenta encontrar e extrair blocos de código JSON Markdown
    json_block_start = text.find('```json')
    if json_block_start != -1:
        json_block_end = text.find('```', json_block_start + 7)
        if json_block_end != -1:
            json_content = text[json_block_start + 7:json_block_end].strip()
            try:
                return json.loads(json_content)
            except json.JSONDecodeError:
                pass

    # Se não encontrar bloco markdown, tenta encontrar o primeiro e último { }
    # Esta é uma tentativa mais "bruta" e pode falhar se o JSON não estiver bem formado
    first_brace = text.find('{')
    last_brace = text.rfind('}')
    if first_brace != -1 and last_brace != -1 and last_brace > first_brace:
        try:
            return json.loads(text[first_brace : last_brace + 1])
        except json.JSONDecodeError:
            pass

    # Se nada funcionar, retorna a string original com um erro
    return {"error": "Falha ao decodificar JSON", "raw_response_snippet": text[:500]}


# --- Função para executar Jina DeepSearch ---
def run_jina_deepsearch(prompt_pergunta, attempt):
    headers = {
        "Authorization": f"Bearer {JINA_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "messages": [
            {"role": "system", "content": "Você é um assistente útil especializado em encontrar informações específicas e detalhadas diretamente da web. Responda de forma precisa e direta, sempre em formato JSON."},
            {"role": "user", "content": prompt_pergunta}
        ]
    }
    
    start_time = time.time()
    result = {
        "api": "Jina DeepSearch",
        "model": JINA_DEEPSEARCH_MODEL_INFO,
        "attempt": attempt,
        "time_taken": "N/A",
        "raw_answer": "N/A", # Para salvar a resposta bruta do Jina
        "parsed_answer": {}, # Para o JSON decodificado ou erro
        "prompt_tokens": "N/A",
        "completion_tokens": "N/A",
        "total_tokens": "N/A",
        "cost_estimate": "N/A"
    }
    
    try:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [Tentativa {attempt}] Iniciando Jina DeepSearch...")
        response = requests.post(JINA_DEEPSEARCH_URL, headers=headers, json=payload, timeout=180) # Aumenta timeout para 180s (3 minutos)
        response.raise_for_status()
        end_time = time.time()
        
        response_json = response.json()
        raw_answer = response_json["choices"][0]["message"]["content"] if response_json and "choices" in response_json and len(response_json["choices"]) > 0 else ""
        
        parsed_answer = extract_json_from_response(raw_answer)

        estimated_tokens_used = JINA_DEEPSEARCH_ESTIMATED_TOKENS_PER_REQUEST
        estimated_cost = (estimated_tokens_used / 1_000_000) * JINA_PRICE_PER_M_TOKENS
        
        result.update({
            "time_taken": end_time - start_time,
            "raw_answer": raw_answer, # Salva a resposta bruta
            "parsed_answer": parsed_answer, # Salva o JSON extraído ou erro
            "total_tokens": estimated_tokens_used,
            "cost_estimate": estimated_cost
        })
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [Tentativa {attempt}] Jina DeepSearch concluído em {result['time_taken']:.2f}s.")
        
    except requests.exceptions.Timeout:
        end_time = time.time()
        result.update({
            "time_taken": end_time - start_time,
            "raw_answer": "Timeout.", # Salva que foi timeout
            "parsed_answer": {"error": "Requisição excedeu o tempo limite (Timeout)."},
            "total_tokens": JINA_DEEPSEARCH_ESTIMATED_TOKENS_PER_REQUEST, # Considera custo de tentativa
            "cost_estimate": (JINA_DEEPSEARCH_ESTIMATED_TOKENS_PER_REQUEST / 1_000_000) * JINA_PRICE_PER_M_TOKENS # Estimativa de custo mesmo no timeout
        })
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [Tentativa {attempt}] Erro: Jina DeepSearch excedeu o tempo limite após {result['time_taken']:.2f}s.")
    except requests.exceptions.HTTPError as http_err:
        end_time = time.time()
        error_response_text = ""
        if hasattr(response, 'text'):
            error_response_text = response.text
        result.update({
            "time_taken": end_time - start_time,
            "raw_answer": error_response_text,
            "parsed_answer": {"error": f"Erro HTTP: {http_err} - Resposta do Servidor: {error_response_text[:500]}..."},
            "total_tokens": JINA_DEEPSEARCH_ESTIMATED_TOKENS_PER_REQUEST, # Considera custo de tentativa
            "cost_estimate": (JINA_DEEPSEARCH_ESTIMATED_TOKENS_PER_REQUEST / 1_000_000) * JINA_PRICE_PER_M_TOKENS
        })
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [Tentativa {attempt}] Erro HTTP no Jina: {http_err} após {result['time_taken']:.2f}s.")
    except Exception as err:
        end_time = time.time()
        result.update({
            "time_taken": end_time - start_time,
            "raw_answer": f"Erro inesperado: {err}",
            "parsed_answer": {"error": f"Erro inesperado: {err}"},
            "total_tokens": JINA_DEEPSEARCH_ESTIMATED_TOKENS_PER_REQUEST, # Considera custo de tentativa
            "cost_estimate": (JINA_DEEPSEARCH_ESTIMATED_TOKENS_PER_REQUEST / 1_000_000) * JINA_PRICE_PER_M_TOKENS
        })
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [Tentativa {attempt}] Erro inesperado no Jina: {err} após {result['time_taken']:.2f}s.")
        
    return result

# --- Função para executar OpenAI Chat (ChatGPT) ---
def run_openai_chat(prompt_pergunta, attempt):
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": OPENAI_MODEL,
        "messages": [
            {"role": "system", "content": "Você é um assistente especializado em encontrar informações específicas usando múltiplas fontes de dados, incluindo busca na web, se o modelo permitir. Responda de forma precisa e direta, **sempre em formato JSON**."},
            {"role": "user", "content": prompt_pergunta}
        ]
    }

    start_time = time.time()
    result = {
        "api": "OpenAI ChatGPT",
        "model": OPENAI_MODEL,
        "attempt": attempt,
        "time_taken": "N/A",
        "raw_answer": "N/A",
        "parsed_answer": {},
        "prompt_tokens": "N/A",
        "completion_tokens": "N/A",
        "total_tokens": "N/A",
        "cost_estimate": "N/A"
    }

    try:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [Tentativa {attempt}] Iniciando OpenAI ChatGPT...")
        response = requests.post(OPENAI_CHAT_URL, headers=headers, json=payload, timeout=90)
        response.raise_for_status()
        end_time = time.time()
        
        response_json = response.json()
        raw_answer = response_json["choices"][0]["message"]["content"] if response_json and "choices" in response_json and len(response_json["choices"]) > 0 else ""
        
        parsed_answer = extract_json_from_response(raw_answer)

        prompt_tokens = response_json["usage"]["prompt_tokens"] if "usage" in response_json else "N/A"
        completion_tokens = response_json["usage"]["completion_tokens"] if "usage" in response_json else "N/A"
        total_tokens = response_json["usage"]["total_tokens"] if "usage" in response_json else "N/A"
        
        cost_estimate = "N/A"
        if isinstance(prompt_tokens, int) and isinstance(completion_tokens, int):
            cost_estimate = (prompt_tokens / 1_000_000) * OPENAI_PRICE_INPUT_PER_M_TOKENS + \
                            (completion_tokens / 1_000_000) * OPENAI_PRICE_OUTPUT_PER_M_TOKENS
        
        result.update({
            "time_taken": end_time - start_time,
            "raw_answer": raw_answer,
            "parsed_answer": parsed_answer,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
            "cost_estimate": cost_estimate
        })
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [Tentativa {attempt}] OpenAI ChatGPT concluído em {result['time_taken']:.2f}s.")
        
    except requests.exceptions.Timeout:
        end_time = time.time()
        result.update({
            "time_taken": end_time - start_time,
            "raw_answer": "Timeout.",
            "parsed_answer": {"error": "Requisição excedeu o tempo limite (Timeout)."},
            "cost_estimate": 0.0 # Timeout no OpenAI geralmente não custa
        })
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [Tentativa {attempt}] Erro: OpenAI ChatGPT excedeu o tempo limite após {result['time_taken']:.2f}s.")
    except requests.exceptions.HTTPError as http_err:
        end_time = time.time()
        error_response_text = ""
        if hasattr(response, 'text'):
            error_response_text = response.text
        result.update({
            "time_taken": end_time - start_time,
            "raw_answer": error_response_text,
            "parsed_answer": {"error": f"Erro HTTP: {http_err} - Resposta do Servidor: {error_response_text[:500]}..."},
            "cost_estimate": 0.0
        })
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [Tentativa {attempt}] Erro HTTP no OpenAI: {http_err} após {result['time_taken']:.2f}s.")
    except Exception as err:
        end_time = time.time()
        result.update({
            "time_taken": end_time - start_time,
            "raw_answer": f"Erro inesperado: {err}",
            "parsed_answer": {"error": f"Erro inesperado: {err}"},
            "cost_estimate": 0.0
        })
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [Tentativa {attempt}] Erro inesperado no OpenAI: {err} após {result['time_taken']:.2f}s.")
        
    return result

# --- Função principal para executar os testes e salvar ---
def run_brutal_comparison_and_save(medical_info, num_attempts=3):
    all_results = []
    
    prompt = create_medical_prompt(medical_info)
    
    print(f"\n--- Iniciando {num_attempts} tentativas para cada API para a Dra. {medical_info['nome']} (CRM: {medical_info['crm']}) ---\n")

    # --- Testar Jina DeepSearch ---
    print("\n##### Testando Jina DeepSearch #####")
    for i in range(1, num_attempts + 1):
        jina_result = run_jina_deepsearch(prompt, i)
        all_results.append(jina_result)
        time.sleep(5) # Pequena pausa entre as tentativas para evitar sobrecarga

    # --- Testar OpenAI ChatGPT ---
    print("\n##### Testando OpenAI ChatGPT #####")
    for i in range(1, num_attempts + 1):
        openai_result = run_openai_chat(prompt, i)
        all_results.append(openai_result)
        time.sleep(5) # Pequena pausa entre as tentativas

    # --- Salvar resultados brutos em output_brutal.txt ---
    output_filename = "output_brutal.txt"
    with open(output_filename, "w", encoding="utf-8") as f:
        f.write("--- Relatório Detalhado de Testes Brutais de APIs ---\n\n")
        f.write(f"Consulta: Dados do(a) médico(a) {medical_info['nome']} (CRM: {medical_info['crm']})\n")
        f.write(f"Número de Tentativas por API: {num_attempts}\n\n")
        
        for r in all_results:
            f.write(f"API: {r['api']}\n")
            f.write(f"Modelo: {r['model']}\n")
            f.write(f"Tentativa: {r['attempt']}\n")
            f.write(f"Tempo de Resposta: {r['time_taken']:.4f} segundos\n" if isinstance(r['time_taken'], (int, float)) else f"Tempo de Resposta: {r['time_taken']} segundos\n")
            f.write(f"**Resposta Bruta (raw_answer):**\n{r['raw_answer']}\n\n") # Salva a resposta bruta aqui
            f.write(f"**Resposta JSON Analisada (parsed_answer):**\n{json.dumps(r['parsed_answer'], indent=2, ensure_ascii=False)}\n") # Formatando JSON
            f.write(f"Tokens de Prompt: {r['prompt_tokens']}\n")
            f.write(f"Tokens de Conclusão: {r['completion_tokens']}\n")
            f.write(f"Total de Tokens Estimados: {r['total_tokens']}\n")
            if isinstance(r['cost_estimate'], (int, float)):
                f.write(f"Custo Estimado: ${r['cost_estimate']:.8f} (aprox.)\n")
            else:
                f.write(f"Custo Estimado: {r['cost_estimate']} (aprox.)\n")
            f.write("=" * 50 + "\n\n")
            
    print(f"\nResultados detalhados de cada tentativa salvos em '{output_filename}'")
    return all_results

if __name__ == "__main__":
    
    # Executar a comparação brutal
    all_test_results = run_brutal_comparison_and_save(MEDICAL_PROF_INFO, num_attempts=3)

    # --- INPUT PARA SOLICITAR O RELATÓRIO FINAL ---
    print("\n" + "="*50)
    print("Testes concluídos. Para gerar o relatório de análise final, por favor, me forneça os saldos e tokens atuais:")
    
    openai_saldo_input = input("Qual o seu SALDO ATUAL em dólar da conta ChatGPT (ex: 2.00)? $")
    jina_tokens_input = input("Quantos TOKENS Jina você tem disponível AGORA (ex: 6700000)? ")
    
    try:
        # Valores iniciais que você me informou
        openai_saldo_inicial = 2.04
        jina_tokens_inicial = 4967502
        
        # Valores pós-teste que você vai inserir
        openai_saldo_apos_teste = float(openai_saldo_input)
        jina_tokens_apos_teste = int(jina_tokens_input)
        
        print("\nGerando relatório de análise em 'analise_comparativa_brutal.txt'...")
        
        with open("analise_comparativa_brutal.txt", "w", encoding="utf-8") as f_analise:
            f_analise.write("--- RELATÓRIO DE ANÁLISE COMPARATIVA BRUTAL DE APIs ---\n\n")
            f_analise.write(f"Data da Análise: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC%z')}\n")
            f_analise.write(f"Consulta: Dados do(a) médico(a) {MEDICAL_PROF_INFO['nome']} (CRM: {MEDICAL_PROF_INFO['crm']})\n")
            f_analise.write(f"Número de Tentativas por API: 3\n\n")
            
            f_analise.write("### Saldos e Custos Iniciais (Informados por você antes dos testes):\n")
            f_analise.write(f"Saldo Inicial ChatGPT (OpenAI): ${openai_saldo_inicial:.2f}\n")
            f_analise.write(f"Tokens Jina Disponíveis Inicialmente: {jina_tokens_inicial:,}\n")
            f_analise.write(f"Preço Jina: ${JINA_PRICE_PER_BILLION_TOKENS} por 1 bilhão de tokens\n")
            f_analise.write(f"Preço OpenAI ({OPENAI_MODEL}): Input ${OPENAI_PRICE_INPUT_PER_M_TOKENS}/M tokens, Output ${OPENAI_PRICE_OUTPUT_PER_M_TOKENS}/M tokens\n\n")

            f_analise.write("### Saldos Pós-Teste (Informados por você após os testes):\n")
            f_analise.write(f"Saldo Pós-Teste ChatGPT (OpenAI): ${openai_saldo_apos_teste:.2f}\n")
            f_analise.write(f"Tokens Jina Disponíveis Pós-Teste: {jina_tokens_apos_teste:,}\n\n")

            f_analise.write("### Resumo Agregado dos Resultados:\n")
            
            openai_success_count = 0
            jina_success_count = 0
            openai_total_time = 0.0
            jina_total_time = 0.0
            openai_total_estimated_cost = 0.0
            jina_total_estimated_cost = 0.0
            
            for r in all_test_results:
                if r['api'] == 'OpenAI ChatGPT':
                    # Considera sucesso se o JSON foi decodificado e não contém o erro padrão de decodificação
                    if "error" not in r['parsed_answer'] or r['parsed_answer'].get('error') not in ["Falha ao decodificar JSON", "Requisição excedeu o tempo limite (Timeout)."]:
                        openai_success_count += 1
                    if isinstance(r['time_taken'], (int, float)):
                        openai_total_time += r['time_taken']
                    if isinstance(r['cost_estimate'], (int, float)):
                        openai_total_estimated_cost += r['cost_estimate']
                elif r['api'] == 'Jina DeepSearch':
                    # Considera sucesso se o JSON foi decodificado e não contém o erro padrão de decodificação
                    if "error" not in r['parsed_answer'] or r['parsed_answer'].get('error') not in ["Falha ao decodificar JSON", "Requisição excedeu o tempo limite (Timeout)."]:
                        jina_success_count += 1
                    if isinstance(r['time_taken'], (int, float)):
                        jina_total_time += r['time_taken']
                    if isinstance(r['cost_estimate'], (int, float)):
                        jina_total_estimated_cost += r['cost_estimate']

            f_analise.write("#### OpenAI ChatGPT:\n")
            f_analise.write(f"Tentativas Bem-Sucedidas (JSON Válido/sem erro): {openai_success_count}/3\n")
            f_analise.write(f"Tempo Médio de Resposta (Total de tentativas): {openai_total_time / num_attempts:.4f}s\n") # Média de todas as tentativas
            f_analise.write(f"Custo Total Estimado pelo Script: ${openai_total_estimated_cost:.8f}\n")
            f_analise.write("#### Jina DeepSearch:\n")
            f_analise.write(f"Tentativas Bem-Sucedidas (JSON Válido/sem erro): {jina_success_count}/3\n")
            f_analise.write(f"Tempo Médio de Resposta (Total de tentativas): {jina_total_time / num_attempts:.4f}s\n") # Média de todas as tentativas
            f_analise.write(f"Custo Total Estimado pelo Script: ${jina_total_estimated_cost:.8f}\n\n")

            f_analise.write("### Análise de Custo e Qualidade (Baseada nos dados observados):\n")
            
            # Cálculo do custo real para OpenAI (se houve consumo)
            openai_custo_observado_pelo_saldo = openai_saldo_inicial - openai_saldo_apos_teste
            
            # Cálculo do custo real para Jina (pelo consumo de tokens)
            jina_tokens_consumidos_observado = jina_tokens_inicial - jina_tokens_apos_teste
            jina_custo_observado = (jina_tokens_consumidos_observado / 1_000_000_000) * JINA_PRICE_PER_BILLION_TOKENS if jina_tokens_consumidos_observado > 0 else 0
            
            f_analise.write(f"Custo Total Observado pelo Saldo (OpenAI): ${openai_custo_observado_pelo_saldo:.8f}\n")
            f_analise.write(f"Tokens Totais Consumidos Observados (Jina): {jina_tokens_consumidos_observado:,}\n")
            f_analise.write(f"Custo Total Observado (Jina): ${jina_custo_observado:.8f}\n\n")

            f_analise.write("#### Qualidade da Resposta:\n")
            f_analise.write("A qualidade é crucial para este teste. Avalie os 'parsed_answer' em `output_brutal.txt`:\n")
            f_analise.write("- **Jina DeepSearch:** Verifique se as tentativas que não deram timeout (ou mesmo as que deram timeout mas retornaram algo) contêm dados válidos e completos no 'raw_answer' e o quão bem o 'parsed_answer' conseguiu extrair o JSON. O resultado que você viu no console indica que ele pode retornar texto livre, o que dificulta a automação.\n")
            f_analise.write("- **OpenAI ChatGPT (`gpt-4o-mini-search-preview-2025-03-11`):** Avalie se os campos do JSON estão preenchidos corretamente e com precisão para DEBORAH ANNA DUWE. Espera-se que ele seja mais consistente na entrega do JSON.\n")
            f_analise.write("  *Critério de Sucesso de Qualidade:* JSON válido e campos preenchidos com dados corretos, especialmente o endereço completo e contatos.\n\n")
            
            f_analise.write("#### Tempo de Resposta:\n")
            f_analise.write("- **Jina DeepSearch:** O tempo médio será influenciado pelos timeouts. Se ele ainda estiver dando timeout após 180s, é um problema sério de desempenho.\n")
            f_analise.write("- **OpenAI ChatGPT:** O tempo médio deve ser significativamente menor e mais consistente. Este é um indicador de confiabilidade para aplicações em tempo real.\n\n")

            f_analise.write("#### Custo:\n")
            f_analise.write("- O 'Custo Total Observado' reflete o consumo real do seu saldo/tokens. Compare-o.\n")
            f_analise.write("- Para o Jina, se ele continuar falhando, mesmo que o custo por token seja baixo, o custo-benefício é ruim se não houver entrega de resultados utilizáveis.\n")
            f_analise.write("- Para o OpenAI, o custo é por token real e é esperado que seja muito eficiente para a quantidade de dados. Se o custo total for significativamente diferente do estimado pelo script, pode haver um problema de cálculo ou com a API da OpenAI.\n\n")
            
            f_analise.write("### Conclusão e Recomendação Final com Base Neste Teste Brutal:\n")
            f_analise.write("Com base nos resultados consolidados deste 'teste brutal' (3 tentativas para cada API para a Dra. DEBORAH ANNA DUWE):\n")
            f_analise.write("- Se o **Jina DeepSearch** continuar a exibir timeouts ou retornos não-JSON inconsistentes, mesmo com timeouts estendidos, isso indica um problema significativo de **estabilidade e parseabilidade** para a sua aplicação. A usabilidade em produção seria comprometida.\n")
            f_analise.write("- Se o **OpenAI ChatGPT (`gpt-4o-mini-search-preview-2025-03-11`)** conseguir retornar **JSONs válidos com dados precisos** (mesmo que alguns campos de contato sejam vazios, o que é esperado para dados privados) e em **tempos de resposta consistentes**, ele se mostra a **opção mais robusta e confiável** para a sua necessidade de busca de dados estruturados a partir da web.\n")
            f_analise.write("A decisão final deve priorizar a **consistência da resposta (JSON válido), a precisão dos dados e a estabilidade/tempo de resposta**.\n")
            f_analise.write("Este teste brutal deve fornecer dados claros para tomar a decisão.\n")
            
        print("Relatório de análise completo salvo em 'analise_comparativa_brutal.txt'.")
        
    except ValueError:
        print("Erro: Saldo ou tokens informados não são números válidos. Relatório de análise não gerado.")
    except Exception as e:
        print(f"Ocorreu um erro ao gerar o relatório de análise: {e}")