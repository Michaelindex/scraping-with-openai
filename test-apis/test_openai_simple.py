import requests
import json

# Configuração da API

OPENAI_CHAT_URL = "https://api.openai.com/v1/chat/completions"
OPENAI_MODEL = "gpt-4o-mini-search-preview-2025-03-11"

def test_simple():
    print("Iniciando teste simples...")
    
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": OPENAI_MODEL,
        "messages": [
            {"role": "user", "content": "Olá, como vai?"}
        ]
    }
    
    try:
        print("Enviando requisição...")
        response = requests.post(OPENAI_CHAT_URL, headers=headers, json=payload)
        
        print(f"\nStatus Code: {response.status_code}")
        
        if response.status_code == 200:
            response_json = response.json()
            print("\nResposta da API:")
            print(json.dumps(response_json, indent=2, ensure_ascii=False))
        else:
            print("\nErro na resposta:")
            print(response.text)
            
    except Exception as e:
        print(f"\nErro: {e}")

if __name__ == "__main__":
    test_simple() 