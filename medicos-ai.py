import openai

client = openai.OpenAI(api_key="")

response = client.chat.completions.create(
    model="gpt-4o-mini-search-preview-2025-03-11",
    messages=[
        {"role": "user", "content": "Preciso saber onde essa médica está atendendo: LÍVIA GONÇALVES DE SOUZA MANHÃES - CRM 1043269"}
    ]
)

print(response.choices[0].message.content)