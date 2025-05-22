import pandas as pd

# Ler o arquivo CSV
print("Lendo arquivo CSV...")
df = pd.read_csv('output.csv')

# Configurar o Excel writer
print("Convertendo para Excel...")
writer = pd.ExcelWriter('output.xlsx', engine='xlsxwriter')

# Escrever o DataFrame para o Excel
df.to_excel(writer, sheet_name='Dados', index=False)

# Ajustar a largura das colunas
workbook = writer.book
worksheet = writer.sheets['Dados']

# Ajustar a largura de todas as colunas
for i, col in enumerate(df.columns):
    # Pegar a largura máxima entre o cabeçalho e o conteúdo
    max_len = max(
        df[col].astype(str).apply(len).max(),  # Largura do conteúdo
        len(str(col))  # Largura do cabeçalho
    )
    # Adicionar um pequeno padding
    worksheet.set_column(i, i, max_len + 2)

# Salvar o arquivo
print("Salvando arquivo Excel...")
writer.close()

print("Conversão concluída! Arquivo 'output.xlsx' criado com sucesso!") 