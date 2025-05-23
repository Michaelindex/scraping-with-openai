import pandas as pd
import os

def separar_por_estados():
    # Caminho para o arquivo CSV
    arquivo_csv = os.path.join('..', 'medicos-output.csv')
    
    # Lê o arquivo CSV
    df = pd.read_csv(arquivo_csv)
    
    # Converte as colunas para maiúsculas para facilitar a busca
    df['UF'] = df['UF'].str.upper()
    df['State A1'] = df['State A1'].str.upper()
    
    # Cria a pasta 'estados' se ela não existir
    pasta_estados = 'estados'
    if not os.path.exists(pasta_estados):
        os.makedirs(pasta_estados)
    
    # Obtém a lista única de estados (combinando UF e State A1)
    estados = pd.concat([df['UF'], df['State A1']]).unique()
    estados = [estado for estado in estados if pd.notna(estado)]  # Remove valores nulos
    
    # Para cada estado, cria um arquivo separado
    for estado in estados:
        # Filtra os dados pelo estado
        df_filtrado = df[(df['UF'] == estado) | (df['State A1'] == estado)]
        
        # Cria o nome do arquivo de saída
        arquivo_saida = os.path.join(pasta_estados, f'output_{estado}.csv')
        
        # Salva o resultado em um novo arquivo CSV
        df_filtrado.to_csv(arquivo_saida, index=False)
        
        print(f'Arquivo output_{estado}.csv criado com sucesso!')
        print(f'Total de registros para {estado}: {len(df_filtrado)}')

if __name__ == '__main__':
    print('Iniciando separação dos dados por estados...')
    separar_por_estados()
    print('\nProcesso concluído! Todos os arquivos foram salvos na pasta "estados".') 