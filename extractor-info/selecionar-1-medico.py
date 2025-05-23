import pandas as pd
import os
import random

def selecionar_um_medico_por_estado():
    # Caminho para o arquivo CSV
    arquivo_csv = os.path.join('..', 'medicos-output.csv')
    
    # Lê o arquivo CSV
    df = pd.read_csv(arquivo_csv)
    
    # Converte as colunas para maiúsculas para facilitar a busca
    df['UF'] = df['UF'].str.upper()
    df['State A1'] = df['State A1'].str.upper()
    
    # Obtém a lista única de estados (combinando UF e State A1)
    estados = pd.concat([df['UF'], df['State A1']]).unique()
    estados = [estado for estado in estados if pd.notna(estado)]  # Remove valores nulos
    
    # Lista para armazenar os médicos selecionados
    medicos_selecionados = []
    
    # Para cada estado, seleciona um médico aleatoriamente
    for estado in estados:
        # Filtra os dados pelo estado
        df_estado = df[(df['UF'] == estado) | (df['State A1'] == estado)]
        
        if len(df_estado) > 0:
            # Seleciona um médico aleatoriamente
            medico_aleatorio = df_estado.sample(n=1)
            medicos_selecionados.append(medico_aleatorio)
            print(f'Estado {estado}: Selecionado 1 médico de {len(df_estado)} disponíveis')
    
    # Combina todos os médicos selecionados em um único DataFrame
    df_final = pd.concat(medicos_selecionados, ignore_index=True)
    
    # Salva o resultado em um novo arquivo CSV
    arquivo_saida = 'um_medico_por_estado.csv'
    df_final.to_csv(arquivo_saida, index=False)
    
    print(f'\nArquivo {arquivo_saida} criado com sucesso!')
    print(f'Total de médicos selecionados: {len(df_final)}')

if __name__ == '__main__':
    print('Iniciando seleção aleatória de um médico por estado...')
    selecionar_um_medico_por_estado()
    print('\nProcesso concluído!') 