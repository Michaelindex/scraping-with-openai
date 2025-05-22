import requests
import csv
import re
import time
import json
from typing import Optional, Dict, List
from urllib.parse import quote

class CEPFinder:
    def __init__(self):
        self.searxng_url = "http://124.81.6.163:8092/search"
        self.viacep_url = "https://viacep.com.br/ws/{}/json/"
        self.cepaberto_url = "https://www.cepaberto.com/api/v3/cep?cep={}"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.cep_pattern = r'\b\d{5}-?\d{3}\b'

    def limpar_texto(self, texto: str) -> str:
        """Limpa o texto removendo caracteres especiais e espaços extras."""
        if not texto:
            return ""
        return re.sub(r'\s+', ' ', texto.strip())

    def formatar_endereco(self, endereco: str, cidade: str, estado: str) -> str:
        """Formata o endereço para busca."""
        endereco = self.limpar_texto(endereco)
        cidade = self.limpar_texto(cidade)
        estado = self.limpar_texto(estado)
        
        if cidade and estado:
            return f"{endereco}, {cidade}, {estado}"
        elif cidade:
            return f"{endereco}, {cidade}"
        elif estado:
            return f"{endereco}, {estado}"
        return endereco

    def extrair_cep_resultados(self, resultados: Dict) -> Optional[str]:
        """Extrai CEP dos resultados da busca."""
        try:
            if 'results' in resultados:
                for resultado in resultados['results']:
                    texto = resultado.get('content', '')
                    cep_match = re.search(self.cep_pattern, texto)
                    if cep_match:
                        return cep_match.group(0).replace('-', '')
        except Exception as e:
            print(f"Erro ao extrair CEP dos resultados: {e}")
        return None

    def buscar_via_searxng(self, query: str) -> Optional[str]:
        """Busca CEP usando a API SearXNG."""
        try:
            response = requests.get(
                self.searxng_url,
                params={
                    'q': query,
                    'format': 'json',
                    'engines': 'google,bing,duckduckgo',
                    'language': 'pt-BR'
                },
                headers=self.headers,
                timeout=30
            )
            if response.status_code == 200:
                return self.extrair_cep_resultados(response.json())
        except Exception as e:
            print(f"Erro na busca SearXNG: {e}")
        return None

    def buscar_via_viacep(self, endereco: str) -> Optional[str]:
        """Busca CEP usando a API ViaCEP."""
        try:
            endereco_formatado = quote(endereco)
            response = requests.get(
                f"https://viacep.com.br/ws/{endereco_formatado}/json/",
                headers=self.headers,
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                if not data.get('erro'):
                    return data.get('cep', '').replace('-', '')
        except Exception as e:
            print(f"Erro na busca ViaCEP: {e}")
        return None

    def buscar_via_cepaberto(self, cep: str) -> Optional[str]:
        """Busca CEP usando a API CEP Aberto."""
        try:
            response = requests.get(
                self.cepaberto_url.format(cep),
                headers=self.headers,
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                return data.get('cep', '').replace('-', '')
        except Exception as e:
            print(f"Erro na busca CEP Aberto: {e}")
        return None

    def gerar_variacoes_busca(self, nome: str, endereco: str, cidade: str, estado: str) -> List[str]:
        """Gera variações de busca para tentar encontrar o CEP."""
        variacoes = []
        
        # Variações com nome
        if nome:
            variacoes.extend([
                f"{nome} {endereco} {cidade} {estado} CEP",
                f"{nome} {endereco} {cidade} CEP",
                f"{nome} {endereco} {estado} CEP",
                f"{nome} {endereco} CEP"
            ])

        # Variações sem nome
        variacoes.extend([
            f"{endereco} {cidade} {estado} CEP",
            f"{endereco} {cidade} CEP",
            f"{endereco} {estado} CEP",
            f"{endereco} CEP"
        ])

        # Variações com partes do endereço
        partes_endereco = endereco.split(',')
        if len(partes_endereco) > 1:
            rua = partes_endereco[0].strip()
            variacoes.extend([
                f"{rua} {cidade} {estado} CEP",
                f"{rua} {cidade} CEP",
                f"{rua} {estado} CEP"
            ])

        return [v for v in variacoes if v.strip()]

    def buscar_cep(self, nome: str, endereco: str, cidade: str, estado: str) -> Optional[str]:
        """Busca CEP usando múltiplas estratégias."""
        # Limpa os dados de entrada
        nome = self.limpar_texto(nome)
        endereco = self.limpar_texto(endereco)
        cidade = self.limpar_texto(cidade)
        estado = self.limpar_texto(estado)

        # Estratégia 1: Busca via SearXNG com variações
        variacoes = self.gerar_variacoes_busca(nome, endereco, cidade, estado)
        for query in variacoes:
            cep = self.buscar_via_searxng(query)
            if cep:
                return cep
            time.sleep(1)  # Evita sobrecarga da API

        # Estratégia 2: Busca via ViaCEP
        endereco_formatado = self.formatar_endereco(endereco, cidade, estado)
        cep = self.buscar_via_viacep(endereco_formatado)
        if cep:
            return cep

        # Estratégia 3: Busca via CEP Aberto (se tiver API key)
        # Implementar se necessário

        return None

def processar_csv(arquivo_entrada: str, arquivo_saida: str):
    """Processa o arquivo CSV e atualiza os CEPs."""
    finder = CEPFinder()
    
    try:
        with open(arquivo_entrada, mode='r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            linhas = list(reader)
            total_linhas = len(linhas)
            ceps_encontrados = 0
            ceps_ja_existentes = 0

            for i, linha in enumerate(linhas, 1):
                if not linha.get('postal code A1'):
                    print(f"Processando linha {i}/{total_linhas}")
                    nome = linha.get('Nome', '')
                    endereco = linha.get('Endereço', '')
                    cidade = linha.get('Cidade', '')
                    estado = linha.get('Estado', '')
                    
                    cep = finder.buscar_cep(nome, endereco, cidade, estado)
                    if cep:
                        linha['postal code A1'] = cep
                        ceps_encontrados += 1
                        print(f"CEP encontrado: {cep}")
                else:
                    ceps_ja_existentes += 1
                
                time.sleep(2)  # Pausa entre requisições

        # Salva o arquivo atualizado
        with open(arquivo_saida, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=reader.fieldnames)
            writer.writeheader()
            writer.writerows(linhas)

        print("\n" + "="*50)
        print("RELATÓRIO DE PROCESSAMENTO")
        print("="*50)
        print(f"Total de registros processados: {total_linhas}")
        print(f"CEPs já existentes: {ceps_ja_existentes}")
        print(f"CEPs encontrados e adicionados: {ceps_encontrados}")
        print(f"CEPs ainda faltando: {total_linhas - (ceps_ja_existentes + ceps_encontrados)}")
        print(f"Taxa de sucesso: {((ceps_ja_existentes + ceps_encontrados) / total_linhas * 100):.2f}%")
        print("="*50)

    except Exception as e:
        print(f"Erro ao processar o arquivo: {e}")

if __name__ == "__main__":
    arquivo_entrada = "medicos-output.csv"
    arquivo_saida = "medicos-output-atualizado.csv"
    processar_csv(arquivo_entrada, arquivo_saida) 