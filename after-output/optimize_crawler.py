#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script de teste e otimização do crawler de médicos em ambiente local

Este script executa testes de desempenho e precisão do crawler em uma amostra local,
ajustando parâmetros para otimização no hardware do usuário.
"""

import os
import sys
import time
import json
import logging
import argparse
import traceback
from datetime import datetime
import pandas as pd
from tqdm import tqdm

# Importar o crawler principal
try:
    from medicos_crawler import MedicosCrawler
except ImportError:
    print("Erro: Não foi possível importar o MedicosCrawler. Verifique se o arquivo medicos_crawler.py está no mesmo diretório.")
    sys.exit(1)

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("otimizacao_crawler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("OtimizacaoCrawler")

class CrawlerOptimizer:
    """Classe para otimização e teste do crawler de médicos."""
    
    def __init__(self, input_file, output_dir="resultados_otimizacao"):
        """
        Inicializa o otimizador.
        
        Args:
            input_file (str): Arquivo de entrada com dados dos médicos
            output_dir (str): Diretório para salvar resultados
        """
        self.input_file = input_file
        self.output_dir = output_dir
        self.medicos = self._load_medicos()
        
        # Criar diretório de saída se não existir
        os.makedirs(output_dir, exist_ok=True)
        
        # Configurações de teste
        self.configs = [
            {
                "nome": "config_basica",
                "max_workers": 2,
                "batch_size": 5,
                "delay_min": 1,
                "delay_max": 3,
                "use_selenium": True,
                "use_playwright": False,
                "use_searx": True,
                "use_ollama": True
            },
            {
                "nome": "config_rapida",
                "max_workers": 4,
                "batch_size": 10,
                "delay_min": 0.5,
                "delay_max": 1.5,
                "use_selenium": True,
                "use_playwright": False,
                "use_searx": True,
                "use_ollama": True
            },
            {
                "nome": "config_completa",
                "max_workers": 4,
                "batch_size": 5,
                "delay_min": 1,
                "delay_max": 3,
                "use_selenium": True,
                "use_playwright": True,
                "use_searx": True,
                "use_ollama": True
            },
            {
                "nome": "config_leve",
                "max_workers": 2,
                "batch_size": 5,
                "delay_min": 1,
                "delay_max": 3,
                "use_selenium": False,
                "use_playwright": False,
                "use_searx": True,
                "use_ollama": True
            }
        ]
    
    def _load_medicos(self):
        """
        Carrega os dados dos médicos do arquivo de entrada.
        
        Returns:
            list: Lista de dicionários com dados dos médicos
        """
        try:
            # Verificar formato do arquivo
            if self.input_file.endswith('.csv'):
                df = pd.read_csv(self.input_file)
                return df.to_dict('records')
            elif self.input_file.endswith('.txt'):
                # Assumir formato específico do arquivo fornecido
                medicos = []
                with open(self.input_file, 'r', encoding='utf-8') as f:
                    header = f.readline().strip().split(',')
                    for line in f:
                        values = line.strip().split(',')
                        if len(values) >= len(header):
                            medico = {header[i]: values[i] for i in range(len(header))}
                            medicos.append(medico)
                return medicos
            else:
                logger.error(f"Formato de arquivo não suportado: {self.input_file}")
                return []
        except Exception as e:
            logger.error(f"Erro ao carregar médicos: {e}")
            return []
    
    def _get_sample(self, size=10):
        """
        Obtém uma amostra aleatória de médicos.
        
        Args:
            size (int): Tamanho da amostra
            
        Returns:
            list: Amostra de médicos
        """
        import random
        
        if not self.medicos:
            return []
            
        # Limitar tamanho da amostra ao número de médicos disponíveis
        size = min(size, len(self.medicos))
        
        # Obter amostra aleatória
        return random.sample(self.medicos, size)
    
    def _evaluate_results(self, results):
        """
        Avalia os resultados da extração.
        
        Args:
            results (list): Lista de resultados
            
        Returns:
            dict: Métricas de avaliação
        """
        total = len(results)
        
        if total == 0:
            return {
                "total": 0,
                "com_especialidade": 0,
                "com_email": 0,
                "com_telefone": 0,
                "com_especialidade_e_email": 0,
                "taxa_especialidade": 0,
                "taxa_email": 0,
                "taxa_telefone": 0,
                "taxa_especialidade_e_email": 0
            }
        
        # Contar resultados com dados
        com_especialidade = sum(1 for r in results if r.get('especialidade'))
        com_email = sum(1 for r in results if r.get('email'))
        com_telefone = sum(1 for r in results if r.get('telefone'))
        com_especialidade_e_email = sum(1 for r in results if r.get('especialidade') and r.get('email'))
        
        return {
            "total": total,
            "com_especialidade": com_especialidade,
            "com_email": com_email,
            "com_telefone": com_telefone,
            "com_especialidade_e_email": com_especialidade_e_email,
            "taxa_especialidade": com_especialidade / total,
            "taxa_email": com_email / total,
            "taxa_telefone": com_telefone / total,
            "taxa_especialidade_e_email": com_especialidade_e_email / total
        }
    
    def test_config(self, config, sample_size=5):
        """
        Testa uma configuração específica.
        
        Args:
            config (dict): Configuração a ser testada
            sample_size (int): Tamanho da amostra
            
        Returns:
            dict: Resultados do teste
        """
        logger.info(f"Testando configuração: {config['nome']}")
        
        # Obter amostra
        sample = self._get_sample(sample_size)
        
        if not sample:
            logger.error("Amostra vazia, não é possível realizar o teste")
            return {
                "config": config,
                "erro": "Amostra vazia",
                "tempo_execucao": 0,
                "resultados": []
            }
        
        # Inicializar crawler com a configuração
        crawler = MedicosCrawler(config)
        
        # Medir tempo de execução
        start_time = time.time()
        
        try:
            # Processar amostra
            results = []
            for medico in tqdm(sample, desc=f"Testando {config['nome']}"):
                result = crawler.process_medico(medico)
                results.append(result)
                
            # Calcular tempo de execução
            execution_time = time.time() - start_time
            
            # Avaliar resultados
            evaluation = self._evaluate_results(results)
            
            # Salvar resultados
            output_file = os.path.join(self.output_dir, f"{config['nome']}_resultados.json")
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump({
                    "config": config,
                    "resultados": results,
                    "avaliacao": evaluation,
                    "tempo_execucao": execution_time
                }, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Teste concluído em {execution_time:.2f} segundos")
            logger.info(f"Taxa de especialidade: {evaluation['taxa_especialidade']:.2%}")
            logger.info(f"Taxa de email: {evaluation['taxa_email']:.2%}")
            
            return {
                "config": config,
                "tempo_execucao": execution_time,
                "avaliacao": evaluation,
                "resultados": results
            }
            
        except Exception as e:
            logger.error(f"Erro ao testar configuração {config['nome']}: {e}")
            logger.error(traceback.format_exc())
            
            return {
                "config": config,
                "erro": str(e),
                "tempo_execucao": time.time() - start_time,
                "resultados": []
            }
        finally:
            # Liberar recursos
            crawler.cleanup()
    
    def run_optimization(self, sample_size=5):
        """
        Executa a otimização testando todas as configurações.
        
        Args:
            sample_size (int): Tamanho da amostra para cada teste
            
        Returns:
            dict: Resultados da otimização
        """
        logger.info(f"Iniciando otimização com amostra de {sample_size} médicos")
        
        results = []
        
        for config in self.configs:
            try:
                result = self.test_config(config, sample_size)
                results.append(result)
            except Exception as e:
                logger.error(f"Erro ao testar configuração {config['nome']}: {e}")
                logger.error(traceback.format_exc())
        
        # Identificar melhor configuração
        valid_results = [r for r in results if "erro" not in r]
        
        if not valid_results:
            logger.error("Nenhum teste válido foi concluído")
            return {
                "status": "erro",
                "mensagem": "Nenhum teste válido foi concluído",
                "resultados": results
            }
        
        # Ordenar por taxa de especialidade e email (prioridade principal)
        by_quality = sorted(
            valid_results, 
            key=lambda r: r["avaliacao"]["taxa_especialidade_e_email"],
            reverse=True
        )
        
        # Ordenar por tempo de execução
        by_speed = sorted(
            valid_results, 
            key=lambda r: r["tempo_execucao"]
        )
        
        # Salvar resultados da otimização
        output_file = os.path.join(self.output_dir, "resultados_otimizacao.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                "data": datetime.now().isoformat(),
                "sample_size": sample_size,
                "resultados": results,
                "melhor_qualidade": by_quality[0]["config"]["nome"] if by_quality else None,
                "mais_rapida": by_speed[0]["config"]["nome"] if by_speed else None
            }, f, ensure_ascii=False, indent=2)
        
        # Gerar recomendação
        recommendation = self._generate_recommendation(by_quality, by_speed)
        
        # Salvar recomendação
        output_file = os.path.join(self.output_dir, "recomendacao.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(recommendation, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Otimização concluída. Resultados salvos em {self.output_dir}")
        
        return {
            "status": "sucesso",
            "resultados": results,
            "melhor_qualidade": by_quality[0] if by_quality else None,
            "mais_rapida": by_speed[0] if by_speed else None,
            "recomendacao": recommendation
        }
    
    def _generate_recommendation(self, by_quality, by_speed):
        """
        Gera uma recomendação de configuração com base nos resultados.
        
        Args:
            by_quality (list): Resultados ordenados por qualidade
            by_speed (list): Resultados ordenados por velocidade
            
        Returns:
            dict: Recomendação de configuração
        """
        if not by_quality or not by_speed:
            return {
                "status": "erro",
                "mensagem": "Dados insuficientes para gerar recomendação"
            }
        
        # Configuração de melhor qualidade
        best_quality = by_quality[0]
        
        # Configuração mais rápida
        fastest = by_speed[0]
        
        # Verificar se a mais rápida tem qualidade aceitável
        fastest_quality = fastest["avaliacao"]["taxa_especialidade_e_email"]
        best_quality_rate = best_quality["avaliacao"]["taxa_especialidade_e_email"]
        
        # Se a mais rápida tiver pelo menos 80% da qualidade da melhor, recomendar a mais rápida
        quality_threshold = 0.8
        
        if best_quality_rate > 0 and fastest_quality / best_quality_rate >= quality_threshold:
            recommended = fastest
            reason = f"Configuração mais rápida com qualidade aceitável ({fastest_quality:.2%} vs {best_quality_rate:.2%})"
        else:
            recommended = best_quality
            reason = f"Priorizada qualidade dos resultados ({best_quality_rate:.2%})"
        
        # Criar configuração recomendada (cópia da configuração original)
        recommended_config = recommended["config"].copy()
        
        # Ajustar parâmetros com base nos resultados
        if recommended["tempo_execucao"] > 10 and recommended_config["delay_min"] > 0.5:
            recommended_config["delay_min"] = max(0.5, recommended_config["delay_min"] * 0.8)
            recommended_config["delay_max"] = max(1.0, recommended_config["delay_max"] * 0.8)
            reason += ", delays reduzidos para melhorar performance"
        
        return {
            "status": "sucesso",
            "config_recomendada": recommended_config,
            "nome_config_original": recommended["config"]["nome"],
            "motivo": reason,
            "metricas": {
                "tempo_execucao": recommended["tempo_execucao"],
                "taxa_especialidade": recommended["avaliacao"]["taxa_especialidade"],
                "taxa_email": recommended["avaliacao"]["taxa_email"],
                "taxa_especialidade_e_email": recommended["avaliacao"]["taxa_especialidade_e_email"]
            }
        }

def main():
    """Função principal."""
    parser = argparse.ArgumentParser(description='Otimização do Crawler de Médicos')
    parser.add_argument('--input', '-i', default='medicoporestado.txt', help='Arquivo de entrada com dados dos médicos')
    parser.add_argument('--output', '-o', default='resultados_otimizacao', help='Diretório de saída para resultados')
    parser.add_argument('--sample', '-s', type=int, default=5, help='Tamanho da amostra para cada teste')
    
    args = parser.parse_args()
    
    print(f"Iniciando otimização do crawler com arquivo {args.input}")
    print(f"Resultados serão salvos em {args.output}")
    print(f"Tamanho da amostra: {args.sample} médicos por teste")
    
    try:
        optimizer = CrawlerOptimizer(args.input, args.output)
        results = optimizer.run_optimization(args.sample)
        
        if results["status"] == "sucesso":
            print("\n" + "="*50)
            print("RESULTADOS DA OTIMIZAÇÃO")
            print("="*50)
            
            if "melhor_qualidade" in results and results["melhor_qualidade"]:
                best_quality = results["melhor_qualidade"]
                print(f"Configuração com melhor qualidade: {best_quality['config']['nome']}")
                print(f"- Taxa de especialidade e email: {best_quality['avaliacao']['taxa_especialidade_e_email']:.2%}")
                print(f"- Tempo de execução: {best_quality['tempo_execucao']:.2f} segundos")
            
            if "mais_rapida" in results and results["mais_rapida"]:
                fastest = results["mais_rapida"]
                print(f"\nConfiguração mais rápida: {fastest['config']['nome']}")
                print(f"- Taxa de especialidade e email: {fastest['avaliacao']['taxa_especialidade_e_email']:.2%}")
                print(f"- Tempo de execução: {fastest['tempo_execucao']:.2f} segundos")
            
            if "recomendacao" in results and results["recomendacao"]:
                rec = results["recomendacao"]
                print("\nRECOMENDAÇÃO:")
                print(f"- Configuração base: {rec['nome_config_original']}")
                print(f"- Motivo: {rec['motivo']}")
                print(f"- Métricas: {rec['metricas']}")
                
                print("\nParâmetros recomendados para execução final:")
                for key, value in rec["config_recomendada"].items():
                    if key != "nome":
                        print(f"- {key}: {value}")
            
            print("\nPara executar o crawler com a configuração recomendada:")
            print(f"python medicos_crawler.py {args.input} resultados_finais.csv --batch-size {rec['config_recomendada'].get('batch_size', 10)} --max-workers {rec['config_recomendada'].get('max_workers', 4)}")
            
            if not rec["config_recomendada"].get("use_selenium", True):
                print(" --no-selenium")
            if not rec["config_recomendada"].get("use_playwright", True):
                print(" --no-playwright")
            if not rec["config_recomendada"].get("use_searx", True):
                print(" --no-searx")
            if not rec["config_recomendada"].get("use_ollama", True):
                print(" --no-ollama")
                
        else:
            print(f"\nErro na otimização: {results.get('mensagem', 'Erro desconhecido')}")
            
    except KeyboardInterrupt:
        print("\nOtimização interrompida pelo usuário")
    except Exception as e:
        print(f"Erro durante a execução: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
