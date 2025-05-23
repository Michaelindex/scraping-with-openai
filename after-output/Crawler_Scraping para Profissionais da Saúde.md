# Crawler/Scraping para Profissionais da Saúde

Este projeto implementa um crawler/scraping robusto para extração de dados de profissionais da área da saúde, com foco principal em especialidades médicas e e-mails de contato.

## Visão Geral

O sistema utiliza uma abordagem híbrida com múltiplos níveis de fallback para garantir a máxima cobertura e precisão na extração de dados. A arquitetura foi projetada para processar milhares de registros médicos de forma eficiente, com mecanismos de validação e classificação inteligente.

## Requisitos

- Python 3.6 ou superior
- Bibliotecas principais:
  - requests
  - beautifulsoup4
  - selenium (opcional, recomendado)
  - playwright (opcional)
  - pandas
  - tqdm

## Instalação

1. Clone ou baixe este repositório
2. Instale as dependências:

```bash
pip install requests beautifulsoup4 pandas tqdm
pip install selenium  # Opcional, mas recomendado
pip install playwright  # Opcional
```

3. Se instalar o Playwright, execute a instalação dos navegadores:

```bash
playwright install chromium
```

## Estrutura do Projeto

- `medicos_crawler.py` - Script principal do crawler (arquivo único)
- `ollama_integration.py` - Módulo de integração com Ollama para classificação
- `searxng_integration.py` - Módulo de integração com SearXNG para busca
- `train_validate.py` - Script para validação da extração de dados
- `optimize_crawler.py` - Script para otimização de parâmetros
- `README.md` - Documentação do projeto

## Uso Básico

Para executar o crawler com configurações padrão:

```bash
python medicos_crawler.py medicoporestado.txt resultados.csv
```

Onde:
- `medicoporestado.txt` é o arquivo de entrada com dados dos médicos
- `resultados.csv` é o arquivo de saída para os resultados

## Parâmetros Disponíveis

```
usage: medicos_crawler.py [-h] [--batch-size BATCH_SIZE] [--max-workers MAX_WORKERS]
                         [--no-selenium] [--no-playwright] [--no-searx] [--no-ollama]
                         [--debug] [input_file] [output_file]

Crawler/Scraping para Profissionais da Saúde

positional arguments:
  input_file            Arquivo de entrada com dados dos médicos
  output_file           Arquivo de saída para os resultados

optional arguments:
  -h, --help            show this help message and exit
  --batch-size BATCH_SIZE
                        Tamanho do lote para processamento
  --max-workers MAX_WORKERS
                        Número máximo de workers para processamento paralelo
  --no-selenium         Desabilitar uso do Selenium
  --no-playwright       Desabilitar uso do Playwright
  --no-searx           Desabilitar uso do SearXNG
  --no-ollama          Desabilitar uso do Ollama
  --debug               Ativar modo de debug
```

## Otimização para seu Hardware

Para encontrar a configuração ideal para seu hardware:

```bash
python optimize_crawler.py --input medicoporestado.txt --sample 5
```

Este script testará diferentes configurações e recomendará a melhor para seu ambiente.

## Formato do Arquivo de Entrada

O sistema aceita arquivos CSV ou TXT com os seguintes campos obrigatórios:
- `CRM` - Número de registro do médico
- `UF` - Estado de registro (sigla)
- `Firstname` - Primeiro nome
- `LastName` - Sobrenome

Campos opcionais que serão utilizados se disponíveis:
- `Medical specialty` - Especialidade médica (se já conhecida)
- `E-mail A1` - E-mail principal (se já conhecido)

## Formato do Arquivo de Saída

O arquivo de saída (CSV) conterá os seguintes campos:
- `nome` - Nome completo do médico
- `crm` - Número de registro
- `uf` - Estado de registro
- `especialidade` - Especialidade médica extraída
- `email` - E-mail de contato extraído
- `telefone` - Telefone de contato extraído (quando disponível)
- `fonte` - Fonte da informação (CRM, Whitelist, SearXNG)

## Configuração Avançada

### Integração com SearXNG

O sistema está configurado para usar o servidor SearXNG em:
```
http://124.81.6.163:8092/search
```

### Integração com Ollama

O sistema está configurado para usar o servidor Ollama em:
```
http://124.81.6.163:11434/api/generate
```

## Estratégias de Fallback

O crawler implementa múltiplos níveis de fallback:

1. **Portais Oficiais dos CRMs** - Consulta direta aos portais dos Conselhos Regionais de Medicina
2. **Plataformas da Whitelist** - Sites como Doctoralia, BoaConsulta, etc. conforme whitelist fornecida
3. **Busca via SearXNG** - Consultas estruturadas combinando nome + CRM + UF
4. **Processamento via IA** - Uso do Ollama para extrair e classificar dados

## Dicas para Melhor Desempenho

1. **Ajuste o número de workers** - Para máquinas com mais núcleos, aumente o valor de `--max-workers`
2. **Otimize o tamanho do lote** - Use `--batch-size` entre 5-20 dependendo da memória disponível
3. **Priorize componentes** - Se a velocidade for crítica, considere usar `--no-playwright`
4. **Monitore o uso de recursos** - Verifique CPU, memória e rede durante a execução

## Solução de Problemas

### Erros de Selenium/Playwright

Se encontrar erros relacionados ao Selenium ou Playwright:

```bash
# Execute sem esses componentes
python medicos_crawler.py medicoporestado.txt resultados.csv --no-selenium --no-playwright
```

### Problemas de Conexão com SearXNG/Ollama

Se os servidores SearXNG ou Ollama estiverem inacessíveis:

```bash
# Execute sem esses componentes
python medicos_crawler.py medicoporestado.txt resultados.csv --no-searx --no-ollama
```

### Logs para Depuração

Para obter logs detalhados:

```bash
python medicos_crawler.py medicoporestado.txt resultados.csv --debug
```

Os logs são salvos em `medicos_crawler.log`

## Exemplos de Uso

### Processamento Básico

```bash
python medicos_crawler.py medicoporestado.txt resultados.csv
```

### Processamento Rápido (menos preciso)

```bash
python medicos_crawler.py medicoporestado.txt resultados_rapidos.csv --batch-size 20 --max-workers 8 --no-playwright
```

### Processamento de Alta Qualidade (mais lento)

```bash
python medicos_crawler.py medicoporestado.txt resultados_qualidade.csv --batch-size 5 --max-workers 2
```

### Modo Leve (para hardware limitado)

```bash
python medicos_crawler.py medicoporestado.txt resultados_leve.csv --no-selenium --no-playwright --batch-size 3 --max-workers 2
```

## Limitações Conhecidas

- A extração depende da disponibilidade e estrutura dos sites fonte
- Alguns sites podem implementar proteções anti-bot que limitam a extração
- A qualidade dos resultados pode variar dependendo da disponibilidade de informações online

## Suporte

Para problemas ou dúvidas, verifique os logs em `medicos_crawler.log` e `otimizacao_crawler.log`.
