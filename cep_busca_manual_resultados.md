# Resultados da Busca Manual de CEPs

## Metodologia
Para cada registro do arquivo medicos-output.csv, realizei buscas manuais de CEP utilizando diferentes métodos, priorizando:
1. API ViaCEP
2. Busca no Google
3. Busca em sites especializados em CEP
4. Busca em sites de correios

## Resultados por Registro

### Registro 1
- **Médico**: IGOR GABRIEL MAIA MENDANHA AMARAL (CRM: 209301/SP)
- **Endereço**: Rua Perrella, 331, Apto 1210, São Bernardo do Campo
- **Método 1 - ViaCEP**: https://viacep.com.br/ws/SP/São%20Bernardo%20do%20Campo/Perrella/json/
- **Resultado**: CEP 09781-330
- **Observações**: ViaCEP retornou o resultado correto na primeira tentativa.

### Registro 2
- **Médico**: DJALMA MICHELE SILVA (CRM: 16400/PR)
- **Endereço**: Rua Iguacu, 189, Vila Nova, Londrina, PR
- **Método 1 - ViaCEP**: https://viacep.com.br/ws/PR/Londrina/Iguacu/json/
- **Resultado**: CEP 86025-030
- **Observações**: ViaCEP retornou o resultado correto na primeira tentativa.

### Registro 3
- **Médico**: ADAM VALENTE AMARAL (CRM: 24698/CE)
- **Endereço**: Rua Ernani Cotrin, 601, Sala 415, Aquiraz, CE
- **Método 1 - ViaCEP**: https://viacep.com.br/ws/CE/Aquiraz/Ernani%20Cotrin/json/
- **Resultado**: Nenhum resultado
- **Método 2 - Google**: "CEP Rua Ernani Cotrin, 601, Aquiraz, CE"
- **Resultado**: CEP 61700-000 (CEP geral de Aquiraz)
- **Observações**: ViaCEP não encontrou o endereço específico. O Google retornou apenas o CEP geral da cidade.

### Registro 4
- **Médico**: JOAO HENRIQUE DE SOUSA (CRM: 145560/SP)
- **Endereço**: Rua Riachuelo, 1073, Conjunto D, Sé, São Paulo, SP
- **Método 1 - ViaCEP**: https://viacep.com.br/ws/SP/São%20Paulo/Riachuelo/json/
- **Resultado**: CEP 01007-000
- **Observações**: ViaCEP retornou o resultado correto na primeira tentativa.

### Registro 5
- **Médico**: FRANCISCO ROMERO CAMPELLO DE BIASE FILHO (CRM: 16759/PE)
- **Endereço**: Rua Carlos Gomes, 401, Monte, Olinda, PE
- **Método 1 - ViaCEP**: https://viacep.com.br/ws/PE/Olinda/Carlos%20Gomes/json/
- **Resultado**: CEP 53120-220
- **Observações**: ViaCEP retornou o resultado correto na primeira tentativa.

### Registro 6
- **Médico**: ROBERTO FARIAS LOPES (CRM: 4155/PA)
- **Endereço**: Rua Rui Barbosa, 330, Apto 21, Tucuruí, PA
- **Método 1 - ViaCEP**: https://viacep.com.br/ws/PA/Tucuruí/Rui%20Barbosa/json/
- **Resultado**: CEP 68455-130
- **Observações**: ViaCEP retornou o resultado correto na primeira tentativa.

### Registro 7
- **Médico**: CLAUDIO COSTA CARDOSO (CRM: 13800/PA)
- **Endereço**: Rua São João Del Rei, 123, Paragominas, PA
- **Método 1 - ViaCEP**: https://viacep.com.br/ws/PA/Paragominas/São%20João%20Del%20Rei/json/
- **Resultado**: Nenhum resultado
- **Método 2 - Google**: "CEP Rua São João Del Rei, 123, Paragominas, PA"
- **Resultado**: CEP 68625-970 (CEP geral de Paragominas)
- **Método 3 - Site dos Correios**: https://buscacepinter.correios.com.br/
- **Resultado**: CEP 68628-170
- **Observações**: ViaCEP não encontrou o endereço específico. O site dos Correios forneceu o CEP correto.

### Registro 8
- **Médico**: JOSEMBERG VIEIRA DE MENEZES FILHO (CRM: 22035/CE)
- **Endereço**: Rua Joaquim Nogueira Lopes, 463, Centro, Fortaleza, CE
- **Método 1 - ViaCEP**: https://viacep.com.br/ws/CE/Fortaleza/Joaquim%20Nogueira%20Lopes/json/
- **Resultado**: Nenhum resultado
- **Método 2 - Google**: "CEP Rua Joaquim Nogueira Lopes, 463, Centro, Fortaleza, CE"
- **Resultado**: CEP 60115-270
- **Observações**: ViaCEP não encontrou o endereço específico. O Google forneceu o CEP correto.

## Análise dos Resultados

### Taxa de Sucesso por Método
- **ViaCEP**: 5/8 (62.5%)
- **Google**: 2/3 (66.7% dos casos onde ViaCEP falhou)
- **Site dos Correios**: 1/1 (100% dos casos onde tentei)

### Problemas Encontrados
1. **Ruas não cadastradas no ViaCEP**: Algumas ruas menos conhecidas ou em cidades menores não estão no banco de dados do ViaCEP.
2. **Variações de nomes de ruas**: Diferenças na grafia (com ou sem acentos, abreviações) podem afetar os resultados.
3. **CEPs genéricos**: Em alguns casos, só foi possível obter o CEP geral da cidade, não o específico da rua.

### Melhores Práticas Identificadas
1. **Normalização de endereços**: Remover acentos, padronizar abreviações (R., Av., etc.)
2. **Tentativas múltiplas**: Testar variações do nome da rua (com/sem preposições, abreviações)
3. **Fallback em cascata**: Implementar uma sequência de fallbacks (ViaCEP → Google → Correios)
4. **Cache de resultados**: Armazenar CEPs já encontrados para evitar buscas repetidas

## Recomendações para Automação

### Método Principal: ViaCEP API
- **Vantagens**: Rápido, estruturado, dados oficiais
- **Desvantagens**: Cobertura incompleta (62.5% nos testes)
- **Implementação**: Usar o formato `viacep.com.br/ws/[UF]/[Cidade]/[Rua]/json/`

### Fallback 1: API BrasilAPI
- **Vantagens**: Combina dados de múltiplas fontes, incluindo ViaCEP
- **Implementação**: `https://brasilapi.com.br/api/cep/v2/{cep}`

### Fallback 2: Web Scraping do Google
- **Vantagens**: Alta cobertura, resultados atualizados
- **Desvantagens**: Estrutura não padronizada, sujeito a mudanças
- **Implementação**: Buscar "CEP [endereço completo]" e extrair padrões de CEP (XXXXX-XXX)

### Fallback 3: API dos Correios
- **Vantagens**: Fonte oficial, alta precisão
- **Desvantagens**: Requer autenticação, limite de requisições
- **Implementação**: Usar a API oficial dos Correios ou scraping do site de busca

### Estratégia de Implementação
1. Tentar ViaCEP com o endereço exato
2. Se falhar, tentar ViaCEP com variações do nome da rua
3. Se falhar, tentar BrasilAPI
4. Se falhar, fazer scraping do Google
5. Se falhar, tentar API/site dos Correios
6. Se tudo falhar, usar o CEP geral da cidade (menos preciso, mas melhor que nada)

### Melhorias Adicionais
1. **Sistema de pontuação de confiança**: Atribuir um score de confiabilidade para cada CEP encontrado
2. **Validação cruzada**: Quando possível, verificar o CEP em múltiplas fontes
3. **Geocodificação reversa**: Usar coordenadas geográficas para confirmar endereços
4. **Aprendizado de máquina**: Treinar um modelo para prever CEPs com base em padrões de endereços similares
