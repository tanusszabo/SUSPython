# Processamento e Análise de Dados CIHA - DATASUS

## Visão Geral do Projeto

Este projeto em Python é projetado para automatizar o download, processamento e análise de dados do Sistema de Informações Hospitalares do SUS (SIH/SUS), especificamente os arquivos CIHA (Boletim de Produção Ambulatorial). Ele baixa os arquivos `.dbc` diretamente do FTP do DATASUS, descompacta-os, enriquece os dados, agrega-os e os exporta para formatos de análise como CSV e Excel, com formatação avançada.

O objetivo principal é transformar dados brutos do CIHA em informações estruturadas e prontas para visualização, focando em procedimentos diagnósticos e categorizando-os por sexo, faixa etária, tipo de procedimento e região corporal detalhada.

## Funcionalidades

*   **Download Automatizado:** Baixa arquivos `.dbc` do FTP do DATASUS, com suporte a HTTP/HTTPS e FTP, incluindo tratamento de erros e atrasos configuráveis para evitar sobrecarga no servidor.
*   **Processamento de Arquivos `.dbc`:** Descompacta e lê os arquivos `.dbc` (formato DBF) usando a biblioteca `pysus`.
*   **Enriquecimento de Dados:**
    *   Extrai UF, ano e mês do nome do arquivo.
    *   Normaliza a coluna `SEXO` para "Masculino", "Feminino" e "Indefinido".
    *   Cria faixas etárias (`FAIXA_ETARIA`) a partir da coluna `IDADE`.
    *   Mapeia códigos de procedimento (PROC_REA) para nomes de grupo, subgrupo e nomes simplificados de procedimento (`PROC_GRU_NOME`).
    *   Classifica procedimentos de Ressonância Magnética (RM), Tomografia Computadorizada (TC) e Raio-X (RX) em `REGIAO_CORPORAL_DETALHADA` (Ex: "Cabeça e pescoço", "Torax / abdomen / cintura / pelve", "Membros superiores", "Membros inferiores").
*   **Filtro por Diagnóstico:** Foca apenas em procedimentos da categoria '02 - Procedimentos com finalidade diagnóstica'.
*   **Agregação de Dados:** Consolida os dados por UF, ano, sexo, faixa etária, tipo de procedimento e região corporal, somando o total de procedimentos.
*   **Exportação para CSV:** Gera um arquivo CSV mestre com todos os dados agregados no formato "long".
*   **Exportação para Excel com Formatação:**
    *   Cria um único arquivo Excel (`.xlsx`).
    *   Cada Unidade da Federação (UF) recebe uma planilha separada.
    *   Os dados em cada planilha são transformados para um formato "wide" (colunas de ano).
    *   As planilhas são formatadas como tabelas Excel, facilitando filtros e análises.
*   **Tratamento de Erros:** Inclui robustos tratamentos de exceção para downloads (conexão, HTTP 404, timeouts) e processamento de arquivos (arquivos vazios, erros de leitura).

## Pré-requisitos

Para executar este projeto, você precisará ter instalado:

*   **Python 3.x**
*   **Pip** (gerenciador de pacotes do Python)

### Bibliotecas Python

Instale as bibliotecas necessárias via pip:

```bash
pip install requests pandas openpyxl xlsxwriter
```

### Biblioteca `pysus` (datasus.read_dbc)

Este projeto utiliza uma versão específica ou compilada da biblioteca `pysus` para ler arquivos `.dbc`. Como ela envolve módulos C e pode ser um desafio de compilação, siga as instruções abaixo:

1.  **Obtenha o `pysus`:**
    *   Clone ou baixe o repositório `pysus` do GitHub (por exemplo, `https://github.com/danicat/pysus`).
    *   Você mencionou que o modificou para funcionar no Windows. Certifique-se de que sua versão modificada esteja pronta.

2.  **Instalação/Configuração para Windows (Exemplo):**
    *   Navegue até a pasta `pysus` (onde está o `setup.py` ou o `Makefile`).
    *   Certifique-se de ter um compilador C compatível com Python (geralmente o Microsoft Visual C++ Build Tools, parte do Visual Studio, é necessário para a versão do Python que você usa).
    *   Tente compilar e instalar o módulo. No terminal da pasta `pysus`:
        ```bash
        python setup.py build
        python setup.py install
        ```
    *   Alternativamente, se a compilação for um problema, você pode tentar colocar os arquivos compilados (como `datasus.cp3x-win_amd64.pyd` ou similar) diretamente em uma pasta que o Python possa encontrar (ex: `venv\Lib\site-packages` do seu ambiente virtual, ou uma pasta `pysus` no diretório raiz do seu projeto).
    *   **Para este projeto, a estrutura esperada é que o módulo `datasus` (com `read_dbc`) seja importável. Se você colocou a pasta `pysus` no diretório raiz do seu projeto, certifique-se de que o `sys.path` a inclua ou ajuste o `import from datasus` conforme necessário.**

## Estrutura do Projeto

É recomendado organizar o projeto da seguinte forma:

```
seu_projeto/
├── data/
│   └── (arquivos .dbc baixados serão colocados aqui)
├── output/
│   ├── datasus_sumario_nacional_long2.csv
│   └── resumo_consolidado_por_uf.xlsx
├── pysus/
│   └── (coloque aqui os módulos compilados do pysus, se não for instalá-lo no ambiente global)
├── download_ciha_data.py  # Script de download (o primeiro que você forneceu)
└── process_ciha_data.py   # Script de processamento e excel (o segundo que você forneceu)
```

**Nota:** Os scripts neste README foram nomeados como `download_ciha_data.py` e `process_ciha_data.py` para clareza. Você pode renomear os seus arquivos ou ajustar as instruções para os nomes que você usa.

## Como Usar

### 1. Preparação

1.  **Crie as pastas `data` e `output`** na raiz do seu projeto.
2.  **Configure o `pysus`** conforme as instruções na seção "Pré-requisitos".
3.  **Ajuste os caminhos nos scripts:**
    *   No `download_ciha_data.py`:
        *   `target_folder = "D:\CODE\DATASUS\data"`: Altere para `os.path.join(base_dir, 'data')` ou o caminho absoluto da sua pasta `data`.
        *   `start_year`, `end_year`, `brazilian_states`: Ajuste conforme necessário.
    *   No `process_ciha_data.py`:
        *   `base_dir = os.path.dirname(os.path.abspath(__file__))`: Este já está configurado para encontrar as pastas `data` e `output` relativas ao script.
        *   `output_csv_master_path` e `output_excel_dir`: Esses já apontam para a pasta `output`.

### 2. Executar o Download dos Dados

Execute o script de download para obter os arquivos `.dbc` do DATASUS:

```bash
python download_ciha_data.py
```

Este script irá baixar os arquivos `.dbc` para a pasta `data/`. Ele pode demorar bastante, dependendo da quantidade de dados (anos, meses, estados) e da velocidade da sua conexão. Arquivos que não existirem no servidor ou falharem no download serão registrados.

### 3. Processar e Gerar Análises

Após o download, execute o script de processamento:

```bash
python process_ciha_data.py
```

Este script fará o seguinte:
1.  Lerá todos os arquivos `.dbc` da pasta `data/`.
2.  Processará cada arquivo, aplicando as regras de enriquecimento e agregação.
3.  Salvará o DataFrame consolidado no formato "long" em `output/datasus_sumario_nacional_long2.csv`.
4.  Criará o arquivo `output/resumo_consolidado_por_uf.xlsx`, com uma planilha para cada UF e os dados formatados como tabelas Excel.

## Estrutura dos Dicionários de Mapeamento

Os dicionários `GRUPO_MAP`, `SUBGRUPO_MAP`, `PROC_GRU_NOME_MAP` e `PROC_REA_TO_REGIAO_MAP` são cruciais para a categorização dos dados. Eles podem ser expandidos ou modificados no script `process_ciha_data.py` para refinar a granularidade da análise.

*   `GRUPO_MAP`: Mapeia os 2 primeiros dígitos do `PROC_REA` para grupos de procedimento.
*   `SUBGRUPO_MAP`: Mapeia os dígitos 3 e 4 do `PROC_REA` para subgrupos, aninhado por grupo.
*   `PROC_GRU_NOME_MAP`: Mapeia os `NOME_SUB_GRUPO` para nomes simplificados de procedimentos (ex: "RM", "TC", "RX").
*   `PROC_REA_TO_REGIAO_MAP`: Mapeia o código completo de 10 dígitos do `PROC_REA` para regiões corporais detalhadas, crucial para RM, TC e RX.

## Personalização

*   **Anos e Estados:** Modifique as variáveis `start_year`, `end_year` e `brazilian_states` no script `download_ciha_data.py`.
*   **Diretórios:** Ajuste `target_folder`, `input_data_dir`, `output_csv_master_path` e `output_excel_dir` para seus caminhos preferidos.
*   **Delay de Download:** Ajuste `download_delay_seconds` no script de download para controlar o tempo entre as requisições FTP/HTTP.
*   **Mapeamentos de Dados:** Altere os dicionários de mapeamento no script `process_ciha_data.py` para adaptar as categorias ou regiões.

## Solução de Problemas Comuns

*   **`ImportError: No module named 'datasus'`:**
    *   Certifique-se de que a biblioteca `pysus` foi instalada corretamente no seu ambiente virtual.
    *   Verifique se a pasta `pysus` (com os arquivos compilados) está em um local acessível ao `sys.path` do Python.
*   **`FileNotFoundError` durante o download:** A URL do arquivo pode estar incorreta, o arquivo pode não existir no servidor DATASUS para aquele ano/mês/estado, ou há um problema de conexão. Verifique o log do script.
*   **Arquivos `.dbc` vazios:** O script `process_single_dbc_file` já lida com arquivos vazios, retornando um DataFrame vazio para não quebrar a concatenação. Isso pode ocorrer para combinações de UF/Ano/Mês onde não há dados.
*   **Erros de Conexão FTP/HTTP:** Verifique sua conexão com a internet e tente novamente. O servidor do DATASUS pode estar temporariamente indisponível ou ter bloqueios de IP se muitas requisições forem feitas em pouco tempo (aumentar `download_delay_seconds` pode ajudar).

---