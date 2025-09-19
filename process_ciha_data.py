import pandas as pd
import os, sys
import glob
from datetime import datetime

# Importe o seu módulo C compilado
try:
    from datasus import read_dbc
except ImportError:
    print("Erro: O módulo 'datasus' não foi encontrado. Certifique-se de que o ambiente virtual está ativado e o módulo foi instalado com 'make install' na pasta pysus.")
    print("Verifique seu sys.path com `python -c 'import sys; print(sys.path)'` e se 'datasus' está em `venv/Lib/site-packages`.")
    exit(1)


# --- 1. Dicionários de Mapeamento ---

# Mapeamento dos primeiros 2 dígitos do PROC_REA para NOME_GRUPO
GRUPO_MAP = {
    '01': 'Ações de promoção e prevenção em saúde',
    '02': 'Procedimentos com finalidade diagnóstica', # Nosso foco principal
    '03': 'Procedimentos clínicos',
    '04': 'Procedimentos cirúrgicos',
    '05': 'Transplantes de orgãos, tecidos e células',
    '06': 'Medicamentos',
    '07': 'Órteses, próteses e materiais especiais',
    '08': 'Ações complementares da atenção à saúde',
    '09': 'Procedimentos para Ofertas de Cuidados Integrados',
}

# Mapeamento dos próximos 2 dígitos (dígitos 3 e 4) para NOME_SUB_GRUPO
# Este dicionário é aninhado para o grupo '02'
SUBGRUPO_MAP = {
    '02': { # Para NOME_GRUPO 'Procedimentos com finalidade diagnóstica'
        '01': 'Coleta de material',
        '02': 'Diagnóstico em laboratório clínico',
        '03': 'Diagnóstico por anatomia patológica e citopatologia',
        '04': 'Diagnóstico por radiologia',
        '05': 'Diagnóstico por ultrassonografia',
        '06': 'Diagnóstico por tomografia',
        '07': 'Diagnóstico por ressonância magnética',
        '08': 'Diagnóstico por medicina nuclear in vivo',
        '09': 'Diagnóstico por endoscopia',
        '10': 'Diagnóstico por radiologia intervencionista',
        '11': 'Métodos diagnósticos em especialidades',
        '12': 'Diagnóstico e procedimentos especiais em hemoterapia',
        '13': 'Diagnóstico em vigilância epidemiológica e ambiental',
        '14': 'Diagnóstico por teste rápido',
    }
    # Se você precisar de subgrupos para outros grupos (ex: '03'), adicione-os aqui
}

# Mapeamento do NOME_SUB_GRUPO para o PROC_GRU_NOME simplificado
PROC_GRU_NOME_MAP = {
    'Diagnóstico por ressonância magnética': 'RM',
    'Diagnóstico por tomografia': 'TC',
    'Diagnóstico por radiologia': 'RX',
    'Diagnóstico por ultrassonografia': 'US',
    'Diagnóstico por medicina nuclear in vivo': 'Medicina Nuclear',
    'Diagnóstico por endoscopia': 'Endoscopia',
    'Diagnóstico em laboratório clínico': 'Laboratório Clínico',
    'Coleta de material': 'Coleta de Material',
    'Diagnóstico por anatomia patológica e citopatologia': 'Anatomia Patológica/Citopatologia',
    'Diagnóstico por radiologia intervencionista': 'Radiologia Intervencionista',
    'Métodos diagnósticos em especialidades': 'Diagnóstico em Especialidades',
    'Diagnóstico e procedimentos especiais em hemoterapia': 'Hemoterapia',
    'Diagnóstico em vigilância epidemiológica e ambiental': 'Vigilância Epidemiológica/Ambiental',
    'Diagnóstico por teste rápido': 'Teste Rápido',
    # Todos os outros subgrupos não listados aqui cairão em 'Outros Diagnósticos'
}

# Mapeamento detalhado de PROC_REA (10 dígitos) para REGIAO_CORPORAL_DETALHADA
# Esta é a parte mais crítica e que exige preenchimento exaustivo para RM, TC, RX
# Os procedimentos não presentes aqui e que não são RM/TC/RX, terão 'Geral' como região.
PROC_REA_TO_REGIAO_MAP = {
    # Ressonância Magnética (0207xx)
    # RM da cabeça, pescoço e coluna vertebral (020701)
    '0207010013': 'Cabeça e pescoço',  # ANGIORESSONANCIA CEREBRAL
    '0207010021': 'Cabeça e pescoço',  # RESSONANCIA MAGNETICA DE ARTICULACAO TEMPORO-MANDIBULAR (BILATERAL)
    '0207010030': 'Cabeça e pescoço',  # RESSONANCIA MAGNETICA DE COLUNA CERVICAL
    '0207010048': 'Torax / abdomen / cintura / pelve',  # RESSONANCIA MAGNETICA DE COLUNA LOMBO-SACRA
    '0207010056': 'Torax / abdomen / cintura / pelve',  # RESSONANCIA MAGNETICA DE COLUNA TORACICA
    '0207010064': 'Cabeça e pescoço',  # RESSONANCIA MAGNETICA DE CRANIO
    '0207010072': 'Cabeça e pescoço',  # RESSONANCIA MAGNETICA DE SELA TURCICA
    # RM do torax e membros superiores (020702)
    '0207020019': 'Torax / abdomen / cintura / pelve', # RESSONANCIA MAGNETICA DE CORACAO / AORTA C/ CINE
    '0207020027': 'Membros superiores', # RESSONANCIA MAGNETICA DE MEMBRO SUPERIOR (UNILATERAL)
    '0207020035': 'Torax / abdomen / cintura / pelve', # RESSONANCIA MAGNETICA DE TORAX
    # RM do abdomen, pelve e membros inferiores (020703)
    '0207030014': 'Torax / abdomen / cintura / pelve', # RESSONANCIA MAGNETICA DE ABDOMEN SUPERIOR
    '0207030022': 'Torax / abdomen / cintura / pelve', # RESSONANCIA MAGNETICA DE BACIA / PELVE
    '0207030030': 'Membros inferiores', # RESSONANCIA MAGNETICA DE MEMBRO INFERIOR (UNILATERAL)
    '0207030049': 'Torax / abdomen / cintura / pelve', # RESSONANCIA MAGNETICA DE VIAS BILIARES

    # Tomografia Computadorizada (0206xx)
    # Tomografia da cabeça, pescoço e coluna vertebral (020601)
    '0206010010': 'Cabeça e pescoço', # TOMOGRAFIA COMPUTADORIZADA DE COLUNA CERVICAL C/ OU S/ CONTRASTE
    '0206010028': 'Torax / abdomen / cintura / pelve', # TOMOGRAFIA COMPUTADORIZADA DE COLUNA LOMBO-SACRA C/ OU S/ CONTRASTE
    '0206010036': 'Torax / abdomen / cintura / pelve', # TOMOGRAFIA COMPUTADORIZADA DE COLUNA TORACICA C/ OU S/ CONTRASTE
    '0206010044': 'Cabeça e pescoço', # TOMOGRAFIA COMPUTADORIZADA DE FACE / SEIOS DA FACE / ARTICULACOES TEMPORO-MANDIBULARES
    '0206010052': 'Cabeça e pescoço', # TOMOGRAFIA COMPUTADORIZADA DE PESCOCO
    '0206010060': 'Cabeça e pescoço', # TOMOGRAFIA COMPUTADORIZADA DE SELA TURCICA
    '0206010079': 'Cabeça e pescoço', # TOMOGRAFIA COMPUTADORIZADA DO CRANIO
    '0206010087': 'Cabeça e pescoço', # TOMOMIELOGRAFIA COMPUTADORIZADA
    # Tomografia do torax e membros superiores (020602)
    '0206020015': 'Membros superiores', # TOMOGRAFIA COMPUTADORIZADA DE ARTICULACOES DE MEMBRO SUPERIOR
    '0206020023': 'Membros superiores', # TOMOGRAFIA COMPUTADORIZADA DE SEGMENTOS APENDICULARES (assumindo superior, ou mapear para mais detalhado se necessário)
    '0206020031': 'Torax / abdomen / cintura / pelve', # TOMOGRAFIA COMPUTADORIZADA DE TORAX
    '0206020040': 'Torax / abdomen / cintura / pelve', # TOMOGRAFIA DE HEMITORAX / MEDIASTINO (POR PLANO)
    # Tomografia do abdomen, pelve e membros inferiores (020603)
    '0206030010': 'Torax / abdomen / cintura / pelve', # TOMOGRAFIA COMPUTADORIZADA DE ABDOMEN
    '0206030029': 'Membros inferiores', # TOMOGRAFIA COMPUTADORIZADA DE ARTICULACOES DE MEMBRO INFERIOR
    '0206030037': 'Torax / abdomen / cintura / pelve', # TOMOGRAFIA COMPUTADORIZADA DE PELVE / BACIA

    # Radiologia (0204xx)
    # Exames radiológicos da cabeça e pescoço (020401)
    '0204010012': 'Cabeça e pescoço', # DACRIOCISTOGRAFIA
    '0204010020': 'Cabeça e pescoço', # PLANIGRAFIA DE LARINGE
    '0204010055': 'Cabeça e pescoço', # RADIOGRAFIA DE ARTICULACAO TEMPORO-MANDIBULAR BILATERAL
    '0204010063': 'Cabeça e pescoço', # RADIOGRAFIA DE CAVUM (LATERAL + HIRTZ)
    '0204010071': 'Cabeça e pescoço', # RADIOGRAFIA DE CRANIO (PA + LATERAL + OBLIGUA / BRETTON + HIRTZ)
    '0204010080': 'Cabeça e pescoço', # RADIOGRAFIA DE CRANIO (PA + LATERAL)
    '0204010101': 'Cabeça e pescoço', # RADIOGRAFIA DE MASTOIDE / ROCHEDOS (BILATERAL)
    '0204010110': 'Cabeça e pescoço', # RADIOGRAFIA DE MAXILAR (PA + OBLIQUA)
    '0204010128': 'Cabeça e pescoço', # RADIOGRAFIA DE OSSOS DA FACE (MN + LATERAL + HIRTZ)
    '0204010144': 'Cabeça e pescoço', # RADIOGRAFIA DE SEIOS DA FACE (FN + MN + LATERAL + HIRTZ)
    '0204010152': 'Cabeça e pescoço', # RADIOGRAFIA DE SELA TURSICA (PA + LATERAL + BRETTON)
    '0204010179': 'Cabeça e pescoço', # RADIOGRAFIA PANORAMICA
    '0204010187': 'Cabeça e pescoço', # RADIOGRAFIA PERI-APICAL INTERPROXIMAL (BITE-WING)
    '0204010195': 'Cabeça e pescoço', # SIALOGRAFIA (POR GLANDULA)
    # Exames radiológicos da coluna vertebral (020402)
    '0204020018': 'Cabeça e pescoço', # MIELOGRAFIA
    '0204020034': 'Cabeça e pescoço', # RADIOGRAFIA DE COLUNA CERVICAL (AP + LATERAL + TO + OBLIQUAS)
    '0204020042': 'Cabeça e pescoço', # RADIOGRAFIA DE COLUNA CERVICAL (AP + LATERAL + TO / FLEXAO)
    '0204020069': 'Torax / abdomen / cintura / pelve', # RADIOGRAFIA DE COLUNA LOMBO-SACRA
    '0204020077': 'Torax / abdomen / cintura / pelve', # RADIOGRAFIA DE COLUNA LOMBO-SACRA (C/ OBLIQUAS)
    '0204020093': 'Torax / abdomen / cintura / pelve', # RADIOGRAFIA DE COLUNA TORACICA (AP + LATERAL)
    '0204020107': 'Torax / abdomen / cintura / pelve', # RADIOGRAFIA DE COLUNA TORACO-LOMBAR
    '0204020123': 'Torax / abdomen / cintura / pelve', # RADIOGRAFIA DE REGIAO SACRO-COCCIGEA
    # Exames radiológicos do torax e mediastino (020403)
    '0204030013': 'Torax / abdomen / cintura / pelve', # BRONCOGRAFIA UNILATERAL
    '0204030021': 'Torax / abdomen / cintura / pelve', # DUCTOGRAFIA (POR MAMA)
    '0204030030': 'Torax / abdomen / cintura / pelve', # MAMOGRAFIA UNILATERAL
    '0204030048': 'Torax / abdomen / cintura / pelve', # MARCACAO PRE-CIRURGICA DE LESAO NAO PALPAVEL DE MAMA ASSOCIADA A MAMOGRAFIA
    '0204030072': 'Torax / abdomen / cintura / pelve', # RADIOGRAFIA DE COSTELAS (POR HEMITORAX)
    '0204030110': 'Torax / abdomen / cintura / pelve', # RADIOGRAFIA DE PNEUMOMEDIASTINO
    '0204030137': 'Torax / abdomen / cintura / pelve', # RADIOGRAFIA DE TORAX (PA + INSPIRACAO + EXPIRACAO + LATERAL)
    '0204030145': 'Torax / abdomen / cintura / pelve', # RADIOGRAFIA DE TORAX (PA + LATERAL + OBLIQUA)
    '0204030153': 'Torax / abdomen / cintura / pelve', # RADIOGRAFIA DE TORAX (PA E PERFIL)
    '0204030161': 'Torax / abdomen / cintura / pelve', # RADIOGRAFIA DE TORAX (PA PADRAO OIT)
    '0204030170': 'Torax / abdomen / cintura / pelve', # RADIOGRAFIA DE TORAX (PA)
    '0204030188': 'Torax / abdomen / cintura / pelve', # MAMOGRAFIA BILATERAL PARA RASTREAMENTO
    # Exames radiológicos da cintura escapular e dos membros superiores (020404)
    '0204040019': 'Membros superiores', # RADIOGRAFIA DE ANTEBRACO
    '0204040027': 'Torax / abdomen / cintura / pelve', # RADIOGRAFIA DE ARTICULACAO ACROMIO-CLAVICULAR
    '0204040035': 'Membros superiores', # RADIOGRAFIA DE ARTICULACAO ESCAPULO-UMERAL
    '0204040043': 'Torax / abdomen / cintura / pelve', # RADIOGRAFIA DE ARTICULACAO ESTERNO-CLAVICULAR
    '0204040051': 'Membros superiores', # RADIOGRAFIA DE BRACO
    '0204040060': 'Torax / abdomen / cintura / pelve', # RADIOGRAFIA DE CLAVICULA
    '0204040078': 'Membros superiores', # RADIOGRAFIA DE COTOVELO
    '0204040086': 'Membros superiores', # RADIOGRAFIA DE DEDOS DA MAO
    '0204040094': 'Membros superiores', # RADIOGRAFIA DE MAO
    '0204040108': 'Membros superiores', # RADIOGRAFIA DE MAO E PUNHO (P/ DETERMINACAO DE IDADE OSSEA)
    '0204040116': 'Membros superiores', # RADIOGRAFIA DE ESCAPULA/OMBRO (TRES POSICOES)
    '0204040124': 'Membros superiores', # RADIOGRAFIA DE PUNHO (AP + LATERAL + OBLIQUA)
    # Exames radiológicos do abdomen e pelve (020405)
    '0204050073': 'Torax / abdomen / cintura / pelve', # PIELOGRAFIA ANTEROGRADA PERCUTANEA
    '0204050090': 'Torax / abdomen / cintura / pelve', # PLANIGRAFIA DE RIM C/ CONTRASTE
    '0204050138': 'Torax / abdomen / cintura / pelve', # RADIOGRAFIA DE ABDOMEN SIMPLES (AP)
    # Exames radiológicos da cintura pétvica e dos membros inferiores (020406)
    '0204060010': 'Membros inferiores', # ARTROGRAFIA
    '0204060028': 'Torax / abdomen / cintura / pelve', # DENSITOMETRIA OSSEA DUO-ENERGETICA DE COLUNA (VERTEBRAS LOMBARES) - Densitometria de coluna pode ser considerada como tronco/centro
    '0204060036': 'Membros inferiores', # ESCANOMETRIA
    '0204060060': 'Membros inferiores', # RADIOGRAFIA DE ARTICULACAO COXO-FEMORAL
    '0204060079': 'Torax / abdomen / cintura / pelve', # RADIOGRAFIA DE ARTICULACAO SACRO-ILIACA (pelve)
    '0204060087': 'Membros inferiores', # RADIOGRAFIA DE ARTICULACAO TIBIO-TARSICA
    '0204060095': 'Torax / abdomen / cintura / pelve', # RADIOGRAFIA DE BACIA
    '0204060109': 'Membros inferiores', # RADIOGRAFIA DE CALCANEO
    '0204060117': 'Membros inferiores', # RADIOGRAFIA DE COXA
    '0204060125': 'Membros inferiores', # RADIOGRAFIA DE JOELHO (AP + LATERAL)
    '0204060133': 'Membros inferiores', # RADIOGRAFIA DE JOELHO OU PATELA (AP + LATERAL + AXIAL)
    '0204060150': 'Membros inferiores', # RADIOGRAFIA DE PE / DEDOS DO PE
    '0204060168': 'Membros inferiores', # RADIOGRAFIA DE PERNA
}


# --- Funções Auxiliares (Mesmas que a estrutura inicial) ---

def calculate_age_group(age_value):
    """
    Agrupa um valor de idade em faixas etárias predefinidas.
    age_value deve ser um número inteiro ou conversível para tal.
    Retorna a faixa etária como string ou 'Idade Desconhecida'.
    """
    faixas_etarias = [
        (0, 4, '0-4'), (5, 9, '5-9'), (10, 14, '10-14'), (15, 19, '15-19'),
        (20, 29, '20-29'), (30, 39, '30-39'), (40, 49, '40-49'), (50, 59, '50-59'),
        (60, 69, '60-69'), (70, 79, '70-79'), (80, 200, '80+') # 200 para garantir que pegue idades muito avançadas
    ]

    try:
        age = int(age_value) # Tenta converter a idade para inteiro
        
        for min_age, max_age, label in faixas_etarias:
            if min_age <= age <= max_age:
                return label
        
        # Se a idade estiver fora das faixas definidas (ex: negativa ou muito alta)
        return 'Idade Desconhecida' 
    except (ValueError, TypeError):
        # Captura erros se age_value não for um número válido
        return 'Idade Desconhecida'


def process_single_dbc_file(filepath, encoding='cp850'):
    """
    Processa um único arquivo .dbc:
    1. Descompacta e lê o DBF em um DataFrame Pandas.
    2. Adiciona colunas de UF, Ano e Mês (extraídas do nome do arquivo).
    3. Enriquece os dados com NOME_GRUPO, NOME_SUB_GRUPO, PROC_GRU_NOME, REGIAO_CORPORAL_DETALHADA.
    4. Cria a coluna FAIXA_ETARIA (usando a coluna 'IDADE' existente).
    5. Filtra para procedimentos de diagnóstico (Grupo 02).
    6. Agrega os dados por todas as dimensões especificadas (formato "long").
    Retorna um DataFrame agregado para o arquivo ou None em caso de erro.
    """
    # 0. VERIFICAR TAMANHO DO ARQUIVO
    try:
        file_size = os.path.getsize(filepath)
        if file_size == 0:
            print(f"  - ALERTA: Arquivo {filepath} vazio.")
            # Retorna um DataFrame vazio com as colunas esperadas
            return pd.DataFrame(columns=['UF_ATENDIMENTO', 'ANO_ATENDIMENTO', 'SEXO', 'FAIXA_ETARIA', 'PROC_GRU_NOME', 'REGIAO_CORPORAL_DETALHADA', 'TOTAL_PROCEDIMENTOS'])
    except FileNotFoundError:
        print(f"  - ERRO: Arquivo {filepath} não encontrado. Pulando.", file=sys.stderr)
        return None
    except Exception as e:
        print(f"  - ERRO ao verificar o tamanho do arquivo {filepath}: {e}. Pulando.", file=sys.stderr)
        return None

    try:
        print(f"Iniciando processamento de: {filepath}")
        # Extrair UF, Ano e Mês do nome do arquivo (ex: SP202301.dbc)
        filename_parts = os.path.basename(filepath).split('.')[0]
        uf = filename_parts[4:6]
        ano_atendimento = int(filename_parts[6:8])
        # mes_atendimento = int(filename_parts[8:10]) # Mês não é usado na agregação final

        # 1. Descompactar e ler o DBF
        dbf_object = read_dbc(filepath, encoding)
        df = pd.DataFrame(list(dbf_object.records)) 

        # Adicionar metadados do arquivo
        df['UF_ATENDIMENTO'] = uf
        df['ANO_ATENDIMENTO'] = ano_atendimento

        # Normalizar SEXO
        df['SEXO'] = df['SEXO'].astype(str).str.strip().replace({'1': 'Masculino', '3': 'Feminino', '0': 'Indefinido'})
        df['SEXO'] = df['SEXO'].fillna('Indefinido')

        # Filtrar o DataFrame para o grupo '02 - Procedimentos com finalidade diagnóstica'
        df_diagnostic = df[df['PROC_REA'].astype(str).str[:2] == '02'].copy()
        if df_diagnostic.empty:
            print(f"  - ATENÇÃO: Nenhum procedimento de diagnóstico encontrado em {filepath}. Retornando vazio.")
            # Retorna um DataFrame vazio com as colunas esperadas para ser concatenado sem problemas
            return pd.DataFrame(columns=['UF_ATENDIMENTO', 'ANO_ATENDIMENTO', 'SEXO', 'FAIXA_ETARIA', 'PROC_GRU_NOME', 'REGIAO_CORPORAL_DETALHADA', 'TOTAL_PROCEDIMENTOS'])

        # 2. Enriquecer colunas de procedimentos
        df_diagnostic['PROC_GRUPO_COD'] = df_diagnostic['PROC_REA'].astype(str).str[:2]
        df_diagnostic['PROC_SUBGRUPO_COD'] = df_diagnostic['PROC_REA'].astype(str).str[2:4]

        df_diagnostic['NOME_GRUPO'] = df_diagnostic['PROC_GRUPO_COD'].map(GRUPO_MAP).fillna('Outro Grupo')
        
        def map_subgroup(row):
            group_code = row['PROC_GRUPO_COD']
            subgroup_code = row['PROC_SUBGRUPO_COD']
            if group_code in SUBGRUPO_MAP:
                return SUBGRUPO_MAP[group_code].get(subgroup_code, 'Outro Subgrupo')
            return 'Outro Subgrupo'
        df_diagnostic['NOME_SUB_GRUPO'] = df_diagnostic.apply(map_subgroup, axis=1)

        df_diagnostic['PROC_GRU_NOME'] = df_diagnostic['NOME_SUB_GRUPO'].map(PROC_GRU_NOME_MAP).fillna('Outros Diagnósticos')

        # Criar REGIAO_CORPORAL_DETALHADA
        df_diagnostic['REGIAO_CORPORAL_DETALHADA'] = 'Geral'
        
        procs_with_detailed_regions = ['RM', 'TC', 'RX']
        mask_detailed_regions = df_diagnostic['PROC_GRU_NOME'].isin(procs_with_detailed_regions)
        
        df_diagnostic.loc[mask_detailed_regions, 'REGIAO_CORPORAL_DETALHADA'] = \
            df_diagnostic.loc[mask_detailed_regions, 'PROC_REA'].astype(str).str.strip().map(PROC_REA_TO_REGIAO_MAP).fillna('Outra Região Diagnóstica')

        # 3. Adicionar coluna FAIXA_ETARIA usando a coluna 'IDADE'
        df_diagnostic['FAIXA_ETARIA'] = df_diagnostic['IDADE'].apply(calculate_age_group)
        
        # 4. Agregação dos dados (formato "long")
        grouping_cols = [
            'UF_ATENDIMENTO',
            'ANO_ATENDIMENTO',
            'SEXO',
            'FAIXA_ETARIA',
            'PROC_GRU_NOME',
            'REGIAO_CORPORAL_DETALHADA'
        ]
        
        df_aggregated_long = df_diagnostic.groupby(grouping_cols).size().reset_index(name='TOTAL_PROCEDIMENTOS')
        print(f"  - Finalizado processamento de {filepath}. {len(df_aggregated_long)} linhas agregadas.")
        return df_aggregated_long

    except Exception as e:
        # Loga o erro, o arquivo e o traceback completo
        print(f"  - ERRO FATAL ao processar o arquivo {filepath}: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr) # Imprime o traceback completo para stderr
        return None


def main_processing_script(input_dir, output_file_csv_master):
    """
    Função principal para orquestrar o processamento de todos os arquivos .dbc.
    1. Encontra todos os arquivos .dbc no diretório de entrada.
    2. Processa cada arquivo, enriquecendo e agregando os dados.
    3. Concatena todos os resultados agregados em um DataFrame mestre "long".
    4. Salva o DataFrame mestre "long" em um arquivo CSV.
    5. Transforma o DataFrame mestre para o formato "wide" por UF e salva em um único arquivo Excel
       com planilhas separadas por UF.
    """
    
    dbc_filepaths = glob.glob(os.path.join(input_dir, '*.dbc'))
    print(f"Encontrados {len(dbc_filepaths)} arquivos .dbc para processar no diretório: {input_dir}")

    all_aggregated_dfs = []
    failed_files = [] # Lista para armazenar informações dos arquivos que falharam

    for filepath in dbc_filepaths:
        df_agg = process_single_dbc_file(filepath)
        if df_agg is not None: # Se processou com sucesso (mesmo que seja um DataFrame vazio)
            all_aggregated_dfs.append(df_agg)
        else: # Se process_single_dbc_file retornou None (indicando erro)
            failed_files.append(filepath)

    if not all_aggregated_dfs:
        print("\nNenhum dado processado com sucesso de todos os arquivos. Saindo.")
        if failed_files:
            print("\nArquivos que falharam no processamento:")
            for f in failed_files:
                print(f"  - {f}")
        return

    final_df_long = pd.concat(all_aggregated_dfs, ignore_index=True)
    
    final_grouping_cols = [
        'UF_ATENDIMENTO',
        'ANO_ATENDIMENTO',
        'SEXO',
        'FAIXA_ETARIA',
        'PROC_GRU_NOME',
        'REGIAO_CORPORAL_DETALHADA'
    ]
    final_df_long = final_df_long.groupby(final_grouping_cols)['TOTAL_PROCEDIMENTOS'].sum().reset_index()

    print(f"\nDataFrame mestre em formato 'long' criado com {len(final_df_long)} linhas.")
    
    # Registra quais UFs acabaram no DataFrame final (pode ser útil para checar se RR e TO apareceram)
    print(f"UFs presentes no DataFrame final: {sorted(final_df_long['UF_ATENDIMENTO'].unique())}")


    os.makedirs(os.path.dirname(output_file_csv_master), exist_ok=True)
    final_df_long.to_csv(output_file_csv_master, index=False, encoding='utf-8')
    print(f"DataFrame mestre salvo em: {output_file_csv_master}")

    # excelll(final_df_long, output_excel_filepath)

    print("\nProcessamento concluído!")
    if failed_files:
        print("\nATENÇÃO: Os seguintes arquivos falharam no processamento:")
        for f in failed_files:
            print(f"  - {f}")
            
    return final_df_long


def excelll(final_df_long, output_consolidated_excel_filepath):
    """
    Transforma o DataFrame mestre "long" para o formato "wide" por UF
    e salva todas as UFs em planilhas separadas dentro de um único arquivo Excel,
    formatando os dados como tabelas Excel.
    """
    os.makedirs(os.path.dirname(output_consolidated_excel_filepath), exist_ok=True)
    
    unique_ufs = sorted(final_df_long['UF_ATENDIMENTO'].unique())
    print(f"Gerando arquivo Excel consolidado em: {output_consolidated_excel_filepath} com {len(unique_ufs)} planilhas...")

    all_years = sorted(final_df_long['ANO_ATENDIMENTO'].unique())
    year_columns_ordered = [f'TOTAL_{year}' for year in all_years]

    excel_index_cols = ['SEXO', 'FAIXA_ETARIA', 'PROC_GRU_NOME', 'REGIAO_CORPORAL_DETALHADA']

    # Usa pd.ExcelWriter com o motor 'xlsxwriter' para acesso a funcionalidades avançadas
    with pd.ExcelWriter(output_consolidated_excel_filepath, engine='xlsxwriter') as writer:
        for uf in unique_ufs:
            print(f"  - Preparando planilha para UF: {uf}")
            df_uf = final_df_long[final_df_long['UF_ATENDIMENTO'] == uf].copy()
            
            df_uf_wide = df_uf.pivot_table(
                index=excel_index_cols,
                columns='ANO_ATENDIMENTO',
                values='TOTAL_PROCEDIMENTOS',
                fill_value=0
            ).reset_index()

            current_year_cols_as_int = [col for col in df_uf_wide.columns if isinstance(col, int)]
            rename_map = {col_int: f'TOTAL_{col_int}' for col_int in current_year_cols_as_int}
            df_uf_wide = df_uf_wide.rename(columns=rename_map)

            for expected_col_name in year_columns_ordered:
                if expected_col_name not in df_uf_wide.columns:
                    df_uf_wide[expected_col_name] = 0

            final_excel_cols_order = excel_index_cols + year_columns_ordered
            df_uf_wide = df_uf_wide[final_excel_cols_order]

            # Salva o DataFrame como uma planilha
            df_uf_wide.to_excel(writer, sheet_name=uf, index=False, startrow=0, startcol=0)

            # --- Adicionar Formato de Tabela Excel ---
            # Obtém o objeto worksheet do xlsxwriter
            workbook = writer.book
            worksheet = writer.sheets[uf]

            # Define o intervalo da tabela (incluindo o cabeçalho)
            # len(df_uf_wide) é o número de linhas de dados
            # len(df_uf_wide.columns) é o número de colunas
            # O Excel é baseado em 0-indexado, então a última linha é len-1, última coluna é len-1
            # E o cabeçalho está na linha 0.
            max_row = len(df_uf_wide)
            max_col = len(df_uf_wide.columns) - 1 # Índices de coluna são 0 a N-1

            # Cria a tabela Excel
            # O nome da tabela pode ser a própria UF ou algo como "Tabela_UF"
            table_name = f"Tabela_{uf}"
            worksheet.add_table(0, 0, max_row, max_col, {
                'name': table_name,
                'columns': [{'header': col} for col in df_uf_wide.columns]
            })

            # Autoajustar a largura das colunas (opcional, pode ser lento para muitas colunas/linhas)
            # for i, col in enumerate(df_uf_wide.columns):
            #     max_len = max(df_uf_wide[col].astype(str).map(len).max(), len(col))
            #     worksheet.set_column(i, i, max_len + 2) # +2 para um pequeno padding
            
            print(f"    - Planilha '{uf}' formatada como tabela Excel.")

# --- Bloco Principal de Execução ---

if __name__ == "__main__":
    # Define os diretórios de entrada e saída
    # Assumindo a seguinte estrutura:
    # Pasta_maior/
    #   data/ (aqui estão seus arquivos .dbc)
    #   output/ (aqui serão salvos os resumos)
    #   r.py (seu script Python)
    
    # Caminho do diretório pai (Pasta_maior)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Diretório onde os arquivos .dbc estão
    input_data_dir = os.path.join(base_dir, 'data') 
    
    
    # Caminho para o arquivo CSV mestre (formato "long")
    output_csv_master_path = os.path.join(base_dir, 'output', 'datasus_sumario_nacional_long.csv')

    final_df_long = main_processing_script(input_data_dir, output_csv_master_path)
    # final_df_long = pd.read_csv(output_csv_master_path, header=0)
    
    # Diretório onde os arquivos Excel por UF serão salvos
    output_excel_dir = os.path.join(base_dir, 'output', 'resumo_consolidado_por_uf.xlsx')
    
    excelll(final_df_long, output_excel_dir)