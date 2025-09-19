import requests
import os
from urllib.parse import urlparse
from ftplib import FTP, all_errors
import time

def download_file_from_http(file_url, local_filepath):
    """
    Baixa um arquivo de uma URL HTTP/HTTPS usando a biblioteca requests.
    Retorna True em caso de sucesso, False caso contrário.
    """
    try:
        print(f"Baixando via HTTP/HTTPS: {file_url}")
        with requests.get(file_url, stream=True, timeout=60) as r:
            r.raise_for_status() # Levanta um erro para códigos de status HTTP ruins (4xx ou 5xx)
            with open(local_filepath, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk: # Filtra chunks vazios para manter a conexão ativa
                        f.write(chunk)
        print(f"Download concluído: {local_filepath}")
        return True
    except requests.exceptions.HTTPError as e:
        print(f"ERRO: HTTP ao tentar baixar {file_url}: {e}")
        if hasattr(e, 'response') and e.response.status_code == 404:
            print("INFO: (HTTP) O arquivo provavelmente não existe no servidor.")
    except requests.exceptions.ConnectionError as e:
        print(f"ERRO: Conexão ao tentar baixar {file_url}: {e}")
    except requests.exceptions.Timeout as e:
        print(f"ERRO: Tempo limite excedido ao tentar baixar {file_url}: {e}")
    except requests.exceptions.RequestException as e:
        print(f"ERRO: Inesperado durante a requisição HTTP: {e}")
    except IOError as e:
        print(f"ERRO: Ao escrever o arquivo '{local_filepath}': {e}")
    return False

def download_file_from_ftp(file_url, local_filepath):
    """
    Baixa um arquivo de uma URL FTP usando a biblioteca ftplib.
    Assume FTP anônimo, que é comum para o DATASUS.
    Retorna True em caso de sucesso, False caso contrário.
    """
    parsed_url = urlparse(file_url)
    hostname = parsed_url.hostname
    path = parsed_url.path.lstrip('/') # Remove a barra inicial do path

    if not hostname:
        print(f"ERRO: Não foi possível extrair o hostname da URL FTP: {file_url}")
        return False

    try:
        print(f"Baixando via FTP de {hostname}:/{path}")
        with FTP(hostname, timeout=60) as ftp:
            ftp.login() # Login anônimo

            remote_dir, remote_filename = os.path.split(path)
            
            # Tentativa de mudar de diretório remoto
            if remote_dir:
                try:
                    ftp.cwd(remote_dir)
                    print(f"INFO: Mudou para o diretório remoto: {remote_dir}")
                    with open(local_filepath, 'wb') as f:
                        ftp.retrbinary(f"RETR {remote_filename}", f.write)
                except all_errors as e:
                    # Se falhar ao mudar de diretório, tenta baixar usando o caminho completo a partir da raiz
                    print(f"AVISO: Erro ao mudar para o diretório remoto '{remote_dir}': {e}. Tentando baixar com o caminho completo do arquivo.")
                    ftp.cwd('/') # Volta para o diretório raiz para tentar o caminho absoluto
                    with open(local_filepath, 'wb') as f:
                        ftp.retrbinary(f"RETR {path}", f.write)
            else:
                # Se não há subdiretórios no URL, baixa o arquivo diretamente
                with open(local_filepath, 'wb') as f:
                    ftp.retrbinary(f"RETR {remote_filename}", f.write)

        print(f"Download concluído: {local_filepath}")
        return True
    except all_errors as e: # Captura todos os erros da ftplib
        print(f"ERRO: FTP ao tentar baixar {file_url}: {e}")
        # Tenta identificar erro de "arquivo não encontrado"
        if "No such file" in str(e) or "550" in str(e):
            print(f"INFO: (FTP) O arquivo não existe ou não está acessível no servidor: {file_url}.")
    except IOError as e:
        print(f"ERRO: Ao escrever o arquivo '{local_filepath}': {e}")
    except Exception as e:
        print(f"ERRO: Geral no download FTP: {e}")
    return False

def download_single_file(file_url, download_directory="Data"):
    """
    Baixa um único arquivo de uma URL específica para um diretório local,
    lidando com protocolos HTTP/HTTPS e FTP.
    Retorna True em caso de sucesso, False caso contrário.
    """
    if not file_url:
        print("AVISO: URL do arquivo não fornecida.")
        return False

    os.makedirs(download_directory, exist_ok=True)
    # print(f"Diretório de download '{download_directory}' garantido.") # Desnecessário repetir toda vez

    parsed_url = urlparse(file_url)
    file_name = os.path.basename(parsed_url.path)

    if not file_name:
        print(f"AVISO: Não foi possível determinar o nome do arquivo a partir da URL: {file_url}. Usando 'downloaded_file' como padrão.")
        file_name = "downloaded_file"

    local_filepath = os.path.join(download_directory, file_name)

    print(f"Salvando como: {local_filepath}")

    if parsed_url.scheme.lower() in ['http', 'https']:
        return download_file_from_http(file_url, local_filepath)
    elif parsed_url.scheme.lower() == 'ftp':
        return download_file_from_ftp(file_url, local_filepath)
    else:
        print(f"ERRO: Protocolo não suportado: {parsed_url.scheme} para URL: {file_url}")
        return False

# --- Geração dos URLs e Loop de Download ---
if __name__ == "__main__":
    brazilian_states = [
        "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS",
        "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC",
        "SP", "SE", "TO"
    ]
    
    # IMPORTANTE: Confirme este caminho no servidor FTP do DATASUS.
    ftp_base_url = "ftp://ftp.datasus.gov.br/dissemin/publicos/CIHA/201101_/Dados/"
    
    start_year = 2011
    end_year = 2025 
    
    target_folder = "D:\CODE\DATASUS\data"

    generated_file_urls = []
    failed_downloads = [] # Lista para armazenar URLs que falharam

    print("Gerando URLs dos arquivos CIHA...")
    for year in range(start_year, end_year + 1):
        year_yy = str(year)[2:] 
        for month in range(1, 13):
            month_mm = f"{month:02d}" 
            for state_code in brazilian_states:
                file_name = f"CIHA{state_code}{year_yy}{month_mm}.dbc"
                full_url = f"{ftp_base_url}{file_name}"
                generated_file_urls.append(full_url)
    print(f"Total de {len(generated_file_urls)} URLs geradas para tentar baixar.")
    print("-" * 50)

    download_delay_seconds = 4

    for i, file_url in enumerate(generated_file_urls):
        print(f"--- Processando arquivo {i+1}/{len(generated_file_urls)} ---")
        success = download_single_file(file_url, download_directory=target_folder)
        
        if not success:
            failed_downloads.append(file_url) # Adiciona à lista de falhas
            print(f"AVISO: Download de {file_url} falhou. Prosseguindo para o próximo arquivo.")
        else:
            print(f"Download de {file_url} concluído com sucesso.")

        # Adiciona um atraso entre os downloads
        time.sleep(download_delay_seconds)
        print("-" * 50)

    print("Processo de download concluído.")
    if failed_downloads:
        print("\n--- Links que FALHARAM no download: ---")
        for failed_url in failed_downloads:
            print(f"- {failed_url}")
    else:
        print("\nTodos os downloads foram concluídos com sucesso!")