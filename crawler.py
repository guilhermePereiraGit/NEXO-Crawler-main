# imports principais
import boto3                   # para ter acesso a AWS e aos buckets S3
import pandas as pd            # para manipular DataFrames e salvar/ler CSVs facilmente
import psutil                  # biblioteca para coletar métricas de sistema/processos
import time                    # sleep / medir tempo
import os                      # operações com sistema de arquivos e variáveis de ambiente
from datetime import datetime  # para timestamps legíveis
from uuid import getnode as get_mac  # retorna o MAC como um inteiro (veja observações abaixo)
from dotenv import load_dotenv # para usar as variáveis do .env
import tempfile                # para criar um arquivo temporário antes de enviar ao bucket
import mysql.connector         # usado para conexão com o banco

# -----------------------------------------------------------------------

conexao = mysql.connector.connect(
    host="174.129.108.106",  # ou o IP do servidor
    user="root",             # seu usuário MySQL
    password="urubu100",     # sua senha
    database="NEXO_DB"
)


# Variáveis globais / configuração inicial
DURACAO_CAPTURA = 5 * 60 # tempo que o programa vai funcionar (5 min).
CAMINHO_PASTA = 'dados_monitoramento'  # pasta onde CSVs/logs serão salvos
MAC_ADRESS = get_mac()                 # retorna um inteiro representando o MAC (ver nota abaixo)

NOME_ARQUIVO = f"Dados.csv"  # nome do arquivo que armazena os dados de máquina
NOME_ARQUIVO_PROCESSO = f"Processos.csv" # nome do arquivo que armazena os processos da máquina
CAMINHO_ARQUIVO = os.path.join(CAMINHO_PASTA, NOME_ARQUIVO) # caminho para a inserção dos dados de máquina
CAMINHO_ARQUIVO_PROCESSO = os.path.join(CAMINHO_PASTA, NOME_ARQUIVO_PROCESSO) # caminho para a inserção dos processos da máquina

NOME_LOG = f"log_processamento_{MAC_ADRESS}.csv"         # nome do arquivo que armazena os arquivos de dados criados
CAMINHO_LOG = os.path.join(CAMINHO_PASTA, NOME_LOG)      # caminho para a inserção dos logs
NOME_CHUNK = f"chunks_processados_{MAC_ADRESS}.csv"      # nome do arquivo que armazena os arquivos de processos criados
CAMINHO_CHUNKS = os.path.join(CAMINHO_PASTA, NOME_CHUNK) # caminho para a inserção dos chunks

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("BUCKET_NAME")

# selects 
informacoesMAC = ()
queryInformacoesMAC = """
SELECT 
    m.nome AS modeloNome,
    m.fkEmpresa
FROM totem t
INNER JOIN modelo m ON t.fkModelo = m.idModelo
WHERE t.numMac = %s;
"""

informacoesMAC = pd.read_sql(queryInformacoesMAC, conexao, params=(MAC_ADRESS,))

modelo = informacoesMAC.iloc[0, 0]  
idEmpresa = informacoesMAC.iloc[0, 1]  

# Funções

def coletar_dados_hardware(momento):

    # Retorna um dicionário com métricas de hardware no momento da chamada:
    #  - timestamp: string formatada com data/hora
    #  - cpu: valor de uso de CPU (psutil.cpu_percent())
    #  - ram: percentagem de uso de memória física
    #  - disco: percentagem de uso do disco raiz '/'
    #  - mac: o identificador MAC do host (usado para correlacionar múltiplos hosts)
        

    return {
        'timestamp': momento,                            # data e hora da captura
        'cpu': psutil.cpu_percent(),                     # uso de CPU total do sistema
        'ram': psutil.virtual_memory().percent,          # uso de RAM em %
        'disco': psutil.disk_usage('/').percent,         # uso do disco da raiz em %
        'qtd_processos': len(psutil.pids()),             # quantidade de processos rodando
        'mac' : MAC_ADRESS,                              # Endereço físico
        'modelo': modelo,                                # modelo do totem associado pelo MAC
        'idEmpresa': idEmpresa                           # id da empresa associada a este MAC
    }


def coletar_dados_processos(momento):

    # Coleta informações dos processos em execução:
    #  - faz uma chamada inicial para proc.cpu_percent(interval=None) em todos os processos
    #    para "inicializar" a medição de CPU por processo.
    #  - aguarda 1 segundo (time.sleep(1)) para que seja possível calcular porcentagens.
    #  - itera novamente sobre processos e coleta:
    #      * cpu: uso de CPU do processo
    #      * disco: bytes escritos convertidos para MB
    #      * ram: porcentagem de RAM usada por processo
    #  - filtra processos com impacto pequeno: (cpu > 0 or ram > 1 or disco > 1)
    #  - lida com exceções comuns (NoSuchProcess, AccessDenied)
    # Observações:
    #  - io_counters() pode lançar AccessDenied ou pode ser None para alguns processos.
    #  - memory_info().rss está em bytes; convertendo para % usamos total de memória do sistema.

    processos_info = []
    # inicializar contadores de CPU por processo
    for proc in psutil.process_iter():
        try:
            proc.cpu_percent(interval=None)  # chamada de inicialização
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            # Esses processos desapareceram ou não tem permissão — ignorados
            continue

    # Precisamos de um intervalo entre as duas chamadas para que psutil possa
    # calcular a diferença e retornar um valor de uso de CPU significativo.
    time.sleep(1)
    horario = momento

    # agora coletamos os valores reais (com base na amostra anterior)
    for proc in psutil.process_iter(['name', 'cpu_percent', 'memory_percent', 'io_counters']):
        try:
            nome = proc.info['name']
            if nome == "System Idle Process" or nome is None:
                continue  
            
            # cpu: normaliza o valor do processo com base no total de núcleos usados no processo
            cpu = round(proc.info['cpu_percent'] / psutil.cpu_count(logical=True), 1)

            # disco: quantos MB foram escritos pelo processo
            # Conversão de bytes -> megabytes:
            try:
                io = proc.io_counters()
                disco = round((io.write_bytes / (1024 ** 2)), 1) if io else 0
            except (psutil.AccessDenied, AttributeError):
                disco = 0
                
            # ram: porcentagem do total de memória usada por esse processo
            ram = round(proc.info['memory_percent'], 1)

            # filtro simples para reduzir ruído: mantemos processos com impacto considerável
            if cpu > 0 or ram > 1 or disco > 1:
                # pequenos ajustes: se ram ou disco vierem menores que 1, forçamos zero
                if ram < 1:
                    ram = 0
                if disco < 1:
                    disco = 0

                # armazena os dados no csv
                processos_info.append({
                    'timestamp' : horario,
                    'processo' : proc.name(),   # nome do executável
                    'cpu' : cpu,
                    'ram' : ram,
                    'disco' : disco,
                    'mac' : MAC_ADRESS,
                    'modelo': modelo,                                # modelo do totem associado pelo MAC
                    'idEmpresa': idEmpresa                           # id da empresa associada a este MAC
                })

        except (psutil.NoSuchProcess, psutil.AccessDenied):
            # Se o processo terminar entre a captura ou não tivermos permissão, pulamos
            continue
        
    processos_info.sort(key=lambda p: (p['ram'], p['cpu']), reverse=True)
    return processos_info[:5]


def salvar_arquivo(dataFrame, CAMINHO):

    # Salva um DataFrame em CSV.

    # Se o arquivo já existe, faz append sem cabeçalho.
    if os.path.exists(CAMINHO):
        dataFrame.to_csv(CAMINHO, mode='a', header=False, index=False)
    else:
        # Se não existe, cria e grava com cabeçalho.
        dataFrame.to_csv(CAMINHO, index=False)


def upload_s3_temp_creds(arquivo_local, bucket, objeto_s3):
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
        region_name=AWS_REGION
    )

    # Diretório do MAC no bucket (garante que exista)
    mac_dir_prefix = f"/registros/{MAC_ADRESS}/"
    try:
        resultado = s3.list_objects_v2(Bucket=bucket, Prefix=mac_dir_prefix, MaxKeys=1)
        if 'Contents' not in resultado:
            # Cria um marcador de diretório vazio
            s3.put_object(Bucket=bucket, Key=mac_dir_prefix)
    except Exception as e:
        print(f"Erro ao verificar/criar diretório no S3: {e}")

    # Caminho completo do arquivo no S3
    objeto_final = f"{mac_dir_prefix}{os.path.basename(objeto_s3)}"

    try:
        temp_path = os.path.join(tempfile.gettempdir(), "temp_s3_append.csv")
        df_novo = pd.read_csv(arquivo_local)

        # Tenta baixar o arquivo existente no S3 para merge
        try:
            s3.download_file(bucket, objeto_final, temp_path)
            df_existente = pd.read_csv(temp_path)
            # Faz merge e remove duplicatas baseadas em timestamp + mac
            df_concat = pd.concat([df_existente, df_novo], ignore_index=True)
            df_concat = df_concat.drop_duplicates(subset=['timestamp', 'mac'], keep='last')
            df_concat.to_csv(temp_path, index=False)
            s3.upload_file(temp_path, bucket, objeto_final)
            print(f"Append (com merge seguro) OK: s3://{bucket}/{objeto_final}")

        except s3.exceptions.ClientError as e:
            # Caso o arquivo não exista ainda (404)
            if e.response['Error']['Code'] == "404":
                s3.upload_file(arquivo_local, bucket, objeto_final)
                print(f"Upload inicial OK: s3://{bucket}/{objeto_final}")
            else:
                raise e

        return True

    except Exception as e:
        print(f"Erro no upload/append: {e}")
        return False

def redefinir_caminho():

    # Gera novos nomes de arquivo baseados no horário atual e atualiza as variáveis globais
    # que apontam para os caminhos dos arquivos.
    #  - Retorna (CAMINHO_ARQUIVO, NOME_ARQUIVO, NOME_ARQUIVO_PROCESSO, CAMINHO_ARQUIVO_PROCESSO)
    # Observações importantes:
    #  - Uso de 'global' somente quando for reescrever variáveis globais (como aqui).
  
    global NOME_ARQUIVO
    global CAMINHO_ARQUIVO
    global NOME_ARQUIVO_PROCESSO
    global CAMINHO_ARQUIVO_PROCESSO
    # global MAC_ADRESS  # removi aqui, o mac do pc literal é imutavel

    NOME_ARQUIVO = f"dados.csv"
    CAMINHO_ARQUIVO = os.path.join(CAMINHO_PASTA, NOME_ARQUIVO)
    NOME_ARQUIVO_PROCESSO = f"processos.csv"
    CAMINHO_ARQUIVO_PROCESSO = os.path.join(CAMINHO_PASTA, NOME_ARQUIVO_PROCESSO)
    return CAMINHO_ARQUIVO, NOME_ARQUIVO, NOME_ARQUIVO_PROCESSO, CAMINHO_ARQUIVO_PROCESSO


# Lógica principal
def main():
    
    # Loop principal de coleta:
    #  - cria a pasta de dados se necessário
    #  - inicia um loop infinito (podemos interromper com Ctrl+C)
    #  - a cada iteração:
    #      * coleta hardware (append na lista dados_coletados)
    #      * verifica limites e envia alertas ao Slack quando ultrapassados
    #      * coleta processos e salva ambos os CSVs
    #  - ao atingir DURACAO_CAPTURA (segundos), redefine o caminho (cria novos arquivos),
    #    registra logs, marca os arquivos no chunks e reseta buffers.

    print("Iniciando o monitoramento. Pressione Ctrl+C a qualquer momento para sair.")
    if not os.path.exists(CAMINHO_PASTA):
        os.makedirs(CAMINHO_PASTA)  # cria a pasta se não existir

    inicio_captura = time.time()
    dados_coletados = []
    processos = []
    redefinir_caminho()  # inicializa nomes/caminhos de arquivo

    while True:
        try:
            # Limites configurados (valores de exemplo), esses são os parametros que viriam do banco
            horario = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

            time.sleep(10)
            dados_coletados.append(coletar_dados_hardware(horario))

            top5 = coletar_dados_processos(horario)
            
            processos.extend(top5)
            

            # Converte listas em DataFrames e salva (sobrescreve o arquivo com o DataFrame completo)
            df_dados = pd.DataFrame(dados_coletados)
            salvar_arquivo(df_dados, CAMINHO_ARQUIVO)

            df_processos = pd.DataFrame(top5)
            df_processos = df_processos.sort_values(by=['ram', 'cpu'], ascending=False)
            salvar_arquivo(df_processos, CAMINHO_ARQUIVO_PROCESSO)
            upload_s3_temp_creds(CAMINHO_ARQUIVO, BUCKET_NAME, objeto_s3=f"dados.csv")
            upload_s3_temp_creds(CAMINHO_ARQUIVO_PROCESSO, BUCKET_NAME, objeto_s3=f"processos.csv")


            # Verifica se o chunk de captura já durou o tempo configurado
            if time.time() - inicio_captura >= DURACAO_CAPTURA:
                redefinir_caminho()

                print(f"Captura finalizada. Dados salvos em {CAMINHO_ARQUIVO} e em {CAMINHO_ARQUIVO_PROCESSO}")

                # reset dos buffers
                inicio_captura = time.time()
                dados_coletados = []
                processos = []
        except KeyboardInterrupt:
            # Tratamento para Ctrl+C
            print("\nMonitoramento interrompido pelo usuário.")
            break
        except Exception as e:
            # Qualquer exceção aqui interrompe o loop atual
            print(f"Ocorreu um erro: {e}")
            break

if __name__ == "__main__":
    main()