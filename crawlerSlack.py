# imports principais
import pandas as pd            # para manipular DataFrames e salvar/ler CSVs facilmente
import psutil                  # biblioteca para coletar métricas de sistema/processos
import time                    # sleep / medir tempo
import os                      # operações com sistema de arquivos e variáveis de ambiente
from datetime import datetime  # para timestamps legíveis
from uuid import getnode as get_mac  # retorna o MAC como um inteiro (veja observações abaixo)
from slack_sdk import WebClient


slack_client = WebClient(token=("xoxb-9455078396583-9524770957313-TbwFeOlSinZD8e6azfX80Jnz")) #isso é tipo o telefone do meu bot

CANAL_ALERTA = "C09EYEPDWGM"  #ID do canal (No caso canal da  BIQ)

# -----------------------------------------------------------------------

# Variáveis globais / configuração inicial
DURACAO_CAPTURA = 0.3 * 60 # tempo que o programa vai funcionar (5 min).
CAMINHO_PASTA = 'dados_monitoramento'  # pasta onde CSVs/logs serão salvos
MAC_ADRESS = get_mac()                 # retorna um inteiro representando o MAC (ver nota abaixo)

NOME_ARQUIVO = f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')} - {MAC_ADRESS}.csv"  # nome do arquivo que armazena os dados de máquina
NOME_ARQUIVO_PROCESSO = f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}-Processos {MAC_ADRESS}.csv" # nome do arquivo que armazena os processos da máquina
CAMINHO_ARQUIVO = os.path.join(CAMINHO_PASTA, NOME_ARQUIVO) # caminho para a inserção dos dados de máquina
CAMINHO_ARQUIVO_PROCESSO = os.path.join(CAMINHO_PASTA, NOME_ARQUIVO_PROCESSO) # caminho para a inserção dos processos da máquina

NOME_LOG = f"log_processamento_{MAC_ADRESS}.csv"         # nome do arquivo que armazena os arquivos de dados criados
CAMINHO_LOG = os.path.join(CAMINHO_PASTA, NOME_LOG)      # caminho para a inserção dos logs
NOME_CHUNK = f"chunks_processados_{MAC_ADRESS}.csv"      # nome do arquivo que armazena os arquivos de processos criados
CAMINHO_CHUNKS = os.path.join(CAMINHO_PASTA, NOME_CHUNK) # caminho para a inserção dos chunks

# Funções

def enviar_alerta_canal(mensagem):
    
    # Envia uma mensagem para o canal Slack definido (CANAL_ALERTA).
    # Usa slack_client.chat_postMessage (método da slack_sdk).
    # Em caso de erro, registra no log local.
    
    try:
        # chat_postMessage recebe channel (ID ou nome com #) e mensagem que aparece no slack.
        slack_client.chat_postMessage(channel=CANAL_ALERTA, text=mensagem)
        print(f"Alerta enviado: {mensagem}")
    except Exception as e:
        print(f"Erro ao enviar alerta: {e}")
        registrar_log(f"ERRO AO ENVIAR ALERTA: {e}")


def coletar_dados_hardware():

    # Retorna um dicionário com métricas de hardware no momento da chamada:
    #  - timestamp: string formatada com data/hora
    #  - cpu: valor de uso de CPU (psutil.cpu_percent())
    #  - ram: percentagem de uso de memória física
    #  - disco: percentagem de uso do disco raiz '/'
    #  - mac: o identificador MAC do host (usado para correlacionar múltiplos hosts)

    return {
        'timestamp': datetime.now().strftime('%Y-%m-%d_%H-%M-%S'), # data e hora da captura
        'cpu': psutil.cpu_percent(),                     # uso de CPU total do sistema
        'ram': psutil.virtual_memory().percent,          # uso de RAM em %
        'disco': psutil.disk_usage('/').percent,         # uso do disco da raiz em %
        'mac' : MAC_ADRESS                               # Endereço físico
    }


def coletar_dados_processos():

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


    # agora coletamos os valores reais (com base na amostra anterior)
    for proc in psutil.process_iter():
        try:
            # cpu: normaliza o valor do processo com base no total de núcleos usados no processo
            cpu = round(proc.cpu_percent(interval=None) / psutil.cpu_count(logical=True), 1)

            # disco: quantos MB foram escritos pelo processo
            # Conversão de bytes -> megabytes:
            disco = round((proc.io_counters().write_bytes / (1024 ** 2)), 1)

            # ram: porcentagem do total de memória usada por esse processo
            ram = round((proc.memory_info().rss * 100 / psutil.virtual_memory().total), 1)

            # filtro simples para reduzir ruído: mantemos processos com impacto considerável
            if cpu > 0 or ram > 1 or disco > 1:
                # pequenos ajustes: se ram ou disco vierem menores que 1, forçamos zero
                if ram < 1:
                    ram = 0
                if disco < 1:
                    disco = 0

                # armazena os dados no csv
                processos_info.append({
                    'timestamp' : datetime.now().strftime('%Y-%m-%d_%H-%M-%S'),
                    'processo' : proc.name(),   # nome do executável
                    'cpu' : cpu,
                    'ram' : ram,
                    'dados_gravados' : disco,
                    'mac' : MAC_ADRESS
                })

        except (psutil.NoSuchProcess, psutil.AccessDenied):
            # Se o processo terminar entre a captura ou não tivermos permissão, pulamos
            continue
    return processos_info


def salvar_arquivo(dataFrame, CAMINHO):

    # Salva um DataFrame em CSV.

    # Se o arquivo já existe, faz append sem cabeçalho.
    if os.path.exists(CAMINHO):
        dataFrame.to_csv(CAMINHO, mode='a', header=False, index=False)
    else:
        # Se não existe, cria e grava com cabeçalho.
        dataFrame.to_csv(CAMINHO, index=False)


def registrar_log(mensagem):
    
    # Registra um evento em um CSV de log.
    #  - Cria um DataFrame com timestamp e a mensagem.
    #  - Usa salvar_arquivo para manter o registro.
    
    log_data = pd.DataFrame([{
        'timestamp': datetime.now(),
        'evento': mensagem,
        'mac' : MAC_ADRESS
    }])
    salvar_arquivo(log_data, CAMINHO_LOG)


def adicionar_a_chunks(nome_arquivo):
    
    # Registra no arquivo de 'chunks' o nome dos arquivos gerados (útil para rastrear arquivos).
    #  - Salva timestamp + nome_arquivo
    
    chunk_data = pd.DataFrame([{
        'timestamp': datetime.now(),
        'nome_arquivo': nome_arquivo
    }])
    salvar_arquivo(chunk_data, CAMINHO_CHUNKS)


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

    NOME_ARQUIVO = f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')} - {MAC_ADRESS}.csv"
    CAMINHO_ARQUIVO = os.path.join(CAMINHO_PASTA, NOME_ARQUIVO)
    NOME_ARQUIVO_PROCESSO = f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}-Processos {MAC_ADRESS}.csv"
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
            LIMITE_CPU = 1   
            LIMITE_RAM = 1   
            LIMITE_DISCO = 1 

            time.sleep(1)
            dados_coletados.append(coletar_dados_hardware())

            ultimo_dado = dados_coletados[-1]  # pega o último dado coletado

            # Verificações de limites — se exceder, envia alerta ao Slack
            if ultimo_dado['cpu'] > LIMITE_CPU:
                enviar_alerta_canal(f"🟥 CPU acima do limite! {ultimo_dado['cpu']}% em {ultimo_dado['timestamp']}")

            if ultimo_dado['ram'] > LIMITE_RAM:
                enviar_alerta_canal(f"🟥 RAM acima do limite! {ultimo_dado['ram']}% em {ultimo_dado['timestamp']}")

            if ultimo_dado['disco'] > LIMITE_DISCO:
                enviar_alerta_canal(f"🟥 Disco acima do limite! {ultimo_dado['disco']}% em {ultimo_dado['timestamp']}")

            processos = coletar_dados_processos()

            # Converte listas em DataFrames e salva (sobrescreve o arquivo com o DataFrame completo)
            df_dados = pd.DataFrame(dados_coletados)
            df_dados.to_csv(CAMINHO_ARQUIVO, index=False)

            df_processo = pd.DataFrame(processos)
            df_processo.to_csv(CAMINHO_ARQUIVO_PROCESSO, index=False)

            # Verifica se o chunk de captura já durou o tempo configurado
            if time.time() - inicio_captura >= DURACAO_CAPTURA:
                redefinir_caminho()

                registrar_log(f"Novo arquivo de dados criado: {NOME_ARQUIVO}")
                registrar_log(f"Novo arquivo de dados criado: {NOME_ARQUIVO_PROCESSO}")

                print(f"Captura finalizada. Dados salvos em {CAMINHO_ARQUIVO} e em {CAMINHO_ARQUIVO_PROCESSO}")

                adicionar_a_chunks(NOME_ARQUIVO_PROCESSO)
                adicionar_a_chunks(NOME_ARQUIVO)

                # reset dos buffers
                inicio_captura = time.time()
                dados_coletados = []
                processos = []
        except KeyboardInterrupt:
            # Tratamento para Ctrl+C
            print("\nMonitoramento interrompido pelo usuário.")
            registrar_log("Monitoramento interrompido manualmente.")
            break
        except Exception as e:
            # Qualquer exceção aqui interrompe o loop atual
            print(f"Ocorreu um erro: {e}")
            registrar_log(f"ERRO: {e}")
            break

if __name__ == "__main__":
    main()