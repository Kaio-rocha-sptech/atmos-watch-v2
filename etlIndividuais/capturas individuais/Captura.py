import csv
import json
import os
import platform
import random
import socket
import sys
import time
import urllib.parse
import urllib.request
from datetime import UTC, datetime

import boto3
import psutil
from dotenv import load_dotenv

try:
    import mysql.connector as mysql_connector
except ImportError:
    mysql_connector = None


# ===================== CONFIG =====================

load_dotenv()

IDENTIFICADOR = os.getenv("IDENTIFICADOR", "Kaio")
EMPRESA = os.getenv("EMPRESA", "empresaX")
HOSTNAME = socket.gethostname()
HOSTNAME_SAFE = HOSTNAME.replace(" ", "_").lower()

BUCKET = os.getenv("S3_BUCKET_NAME")

INTERVALO_CAPTURA = int(os.getenv("INTERVALO_CAPTURA", "60"))
UPLOAD_MAX_COLETAS = int(os.getenv("UPLOAD_MAX_COLETAS", "100"))
UPLOAD_INTERVALO_SEGUNDOS = int(os.getenv("UPLOAD_INTERVALO_SEGUNDOS", "600"))

PREDITIVO_EMPRESA = os.getenv("PREDITIVO_EMPRESA") or os.getenv("FKEMPRESA") or "1"
PREDITIVO_INTERVALO_SEGUNDOS = int(os.getenv("PREDITIVO_INTERVALO_SEGUNDOS", "600"))

PROCESSOS_MAX_HISTORICO = int(os.getenv("MAX_HISTORICO_PROCESSOS", "60"))
PROCESSOS_MAX_CLIENT = int(os.getenv("MAX_PROCESSOS_CLIENT", "60"))

CLIMA_INTERVALO_SEGUNDOS = int(os.getenv("CLIMA_INTERVALO_SEGUNDOS", "600"))
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

ARQUIVO_CACHE_PROCESSOS = "cache_historico_processos.json"
ARQUIVO_SNAPSHOT_PROCESSOS = "snapshot_analitico.json"

NOMES_IGNORADOS = {"system idle process", "idle"}
SUFIXOS_NOME = (".exe", ".app")

historico_processos = {}

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
    region_name=os.getenv("AWS_DEFAULT_REGION"),
    endpoint_url=os.getenv("S3_ENDPOINT_URL") or None,
)


# ===================== LOG =====================

def log(mensagem, nivel="INFO"):
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{agora}] {nivel:<7} | {mensagem}")


# ===================== ENV / MYSQL / CLIMA =====================

def ler_env(caminho=".env.dev"):
    caminhos = [
        caminho,
        os.path.join(os.getcwd(), caminho),
        os.path.join(os.path.dirname(os.getcwd()), caminho),
    ]

    for caminho_env in caminhos:
        if not os.path.exists(caminho_env):
            continue

        with open(caminho_env, "r", encoding="utf-8") as arquivo:
            for linha in arquivo:
                linha = linha.strip()

                if not linha or linha.startswith("#") or "=" not in linha:
                    continue

                chave, valor = linha.split("=", 1)
                os.environ.setdefault(
                    chave.strip(),
                    valor.strip().strip("'").strip('"'),
                )

        return caminho_env

    return None


def obter_env_obrigatoria(nome):
    valor = os.getenv(nome)

    if valor is None or valor == "":
        raise RuntimeError(f"Configure a variavel de ambiente obrigatoria {nome}.")

    return valor


def conectar_mysql():
    if mysql_connector is None:
        raise RuntimeError("Dependencia ausente: mysql-connector-python")

    return mysql_connector.connect(
        host=obter_env_obrigatoria("DB_HOST"),
        database=obter_env_obrigatoria("DB_DATABASE"),
        user=obter_env_obrigatoria("DB_USER"),
        password=os.getenv("DB_PASSWORD", ""),
        port=int(obter_env_obrigatoria("DB_PORT")),
    )


def buscar_clima_atual():
    latitude = obter_env_obrigatoria("OPEN_METEO_LATITUDE")
    longitude = obter_env_obrigatoria("OPEN_METEO_LONGITUDE")
    parametros = {
        "latitude": latitude,
        "longitude": longitude,
        "current": "temperature_2m,precipitation,weather_code",
        "timezone": os.getenv("OPEN_METEO_TIMEZONE", "America/Sao_Paulo"),
    }
    url = f"{OPEN_METEO_URL}?{urllib.parse.urlencode(parametros)}"

    with urllib.request.urlopen(url, timeout=15) as resposta:
        dados = resposta.read().decode("utf-8")

    current = json.loads(dados)["current"]

    return {
        "temperatura": float(current.get("temperature_2m", 0)),
        "precipitacao": float(current.get("precipitation", 0)),
        "weather_code": int(current.get("weather_code", 0)),
        "data_hora": current.get("time"),
    }


def classificar_evento(weather_code, temperatura, precipitacao):
    if weather_code in [95, 96, 99]:
        return "Tempestade", "tempestade"

    if weather_code in [61, 63, 65, 80, 81, 82] or precipitacao >= 8:
        return "Chuva intensa", "chuva intensa"

    if temperatura >= 33:
        return "Onda de calor", "onda de calor"

    if temperatura <= 15 and weather_code in [3, 45, 48, 51, 53, 55, 56, 57]:
        return "Frente fria", "frente fria"

    return "Sem evento relevante", "sem evento relevante"


def obter_requisicoes_reais():
    return int(obter_env_obrigatoria("ATMOS_REQUISICOES_MINUTO"))


def calcular_pressao(requisicoes, faixa_esperada):
    limite = requisicoes / faixa_esperada

    if limite <= 1:
        return "baixa", "baixo"

    if limite <= 1.2:
        return "moderada", "medio"

    if limite <= 1.5:
        return "alta", "alto"

    return "critica", "critico"


def salvar_medicao_climatica(medicao):
    conexao = conectar_mysql()
    cursor = conexao.cursor()
    servidor_id = int(obter_env_obrigatoria("ATMOS_SERVIDOR_ID"))
    faixa_esperada = int(obter_env_obrigatoria("ATMOS_FAIXA_ESPERADA"))

    cursor.execute(
        """
        INSERT INTO analise_carga_climatica (
            data_hora,
            servidor_id,
            requisicoes_minuto,
            cpu_percentual,
            evento_climatico,
            tipo_evento,
            temperatura_externa,
            precipitacao,
            faixa_esperada,
            pressao_operacional,
            nivel_risco
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            medicao["data_hora"],
            servidor_id,
            medicao["requisicoes"],
            medicao["cpu"],
            medicao["evento_climatico"],
            medicao["tipo_evento"],
            medicao["temperatura"],
            medicao["precipitacao"],
            faixa_esperada,
            medicao["pressao"],
            medicao["risco"],
        ),
    )

    conexao.commit()
    cursor.close()
    conexao.close()


def executar_captura_climatica():
    clima = buscar_clima_atual()
    evento_climatico, tipo_evento = classificar_evento(
        clima["weather_code"],
        clima["temperatura"],
        clima["precipitacao"],
    )
    requisicoes = obter_requisicoes_reais()
    cpu = psutil.cpu_percent(interval=1)
    faixa_esperada = int(obter_env_obrigatoria("ATMOS_FAIXA_ESPERADA"))
    pressao, risco = calcular_pressao(requisicoes, faixa_esperada)
    data_hora = datetime.fromisoformat(clima["data_hora"]).strftime("%Y-%m-%d %H:%M:%S")

    medicao = {
        "data_hora": data_hora,
        "requisicoes": requisicoes,
        "cpu": cpu,
        "evento_climatico": evento_climatico,
        "tipo_evento": tipo_evento,
        "temperatura": clima["temperatura"],
        "precipitacao": clima["precipitacao"],
        "pressao": pressao,
        "risco": risco,
    }

    salvar_medicao_climatica(medicao)
    log(f"Medição climática salva: {medicao}")


# ===================== CAPTURA PRINCIPAL =====================

def obter_root_disco():
    sistema_operacional = platform.system()

    if sistema_operacional == "Linux":
        return "/"

    if sistema_operacional == "Windows":
        return "C:\\"

    log("Plataforma não suportada", "ERROR")
    sys.exit(1)


def coletar_dados(top_n=5):
    root = obter_root_disco()
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

    disk1 = psutil.disk_io_counters()
    net1 = psutil.net_io_counters()
    psutil.cpu_percent(interval=None)

    ram = psutil.virtual_memory()
    disco = psutil.disk_usage(root)

    time.sleep(1)

    disk2 = psutil.disk_io_counters()
    net2 = psutil.net_io_counters()

    cpu_percent = psutil.cpu_percent(interval=None)
    cpu_freq = psutil.cpu_freq().current if psutil.cpu_freq() else 0

    disk_throughput = (
        disk2.read_bytes
        + disk2.write_bytes
        - disk1.read_bytes
        - disk1.write_bytes
    ) / (1024**2)
    upload = (net2.bytes_sent - net1.bytes_sent) / (1024**2)
    download = (net2.bytes_recv - net1.bytes_recv) / (1024**2)

    conexoes = len(psutil.net_connections())

    processos = []
    for proc in psutil.process_iter(["pid", "name"]):
        try:
            proc.cpu_percent(interval=None)
        except Exception:
            pass

    time.sleep(0.5)

    for proc in psutil.process_iter(["pid", "name"]):
        try:
            nome = proc.info["name"]

            if nome == "System Idle Process":
                continue

            cpu = proc.cpu_percent(interval=None) / psutil.cpu_count()
            mem = proc.memory_percent()

            processos.append(
                {
                    "pid": proc.pid,
                    "name": nome,
                    "cpu": cpu,
                    "mem": mem,
                }
            )
        except Exception:
            pass

    return {
        "datahora": timestamp,
        "cpu_perc": cpu_percent,
        "cpu_freq": cpu_freq,
        "ram_perc": ram.percent,
        "ram_usada": ram.used,
        "ram_livre": ram.available,
        "disco_perc": disco.percent,
        "disco_usado": disco.used,
        "disco_livre": disco.free,
        "disco_total": disco.total,
        "disco_throughput": round(disk_throughput, 2),
        "upload": round(upload, 2),
        "download": round(download, 2),
        "network_total": round(upload + download, 2),
        "conexoes": conexoes,
        "total_processos": len(processos),
        "identificador": IDENTIFICADOR,
        "hostname": HOSTNAME,
        "empresa": EMPRESA,
    }


def gerar_nome_arquivo_metricas():
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    pasta = os.path.join("amostras", EMPRESA, HOSTNAME)
    os.makedirs(pasta, exist_ok=True)

    return os.path.join(pasta, f"{IDENTIFICADOR}_{timestamp}.csv")


def salvar_csv(caminho, dados):
    arquivo_existe = os.path.isfile(caminho)

    with open(caminho, mode="a", newline="", encoding="utf-8") as arquivo:
        writer = csv.DictWriter(arquivo, fieldnames=dados.keys())

        if not arquivo_existe:
            writer.writeheader()

        writer.writerow(dados)


# ===================== ANALISE PREDITIVA RAW =====================

def gerar_nome_arquivo_preditivo():
    pasta = os.path.join("amostras", "preditivo", str(PREDITIVO_EMPRESA))
    os.makedirs(pasta, exist_ok=True)

    return os.path.join(pasta, "raw.csv")


def coletar_raw_preditivo():
    cpu = psutil.cpu_percent(interval=1)
    variacao = random.uniform(-1, 1)
    temperatura = 45 + (cpu * 0.5) + variacao

    return {
        "timestamp": time.strftime("%H:%M"),
        "cpu": round(cpu, 1),
        "temperatura": round(temperatura, 1),
    }


# ===================== PROCESSOS =====================

def carregar_cache_processos():
    global historico_processos

    if not os.path.exists(ARQUIVO_CACHE_PROCESSOS):
        return

    with open(ARQUIVO_CACHE_PROCESSOS, "r", encoding="utf-8") as arquivo:
        historico_processos = json.load(arquivo)


def salvar_cache_processos():
    with open(ARQUIVO_CACHE_PROCESSOS, "w", encoding="utf-8") as arquivo:
        json.dump(historico_processos, arquivo)


def resetar_ciclo_processos():
    global historico_processos

    historico_processos = {}

    if os.path.exists(ARQUIVO_CACHE_PROCESSOS):
        os.remove(ARQUIVO_CACHE_PROCESSOS)


def normalizar_nome(nome):
    nome = (nome or "Processo sem nome").strip()
    nome_normalizado = nome.lower()

    for sufixo in SUFIXOS_NOME:
        if nome_normalizado.endswith(sufixo):
            nome_normalizado = nome_normalizado[: -len(sufixo)]
            break

    apelidos = {
        "chrome": "Google Chrome",
        "msedge": "Microsoft Edge",
        "msedgewebview2": "Microsoft Edge WebView",
        "firefox": "Mozilla Firefox",
        "discord": "Discord",
        "code": "Visual Studio Code",
        "python": "Python",
        "node": "Node.js",
    }

    return apelidos.get(nome_normalizado, nome)


def chave_processo(nome):
    return normalizar_nome(nome).lower()


def numero(valor, casas=2):
    try:
        return round(float(valor), casas)
    except (TypeError, ValueError):
        return 0


def obter_io_mb(processo):
    try:
        io = processo.io_counters()
        return numero((io.read_bytes + io.write_bytes) / 1024 / 1024)
    except (
        psutil.AccessDenied,
        psutil.NoSuchProcess,
        psutil.ZombieProcess,
        AttributeError,
    ):
        return 0


def obter_filhos_diretos(processo):
    try:
        return processo.children(recursive=False)
    except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess):
        return []


def nivel(percentil):
    if percentil >= 97:
        return "critico"

    if percentil >= 90:
        return "alerta"

    if percentil >= 70:
        return "atencao"

    return "normal"


def score(cpu, ram, io, sub):
    return round((cpu * 0.40 + ram * 0.35 + io * 0.20 + sub * 0.05) / 100, 2)


def percentil_valor(valores, percentil):
    if not valores:
        return 0

    posicao = (len(valores) - 1) * percentil
    inferior = int(posicao)
    superior = min(inferior + 1, len(valores) - 1)
    peso = posicao - inferior

    return valores[inferior] * (1 - peso) + valores[superior] * peso


def estatisticas(vetor):
    valores = sorted(float(v) for v in vetor if isinstance(v, (int, float)))

    if not valores:
        valores = [0.0]

    tamanho = len(valores)
    media = sum(valores) / tamanho
    mediana = (
        valores[tamanho // 2]
        if tamanho % 2
        else (valores[tamanho // 2 - 1] + valores[tamanho // 2]) / 2
    )
    variancia = (
        sum((valor - media) ** 2 for valor in valores) / (tamanho - 1)
        if tamanho > 1
        else 0
    )

    return {
        "media": numero(media),
        "mediana": numero(mediana),
        "desvioPadrao": numero(variancia**0.5),
        "percentil90": numero(percentil_valor(valores, 0.90)),
        "percentil95": numero(percentil_valor(valores, 0.95)),
        "maximo": numero(max(valores)),
    }


def percentil_score(valores, valor):
    valores_validos = [float(v) for v in valores if isinstance(v, (int, float))]

    if not valores_validos:
        return 0

    abaixo_ou_igual = sum(1 for item in valores_validos if item <= valor)
    return round((abaixo_ou_igual / len(valores_validos)) * 100)


def atualizar_historico_processo(chave_historico, chave_metrica, valor):
    chave_historico = str(chave_historico)

    if chave_historico not in historico_processos:
        historico_processos[chave_historico] = {
            "cpu": [],
            "ram": [],
            "ioDisco": [],
            "subprocessos": [],
        }

    historico_processos[chave_historico][chave_metrica].append(valor)

    if len(historico_processos[chave_historico][chave_metrica]) > PROCESSOS_MAX_HISTORICO:
        historico_processos[chave_historico][chave_metrica].pop(0)


def preparar_cpu_processos():
    for proc in psutil.process_iter():
        try:
            proc.cpu_percent(None)
        except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess):
            pass


def capturar_processos_individuais():
    processos = []

    for proc in psutil.process_iter(["pid", "name", "username", "status", "create_time"]):
        try:
            nome_original = proc.info.get("name") or "Processo sem nome"

            if nome_original.lower() in NOMES_IGNORADOS:
                continue

            processos.append(
                {
                    "pid": proc.info["pid"],
                    "ppid": proc.ppid(),
                    "nome": nome_original,
                    "nomeNormalizado": normalizar_nome(nome_original),
                    "usuario": proc.info.get("username"),
                    "status": proc.info.get("status"),
                    "tempoAtivoSegundos": round(
                        time.time() - proc.info.get("create_time", time.time())
                    ),
                    "cpu": numero(proc.cpu_percent(None)),
                    "ram": numero(proc.memory_info().rss / 1024 / 1024),
                    "ioDisco": obter_io_mb(proc),
                    "filhosDiretos": [filho.pid for filho in obter_filhos_diretos(proc)],
                }
            )
        except (
            psutil.AccessDenied,
            psutil.NoSuchProcess,
            psutil.ZombieProcess,
            RuntimeError,
        ):
            pass

    return processos


def agrupar_processos(processos_individuais):
    grupos = {}

    for proc in processos_individuais:
        chave = chave_processo(proc["nome"])

        if chave not in grupos:
            grupos[chave] = {
                "chaveHistorico": chave,
                "pid": proc["pid"],
                "pids": [],
                "nome": proc["nomeNormalizado"],
                "usuario": proc["usuario"],
                "status": proc["status"],
                "tempoAtivoSegundos": proc["tempoAtivoSegundos"],
                "cpu": 0,
                "ram": 0,
                "ioDisco": 0,
                "subprocessos": 0,
                "filhosDiretos": [],
            }

        grupo = grupos[chave]
        grupo["pids"].append(proc["pid"])
        grupo["cpu"] += proc["cpu"]
        grupo["ram"] += proc["ram"]
        grupo["ioDisco"] += proc["ioDisco"]
        grupo["filhosDiretos"].extend(proc["filhosDiretos"])

        if proc["tempoAtivoSegundos"] > grupo["tempoAtivoSegundos"]:
            grupo["pid"] = proc["pid"]
            grupo["usuario"] = proc["usuario"]
            grupo["status"] = proc["status"]
            grupo["tempoAtivoSegundos"] = proc["tempoAtivoSegundos"]

    processos = []
    vetores = {"cpu": [], "ram": [], "ioDisco": [], "subprocessos": []}

    for grupo in grupos.values():
        grupo["cpu"] = numero(grupo["cpu"])
        grupo["ram"] = numero(grupo["ram"])
        grupo["ioDisco"] = numero(grupo["ioDisco"])
        grupo["quantidadeInstancias"] = len(grupo["pids"])
        pids_do_grupo = set(grupo["pids"])
        grupo["subprocessos"] = len(
            {pid for pid in grupo["filhosDiretos"] if pid not in pids_do_grupo}
        )

        for chave in vetores:
            vetores[chave].append(grupo[chave])
            atualizar_historico_processo(grupo["chaveHistorico"], chave, grupo[chave])

        processos.append(grupo)

    return processos, vetores


def coletar_processos():
    return agrupar_processos(capturar_processos_individuais())


def enriquecer_processos(processos, vetores):
    stats = {chave: estatisticas(vetor) for chave, vetor in vetores.items()}
    resultado = []

    for processo in processos:
        percentis = {
            chave: percentil_score(vetores[chave], processo[chave])
            for chave in vetores
        }
        score_global = score(
            percentis["cpu"],
            percentis["ram"],
            percentis["ioDisco"],
            percentis["subprocessos"],
        )
        processo_final = {
            "pid": processo["pid"],
            "pids": processo["pids"],
            "nome": processo["nome"],
            "usuario": processo["usuario"],
            "status": processo["status"],
            "tempoAtivoSegundos": processo["tempoAtivoSegundos"],
            "quantidadeInstancias": processo["quantidadeInstancias"],
            "scoreGlobal": {"valor": score_global, "nivel": nivel(score_global * 100)},
            "metricas": {},
        }
        unidades = {"cpu": "%", "ram": "MB", "ioDisco": "MB/s", "subprocessos": ""}
        chave_historico = str(processo["chaveHistorico"])

        for chave in vetores:
            processo_final["metricas"][chave] = {
                "valor": processo[chave],
                "unidade": unidades[chave],
                "percentil": percentis[chave],
                "desvioMedia": numero(processo[chave] - stats[chave]["media"]),
                "anomalia": percentis[chave] >= 90,
                "nivel": nivel(percentis[chave]),
                "historico": historico_processos[chave_historico][chave],
            }

        resultado.append(processo_final)

    resultado.sort(key=lambda x: x["scoreGlobal"]["valor"], reverse=True)

    return resultado[:PROCESSOS_MAX_CLIENT], stats


def gerar_snapshot_processos():
    processos, vetores = coletar_processos()
    processos, stats = enriquecer_processos(processos, vetores)

    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "janelaMonitoramento": {
            "intervaloCapturaSegundos": INTERVALO_CAPTURA,
            "quantidadeRegistros": PROCESSOS_MAX_HISTORICO,
            "duracaoTotalMinutos": round(
                (INTERVALO_CAPTURA * PROCESSOS_MAX_HISTORICO) / 60,
                2,
            ),
        },
        "servidor": {
            "hostname": HOSTNAME,
            "ip": socket.gethostbyname(HOSTNAME),
            "sistemaOperacional": f"{platform.system()} {platform.release()}",
            "status": "online",
            "uptimeSegundos": round(time.time() - psutil.boot_time()),
            "totalProcessos": len(processos),
        },
        "estatisticasGlobais": stats,
        "processos": processos,
    }


def salvar_snapshot_processos_local(snapshot):
    with open(ARQUIVO_SNAPSHOT_PROCESSOS, "w", encoding="utf-8") as arquivo:
        json.dump(snapshot, arquivo, ensure_ascii=False, indent=4)


# ===================== UPLOAD S3 =====================

def upload_arquivo_s3(caminho_local, key):
    if not BUCKET:
        log("UPLOAD_ERROR bucket não definido no .env", "WARNING")
        return

    s3.upload_file(caminho_local, BUCKET, key)
    log(f"UPLOAD_OK bucket={BUCKET} key={key}")


def upload_json_s3(dados, key):
    if not BUCKET:
        log("UPLOAD_ERROR bucket não definido no .env", "WARNING")
        return

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(dados, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    log(f"UPLOAD_OK bucket={BUCKET} key={key}")


def upload_pacote_s3(caminho_metricas, caminho_preditivo, snapshot_processos):
    nome_metricas = os.path.basename(caminho_metricas)
    key_metricas = f"raw/{EMPRESA}/{HOSTNAME_SAFE}/{nome_metricas}"
    key_preditivo = f"raw/{PREDITIVO_EMPRESA}/raw.csv"
    key_processos = (
        f"client/{EMPRESA}/processos/servidor/{HOSTNAME}/snapshot_{HOSTNAME}.json"
    )

    log("UPLOAD_START pacote_s3")

    if os.path.exists(caminho_metricas):
        upload_arquivo_s3(caminho_metricas, key_metricas)

    if os.path.exists(caminho_preditivo):
        upload_arquivo_s3(caminho_preditivo, key_preditivo)

    if snapshot_processos:
        upload_json_s3(snapshot_processos, key_processos)

    log("UPLOAD_END pacote_s3")


# ===================== LOOP PRINCIPAL =====================

def main():
    ler_env()
    preparar_cpu_processos()
    carregar_cache_processos()

    caminho_metricas = gerar_nome_arquivo_metricas()
    caminho_preditivo = gerar_nome_arquivo_preditivo()
    snapshot_processos = None

    contador = 0
    contador_processos = 0
    ultimo_upload = time.time()
    ultima_predicao = 0
    ultima_climatica = 0

    log(f"DEBUG_BUCKET={BUCKET}")
    log(f"START coleta empresa={EMPRESA} host={HOSTNAME}")

    while True:
        inicio = time.time()

        dados = coletar_dados()
        salvar_csv(caminho_metricas, dados)
        contador += 1

        try:
            snapshot_processos = gerar_snapshot_processos()
            salvar_snapshot_processos_local(snapshot_processos)
            salvar_cache_processos()
            contador_processos += 1
        except Exception as e:
            log(f"Processos não capturados: {e}", "WARNING")

        agora = time.time()

        if agora - ultima_predicao >= PREDITIVO_INTERVALO_SEGUNDOS:
            try:
                salvar_csv(caminho_preditivo, coletar_raw_preditivo())
                ultima_predicao = agora
            except Exception as e:
                log(f"Raw preditivo não capturado: {e}", "WARNING")

        if agora - ultima_climatica >= CLIMA_INTERVALO_SEGUNDOS:
            try:
                executar_captura_climatica()
                ultima_climatica = agora
            except Exception as e:
                log(f"Captura climática não executada: {e}", "WARNING")

        tempo_execucao = time.time() - inicio
        tempo_total = time.time() - ultimo_upload
        upload_devido = contador >= UPLOAD_MAX_COLETAS or tempo_total >= UPLOAD_INTERVALO_SEGUNDOS

        log(
            f"LOOP count={contador} processos={contador_processos} "
            f"arquivo={os.path.basename(caminho_metricas)} "
            f"execucao_s={round(tempo_execucao, 2)} "
            f"desde_upload_s={round(tempo_total, 2)}"
        )

        if upload_devido:
            try:
                upload_pacote_s3(caminho_metricas, caminho_preditivo, snapshot_processos)
            except Exception as e:
                log(f"UPLOAD_ERROR pacote_s3 erro={e}", "ERROR")

            if contador_processos >= PROCESSOS_MAX_HISTORICO:
                resetar_ciclo_processos()

            contador = 0
            contador_processos = 0
            ultimo_upload = time.time()
            caminho_metricas = gerar_nome_arquivo_metricas()

            log("NOVO_ARQUIVO iniciado")

        time.sleep(max(0, INTERVALO_CAPTURA - tempo_execucao))


if __name__ == "__main__":
    main()
