import json
import os
import platform
import socket
import time
from datetime import UTC, datetime

import psutil

try:
    import boto3
    from dotenv import load_dotenv
except ImportError:
    boto3 = None

    def load_dotenv(*args, **kwargs):
        return False


# =====================================================
# CONFIG
# =====================================================

load_dotenv()

INTERVALO = int(os.getenv("INTERVALO_PROCESSOS", "60"))
MAX_HISTORICO = int(os.getenv("MAX_HISTORICO_PROCESSOS", "60"))
MAX_PROCESSOS_CLIENT = int(os.getenv("MAX_PROCESSOS_CLIENT", "60"))

EMPRESA = os.getenv("EMPRESA_PROCESSOS") or os.getenv("EMPRESA") or "ClimaTech LTDA"
BUCKET = os.getenv("S3_BUCKET_NAME")

ARQUIVO_CACHE = "cache_historico.json"
ARQUIVO_SNAPSHOT = "snapshot_analitico.json"

NOMES_IGNORADOS = {"system idle process", "idle"}
SUFIXOS_NOME = (".exe", ".app")

historico = {}


# =====================================================
# LOG
# =====================================================


def log(mensagem, nivel="INFO"):
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{agora} | {nivel:<7} | {mensagem}")


# =====================================================
# CACHE
# =====================================================


def carregar_cache():
    global historico

    if not os.path.exists(ARQUIVO_CACHE):
        return

    with open(ARQUIVO_CACHE, "r", encoding="utf-8") as f:
        historico = json.load(f)


def salvar_cache():
    with open(ARQUIVO_CACHE, "w", encoding="utf-8") as f:
        json.dump(historico, f)


def resetar_ciclo():
    global historico

    historico = {}

    if os.path.exists(ARQUIVO_CACHE):
        os.remove(ARQUIVO_CACHE)


# =====================================================
# HELPERS
# =====================================================


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


def percentil_valor(valores, percentil):
    if not valores:
        return 0

    posicao = (len(valores) - 1) * percentil
    inferior = int(posicao)
    superior = min(inferior + 1, len(valores) - 1)
    peso = posicao - inferior

    return valores[inferior] * (1 - peso) + valores[superior] * peso


def percentil_score(valores, valor):
    valores_validos = [float(v) for v in valores if isinstance(v, (int, float))]

    if not valores_validos:
        return 0

    abaixo_ou_igual = sum(1 for item in valores_validos if item <= valor)
    return round((abaixo_ou_igual / len(valores_validos)) * 100)


def atualizar_historico(chave_historico, chave_metrica, valor):
    chave_historico = str(chave_historico)

    if chave_historico not in historico:
        historico[chave_historico] = {
            "cpu": [],
            "ram": [],
            "ioDisco": [],
            "subprocessos": [],
        }

    historico[chave_historico][chave_metrica].append(valor)

    if len(historico[chave_historico][chave_metrica]) > MAX_HISTORICO:
        historico[chave_historico][chave_metrica].pop(0)


# =====================================================
# COLETA
# =====================================================


def preparar_cpu():
    for proc in psutil.process_iter():
        try:
            proc.cpu_percent(None)
        except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess):
            pass


def capturar_processos_individuais():
    processos = []

    for proc in psutil.process_iter(
        ["pid", "name", "username", "status", "create_time"]
    ):
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
                    "filhosDiretos": [
                        filho.pid for filho in obter_filhos_diretos(proc)
                    ],
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
            atualizar_historico(grupo["chaveHistorico"], chave, grupo[chave])

        processos.append(grupo)

    return processos, vetores


def coletar_processos():
    return agrupar_processos(capturar_processos_individuais())


# =====================================================
# ENRIQUECER
# =====================================================


def enriquecer_processos(processos, vetores):
    stats = {chave: estatisticas(vetor) for chave, vetor in vetores.items()}
    resultado = []

    for p in processos:
        percentis = {
            chave: percentil_score(vetores[chave], p[chave]) for chave in vetores
        }

        score_global = score(
            percentis["cpu"],
            percentis["ram"],
            percentis["ioDisco"],
            percentis["subprocessos"],
        )

        processo_final = {
            "pid": p["pid"],
            "pids": p["pids"],
            "nome": p["nome"],
            "usuario": p["usuario"],
            "status": p["status"],
            "tempoAtivoSegundos": p["tempoAtivoSegundos"],
            "quantidadeInstancias": p["quantidadeInstancias"],
            "scoreGlobal": {"valor": score_global, "nivel": nivel(score_global * 100)},
            "metricas": {},
        }

        unidades = {"cpu": "%", "ram": "MB", "ioDisco": "MB/s", "subprocessos": ""}
        chave_historico = str(p["chaveHistorico"])

        for chave in vetores:
            processo_final["metricas"][chave] = {
                "valor": p[chave],
                "unidade": unidades[chave],
                "percentil": percentis[chave],
                "desvioMedia": numero(p[chave] - stats[chave]["media"]),
                "anomalia": percentis[chave] >= 90,
                "nivel": nivel(percentis[chave]),
                "historico": historico[chave_historico][chave],
            }

        resultado.append(processo_final)

    resultado.sort(key=lambda x: x["scoreGlobal"]["valor"], reverse=True)

    return resultado[:MAX_PROCESSOS_CLIENT], stats


# =====================================================
# S3
# =====================================================


def criar_cliente_s3():
    if boto3 is None or not BUCKET:
        return None

    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
        region_name=os.getenv("AWS_DEFAULT_REGION"),
        endpoint_url=os.getenv("S3_ENDPOINT_URL") or None,
    )


def salvar_snapshot_s3(snapshot):
    s3 = criar_cliente_s3()

    if s3 is None:
        log("S3 ignorado: bucket ou dependencias nao configurados", "WARNING")
        return

    hostname = snapshot["servidor"]["hostname"]
    key = f"client/{EMPRESA}/processos/servidor/{hostname}/snapshot_{hostname}.json"

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(snapshot, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )

    log(f"Snapshot de processos enviado para s3://{BUCKET}/{key}", "SUCCESS")


# =====================================================
# JSON FINAL
# =====================================================


def gerar_snapshot():
    processos, vetores = coletar_processos()
    processos, stats = enriquecer_processos(processos, vetores)

    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "janelaMonitoramento": {
            "intervaloCapturaSegundos": INTERVALO,
            "quantidadeRegistros": MAX_HISTORICO,
            "duracaoTotalMinutos": round((INTERVALO * MAX_HISTORICO) / 60, 2),
        },
        "servidor": {
            "hostname": socket.gethostname(),
            "ip": socket.gethostbyname(socket.gethostname()),
            "sistemaOperacional": f"{platform.system()} {platform.release()}",
            "status": "online",
            "uptimeSegundos": round(time.time() - psutil.boot_time()),
            "totalProcessos": len(processos),
        },
        "estatisticasGlobais": stats,
        "processos": processos,
    }


# =====================================================
# ORQUESTRACAO
# =====================================================


def salvar_snapshot_local(snapshot):
    with open(ARQUIVO_SNAPSHOT, "w", encoding="utf-8") as f:
        json.dump(
            snapshot,
            f,
            ensure_ascii=False,
            indent=4,
        )


def salvar_amostra(snapshot):
    pasta_destino = os.path.join(os.getcwd(), "amostras")
    os.makedirs(pasta_destino, exist_ok=True)

    caminho_arquivo = os.path.join(pasta_destino, f"snapshot_{int(time.time())}.json")

    with open(caminho_arquivo, "w", encoding="utf-8") as arquivo:
        json.dump(
            snapshot,
            arquivo,
            ensure_ascii=False,
            indent=4,
        )

    log(f"Amostra salva em: {caminho_arquivo}")


def main():
    contador = 0

    preparar_cpu()
    carregar_cache()

    while True:
        inicio = time.time()
        snapshot = gerar_snapshot()

        salvar_snapshot_local(snapshot)
        salvar_cache()

        contador += 1
        log(
            f"Captura {contador}/{MAX_HISTORICO} | "
            f"processos exibidos={len(snapshot['processos'])}"
        )

        if contador >= MAX_HISTORICO:
            log("Snapshot pronto para front/S3", "SUCCESS")
            salvar_amostra(snapshot)
            salvar_snapshot_s3(snapshot)

            contador = 0
            resetar_ciclo()

        tempo_execucao = time.time() - inicio
        time.sleep(max(0, INTERVALO - tempo_execucao))


if __name__ == "__main__":
    main()
