import boto3
import pandas as pd
import os
import json
from dotenv import load_dotenv
from io import BytesIO
from datetime import datetime, timezone

# =========================
# CONFIGURAÇÃO
# =========================

load_dotenv()

NOME_BUCKET = os.getenv("S3_BUCKET_NAME")

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_DEFAULT_REGION"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
    endpoint_url=os.getenv("S3_ENDPOINT_URL") or None
)

# =========================
# LIMITES DE CPU, RAM E DISCO P CAPTURA DE INCIDENTES
# =========================

LIMITES = {
      "cpu_perc": 90,
      "ram_perc": 80,
      "disco_perc": 85
  }


# =========================
# UTIL - LOG PADRONIZADO
# =========================

def log(mensagem, nivel="INFO"):
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    niveis = {
        "INFO":    {"cor": "\033[36m"},  # ciano
        "SUCCESS": {"cor": "\033[32m"},  # verde
        "WARNING": {"cor": "\033[33m"},  # amarelo
        "ERROR":   {"cor": "\033[31m"},  # vermelho
        "DEBUG":   {"cor": "\033[35m"}   # magenta
    }

    reset = "\033[0m"
    cor = niveis.get(nivel.upper(), niveis["INFO"])["cor"]

    print(
        f"{cor}"
        f"{agora} | {nivel.upper():<7} | {mensagem}"
        f"{reset}"
    )


# =========================
# EXTRAÇÃO (S3)
# =========================

def listar_empresas_s3():
    """
    Lista as empresas com base nos diretórios dentro de /raw/
    """
    resposta = s3.list_objects_v2(
        Bucket=NOME_BUCKET,
        Prefix="raw/",
        Delimiter="/"
    )

    empresas = [
        prefix["Prefix"].split("/")[1]
        for prefix in resposta.get("CommonPrefixes", [])
    ]

    return empresas


def listar_arquivos_empresa_s3(empresa):
    """
    Lista todos os arquivos CSV de uma empresa no S3
    """
    prefixo = f"raw/{empresa}/"

    resposta = s3.list_objects_v2(
        Bucket=NOME_BUCKET,
        Prefix=prefixo
    )

    return [
        obj["Key"]
        for obj in resposta.get("Contents", [])
        if obj["Key"].endswith(".csv")
    ]


def extrair_dados_empresa(empresa):
    """
    Faz download dos CSVs da empresa e concatena em um único DataFrame
    """
    arquivos = listar_arquivos_empresa_s3(empresa)
    dataframes = []

    for key in arquivos:
        log(f"Lendo S3: {key}")

        obj = s3.get_object(Bucket=NOME_BUCKET, Key=key)
        df = pd.read_csv(BytesIO(obj["Body"].read()))

        df["empresa"] = empresa
        dataframes.append(df)

    if dataframes:
        return pd.concat(dataframes, ignore_index=True)

    return None


# =========================
# TRANSFORMAÇÃO
# =========================

def transformar_dados(df):
    """
    - Converte tipos
    - Cria métricas derivadas
    - Renomeia colunas
    - Define schema final (trusted)
    """
    BYTES_PARA_GB = 1024 ** 3

    # -------------------------
    # Conversão de tipos
    # -------------------------
    colunas_numericas = [
        "cpu_perc", "cpu_freq",
        "ram_perc", "ram_usada", "ram_livre",
        "disco_perc", "disco_usado", "disco_livre", "disco_total",
        "disco_throughput",
        "upload", "download",
        "conexoes", "total_processos"
    ]

    for col in colunas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # -------------------------
    # Métricas derivadas
    # -------------------------
    df["ram_usada_gb"] = (df["ram_usada"] / BYTES_PARA_GB).round(2)
    df["ram_livre_gb"] = (df["ram_livre"] / BYTES_PARA_GB).round(2)
    df["ram_total_gb"] = df["ram_usada_gb"] + df["ram_livre_gb"]

    df["disco_usado_gb"] = (df["disco_usado"] / BYTES_PARA_GB).round(2)
    df["disco_livre_gb"] = (df["disco_livre"] / BYTES_PARA_GB).round(2)
    df["disco_total_gb"] = (df["disco_total"] / BYTES_PARA_GB).round(2)

    df["rede_total_mb_s"] = (df["upload"] + df["download"]).round(2)

    # -------------------------
    # Renomeação de colunas
    # -------------------------
    df = df.rename(columns={
        "datahora": "timestamp",
        "cpu_perc": "cpu_percent",
        "cpu_freq": "cpu_freq_mhz",
        "ram_perc": "ram_percent",
        "disco_perc": "disco_percent",
        "disco_throughput": "disco_throughput_mb_s",
        "upload": "rede_upload_mb_s",
        "download": "rede_download_mb_s",
        "conexoes": "rede_conexoes",
        "total_processos": "processos",
        "identificador": "host_id"
    })

    # -------------------------
    # Seleção final de colunas
    # -------------------------
    colunas_finais = [
        "timestamp", "host_id", "hostname", "empresa",
        "cpu_percent", "cpu_freq_mhz",
        "ram_percent", "ram_usada_gb", "ram_livre_gb", "ram_total_gb",
        "disco_percent", "disco_usado_gb", "disco_livre_gb", "disco_total_gb",
        "disco_throughput_mb_s",
        "rede_upload_mb_s", "rede_download_mb_s", "rede_total_mb_s",
        "rede_conexoes", "processos"
    ]

    return df[colunas_finais]


# =========================
# CLIENT (JSON FINAL)
# =========================

def construir_json_client(df, empresa):

    # Constrói JSON no formato de listas (time-series)

    log(f"[{empresa}] Construindo JSON client")

    df = df.copy()
    df["timestamp"] = df["timestamp"].astype(str)

    resumo = {
        "cpu_media": round(df["cpu_percent"].mean(), 2),
        "cpu_pico": round(df["cpu_percent"].max(), 2),
        "ram_media": round(df["ram_percent"].mean(), 2),
        "hosts_ativos": int(df["host_id"].nunique()),
        "total_registros": int(len(df))

    }

    hosts = []

    for host_id, group in df.groupby("host_id"):
        group = group.sort_values("timestamp").tail(100)

        host_data = {
            "host_id": host_id,
            "hostname": group.iloc[-1].get("hostname"),

            "metricas": {
                "cpu_percent": group["cpu_percent"].tolist(),
                "cpu_freq": group["cpu_freq_mhz"].tolist(),
                "cpu_percentil_maquina": round(group["cpu_percent"].quantile(0.99), 2),
                "cpu_pico": [
                    group["cpu_percent"].max(),
                    group["cpu_percent"].min()
                ],

                "ram_perc": group["ram_percent"].tolist(),
                "ram_usada": group["ram_usada_gb"].tolist(),
                "ram_livre": group["ram_livre_gb"].tolist(),
                "ram_percentil_maquina": round(group["ram_percent"].quantile(0.90), 2),
                "ram_pico": [
                    group["ram_percent"].max(),
                    group["ram_percent"].min()
                ],

                "disco_porcentagem": group["disco_percent"].tolist(),
                "disco_usado": group["disco_usado_gb"].tolist(),
                "disco_livre": group["disco_livre_gb"].tolist(),
                "disco_total": group["disco_total_gb"].tolist(),
                "disco_throughput": group["disco_throughput_mb_s"].tolist(),
                "disco_percentil_maquina": round(group["disco_throughput_mb_s"].quantile(0.95), 2 ),
                "diskIO_pico": [
                    group["disco_throughput_mb_s"].max(),
                    group["disco_throughput_mb_s"].min()
                ],

                "rede_upload": group["rede_upload_mb_s"].tolist(),
                "rede_download": group["rede_download_mb_s"].tolist(),
                "rede_network_total": group["rede_total_mb_s"].tolist(),
                "rede_conexoes": group["rede_conexoes"].tolist(),
                "rede_percentil": round(group["rede_total_mb_s"].quantile(0.90), 2),
                "rede_pico": [
                    group["rede_total_mb_s"].max(),
                    group["rede_total_mb_s"].min()
                ],

                "processos": group["processos"].tolist(),
                "processos_percentil": round(group["processos"].quantile(0.90), 2),
                "processos_pico": [
                    int(group["processos"].max()),
                    int(group["processos"].min())
                ],
                "datahora": group["timestamp"].tolist()
            }
        }

        hosts.append(host_data)

    return {
        "empresa": empresa,
        "gerado_em": datetime.now(timezone.utc).isoformat(),
        "resumo": resumo,
        "hosts": hosts
    }


# =========================
# LOAD (S3)
# =========================

def salvar_trusted_s3(df, empresa):
    """
    Salva CSV tratado na camada trusted
    """
    buffer = BytesIO()
    df.to_csv(buffer, index=False)

    key = f"trusted/{empresa}/trusted_{empresa}.csv"

    s3.put_object(
        Bucket=NOME_BUCKET,
        Key=key,
        Body=buffer.getvalue()
    )

    log(f"[{empresa}] Trusted salvo no S3")


def salvar_client_s3(json_data, empresa):
    """
    Salva JSON final (client)
    """
    key = f"client/{empresa}/client.json"
    # # salvar local para teste =================================

    # with open('Json_s3.json', 'w', encoding="utf-8") as a:
    #     json.dump(json_data, a, indent=4, ensure_ascii=False)

    # ===========================================================
    s3.put_object(
        Bucket=NOME_BUCKET,
        Key=key,
        Body=json.dumps(json_data).encode("utf-8"),
        ContentType="application/json"
    )

    log(f"[{empresa}] Client salvo no S3")


# =========================
# DASHBOARD (INCIDENTES + MÉTRICAS)
# =========================

def processar_incidente(df_empresa):
    incidentes = {}

    for _, linha in df_empresa.iterrows():
        try:
            ts = pd.to_datetime(linha['datahora'])
            cpu = float(linha['cpu_perc'])
            ram = float(linha['ram_perc'])
            disco = float(linha['disco_perc'])
        except:
            continue

        servidor = str(linha.get('hostname', ''))
        data_str = ts.strftime('%Y-%m-%d')

        alertas = []
        if cpu > LIMITES['cpu_perc']: alertas.append(('cpu', cpu))
        if ram > LIMITES['ram_perc']: alertas.append(('ram', ram))
        if disco > LIMITES['disco_perc']: alertas.append(('disco', disco))

        if not alertas:
            continue

        if servidor not in incidentes:
            incidentes[servidor] = {
                "TotalIncidentes": {"total_mes": 0, "por_dia": {}},
                "DataIncidente": [],
                "Componentes": {"cpu": [], "ram": [], "disco": []},
                "detalhes": []
            }

        sv = incidentes[servidor]

        for componente, valor in alertas:
            sv["DataIncidente"].append(data_str)
            sv["TotalIncidentes"]["total_mes"] += 1
            sv["TotalIncidentes"]["por_dia"][data_str] = (
                sv["TotalIncidentes"]["por_dia"].get(data_str, 0) + 1
            )
            sv["Componentes"][componente].append(data_str)
            sv["detalhes"].append({
                "data": data_str,
                "servidor": servidor,
                "componente": componente,
                "valor": round(valor, 2),
                "limite": LIMITES[f"{componente}_perc"],
                "descricao": f"{componente.upper()} atingiu {round(valor, 2)}% (limite: {LIMITES[f'{componente}_perc']}%)"
            })

    return incidentes


def processar_metricas(df_empresa):
    metricas = {}

    if 'tipo_incidente' in df_empresa.columns:
        df_met = df_empresa[df_empresa['tipo_incidente'].isna()]
    else:
        df_met = df_empresa

    for _, linha in df_met.iterrows():
        try:
            ts = pd.to_datetime(linha['datahora'])
            cpu = float(linha['cpu_perc'])
            ram = float(linha['ram_perc'])
            disco = float(linha['disco_perc'])
        except:
            continue

        servidor = str(linha.get('hostname', ''))
        data_str = ts.strftime('%Y-%m-%d')

        if servidor not in metricas:
            metricas[servidor] = {
                "pico_mes": {"cpu": 0, "ram": 0, "disco": 0},
                "dias": {}
            }

        sv = metricas[servidor]

        if data_str not in sv["dias"]:
            sv["dias"][data_str] = {"cpu_pico": 0, "ram_pico": 0, "disco_pico": 0}

        dia = sv["dias"][data_str]

        if cpu > dia["cpu_pico"]: dia["cpu_pico"] = round(cpu, 2)
        if ram > dia["ram_pico"]: dia["ram_pico"] = round(ram, 2)
        if disco > dia["disco_pico"]: dia["disco_pico"] = round(disco, 2)

        if cpu > sv["pico_mes"]["cpu"]: sv["pico_mes"]["cpu"] = round(cpu, 2)
        if ram > sv["pico_mes"]["ram"]: sv["pico_mes"]["ram"] = round(ram, 2)
        if disco > sv["pico_mes"]["disco"]: sv["pico_mes"]["disco"] = round(disco, 2)

    return metricas


def salvar_dashboard_s3(incidentes, metricas, empresa):
    import calendar
    from datetime import date

    hoje = date.today()
    mes = hoje.month
    ano = hoje.year
    total_dias = calendar.monthrange(ano, mes)[1]

    output = {
        "empresa": empresa,
        "gerado_em": datetime.now(timezone.utc).isoformat(),
        "mes_referencia": {"mes": mes, "ano": ano},
        "total_dias_mes": total_dias,
        "incidentes": incidentes,
        "metricas": metricas
    }

    key = f"client/{empresa}/dashboard/client_{mes:02d}_{ano}.json"

    s3.put_object(
        Bucket=NOME_BUCKET,
        Key=key,
        Body=json.dumps(output, ensure_ascii=False, indent=2),
        ContentType='application/json'
    )

    log(f"[{empresa}] Dashboard salvo no S3")


# =========================
# ORQUESTRADOR
# =========================

def executar_etl():
    """
    Pipeline completa:
    Extract → Transform → Load
    """
    log("ETL_INICIADA")

    empresas = listar_empresas_s3()
    log(f"Empresas encontradas: {empresas}")

    for empresa in empresas:
        log(f"[{empresa}] INICIANDO")

        try:
            # EXTRACT
            df_raw = extrair_dados_empresa(empresa)

            if df_raw is None or df_raw.empty:
                log(f"[{empresa}] Sem dados", "WARNING")
                continue

            # TRANSFORM
            df_trusted = transformar_dados(df_raw)

            # CLIENT
            json_client = construir_json_client(df_trusted, empresa)

            # LOAD
            salvar_trusted_s3(df_trusted, empresa)
            salvar_client_s3(json_client, empresa)

            # DASHBOARD GESTOR INCIDENTES
            incidentes = processar_incidente(df_raw, empresa)
            metricas = processar_metricas(df_raw)
            salvar_dashboard_s3(incidentes, metricas, empresa)

            log(f"[{empresa}] FINALIZADO")

        except Exception as e:
            log(f"[{empresa}] ERRO: {e}", "ERROR")

    log("ETL_FINALIZADA")


# =========================
# ENTRYPOINT
# =========================
if __name__ == "__main__":
    executar_etl()
