import boto3
import pandas as pd
import os
import json
from dotenv import load_dotenv
from io import BytesIO
from datetime import date, datetime, timedelta, timezone

try:
    import numpy as np
except ImportError:
    np = None

try:
    import mysql.connector as mysql_connector
except ImportError:
    mysql_connector = None

# =========================
# CONFIGURAÃ‡ÃƒO
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

COLUNAS_RAW_OBRIGATORIAS = {
    "datahora", "identificador", "hostname",
    "cpu_perc", "cpu_freq",
    "ram_perc", "ram_usada", "ram_livre",
    "disco_perc", "disco_usado", "disco_livre", "disco_total",
    "disco_throughput",
    "upload", "download",
    "conexoes", "total_processos"
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
# EXTRAÃ‡ÃƒO (S3)
# =========================

def listar_empresas_s3():
    """
    Lista as empresas com base nos diretÃ³rios dentro de /raw/
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
    Faz download dos CSVs da empresa e concatena em um Ãºnico DataFrame
    """
    arquivos = listar_arquivos_empresa_s3(empresa)
    dataframes = []

    for key in arquivos:
        log(f"Lendo S3: {key}")

        obj = s3.get_object(Bucket=NOME_BUCKET, Key=key)
        df = pd.read_csv(BytesIO(obj["Body"].read()))

        colunas_faltantes = COLUNAS_RAW_OBRIGATORIAS - set(df.columns)
        if colunas_faltantes:
            log(
                f"Ignorando S3: {key} (schema incompatÃ­vel: {sorted(colunas_faltantes)})",
                "WARNING"
            )
            continue

        df["empresa"] = empresa
        dataframes.append(df)

    if dataframes:
        return pd.concat(dataframes, ignore_index=True)

    return None


# =========================
# TRANSFORMAÃ‡ÃƒO
# =========================

def transformar_dados(df):
    """
    - Converte tipos
    - Cria mÃ©tricas derivadas
    - Renomeia colunas
    - Define schema final (trusted)
    """
    BYTES_PARA_GB = 1024 ** 3

    # -------------------------
    # ConversÃ£o de tipos
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
    # MÃ©tricas derivadas
    # -------------------------
    df["ram_usada_gb"] = (df["ram_usada"] / BYTES_PARA_GB).round(2)
    df["ram_livre_gb"] = (df["ram_livre"] / BYTES_PARA_GB).round(2)
    df["ram_total_gb"] = df["ram_usada_gb"] + df["ram_livre_gb"]

    df["disco_usado_gb"] = (df["disco_usado"] / BYTES_PARA_GB).round(2)
    df["disco_livre_gb"] = (df["disco_livre"] / BYTES_PARA_GB).round(2)
    df["disco_total_gb"] = (df["disco_total"] / BYTES_PARA_GB).round(2)

    df["rede_total_mb_s"] = (df["upload"] + df["download"]).round(2)

    # -------------------------
    # RenomeaÃ§Ã£o de colunas
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
    # SeleÃ§Ã£o final de colunas
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

    # ConstrÃ³i JSON no formato de listas (time-series)

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
# DASHBOARD (INCIDENTES + MÃ‰TRICAS)
# =========================

def obter_periodo_mes_atual():
    import calendar

    hoje = date.today()
    mes = hoje.month
    ano = hoje.year
    total_dias = calendar.monthrange(ano, mes)[1]

    return {
        "mes": mes,
        "ano": ano,
        "total_dias": total_dias,
        "data_inicio": date(ano, mes, 1),
        "data_fim": date(ano, mes, total_dias)
    }


def processar_incidente(df_empresa, data_inicio=None, data_fim=None):
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
        data_registro = ts.date()

        if data_inicio and data_fim and not (data_inicio <= data_registro <= data_fim):
            continue

        data_str = data_registro.strftime('%Y-%m-%d')

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
            if componente in sv["Componentes"]:
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


def processar_metricas(df_empresa, data_inicio=None, data_fim=None):
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
        data_registro = ts.date()

        if data_inicio and data_fim and not (data_inicio <= data_registro <= data_fim):
            continue

        data_str = data_registro.strftime('%Y-%m-%d')

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
    periodo = obter_periodo_mes_atual()

    output = {
        "empresa": empresa,
        "gerado_em": datetime.now(timezone.utc).isoformat(),
        "mes_referencia": {"mes": periodo["mes"], "ano": periodo["ano"]},
        "total_dias_mes": periodo["total_dias"],
        "incidentes": incidentes,
        "metricas": metricas
    }

    key = f"client/{empresa}/dashboard/client_{periodo['mes']:02d}_{periodo['ano']}.json"

    s3.put_object(
        Bucket=NOME_BUCKET,
        Key=key,
        Body=json.dumps(output, ensure_ascii=False, indent=2),
        ContentType='application/json'
    )

    log(f"[{empresa}] Dashboard salvo no S3")


# =========================
# ANALISE PREDITIVA - LAMBDA
# =========================

COLUNAS_PREDITIVAS_OBRIGATORIAS = {"timestamp", "cpu", "temperatura"}


def verificar_dependencias_preditivas():
    if np is None:
        raise RuntimeError("DependÃªncia ausente para anÃ¡lise preditiva: numpy")


def carregar_previsoes_preditivas_s3(empresa):
    key = f"previsoes/{empresa}.json"

    try:
        obj = s3.get_object(Bucket=NOME_BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception:
        return []


def salvar_previsoes_preditivas_s3(previsoes, empresa):
    key = f"previsoes/{empresa}.json"

    s3.put_object(
        Bucket=NOME_BUCKET,
        Key=key,
        Body=json.dumps(previsoes, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json"
    )


def carregar_raw_preditivo_s3(empresa):
    key = f"raw/{empresa}/raw.csv"

    obj = s3.get_object(Bucket=NOME_BUCKET, Key=key)
    df = pd.read_csv(BytesIO(obj["Body"].read()))

    colunas_faltantes = COLUNAS_PREDITIVAS_OBRIGATORIAS - set(df.columns)
    if colunas_faltantes:
        raise ValueError(
            f"Raw preditivo com schema incompatÃ­vel em {key}: "
            f"faltando {sorted(colunas_faltantes)}"
        )

    return df


def salvar_client_preditivo_s3(dados, empresa):
    key = f"client/{empresa}.json"

    s3.put_object(
        Bucket=NOME_BUCKET,
        Key=key,
        Body=json.dumps(dados, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json"
    )


def calcular_regressao_preditiva(historico):
    verificar_dependencias_preditivas()

    if len(historico) < 2:
        return 0, 0

    x = np.array([d["cpu"] for d in historico])
    y = np.array([d["temperatura"] for d in historico])
    a, b = np.polyfit(x, y, 1)

    return float(a), float(b)


def calcular_regressao_tempo_preditiva(historico):
    verificar_dependencias_preditivas()

    if len(historico) < 10:
        return None, None, None

    janela = historico[-30:]

    x = np.arange(len(janela))
    y = np.array([d["temperatura"] for d in janela])
    a, b = np.polyfit(x, y, 1)

    return float(a), float(b), len(janela)


def buscar_temperatura_real_preditiva(historico, timestamp_alvo):
    for leitura in reversed(historico):
        if leitura["timestamp"] == timestamp_alvo:
            return leitura["temperatura"]

    return None


def atualizar_previsoes_preditivas(previsoes, historico):
    agora = datetime.now()

    for prev in previsoes:
        if prev["temperaturaReal"] is not None:
            continue

        horario_prev = datetime.strptime(prev["timestamp"], "%H:%M")
        horario_atual = datetime.strptime(agora.strftime("%H:%M"), "%H:%M")

        if horario_atual >= horario_prev:
            temp_real = buscar_temperatura_real_preditiva(historico, prev["timestamp"])

            if temp_real is not None:
                prev["temperaturaReal"] = temp_real

    return previsoes


def transformar_preditivo(df, previsoes, empresa):
    verificar_dependencias_preditivas()

    df = df.copy()
    df["cpu"] = pd.to_numeric(df["cpu"], errors="coerce")
    df["temperatura"] = pd.to_numeric(df["temperatura"], errors="coerce")
    df = df.dropna(subset=["timestamp", "cpu", "temperatura"])

    historico = df.tail(100).to_dict(orient="records")

    if not historico:
        raise ValueError("Sem histÃ³rico vÃ¡lido para anÃ¡lise preditiva")

    a, b = calcular_regressao_preditiva(historico)

    cpu_p90 = round(float(np.percentile(df["cpu"], 90)), 1)
    temperatura_atual = float(historico[-1]["temperatura"])

    a_tempo, b_tempo, n_janela = calcular_regressao_tempo_preditiva(historico)
    passos_1h = 6

    if a_tempo is not None:
        indice_futuro = (n_janela - 1) + passos_1h
        temperatura_prevista = round(float(a_tempo * indice_futuro + b_tempo), 1)
    else:
        temperatura_prevista = round(temperatura_atual, 1)

    timestamp_1h = (datetime.now() + timedelta(hours=1)).strftime("%H:%M")

    if timestamp_1h not in [p["timestamp"] for p in previsoes]:
        previsoes.append({
            "timestamp": timestamp_1h,
            "temperaturaPrevista": temperatura_prevista,
            "temperaturaReal": None
        })

    previsoes = atualizar_previsoes_preditivas(previsoes, historico)[-144:]
    impacto_por_10pct = round(a * 10, 2)

    regressao = []

    for x in range(0, 101, 10):
        regressao.append({
            "x": x,
            "y": round((a * x) + b, 1)
        })

    status = (
        "CRITICO"
        if temperatura_atual >= 85
        else "ALERTA"
        if temperatura_atual >= 70
        else "NORMAL"
    )

    return {
        "kpis": {
            "fkEmpresa": empresa,
            "cpuP90": cpu_p90,
            "temperaturaAtual": temperatura_atual,
            "temperaturaPrevista": temperatura_prevista,
            "status": status,
            "impactoCpuPor10pct": impacto_por_10pct
        },
        "hosts": [
            {
                "host_id": os.getenv("PREDITIVO_HOST_ID", "Anderson"),
                "hostname": os.getenv("PREDITIVO_HOSTNAME", "laptop-ANDERSON"),
                "grafico": historico,
                "previsoes": previsoes,
                "regressao": regressao
            }
        ]
    }, previsoes


def executar_analise_preditiva(empresa):
    previsoes = carregar_previsoes_preditivas_s3(empresa)
    df = carregar_raw_preditivo_s3(empresa)

    dados, previsoes = transformar_preditivo(df, previsoes, empresa)

    salvar_client_preditivo_s3(dados, empresa)
    salvar_previsoes_preditivas_s3(previsoes, empresa)

    log(f"[{empresa}] AnÃ¡lise preditiva salva no S3")


# =========================
# ANALISE DE CARGA CLIMATICA (MYSQL)
# =========================

def ler_env_mysql(caminho=".env.dev"):
    """
    Reutiliza variaveis de ambiente do projeto quando o arquivo existir.
    """
    caminhos_possiveis = [
        caminho,
        os.path.join(os.getcwd(), caminho),
        os.path.join(os.path.dirname(os.getcwd()), caminho)
    ]

    for caminho_env in caminhos_possiveis:
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
                    valor.strip().strip("'").strip('"')
                )

        return caminho_env

    return None


def obter_env_obrigatoria(nome):
    valor = os.getenv(nome)

    if valor is None or valor == "":
        raise RuntimeError(f"Configure a variavel de ambiente obrigatoria {nome}.")

    return valor


def conectar_mysql_carga_climatica():
    if mysql_connector is None:
        raise RuntimeError("Dependencia ausente para MySQL: mysql-connector-python")

    return mysql_connector.connect(
        host=obter_env_obrigatoria("DB_HOST"),
        database=obter_env_obrigatoria("DB_DATABASE"),
        user=obter_env_obrigatoria("DB_USER"),
        password=os.getenv("DB_PASSWORD", ""),
        port=int(obter_env_obrigatoria("DB_PORT")),
    )


def classificar_pressao(requisicoes, faixa_esperada):
    limite = requisicoes / faixa_esperada

    if limite <= 1:
        return "baixa", "baixo"

    if limite <= 1.2:
        return "moderada", "medio"

    if limite <= 1.5:
        return "alta", "alto"

    return "critica", "critico"


def atualizar_linhas_carga_climatica():
    """
    Calcula faixa, pressao e risco para linhas existentes no MySQL.
    """
    faixa_padrao = int(obter_env_obrigatoria("ATMOS_FAIXA_ESPERADA"))
    conexao = conectar_mysql_carga_climatica()
    cursor = conexao.cursor(dictionary=True)

    cursor.execute("""
        SELECT id, requisicoes_minuto, faixa_esperada
        FROM analise_carga_climatica
        ORDER BY data_hora
    """)

    linhas = cursor.fetchall()

    for linha in linhas:
        faixa = int(linha["faixa_esperada"] or faixa_padrao)
        pressao, risco = classificar_pressao(int(linha["requisicoes_minuto"]), faixa)

        cursor.execute("""
            UPDATE analise_carga_climatica
            SET faixa_esperada = %s,
                pressao_operacional = %s,
                nivel_risco = %s
            WHERE id = %s
        """, (faixa, pressao, risco, linha["id"]))

    conexao.commit()
    cursor.close()
    conexao.close()

    return len(linhas)


def gerar_resumo_carga_climatica():
    """
    Retorna os principais indicadores calculados pelo ETL de carga climatica.
    """
    conexao = conectar_mysql_carga_climatica()
    cursor = conexao.cursor(dictionary=True)

    cursor.execute("""
        SELECT data_hora, requisicoes_minuto, tipo_evento
        FROM analise_carga_climatica
        ORDER BY requisicoes_minuto DESC
        LIMIT 1
    """)
    pico = cursor.fetchone()

    cursor.execute("""
        SELECT tipo_evento, AVG(requisicoes_minuto - faixa_esperada) AS pressao_media
        FROM analise_carga_climatica
        WHERE tipo_evento <> 'sem evento relevante'
        GROUP BY tipo_evento
        ORDER BY pressao_media DESC
        LIMIT 1
    """)
    evento_pressao = cursor.fetchone()

    cursor.execute("""
        SELECT HOUR(data_hora) AS hora, COUNT(*) AS ocorrencias
        FROM analise_carga_climatica
        WHERE requisicoes_minuto > faixa_esperada
        GROUP BY HOUR(data_hora)
        ORDER BY ocorrencias DESC, hora
        LIMIT 1
    """)
    horario = cursor.fetchone()

    cursor.close()
    conexao.close()

    return {
        "maior_pico": pico,
        "evento_maior_pressao": evento_pressao,
        "horario_critico_recorrente": horario
    }


def executar_carga_climatica_mysql():
    ler_env_mysql()

    total = atualizar_linhas_carga_climatica()
    resumo = gerar_resumo_carga_climatica()

    log(f"Carga climatica MySQL finalizada. Linhas processadas: {total}")
    log(f"Carga climatica - maior pico: {resumo['maior_pico']}", "DEBUG")
    log(f"Carga climatica - evento de maior pressao: {resumo['evento_maior_pressao']}", "DEBUG")
    log(f"Carga climatica - horario critico: {resumo['horario_critico_recorrente']}", "DEBUG")

    return {
        "linhas_processadas": total,
        "resumo": resumo
    }


# =========================
# ORQUESTRADOR
# =========================

def executar_etl():
    """
    Pipeline completa:
    Extract â†’ Transform â†’ Load
    """
    log("ETL_INICIADA")

    try:
        executar_carga_climatica_mysql()
    except Exception as e:
        log(f"Análise de carga climática MySQL não executada: {e}", "WARNING")

    empresas = listar_empresas_s3()
    log(f"Empresas encontradas: {empresas}")

    for empresa in empresas:
        log(f"[{empresa}] INICIANDO")

        try:
            # EXTRACT
            df_raw = extrair_dados_empresa(empresa)

            if df_raw is None or df_raw.empty:
                log(f"[{empresa}] Sem dados", "WARNING")
                try:
                    executar_analise_preditiva(empresa)
                except Exception as e:
                    log(f"[{empresa}] Analise    preditiva não executada: {e}", "WARNING")
                continue

            # TRANSFORM
            df_trusted = transformar_dados(df_raw)

            # CLIENT
            json_client = construir_json_client(df_trusted, empresa)

            # LOAD
            salvar_trusted_s3(df_trusted, empresa)
            salvar_client_s3(json_client, empresa)

            # DASHBOARD GESTOR INCIDENTES
            periodo = obter_periodo_mes_atual()
            incidentes = processar_incidente(df_raw, periodo["data_inicio"], periodo["data_fim"])
            metricas = processar_metricas(df_raw, periodo["data_inicio"], periodo["data_fim"])
            salvar_dashboard_s3(incidentes, metricas, empresa)

            try:
                executar_analise_preditiva(empresa)
            except Exception as e:
                log(f"[{empresa}] Analise preditiva não executada: {e}", "WARNING")

            log(f"[{empresa}] FINALIZADO")

        except Exception as e:
            log(f"[{empresa}] ERRO: {e}", "ERROR")

    log("ETL_FINALIZADA")


# =========================
# ENTRYPOINT
# =========================
if __name__ == "__main__":
    executar_etl()
