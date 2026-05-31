import os
import time
import random
import psutil
import json
import boto3
import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv(r"D:/Luana/Desktop/ATMOS_ETL/atmos-watch-v2/.env")

BUCKET = os.getenv("S3_BUCKET_NAME")

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
    region_name=os.getenv("AWS_DEFAULT_REGION")
)

FKEMPRESA = 1
tempoCaptura = 10 * 60 


def carregar_previsoes_s3():
    key = f"previsoes/{FKEMPRESA}.json"
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except:
        return []


def salvar_previsoes_s3(previsoes):
    key = f"previsoes/{FKEMPRESA}.json"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(previsoes, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json"
    )


def salvar_raw_s3(leitura):
    key = f"raw/{FKEMPRESA}/raw.csv"
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        conteudo = obj["Body"].read().decode("utf-8")
    except:
        conteudo = "timestamp,cpu,temperatura\n"

    conteudo += (
        f"{leitura['timestamp']},"
        f"{leitura['cpu']},"
        f"{leitura['temperatura']}\n"
    )

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=conteudo.encode("utf-8")
    )


def carregar_raw_s3():
    key = f"raw/{FKEMPRESA}/raw.csv"
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_csv(BytesIO(obj["Body"].read()))


def salvar_client_s3(dados):
    key = f"client/{FKEMPRESA}.json"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(dados, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json"
    )


def raw():
    cpu = psutil.cpu_percent(interval=1)
    variacao = random.uniform(-1, 1)
    temperatura = 45 + (cpu * 0.5) + variacao
    return {
        "timestamp": datetime.now().strftime("%H:%M"),
        "cpu": round(cpu, 1),
        "temperatura": round(temperatura, 1)
    }


def calcular_regressao(historico):
    if len(historico) < 2:
        return 0, 0

    x = np.array([d["cpu"] for d in historico])
    y = np.array([d["temperatura"] for d in historico])
    a, b = np.polyfit(x, y, 1)
    return float(a), float(b)


def calcular_regressao_tempo(historico):

    if len(historico) < 10:
        return None, None, None

    janela = historico[-30:]
    n_janela = len(janela)

    x = np.arange(n_janela)
    y = np.array([d["temperatura"] for d in janela])
    a, b = np.polyfit(x, y, 1)

    return float(a), float(b), n_janela


def buscar_temperatura_real(historico, timestamp_alvo):
    for leitura in reversed(historico):
        if leitura["timestamp"] == timestamp_alvo:
            return leitura["temperatura"]
    return None


def atualizar_previsoes(previsoes, historico):
    agora = datetime.now()

    for prev in previsoes:
        if prev["temperaturaReal"] is not None:
            continue

        horario_prev = datetime.strptime(prev["timestamp"], "%H:%M")
        horario_atual = datetime.strptime(agora.strftime("%H:%M"), "%H:%M")

        if horario_atual >= horario_prev:
            temp_real = buscar_temperatura_real(historico, prev["timestamp"])
            if temp_real is not None:
                prev["temperaturaReal"] = temp_real

    return previsoes


def trusted(df, previsoes):
    historico = df.tail(100).to_dict(orient="records")

    a, b = calcular_regressao(historico)

    cpu_p90 = round(float(np.percentile(df["cpu"], 90)), 1)

    temperatura_atual = float(historico[-1]["temperatura"])

    a_tempo, b_tempo, n_janela = calcular_regressao_tempo(historico)

    PASSOS_1H = 6 

    if a_tempo is not None:
      
        indice_futuro = (n_janela - 1) + PASSOS_1H
        temperatura_prevista = round(float(a_tempo * indice_futuro + b_tempo), 1)

        temperatura_prevista = max(25.0, min(temperatura_prevista, 120.0))


        if abs(a_tempo) < 0.05:
            temps_recentes = [d["temperatura"] for d in historico[-10:]]
            temperatura_prevista = round(float(np.mean(temps_recentes)), 1)

    else:
        temperatura_prevista = round(temperatura_atual, 1)

    timestamp_1h = (datetime.now() + timedelta(hours=1)).strftime("%H:%M")
    existentes = [p["timestamp"] for p in previsoes]

    if timestamp_1h not in existentes:
        previsoes.append({
            "timestamp": timestamp_1h,
            "temperaturaPrevista": temperatura_prevista,
            "temperaturaReal": None
        })

    previsoes = atualizar_previsoes(previsoes, historico)
    previsoes = previsoes[-144:]

    impacto_por_10pct = round(a * 10, 2)

    regressao = []
    for x in range(0, 101, 10):
        regressao.append({
            "x": x,
            "y": round((a * x) + b, 1)
        })

    status = (
        "CRITICO" if temperatura_atual >= 85
        else "ALERTA" if temperatura_atual >= 70
        else "NORMAL"
    )

    return {
        "kpis": {
            "fkEmpresa": FKEMPRESA,
            "cpuP90": cpu_p90,
            "temperaturaAtual": temperatura_atual,
            "temperaturaPrevista": temperatura_prevista,
            "status": status,
            "impactoCpuPor10pct": impacto_por_10pct
        },
        "grafico": historico,
        "previsoes": previsoes,
        "regressao": regressao
    }, previsoes

previsoes = carregar_previsoes_s3()

while True:
    leitura = raw()
    salvar_raw_s3(leitura)

    df = carregar_raw_s3()
    dados, previsoes = trusted(df, previsoes)

    salvar_client_s3(dados)
    salvar_previsoes_s3(previsoes)

    print(
        f"CPU: {leitura['cpu']}% | "
        f"Temp: {leitura['temperatura']}°C | "
        f"Prevista 1h: {dados['kpis']['temperaturaPrevista']}°C | "
        f"Status: {dados['kpis']['status']}"
    )

    time.sleep(tempoCaptura)