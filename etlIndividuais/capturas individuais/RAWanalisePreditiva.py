import os
import time
import random
import psutil
import boto3
from dotenv import load_dotenv

load_dotenv()

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


def raw():
    cpu = psutil.cpu_percent(interval=1)
    variacao = random.uniform(-1, 1)
    temperatura = 45 + (cpu * 0.5) + variacao

    return {
        "timestamp": time.strftime("%H:%M"),
        "cpu": round(cpu, 1),
        "temperatura": round(temperatura, 1)
    }


while True:
    leitura = raw()

    salvar_raw_s3(leitura)

    print(
        f"CPU: {leitura['cpu']}% | "
        f"Temp: {leitura['temperatura']}°C"
    )

    time.sleep(tempoCaptura)