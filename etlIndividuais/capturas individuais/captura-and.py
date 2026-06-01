import os
import urllib.parse
import urllib.request
from datetime import datetime

import mysql.connector
import psutil


OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


def ler_env(caminho=".env.dev"):
    """Carrega um .env simples para o script Python usar as mesmas credenciais do Node."""
    if not os.path.exists(caminho):
        return

    with open(caminho, "r", encoding="utf-8") as arquivo:
        for linha in arquivo:
            linha = linha.strip()
            if not linha or linha.startswith("#") or "=" not in linha:
                continue
            chave, valor = linha.split("=", 1)
            os.environ.setdefault(chave.strip(), valor.strip().strip("'").strip('"'))


def buscar_clima_atual():
    """Consulta obrigatoria na API Open-Meteo para obter clima atual."""
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

    import json
    current = json.loads(dados)["current"]
    return {
        "temperatura": float(current.get("temperature_2m", 0)),
        "precipitacao": float(current.get("precipitation", 0)),
        "weather_code": int(current.get("weather_code", 0)),
        "data_hora": current.get("time"),
    }


def classificar_evento(weather_code, temperatura, precipitacao):
    """Traduz weather_code e variaveis atuais para os grupos usados na dashboard."""
    if weather_code in [95, 96, 99]:
        return "Tempestade", "tempestade"
    if weather_code in [61, 63, 65, 80, 81, 82] or precipitacao >= 8:
        return "Chuva intensa", "chuva intensa"
    if temperatura >= 33:
        return "Onda de calor", "onda de calor"
    if temperatura <= 15 and weather_code in [3, 45, 48, 51, 53, 55, 56, 57]:
        return "Frente fria", "frente fria"
    return "Sem evento relevante", "sem evento relevante"


def obter_env_obrigatoria(nome):
    valor = os.getenv(nome)
    if valor is None or valor == "":
        raise RuntimeError(f"Configure a variavel de ambiente obrigatoria {nome}.")
    return valor


def obter_requisicoes_reais():
    """Le o volume real recebido pela integracao do projeto.

    A dashboard nao deve inventar requisicoes. Enquanto a fonte definitiva
    nao existir no banco/cloud, informe ATMOS_REQUISICOES_MINUTO a partir do
    coletor real usado pela equipe.
    """
    return int(obter_env_obrigatoria("ATMOS_REQUISICOES_MINUTO"))


def calcular_pressao(requisicoes, faixa_esperada):
    """Mesma regra inicial pedida para o ETL, aplicada tambem na captura."""
    limite = requisicoes / faixa_esperada
    if limite <= 1:
        return "baixa", "baixo"
    if limite <= 1.2:
        return "moderada", "medio"
    if limite <= 1.5:
        return "alta", "alto"
    return "critica", "critico"


def conectar_mysql():
    return mysql.connector.connect(
        host=obter_env_obrigatoria("DB_HOST"),
        database=obter_env_obrigatoria("DB_DATABASE"),
        user=obter_env_obrigatoria("DB_USER"),
        password=os.getenv("DB_PASSWORD", ""),
        port=int(obter_env_obrigatoria("DB_PORT")),
    )


def salvar_medicao(medicao):
    conexao = conectar_mysql()
    cursor = conexao.cursor()
    sql = """
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
    """
    servidor_id = int(obter_env_obrigatoria("ATMOS_SERVIDOR_ID"))
    faixa_esperada = int(obter_env_obrigatoria("ATMOS_FAIXA_ESPERADA"))
    cursor.execute(sql, (
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
    ))
    conexao.commit()
    cursor.close()
    conexao.close()


def main():
    ler_env()
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
    salvar_medicao(medicao)
    print("Medicao de analise de carga salva:", medicao)


if __name__ == "__main__":
    main()
