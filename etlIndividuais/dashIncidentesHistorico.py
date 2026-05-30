import boto3       #SDK da AWS — usada pra acessar o S3
import csv         #lê e interpreta arquivos CSV
import json        #converte dicionários Python em JSON
import io          #permite tratar texto como se fosse um arquivo
import os          #acessa variáveis de ambiente
import calendar    #calendário (pra saber quantos dias tem o mês)
from datetime import datetime, date, timezone  #manipulação de datas e horários

#conexão com o S3 (Lambda usa IAM role, sem credenciais explícitas)
s3 = boto3.client('s3')

#configurações
BUCKET = os.getenv('S3_BUCKET_NAME')

LIMITES = {
    "cpu_perc":   90,
    "ram_perc":   80,
    "disco_perc": 85
}

#nomes das colunas do csv de métricas
COL_TIMESTAMP = 'datahora'
COL_CPU = 'cpu_perc'
COL_RAM = 'ram_perc'
COL_DISCO = 'disco_perc'
COL_HOSTNAME = 'hostname'

#aqui inicia a lambda
def lambda_handler(event, _):

    empresa = event['empresa']

    hoje = date.today()
    mes = hoje.month
    ano = hoje.year
    total_dias  = calendar.monthrange(ano, mes)[1]  #quantos dias tem o mês (28/29/30/31)
    data_inicio = date(ano, mes, 1)                 #primeiro dia do mês
    data_fim = date(ano, mes, total_dias)         #último dia do mês

    #Aqui é onde ele busca o "raw" da empresa
    prefix = f"raw/{empresa}/"

    incidentes = {}  #esse dicionario vai ser usado pro heatmap
    metricas = {}  #esse pros graficos

    # O paginator busca todos os arquivos do S3, mesmo que sejam mais de 1000
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=BUCKET, Prefix=prefix)

    for page in pages:
        for obj in page.get('Contents', []):
            chave = obj['Key']  #caminho completo do arquivo no S3

            if not chave.endswith('.csv'):
                continue

            csv_obj = s3.get_object(Bucket=BUCKET, Key=chave)
            conteudo = csv_obj['Body'].read().decode('utf-8')
            reader = csv.DictReader(io.StringIO(conteudo))
            if not reader.fieldnames:
                continue

            #Aqui ele vai alimentar os dicionários
            processar_incidente(reader, data_inicio, data_fim, incidentes)

            # relê o CSV pra processar métricas
            reader2 = csv.DictReader(io.StringIO(conteudo))
            processar_metrica(reader2, data_inicio, data_fim, metricas)

    #json final
    output = {
        "empresa": empresa,
        "gerado_em": datetime.now(timezone.utc).isoformat(), #timestamp de quando a ETL rodou
        "mes_referencia": {"mes": mes, "ano": ano},
        "total_dias_mes": total_dias,
        "incidentes": incidentes, # dados pro heatmap
        "metricas": metricas # dados pros gráficos
    }

    chave_saida = f"client/{empresa}/dashboard/client_{mes:02d}_{ano}.json"

    s3.put_object(
        Bucket=BUCKET,
        Key=chave_saida,
        Body=json.dumps(output, ensure_ascii=False, indent=2),
        ContentType='application/json'
    )

    #confirma que tudo rodou e informa o arquivo gerado
    return {
        'statusCode': 200,
        'body': json.dumps({
            'mensagem': 'ETL concluída',
            'arquivo':  chave_saida
        })
    }


def processar_incidente(reader, data_inicio, data_fim, incidentes):
    #detecta incidentes comparando métricas com os limites definidos
    #serve pro heatmap com qtd de incidentes por dia

    for linha in reader:
        #converte o texto do timestamp para um objeto de data real
        try:
            ts = datetime.strptime(linha[COL_TIMESTAMP], '%Y-%m-%d %H:%M:%S')
            cpu = float(linha[COL_CPU])
            ram = float(linha[COL_RAM])
            disco = float(linha[COL_DISCO])
        except (ValueError, KeyError):
            continue  #pula linhas com formato inválido ou coluna ausente

        data_registro = ts.date()

        #aqui ele vai ignorar se nao for do mes do filtro
        if not (data_inicio <= data_registro <= data_fim):
            continue

        servidor = linha.get(COL_HOSTNAME, '')
        data_str = data_registro.strftime('%Y-%m-%d')

        #verifica quais componentes ultrapassaram o limite
        alertas = []
        if cpu > LIMITES['cpu_perc']: alertas.append(('cpu', cpu, LIMITES['cpu_perc']))
        if ram > LIMITES['ram_perc']: alertas.append(('ram', ram, LIMITES['ram_perc']))
        if disco > LIMITES['disco_perc']: alertas.append(('disco', disco, LIMITES['disco_perc']))

        #se nenhum limite foi ultrapassado, pula a linha
        if not alertas:
            continue

        if servidor not in incidentes:
            incidentes[servidor] = {
                "TotalIncidentes": {
                    "total_mes": 0,
                    "por_dia": {}
                },
                "DataIncidente": [],
                "Componentes": {
                    "cpu": [],
                    "ram": [],
                    "disco": [],
                },
                "detalhes": []
            }

        sv = incidentes[servidor]

        # junta os incidentes do mesmo dia
        for componente, valor, limite in alertas:
            sv["DataIncidente"].append(data_str)
            sv["TotalIncidentes"]["total_mes"] += 1
            sv["TotalIncidentes"]["por_dia"][data_str] = (
                sv["TotalIncidentes"]["por_dia"].get(data_str, 0) + 1
            )

            #descricao p matheus usar
            sv["detalhes"].append({
                "data": data_str,
                "servidor": servidor,
                "componente": componente,
                "valor": round(valor, 2),
                "limite": limite,
                "descricao": f"{componente.upper()} atingiu {round(valor, 2)}% (limite: {limite}%)"
            })

            #mostra o componente q deu b.o
            if componente in sv["Componentes"]:
                sv["Componentes"][componente].append(data_str)


#aqui é p processar os graficos
def processar_metrica(reader, data_inicio, data_fim, metricas):

    for linha in reader:
        try:
            ts = datetime.strptime(linha[COL_TIMESTAMP], '%Y-%m-%d %H:%M:%S')
            cpu = float(linha[COL_CPU])
            ram = float(linha[COL_RAM])
            disco = float(linha[COL_DISCO])
        except (ValueError, KeyError):
            continue

        data_registro = ts.date()

        # se n for do mês ele pula
        if not (data_inicio <= data_registro <= data_fim):
            continue

        servidor = linha.get(COL_HOSTNAME, '')
        data_str = data_registro.strftime('%Y-%m-%d')

        # Inicializa a estrutura do servidor se for a primeira vez que aparece
        if servidor not in metricas:
            metricas[servidor] = {
                "pico_mes": {"cpu": 0, "ram": 0, "disco": 0},  # maior valor do mês inteiro
                "dias": {}                                   # maior valor de cada dia
            }

        sv = metricas[servidor]

        # Inicializa o dia se ainda não existe
        if data_str not in sv["dias"]:
            sv["dias"][data_str] = {
                "cpu_pico": 0,
                "ram_pico": 0,
                "disco_pico": 0
            }

        dia = sv["dias"][data_str]

        #atualiza o pico do dia
        if cpu > dia["cpu_pico"]: dia["cpu_pico"] = round(cpu, 2)
        if ram > dia["ram_pico"]: dia["ram_pico"] = round(ram, 2)
        if disco > dia["disco_pico"]: dia["disco_pico"] = round(disco, 2)

        #atualiza o pico do mês
        if cpu > sv["pico_mes"]["cpu"]: sv["pico_mes"]["cpu"] = round(cpu, 2)
        if ram > sv["pico_mes"]["ram"]: sv["pico_mes"]["ram"] = round(ram, 2)
        if disco > sv["pico_mes"]["disco"]: sv["pico_mes"]["disco"] = round(disco, 2)
