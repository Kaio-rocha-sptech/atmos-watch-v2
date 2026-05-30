import json
import random
import calendar
from datetime import date

EMPRESA = "empresaX"

SERVIDORES = [
    "laptop-ANDERSON",
    "laptop-AURORA",
    "laptop-NZ13",
    "laptop-LUANA",
    "laptop-LUIZ",
    "laptop-MATHEUS",
]

LIMITES = {"cpu_perc": 90, "ram_perc": 80, "disco_perc": 85}

MESES = [
    (3, 2026),
    (4, 2026),
    (5, 2026),
]


def gerar_valor(base, variacao, minv=0, maxv=100):
    return round(min(maxv, max(minv, base + random.uniform(-variacao, variacao))), 1)


def gerar_mes(mes, ano):
    total_dias = calendar.monthrange(ano, mes)[1]
    incidentes = {}
    metricas   = {}

    for servidor in SERVIDORES:
        sv_inc = {
            "TotalIncidentes": {"total_mes": 0, "por_dia": {}},
            "DataIncidente": [],
            "Componentes": {"cpu": [], "ram": [], "disco": []},
            "detalhes": []
        }
        sv_met = {
            "pico_mes": {"cpu": 0, "ram": 0, "disco": 0},
            "dias": {}
        }

        for dia in range(1, total_dias + 1):
            data_str = date(ano, mes, dia).strftime("%Y-%m-%d")

            cpu   = gerar_valor(42, 22)
            ram   = gerar_valor(72, 15)
            disco = gerar_valor(76, 8)

            sv_met["dias"][data_str] = {
                "cpu_pico":   cpu,
                "ram_pico":   ram,
                "disco_pico": disco
            }

            if cpu   > sv_met["pico_mes"]["cpu"]:   sv_met["pico_mes"]["cpu"]   = cpu
            if ram   > sv_met["pico_mes"]["ram"]:   sv_met["pico_mes"]["ram"]   = ram
            if disco > sv_met["pico_mes"]["disco"]: sv_met["pico_mes"]["disco"] = disco

            alertas = []
            if cpu   > LIMITES["cpu_perc"]:   alertas.append(("cpu",   cpu,   LIMITES["cpu_perc"]))
            if ram   > LIMITES["ram_perc"]:   alertas.append(("ram",   ram,   LIMITES["ram_perc"]))
            if disco > LIMITES["disco_perc"]: alertas.append(("disco", disco, LIMITES["disco_perc"]))

            for componente, valor, limite in alertas:
                sv_inc["DataIncidente"].append(data_str)
                sv_inc["TotalIncidentes"]["total_mes"] += 1
                sv_inc["TotalIncidentes"]["por_dia"][data_str] = (
                    sv_inc["TotalIncidentes"]["por_dia"].get(data_str, 0) + 1
                )
                sv_inc["Componentes"][componente].append(data_str)
                sv_inc["detalhes"].append({
                    "data":       data_str,
                    "servidor":   servidor,
                    "componente": componente,
                    "valor":      valor,
                    "limite":     limite,
                    "descricao":  f"{componente.upper()} atingiu {valor}% (limite: {limite}%)"
                })

        if sv_inc["TotalIncidentes"]["total_mes"] > 0:
            incidentes[servidor] = sv_inc

        metricas[servidor] = sv_met

    return {
        "empresa":        EMPRESA,
        "gerado_em":      f"{ano}-{mes:02d}-{total_dias:02d}T23:59:00+00:00",
        "mes_referencia": {"mes": mes, "ano": ano},
        "total_dias_mes": total_dias,
        "incidentes":     incidentes,
        "metricas":       metricas
    }


import os
os.makedirs("historico", exist_ok=True)

for mes, ano in MESES:
    dados = gerar_mes(mes, ano)
    nome  = f"historico/client_{mes:02d}_{ano}.json"

    with open(nome, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)

    total_inc = sum(
        sv["TotalIncidentes"]["total_mes"]
        for sv in dados["incidentes"].values()
    )
    print(f"Gerado: {nome} | Servidores: {len(SERVIDORES)} | Incidentes total: {total_inc}")
