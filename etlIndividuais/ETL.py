import os

import mysql.connector


def ler_env(caminho=".env.dev"):
    """Reutiliza as credenciais do projeto Node sem duplicar configuracao."""
    if not os.path.exists(caminho):
        return

    with open(caminho, "r", encoding="utf-8") as arquivo:
        for linha in arquivo:
            linha = linha.strip()
            if not linha or linha.startswith("#") or "=" not in linha:
                continue
            chave, valor = linha.split("=", 1)
            os.environ.setdefault(chave.strip(), valor.strip().strip("'").strip('"'))


def obter_env_obrigatoria(nome):
    valor = os.getenv(nome)
    if valor is None or valor == "":
        raise RuntimeError(f"Configure a variavel de ambiente obrigatoria {nome}.")
    return valor


def conectar_mysql():
    return mysql.connector.connect(
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


def atualizar_linhas_processadas():
    """Calcula faixa, pressao e risco para linhas existentes."""
    faixa_padrao = int(obter_env_obrigatoria("ATMOS_FAIXA_ESPERADA"))
    conexao = conectar_mysql()
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


def gerar_resumo_console():
    """Mostra no terminal os principais indicadores calculados pelo ETL."""
    conexao = conectar_mysql()
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

    print("Maior pico:", pico)
    print("Evento de maior pressao:", evento_pressao)
    print("Horario critico mais recorrente:", horario)


def main():
    ler_env()
    total = atualizar_linhas_processadas()
    print(f"ETL finalizado. Linhas processadas: {total}")
    gerar_resumo_console()


if __name__ == "__main__":
    main()
