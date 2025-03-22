import pandas as pd
import numpy as np
import requests
import zipfile
import io
import xlsxwriter
import zipfile
import io
import time
#import investpy
import os
import re
from collections import defaultdict
from datetime import datetime
from datetime import date

def filtrar_e_organizar_arquivos(arquivos, padroes):
    """
    Filtra e organiza arquivos em grupos com base em padrões.
    
    Args:
        arquivos (list): Lista de nomes de arquivos.
        padroes (dict): Dicionário com o nome do grupo como chave e o padrão como valor.
        
    Returns:
        dict: Dicionário com grupos como chaves e listas de arquivos correspondentes como valores.
    """
    grupos = defaultdict(list)
    for arquivo in arquivos:
        for grupo, padrao in padroes.items():
            if re.search(padrao, arquivo):
                grupos[grupo].append(arquivo)
              
    return grupos

def buscar_dados_empresas():
    link = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv'
    r = requests.get(link)
    linhas = [i.strip().split(';') for i in r.text.split('\n')]
    data = pd.DataFrame(linhas[1:], columns = linhas[0])
    return data

def calculando_dados_trimestrais(empresa, tabela ,ano, contas_desejadas, dt_ini, dt_fim):
    #QUANDO TERMINAR O PRIMEIRO TRIMESTRE ANALIZAR O CÓDIGO PARA VER COMO ELE VAI SE COMPORTAR COM 2 ANOS, 2024 E 2025, POR EXEMPLO.
    """
Calcula dados trimestrais para uma lista de empresas e contas desejadas.

Parâmetros:
    empresa (str): Empresa a ser analisada.
    dre_trimestral (pd.DataFrame): DataFrame com os dados trimestrais.
    ano (str): Ano dos dados (formato "YYYY").
    contas_desejadas (list): Lista de contas a serem filtradas (ex.: ["Resultado Bruto", "Receita de Venda de Bens e/ou Serviços"]).
    dt_ini (str): Data inicial do período (formato "MM-DD").
    dt_fim (str): Data final do período (formato "MM-DD").
    ordem_exerc (str): Ordem do exercício ("ÚLTIMO" ou "PENÚLTIMO").

Retorna:
    pd.DataFrame: DataFrame com os resultados organizados por empresa e conta.
"""
    resultados = []
    if "DT_INI_EXERC" in tabela.columns:
        filtro = pd.Series(
            (tabela["DT_INI_EXERC"] == f"{ano}-{dt_ini}") &
            (tabela["DT_FIM_EXERC"] == f"{ano}-{dt_fim}") &
            (tabela["ORDEM_EXERC"] == "ÚLTIMO") &
            (tabela["DS_CONTA"].isin(contas_desejadas)) &
            (tabela["DENOM_CIA"] == empresa)
        )
    else:
        filtro = pd.Series(
            (tabela["DT_FIM_EXERC"] == f"{ano}-{dt_fim}") &
            (tabela["ORDEM_EXERC"] == "ÚLTIMO") &
            (tabela["DS_CONTA"].isin(contas_desejadas)) &
            (tabela["DENOM_CIA"] == empresa)
        )
    dados_filtrados = tabela.loc[filtro, ["DS_CONTA", "VL_AJUSTADO"]]
    for conta in contas_desejadas:
            valor = dados_filtrados.loc[dados_filtrados["DS_CONTA"] == conta, "VL_AJUSTADO"].sum()
            resultados.append({
                "Empresa": empresa,
                "Conta": conta,
                "Valor": valor,
                "Ano": ano,
                "Periodo": f"{ano}-{dt_ini} a {ano}-{dt_fim}"
            })
    return pd.DataFrame(resultados)

def calculando_dados_anuais(empresa, tabela_anual, ano, contas_desejadas):
    """
    Calcula dados anuais para uma lista de empresas e contas desejadas.

    Parâmetros:
        lista_de_empresas (list): Lista de empresas a serem analisadas.
        dre_anual (pd.DataFrame): DataFrame com os dados anuais.
        ano (str): Ano dos dados (formato "YYYY").
        contas_desejadas (list): Lista de contas a serem filtradas.

    Retorna:
        pd.DataFrame: DataFrame com os resultados organizados por empresa e conta.
    """
    return calculando_dados_trimestrais(
        empresa=empresa,
        tabela=tabela_anual,
        ano=ano,
        contas_desejadas=contas_desejadas,
        dt_ini="01-01",
        dt_fim="12-31"
    )

def trim_to_dt_ini(trim):
    mapeamento = {"T1": "01-01", "T2": "04-01", "T3": "07-01"}
    return mapeamento.get(trim)

def trim_to_dt_fim(trim):
    mapeamento = {"T1": "03-31", "T2": "06-30", "T3": "09-30"}
    return mapeamento.get(trim)

def verificar_trimestres(lista_cvm_ativos):
    
    hoje = datetime.today()
    #hoje = date(2024, 7, 10)
    ano_atual = hoje.year
    mes_atual = hoje.month

    trimestre_atual = (mes_atual - 1) // 3 + 1

    trimestre_atual -= 1
    if trimestre_atual == 0:  
        trimestre_atual = 4
        ano_atual -= 1

    trimestres = []
    for _ in range(4):
        trimestres.append(f"{ano_atual}-T{trimestre_atual}")
        trimestre_atual -= 1
        if trimestre_atual == 0: 
            trimestre_atual = 4
            ano_atual -= 1
    
    primeiro_trimestre = trimestres[0].split('-')[-1]
    ano_atual = hoje.year
    ano_corrente = int(ano_atual)
    ano_anterior = ano_corrente - 1
    
    if primeiro_trimestre == 'T4':
        extrair_demonstrativos_trimestrais(lista_cvm_ativos, str(ano_anterior))
        extrair_demonstrativos_anuais(lista_cvm_ativos, str(ano_anterior))
        
    else:
        extrair_demonstrativos_trimestrais(lista_cvm_ativos, str(ano_corrente))
        extrair_demonstrativos_trimestrais(lista_cvm_ativos, str(ano_anterior))
        extrair_demonstrativos_anuais(lista_cvm_ativos, str(ano_anterior))
    
    return trimestres, primeiro_trimestre, ano_corrente, ano_anterior

def processando_arquivos(primeiro_trimestre, lista_cvm_ativos, ano_corrente, ano_anterior):
    # Essa função está com um erro quando busco dados de anos diferentes
    caminho = os.getcwd()
    arquivos = os.listdir(caminho)

    padroes = {
        "anual": r"Demonstrativos Anual Empresa \d+_\d{4}\.xlsx",
        "trimestral_corrente": f"Demonstrativos Trimestrais Empresa \d+_{ano_corrente}\.xlsx",
        "trimestral_anterior": f"Demonstrativos Trimestrais Empresa \d+_{ano_anterior}\.xlsx",
    }

    grupos_de_arquivos = filtrar_e_organizar_arquivos(arquivos, padroes)

    if primeiro_trimestre == 'T4':
    
        # Verificar se o grupo existe no dicionário
        trimestral_ano_anterior = "trimestral_anterior"
        if trimestral_ano_anterior in grupos_de_arquivos:
            arquivos_trimestral_ano_anterior = grupos_de_arquivos[trimestral_ano_anterior]
            print(f"Processando grupo: {trimestral_ano_anterior}")
            lista_de_empresas, n_empresas, dre_trimestral_ano_anterior, bpa_trimestral_ano_anterior, bpp_trimestral_ano_anterior = criando_tabelas_dre_bpa_bpp(arquivos_trimestral_ano_anterior)
            print(f"Arquivos processados no grupo '{trimestral_ano_anterior}': {arquivos_trimestral_ano_anterior}")
        else:
            print(f"O grupo '{trimestral_ano_anterior}' não foi encontrado!")
        
        anual = "anual"
        if anual in grupos_de_arquivos:
            arquivos_anual = grupos_de_arquivos[anual]
            print(f"Processando grupo: {anual}")
            lista_de_empresas, n_empresas, dre_anual, bpa_anual, bpp_anual  = criando_tabelas_dre_bpa_bpp(arquivos_anual)
            print(f"Arquivos processados no grupo '{anual}': {arquivos_anual}")
        else:
            print(f"O grupo '{anual}' não foi encontrado!")

        dre_trimestral_ano_corrente=0
        bpa_trimestral_ano_corrente=0
        bpp_trimestral_ano_corrente=0


    else:
        trimestral_ano_corrente = "trimestral_corrente"  
        # Verificar se o grupo existe no dicionário
        if trimestral_ano_corrente in grupos_de_arquivos:
            arquivos_trimestral_ano_corrente = grupos_de_arquivos[trimestral_ano_corrente]
            print(f"Processando grupo: {trimestral_ano_corrente}")
            lista_de_empresas, n_empresas, dre_trimestral_ano_corrente, bpa_trimestral_ano_corrente, bpp_trimestral_ano_corrente = criando_tabelas_dre_bpa_bpp(arquivos_trimestral_ano_corrente)
            print(f"Arquivos processados no grupo '{trimestral_ano_corrente}': {arquivos_trimestral_ano_corrente}")
        else:
            print(f"O grupo '{trimestral_ano_corrente}' não foi encontrado!")

        trimestral_ano_anterior = "trimestral_anterior"
        if trimestral_ano_anterior in grupos_de_arquivos:
            arquivos_trimestral_ano_anterior = grupos_de_arquivos[trimestral_ano_anterior]
            print(f"Processando grupo: {trimestral_ano_anterior}")
            lista_de_empresas, n_empresas, dre_trimestral_ano_anterior, bpa_trimestral_ano_anterior, bpp_trimestral_ano_anterior = criando_tabelas_dre_bpa_bpp(arquivos_trimestral_ano_anterior)
            print(f"Arquivos processados no grupo '{trimestral_ano_anterior}': {arquivos_trimestral_ano_anterior}")
        else:
            print(f"O grupo '{trimestral_ano_anterior}' não foi encontrado!")

        anual = "anual"
        if anual in grupos_de_arquivos:
            arquivos_anual = grupos_de_arquivos[anual]
            print(f"Processando grupo: {anual}")
            lista_de_empresas, n_empresas, dre_anual = criando_tabelas_dre_bpa_bpp(arquivos_anual)
            print(f"Arquivos processados no grupo '{anual}': {arquivos_anual}")
        else:
            print(f"O grupo '{anual}' não foi encontrado!")
    return lista_de_empresas, n_empresas, dre_anual, bpa_anual, bpp_anual, dre_trimestral_ano_anterior, bpa_trimestral_ano_anterior, bpp_trimestral_ano_anterior, dre_trimestral_ano_corrente, bpa_trimestral_ano_corrente, bpp_trimestral_ano_corrente

def extrair_demonstrativos_trimestrais(lista_cvm_ativos, ano):
    #recebe a lista de ativos para pesquisa
    
    link = f'https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/itr_cia_aberta_{ano}.zip'
    arquivo_zip = requests.get(link)
    arquivo = f'itr_cia_aberta_DRE_con_{ano}.csv'
    zf = zipfile.ZipFile(io.BytesIO(arquivo_zip.content))
    dre = zf.open(arquivo)
    linhas = dre.readlines()
    linhas = [i.strip().decode('ISO-8859-1') for i in linhas]
    linhas = [i.split(';') for i in linhas]
    df = pd.DataFrame(linhas[1:], columns = linhas[0])

    demonstrativos = ['DFC_MD', 'DFC_MI', 'BPA', 'DRE', 'BPP']
    lista_listas = []
    ativo = 0
    for i in lista_cvm_ativos:
        lista_df = []
        demonstrativo = 0
        for k in demonstrativos:
            link = f'https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/itr_cia_aberta_{ano}.zip'
            arquivo_zip = requests.get(link)
            arquivo = f'itr_cia_aberta_{str(k)}_con_2024.csv'

            zf = zipfile.ZipFile(io.BytesIO(arquivo_zip.content))
            dados = zf.open(arquivo)
            linhas = dados.readlines()
            linhas = [i.strip().decode('ISO-8859-1') for i in linhas]
            linhas = [i.split(';') for i in linhas]
            df = pd.DataFrame(linhas[1:], columns = linhas[0])

            df['VL_AJUSTADO'] = pd.to_numeric(df['VL_CONTA'])
            df['DT_AJUSTADO'] = pd.to_datetime(df['DT_REFER'])

            filtro = df[df['CD_CVM'] == '0'+ str(i)]
            lista_df.append(filtro)
            print(f'Trabalhando com a empresa {i} e seu demonstrativo {k}. As dimensões são {filtro.shape}')
        lista_listas.append(lista_df)
        writer = pd.ExcelWriter(f'Demonstrativos Trimestrais Empresa {str(i)}_{ano}.xlsx', engine = 'xlsxwriter')
        for j in demonstrativos:
            lista_listas[ativo][demonstrativo].to_excel(writer, sheet_name = j)
            demonstrativo += 1
        writer.close()
        ativo += 1
        print(f'Empresa {i} finalizada. \n')
    
def extrair_demonstrativos_anuais(lista_cvm_ativos, ano):
    
    demonstrativos = ['DRE', 'BPA', 'BPP']
    lista_listas = []
    ativo = 0
    for i in lista_cvm_ativos:
        lista_df = []
        demonstrativo = 0
        for k in demonstrativos:
            link = f'https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_{ano}.zip'
            arquivo_zip = requests.get(link)
            arquivo = f'dfp_cia_aberta_{str(k)}_con_{ano}.csv'

            zf = zipfile.ZipFile(io.BytesIO(arquivo_zip.content))
            dados = zf.open(arquivo)
            linhas = dados.readlines()
            linhas = [i.strip().decode('ISO-8859-1') for i in linhas]
            linhas = [i.split(';') for i in linhas]
            df = pd.DataFrame(linhas[1:], columns = linhas[0])

            df['VL_AJUSTADO'] = pd.to_numeric(df['VL_CONTA'])
            df['DT_AJUSTADO'] = pd.to_datetime(df['DT_REFER'])

            filtro = df[df['CD_CVM'] == '0'+ str(i)]
            lista_df.append(filtro)
            print(f'Trabalhando com a empresa {i} e seu demonstrativo {k}. As dimensões são {filtro.shape}')
        lista_listas.append(lista_df)
        # utilizando a biblioteca xlsxwriter
        writer = pd.ExcelWriter(f'Demonstrativos Anual Empresa {str(i)}_{ano}.xlsx', engine = 'xlsxwriter')
        # especifique o que esse arquivo de excel vai conter
        for j in demonstrativos:
            lista_listas[ativo][demonstrativo].to_excel(writer, sheet_name = j)
            demonstrativo += 1
        writer.close()
        ativo += 1
        print(f'Empresa {i} finalizada. \n')

def extrair_valor_por_conta(df, conta):
    """
    Extrai o valor de uma conta específica de um DataFrame no formato longo.

    Parâmetros:
        df (pd.DataFrame): DataFrame com as colunas ["Conta", "Valor"].
        conta (str): Nome da conta a ser extraída.

    Retorna:
        float: Valor correspondente à conta, ou None se a conta não for encontrada.
    """
    filtro = df["Conta"] == conta
    if filtro.any():
        return df.loc[filtro, "Valor"].values[0]
    else:
        print(f"Aviso: Conta '{conta}' não encontrada no DataFrame.")
        return None

def criando_tabelas_dre_bpa_bpp(arquivos):
    dre = pd.DataFrame()
    bpa = pd.DataFrame()
    bpp = pd.DataFrame()
    planilhas_existentes = {"DRE": False, "BPA": False, "BPP": False}
    
    for f in arquivos:
        # Verifica quais planilhas estão disponíveis no arquivo
        planilhas_disponiveis = pd.ExcelFile(f).sheet_names
        
        if "DRE" in planilhas_disponiveis:
            planilhas_existentes["DRE"] = True
            df = pd.read_excel(f, sheet_name="DRE")
            df['DT_INI_EXERC'] = pd.to_datetime(df['DT_INI_EXERC'])
            df['DT_FIM_EXERC'] = pd.to_datetime(df['DT_FIM_EXERC'])
            dre = pd.concat([dre, df])
        
        if "BPA" in planilhas_disponiveis:
            planilhas_existentes["BPA"] = True
            df = pd.read_excel(f, sheet_name="BPA")
            df['DT_FIM_EXERC'] = pd.to_datetime(df['DT_FIM_EXERC'])
            bpa = pd.concat([bpa, df])
        
        if "BPP" in planilhas_disponiveis:
            planilhas_existentes["BPP"] = True
            df = pd.read_excel(f, sheet_name="BPP")
            df['DT_FIM_EXERC'] = pd.to_datetime(df['DT_FIM_EXERC'])
            bpp = pd.concat([bpp, df])
    if not dre.empty:
        lista_de_empresas = dre['DENOM_CIA'].unique()
        
    else:
        lista_de_empresas = []

    n_empresas = len(lista_de_empresas)
    
    # Retorna apenas as tabelas que existem
    retorno = [lista_de_empresas, n_empresas]
    if planilhas_existentes["DRE"]:
        retorno.append(dre)
    if planilhas_existentes["BPA"]:
        retorno.append(bpa)
    if planilhas_existentes["BPP"]:
        retorno.append(bpp)
    
    return tuple(retorno)

def calculando_margem_bruta(lista_de_empresas, dre_trimestral_ano_corrente, dre_trimestral_ano_anterior, dre_anual, trimestre):
    contas_desejadas = [
        'Resultado Bruto',
        'Receita de Venda de Bens e/ou Serviços'
    ]
    resultados = {}
    for empresa in lista_de_empresas:
        resultados_empresa = {}
        for trimestre in trimestres:
            periodo = trimestre
            ano, trim = trimestre.split("-")
            if trim == 'T4':
                t1 = calculando_dados_trimestrais(empresa, dre_trimestral_ano_anterior, ano, contas_desejadas, dt_ini = '01-01', dt_fim= '03-31')
                t2 = calculando_dados_trimestrais(empresa, dre_trimestral_ano_anterior, ano, contas_desejadas, dt_ini = '04-01', dt_fim= '06-30')
                t3 = calculando_dados_trimestrais(empresa, dre_trimestral_ano_anterior, ano, contas_desejadas, dt_ini = '07-01', dt_fim= '09-30')
                anual = calculando_dados_anuais(empresa, dre_anual, ano, contas_desejadas)
                
                resultado_bruto_t4 = (
                    extrair_valor_por_conta(anual, "Resultado Bruto") -
                    (
                        extrair_valor_por_conta(t1, "Resultado Bruto") +
                        extrair_valor_por_conta(t2, "Resultado Bruto") +
                        extrair_valor_por_conta(t3, "Resultado Bruto")
                    )
                )
                
                receita_vendas_t4 = (
                    extrair_valor_por_conta(anual, "Receita de Venda de Bens e/ou Serviços") -
                    (
                        extrair_valor_por_conta(t1, "Receita de Venda de Bens e/ou Serviços") +
                        extrair_valor_por_conta(t2, "Receita de Venda de Bens e/ou Serviços") +
                        extrair_valor_por_conta(t3, "Receita de Venda de Bens e/ou Serviços")
                    )
                )

                # Calcula a margem bruta
                if receita_vendas_t4 and receita_vendas_t4 != 0:
                    margem_t4 = resultado_bruto_t4 / receita_vendas_t4
                else:
                    margem_t4 = None  # Evita divisão por zero
                resultados_empresa[periodo] = margem_t4
            else:
                dados_periodo = calculando_dados_trimestrais(empresa, dre_trimestral_ano_anterior, ano, contas_desejadas, dt_ini=f"{trim_to_dt_ini(trim)}", dt_fim=f"{trim_to_dt_fim(trim)}")
                
                resultado_bruto = extrair_valor_por_conta(dados_periodo, "Resultado Bruto")
                receita_vendas = extrair_valor_por_conta(dados_periodo, "Receita de Venda de Bens e/ou Serviços")

                if receita_vendas and receita_vendas != 0:
                    margem = resultado_bruto / receita_vendas
                else:
                    margem = None  # Evita divisão por zero
                resultados_empresa[periodo] = margem
        resultados[empresa] = resultados_empresa
        df = pd.DataFrame.from_dict(resultados, orient="index")
        df = pd.DataFrame.from_dict(resultados, orient="columns")
        df = df.dropna()
        df_resetado = df.reset_index()
        df_final = df_resetado.drop(columns=["index"])
        df_final = df_final.head(1)
        margem_bruta = df_final.melt(var_name="Empresa", value_name="Margem_bruta")
    return margem_bruta

def calculando_margem_liquida(lista_de_empresas, trimestres, dre_trimestral_ano_corrente, dre_trimestral_ano_anterior):
    contas_desejadas = [
        'Lucro/Prejuízo Consolidado do Período',
        'Receita de Venda de Bens e/ou Serviços'
    ]
    resultados = {}
    for empresa in lista_de_empresas:
        resultados_empresa = {}
        for trimestre in trimestres:
            periodo = trimestre
            ano, trim = trimestre.split("-")
            if trim == 'T4':
                t1 = calculando_dados_trimestrais(empresa, dre_trimestral_ano_anterior, ano, contas_desejadas, dt_ini = '01-01', dt_fim= '03-31')
                t2 = calculando_dados_trimestrais(empresa, dre_trimestral_ano_anterior, ano, contas_desejadas, dt_ini = '04-01', dt_fim= '06-30')
                t3 = calculando_dados_trimestrais(empresa, dre_trimestral_ano_anterior, ano, contas_desejadas, dt_ini = '07-01', dt_fim= '09-30')
                anual = calculando_dados_anuais(empresa, dre_anual, ano, contas_desejadas)
                
                resultado_lucro_prej_t4 = (
                    extrair_valor_por_conta(anual, 'Lucro/Prejuízo Consolidado do Período') -
                    (
                        extrair_valor_por_conta(t1, 'Lucro/Prejuízo Consolidado do Período') +
                        extrair_valor_por_conta(t2, 'Lucro/Prejuízo Consolidado do Período') +
                        extrair_valor_por_conta(t3, 'Lucro/Prejuízo Consolidado do Período')
                    )
                )
                receita_vendas_t4 = (
                    extrair_valor_por_conta(anual, "Receita de Venda de Bens e/ou Serviços") -
                    (
                        extrair_valor_por_conta(t1, "Receita de Venda de Bens e/ou Serviços") +
                        extrair_valor_por_conta(t2, "Receita de Venda de Bens e/ou Serviços") +
                        extrair_valor_por_conta(t3, "Receita de Venda de Bens e/ou Serviços")
                    )
                )
                
                # Calcula a margem liquida
                if receita_vendas_t4 and receita_vendas_t4 != 0:
                    margem_t4 = resultado_lucro_prej_t4 / receita_vendas_t4
                else:
                    margem_t4 = None  # Evita divisão por zero
                resultados_empresa[periodo] = margem_t4
                
            else:
                dados_periodo = calculando_dados_trimestrais(empresa, dre_trimestral_ano_anterior, ano, contas_desejadas, dt_ini=f"{trim_to_dt_ini(trim)}", dt_fim=f"{trim_to_dt_fim(trim)}")
                
                resultado_lucro_prej = extrair_valor_por_conta(dados_periodo, "Lucro/Prejuízo Consolidado do Período")
                receita_vendas = extrair_valor_por_conta(dados_periodo, "Receita de Venda de Bens e/ou Serviços")

                if receita_vendas and receita_vendas != 0:

                    margem = resultado_lucro_prej / receita_vendas
                else:
                    margem = None  # Evita divisão por zero
                resultados_empresa[periodo] = margem
        resultados[empresa] = resultados_empresa
        
    df = pd.DataFrame.from_dict(resultados, orient="index")
    df = pd.DataFrame.from_dict(resultados, orient="columns")
    df = df.dropna()
    df_resetado = df.reset_index()
    df_final = df_resetado.drop(columns=["index"])
    df_final = df_final.head(1)
    margem_liquida = df_final.melt(var_name="Empresa", value_name="Margem_liquida")
    return margem_liquida

def calculando_divida_bruta_patrimonio_liquido(lista_de_empresas, n_empresas, bpp):
    #criar uma lógica para calcular o valor do 4t
    pl_ajustado = pd.DataFrame()
    divida_bruta = pd.DataFrame()
    bpp = bpp.set_index("DENOM_CIA")
    for i in range(0, n_empresas):
        empresa = lista_de_empresas[i]
        try:
            dados_empresa = bpp.query("DENOM_CIA == @empresa")
            
            if dados_empresa.empty:
                print(f"A empresa {empresa} não foi encontrada no DataFrame bpp.")
                continue 

            pl_adj = (
            dados_empresa.loc[dados_empresa["DS_CONTA"] == "Patrimônio Líquido Consolidado", "VL_AJUSTADO"].iloc[-1] -
            dados_empresa.loc[dados_empresa["DS_CONTA"] == "Participação dos Acionistas Não Controladores", "VL_AJUSTADO"].iloc[-1]
            )
            
            pl_ajustado[empresa] = [pl_adj]
    
            dbpl = (
                dados_empresa.loc[dados_empresa["DS_CONTA"] == "Empréstimos e Financiamentos", "VL_AJUSTADO"].iloc[-2] +
                dados_empresa.loc[dados_empresa["DS_CONTA"] == "Empréstimos e Financiamentos", "VL_AJUSTADO"].iloc[0]
            )
            
            divida_bruta[empresa] = [dbpl]
        except Exception as e:
            print(f"Erro ao processar a empresa {empresa}: {e}")
            continue
    divida_bruta_pl = divida_bruta/pl_ajustado
    
    df_final_dbpl = divida_bruta_pl.melt(var_name="Empresa", value_name="Divida_bruta_pl")
    return df_final_dbpl, pl_ajustado

def calculando_caixa(lista_de_empresas, bpa):
    #criar uma lógica para calcular o valor do 4t
    caixa = pd.DataFrame()
    df_caixa = pd.DataFrame()
    bpa = bpa.set_index("DENOM_CIA")
    for empresa in lista_de_empresas:
        
        if empresa in bpa.index:
            filtro = (bpa.index == empresa) & (bpa['DS_CONTA'].isin(['Caixa e Equivalentes de Caixa', 'Aplicações Financeiras']))
            dados_filtrados = bpa.loc[filtro, ['DS_CONTA','ORDEM_EXERC', 'VL_AJUSTADO', 'DT_AJUSTADO']]
            
            if not dados_filtrados.empty:
                dados_filtrados = dados_filtrados.sort_values(by='DT_AJUSTADO', ascending=False) 
                dados_filtrados_ = dados_filtrados[dados_filtrados['ORDEM_EXERC'] != 'PENÚLTIMO']
                
                valores = {conta: dados_filtrados_.loc[dados_filtrados_['DS_CONTA'] == conta, 'VL_AJUSTADO'].iloc[0] 
                    for conta in dados_filtrados_['DS_CONTA'].unique()}
                
                caixa[empresa] = valores
                
            else:
                print(f"Nenhum dado encontrado para a empresa '{empresa}'.")
        else:
            print(f"A empresa '{empresa}' não foi encontrada no DataFrame 'bpa'.")

    df_caixa = pd.concat([df_caixa, caixa], axis=1)
    df_somado = df_caixa.sum().to_frame().T
    df_final = df_somado.melt(var_name="Empresa", value_name="Caixa")
    return df_final

def calculando_liquidez_corrente(lista_de_empresas, n_empresas, bpa, bpp):
    #criar uma lógica para calcular o valor do 4t
    liquidez_corrente = pd.DataFrame()
    ativo_circ = pd.DataFrame()
    passivo_circ = pd.DataFrame()
    bpa = bpa.set_index("DENOM_CIA")
    bpp = bpp.set_index("DENOM_CIA")
    

    for empresa in lista_de_empresas:
        if empresa in bpa.index:
            filtro = (bpa.index == empresa) & (bpa['DS_CONTA'].isin(['Ativo Circulante']))
            dados_filtrados = bpa.loc[filtro, ['DS_CONTA','ORDEM_EXERC', 'VL_AJUSTADO', 'DT_AJUSTADO']]
            
            if not dados_filtrados.empty:
                dados_filtrados = dados_filtrados.sort_values(by='DT_AJUSTADO', ascending=False) 
                dados_filtrados_ = dados_filtrados[dados_filtrados['ORDEM_EXERC'] != 'PENÚLTIMO']
                
                valores = {conta: dados_filtrados_.loc[dados_filtrados_['DS_CONTA'] == conta, 'VL_AJUSTADO'].iloc[0] 
                    for conta in dados_filtrados_['DS_CONTA'].unique()}
                
                ativo_circ[empresa] = valores
                
            else:
                print(f"Nenhum dado encontrado para a empresa '{empresa}'.")
        else:
            print(f"A empresa '{empresa}' não foi encontrada no DataFrame 'bpa'.")

    for empresa in lista_de_empresas:
        if empresa in bpp.index:
            filtro = (bpp.index == empresa) & (bpp['DS_CONTA'].isin(['Passivo Circulante']))
            dados_filtrados = bpp.loc[filtro, ['DS_CONTA',  'ORDEM_EXERC', 'VL_AJUSTADO', 'DT_AJUSTADO']]
            
            if not dados_filtrados.empty:
                dados_filtrados = dados_filtrados.sort_values(by='DT_AJUSTADO', ascending=False) 
                dados_filtrados_ = dados_filtrados[dados_filtrados['ORDEM_EXERC'] != 'PENÚLTIMO']
                
                valores = {conta: dados_filtrados_.loc[dados_filtrados_['DS_CONTA'] == conta, 'VL_AJUSTADO'].iloc[0] 
                    for conta in dados_filtrados_['DS_CONTA'].unique()}
                
                passivo_circ[empresa] = valores
                
            else:
                print(f"Nenhum dado encontrado para a empresa '{empresa}'.")
        else:
            print(f"A empresa '{empresa}' não foi encontrada no DataFrame 'bpa'.")
    ativo_circ = ativo_circ.reset_index().drop(columns='index')
    passivo_circ = passivo_circ.reset_index().drop(columns='index')
    
    liquidez_corrente = ativo_circ/passivo_circ
    df_final = liquidez_corrente.melt(var_name="Empresa", value_name="Liquidez_corrente")
    return df_final

def calculando_ebit(lista_de_empresas, dre_trimestral_ano_anterior, dre_anual, trimestres):

    contas_desejadas = [
        'Receita de Venda de Bens e/ou Serviços', 'Custo dos Bens e/ou Serviços Vendidos',
        'Despesas com Vendas', 'Despesas Gerais e Administrativas'
    ]

    resultados = {}
    for empresa in lista_de_empresas:
        resultados_empresa = {}
        for trimestre in trimestres:
            periodo = trimestre
            ano, trim = trimestre.split("-")
            if trim == 'T4':
                t1 = calculando_dados_trimestrais(empresa, dre_trimestral_ano_anterior, ano, contas_desejadas, dt_ini = '01-01', dt_fim= '03-31')
                t2 = calculando_dados_trimestrais(empresa, dre_trimestral_ano_anterior, ano, contas_desejadas, dt_ini = '04-01', dt_fim= '06-30')
                t3 = calculando_dados_trimestrais(empresa, dre_trimestral_ano_anterior, ano, contas_desejadas, dt_ini = '07-01', dt_fim= '09-30')
                anual = calculando_dados_anuais(empresa, dre_anual, ano, contas_desejadas)
                receita_vendas_t4 = (
                    extrair_valor_por_conta(anual, 'Receita de Venda de Bens e/ou Serviços') -
                    (
                        extrair_valor_por_conta(t1, 'Receita de Venda de Bens e/ou Serviços') +
                        extrair_valor_por_conta(t2, 'Receita de Venda de Bens e/ou Serviços') +
                        extrair_valor_por_conta(t3, 'Receita de Venda de Bens e/ou Serviços')
                    )
                )
                
                custo_bens_t4 = (
                    extrair_valor_por_conta(anual, 'Custo dos Bens e/ou Serviços Vendidos') -
                    (
                        extrair_valor_por_conta(t1, 'Custo dos Bens e/ou Serviços Vendidos') +
                        extrair_valor_por_conta(t2, 'Custo dos Bens e/ou Serviços Vendidos') +
                        extrair_valor_por_conta(t3, 'Custo dos Bens e/ou Serviços Vendidos')
                    )
                )
                despesa_vendas_t4 = (
                    extrair_valor_por_conta(anual, 'Despesas com Vendas') -
                    (
                        extrair_valor_por_conta(t1, 'Despesas com Vendas') +
                        extrair_valor_por_conta(t2, 'Despesas com Vendas') +
                        extrair_valor_por_conta(t3, 'Despesas com Vendas')
                    )
                )
                despesas_gerais_t4 = (
                    extrair_valor_por_conta(anual, 'Despesas Gerais e Administrativas') -
                    (
                        extrair_valor_por_conta(t1, 'Despesas Gerais e Administrativas') +
                        extrair_valor_por_conta(t2, 'Despesas Gerais e Administrativas') +
                        extrair_valor_por_conta(t3, 'Despesas Gerais e Administrativas')
                    )
                )

                margem_t4 = receita_vendas_t4 + custo_bens_t4 + despesa_vendas_t4 + despesas_gerais_t4
                
                resultados_empresa[periodo] = margem_t4
            else:
                dados_periodo = calculando_dados_trimestrais(empresa, dre_trimestral_ano_anterior, ano, contas_desejadas, dt_ini=f"{trim_to_dt_ini(trim)}", dt_fim=f"{trim_to_dt_fim(trim)}")
                
                custo_bens = extrair_valor_por_conta(dados_periodo, 'Custo dos Bens e/ou Serviços Vendidos')
                receita_vendas = extrair_valor_por_conta(dados_periodo, "Receita de Venda de Bens e/ou Serviços")
                despesa_vendas = extrair_valor_por_conta(dados_periodo, 'Despesas com Vendas')
                despesas_gerais = extrair_valor_por_conta(dados_periodo, 'Despesas Gerais e Administrativas')
                
                margem = custo_bens + receita_vendas + despesa_vendas + despesas_gerais
                
                resultados_empresa[periodo] = margem
        resultados[empresa] = resultados_empresa
        df = pd.DataFrame.from_dict(resultados, orient="index")
        df = pd.DataFrame.from_dict(resultados, orient="columns")
        df = df.dropna()
        df_resetado = df.reset_index()
        df_final = df_resetado.drop(columns=["index"])
        df_final = df_final.head(1)
        ebit = df_final.melt(var_name="Empresa", value_name="Ebit")
    return ebit

def calculando_roe(lista_de_empresas, n_empresas, bpp_trimestral_ano_anterior, bpp_trimestral_ano_corrente, 
                     dre_trimestral_ano_anterior, dre_trimestral_ano_corrente, dre_anual, bpp_anual):
    #refatorando o código. preciso criar uma for do código finalizar o loop
    contas_desejadas = [
            'Atribuído a Sócios da Empresa Controladora'
        ]
    resultados = pd.DataFrame()
    divida_bruta_pl, pl_ajustado = calculando_divida_bruta_patrimonio_liquido(lista_de_empresas, n_empresas, bpp_trimestral_ano_anterior)
    pl_ajustado_melted = pl_ajustado.melt(var_name='Empresa', value_name='pl')
    ano, trim = trimestres[0].split("-")
    if trimestres[0] == f'{ano}-T4':
        for empresa in lista_de_empresas:
    
            periodo_resultado = calculando_dados_anuais(empresa, dre_anual, ano, contas_desejadas)
            resultados_empresa = pd.DataFrame(periodo_resultado)
            resultados = pd.concat([resultados, resultados_empresa], ignore_index=True)
            
        df_merged = resultados.merge(pl_ajustado_melted, on='Empresa', how='left')
        df_merged['roe'] = df_merged['Valor'] / df_merged['pl']
        df_final = df_merged.drop(columns=['Conta', 'Valor', 'Ano', 'Periodo', 'pl'])
        return df_final
    else:
        #quando virar o trimestre preciso testar essa parte do código
        resultados_lista = []
        for empresa in lista_de_empresas:
            periodo_resultado = {'Empresa': empresa}
            periodo_resultado['Empresa'] = empresa
            
            for trimestre in trimestres:
                periodo = trimestre
                ano, trim = trimestre.split("-")
                
                if trim == 'T1':
                    t1 = calculando_dados_trimestrais(empresa, dre_trimestral_ano_anterior, ano, contas_desejadas, dt_ini = '01-01', dt_fim= '03-31')
                    t1 = extrair_valor_por_conta(t1, 'Atribuído a Sócios da Empresa Controladora')
                    periodo_resultado['t1'] = t1
                elif trim == 'T2':
                    t2 = calculando_dados_trimestrais(empresa, dre_trimestral_ano_anterior, ano, contas_desejadas, dt_ini = '04-01', dt_fim= '06-30')
                    t2 = extrair_valor_por_conta(t2, 'Atribuído a Sócios da Empresa Controladora')
                    periodo_resultado['t2'] = t2
                elif trim == 'T3': 
                    t3 = calculando_dados_trimestrais(empresa, dre_trimestral_ano_anterior, ano, contas_desejadas, dt_ini = '07-01', dt_fim= '09-30')
                    t3 = extrair_valor_por_conta(t3, 'Atribuído a Sócios da Empresa Controladora')
                    periodo_resultado['t3'] = t3
                else:
                    t1_para_t4 = calculando_dados_trimestrais(empresa, dre_trimestral_ano_anterior, ano, contas_desejadas, dt_ini = '01-01', dt_fim= '03-31')
                    t2_para_t4 = calculando_dados_trimestrais(empresa, dre_trimestral_ano_anterior, ano, contas_desejadas, dt_ini = '04-01', dt_fim= '06-30')
                    t3_para_t4 = calculando_dados_trimestrais(empresa, dre_trimestral_ano_anterior, ano, contas_desejadas, dt_ini = '07-01', dt_fim= '09-30')
                    anual_para_t4 = calculando_dados_anuais(empresa, dre_anual, ano, contas_desejadas)
                    t4 = (
                            extrair_valor_por_conta(anual_para_t4, contas_desejadas) -
                            (
                                extrair_valor_por_conta(t1_para_t4, contas_desejadas) +
                                extrair_valor_por_conta(t2_para_t4, contas_desejadas) +
                                extrair_valor_por_conta(t3_para_t4, contas_desejadas)
                            )
                        )
                    periodo_resultado['t4'] = t4
                resultados_lista.append(periodo_resultado) 
        df= pd.DataFrame(resultados_lista)
        df = df.groupby('Empresa', as_index=False).first()

        df_merged = df.merge(pl_ajustado_melted, on='Empresa', how='left')
        df_merged['roe'] = (df_merged['t1']+df_merged['t2']+df_merged['t3']+df_merged['t4']) / df_merged['pl']
        df_final = df_merged.drop(columns=['t4', 't3', 't2', 't1','pl'])
        return df_final

def calculando_ebit_ano(lista_de_empresas, dre_trimestral_ano_anterior, dre_anual, trimestres):
    
    # EBIT ANO
    # É A SOMA DA RECEITA LIQUIDA DOS ÚLTIMOS 4 TRIMESTRES
    #estou em março 2025, portanto devo considerar os 4 trimestres do ano anterior
    contas_desejadas = [
            'Receita de Venda de Bens e/ou Serviços',
            'Custo dos Bens e/ou Serviços Vendidos',
            'Despesas com Vendas',
            'Despesas Gerais e Administrativas'
        ]
    resultados = pd.DataFrame()
    df_final = pd.DataFrame()
    ano, trim = trimestres[0].split("-")
    if trimestres[0] == f'{ano}-T4':
        for empresa in lista_de_empresas:
            resultados_empresa = pd.DataFrame()
            periodo_resultado = calculando_dados_anuais(empresa, dre_anual, ano, contas_desejadas)
            resultados_empresa = pd.DataFrame(periodo_resultado)
            resultados_empresa['Ebit_ano'] = resultados_empresa['Valor'].sum()
            resultados = pd.concat([resultados, resultados_empresa.head(1)], ignore_index=True)
            
        df_final = pd.concat([df_final, resultados], ignore_index=True)
        df_final = df_final.drop(columns=['Conta', 'Valor', 'Ano', 'Periodo'])
        return df_final
    else:
        resultados_lista = []
        for empresa in lista_de_empresas:

            periodo_resultado = {'Empresa': empresa}
            periodo_resultado['Empresa'] = empresa
            for trimestre in trimestres:
                ano, trim = trimestre.split("-")
                if trim == 'T1':
                    dt_ini = '01-01'
                    dt_fim = '03-31'
                    liq_t1 = calculando_dados_trimestrais(empresa, dre_trimestral_ano_anterior, ano, contas_desejadas, dt_ini, dt_fim)
                    liq_t1 = extrair_valor_por_conta(liq_t1, contas_desejadas)
                    periodo_resultado['liq_t1'] = float(liq_t1)
                elif trim == 'T2':
                    dt_ini = '04-01'
                    dt_fim = '06-30'
                    liq_t2 = calculando_dados_trimestrais(empresa, dre_trimestral_ano_anterior, ano, contas_desejadas, dt_ini, dt_fim)
                    liq_t2 = extrair_valor_por_conta(liq_t2, contas_desejadas)
                    periodo_resultado['liq_t2'] = float(liq_t2)
                elif trim == 'T3':
                    dt_ini = '07-01'
                    dt_fim = '09-30'
                    liq_t3 = calculando_dados_trimestrais(empresa, dre_trimestral_ano_anterior, ano, contas_desejadas, dt_ini, dt_fim)
                    liq_t3 = extrair_valor_por_conta(liq_t3, contas_desejadas)
                    periodo_resultado['liq_t3'] = float(liq_t3)
                else:
                    t1_para_t4 = calculando_dados_trimestrais(empresa, dre_trimestral_ano_anterior, ano, contas_desejadas, dt_ini = '01-01', dt_fim= '03-31')
                    t2_para_t4 = calculando_dados_trimestrais(empresa, dre_trimestral_ano_anterior, ano, contas_desejadas, dt_ini = '04-01', dt_fim= '06-30')
                    t3_para_t4 = calculando_dados_trimestrais(empresa, dre_trimestral_ano_anterior, ano, contas_desejadas, dt_ini = '07-01', dt_fim= '09-30')
                    anual_para_t4 = calculando_dados_anuais(empresa, dre_anual, ano, contas_desejadas)
                    liq_t4 = (
                                extrair_valor_por_conta(anual_para_t4, contas_desejadas) -
                                (
                                    extrair_valor_por_conta(t1_para_t4, contas_desejadas) +
                                    extrair_valor_por_conta(t2_para_t4, contas_desejadas) +
                                    extrair_valor_por_conta(t3_para_t4, contas_desejadas)
                                )
                            )
                    periodo_resultado['liq_t4'] = float(liq_t4)
                resultados_lista.append(periodo_resultado)
            df= pd.DataFrame(resultados_lista)
            df = df.groupby('Empresa', as_index=False).first()
        df['ebit_ano'] = df[['liq_t1', 'liq_t2', 'liq_t3', 'liq_t4']].sum(axis=1)
        ebit_ano = df.drop(columns=['liq_t1', 'liq_t2', 'liq_t3', 'liq_t4'])
        return ebit_ano
  
def calculando_ebit_ativo(ebit_ano, bpa_trimestral, bpa_anual, lista_de_empresas, trimestres):
    
    contas_desejadas = [
        'Ativo Total'
    ]
    bpa_trimestral = bpa_trimestral.set_index("DENOM_CIA")
    ebit_ativo = pd.DataFrame()
    # resultados_lista = []
    # ano, trim = trimestres[0].split("-")
    # if trimestres[0] == f'{ano}-T4':
        
    #     for empresa in lista_de_empresas:
    #         periodo_resultado = {'Empresa': empresa}
    #         periodo_resultado['Empresa'] = empresa
    #         t1_para_t4 = calculando_dados_trimestrais(empresa, bpa_trimestral, ano, contas_desejadas, dt_ini = '01-01', dt_fim= '03-31')
    #         t2_para_t4 = calculando_dados_trimestrais(empresa, bpa_trimestral, ano, contas_desejadas, dt_ini = '04-01', dt_fim= '06-30')
    #         t3_para_t4 = calculando_dados_trimestrais(empresa, bpa_trimestral, ano, contas_desejadas, dt_ini = '07-01', dt_fim= '09-30')
    #         anual_para_t4 = calculando_dados_anuais(empresa, bpa_anual, ano, contas_desejadas)
    #         t4 = (
    #                 extrair_valor_por_conta(anual_para_t4, contas_desejadas) -
    #                 (
    #                     extrair_valor_por_conta(t1_para_t4, contas_desejadas) +
    #                     extrair_valor_por_conta(t2_para_t4, contas_desejadas) +
    #                     extrair_valor_por_conta(t3_para_t4, contas_desejadas)
    #                 )
    #             )
    #         periodo_resultado['t4'] = t4
    #         resultados_lista.append(periodo_resultado)
    #     df = pd.DataFrame(resultados_lista)
    #     df['ebit_ativo'] = ebit_ano['Ebit_ano']/df['t4']
    #     a=1 
    # else:
    for empresa in lista_de_empresas:
        if empresa in bpa_trimestral.index:
            filtro = (bpa_trimestral.index == empresa) & (bpa_trimestral['DS_CONTA'] == 'Ativo Total')
            
            dados_filtrados = bpa_trimestral.loc[filtro, 'VL_AJUSTADO']
            
            if not dados_filtrados.empty:
                ativo_total = dados_filtrados.iloc[-1]  # Pegar o último valor
                ebit_ativo[empresa] = [ativo_total]
            else:
                print(f"Nenhum dado encontrado para 'Ativo Total' da empresa '{empresa}'.")
                continue
        else:
            print(f"A empresa '{empresa}' não foi encontrada no DataFrame 'bpa'.")
    df_ebit_ativo = ebit_ativo.T.reset_index()
    df_ebit_ativo.columns = ['Empresa', 'Ebit']
    df_combinado = pd.merge(df_ebit_ativo, ebit_ano, on='Empresa')
    df_combinado['Ebitda'] = df_combinado['Ebit_ano']/df_combinado['Ebit']
    ebitda = df_combinado.drop(columns=['Ebit', 'Ebit_ano'])
    return ebitda

def calculando_roic(ebit_ano, pl_ajustado, divida_bruta_pl_df, caixa_ajustado):
    
    pl = pl_ajustado.melt(var_name="Empresa", value_name="Margem_bruta")
    df_combinado = pd.merge(ebit_ano, pl, on='Empresa', how='inner')
    df_combinado = pd.merge(df_combinado, divida_bruta_pl_df, on='Empresa', how='inner')
    df_combinado = pd.merge(df_combinado, caixa_ajustado, on='Empresa', how='inner')
    df_combinado['Roic'] = df_combinado['Ebit_ano'] / (
    df_combinado['Margem_bruta'] +
    df_combinado['Divida_bruta_pl'] +
    df_combinado['Caixa']
)   
    roic = df_combinado.drop(columns=['Ebit_ano', 'Margem_bruta', 'Divida_bruta_pl', 'Caixa'])
    #roic['Roic'] = ebit_ano['Ebit_ano']/(pl['Margem_bruta']+divida_bruta_pl_df['Divida_bruta_pl']+caixa_ajustado['Caixa'])
    return roic


start_time = time.time()

# #testando o código
lista_cvm_ativos = ['20494', '20605', '20982']
# trimestres, primeiro_trimestre, ano_corrente, ano_anterior = verificar_trimestres(lista_cvm_ativos) 
trimestres = ['2024-T4', '2024-T3', '2024-T2', '2024-T1']
primeiro_trimestre = 'T4'
ano_corrente = '2025'
ano_anterior = '2024'
lista_de_empresas, n_empresas, dre_anual, bpa_anual, bpp_anual, dre_trimestral_ano_anterior, bpa_trimestral_ano_anterior, bpp_trimestral_ano_anterior, dre_trimestral_ano_corrente, bpa_trimestral_ano_corrente, bpp_trimestral_ano_corrente = processando_arquivos(primeiro_trimestre, lista_cvm_ativos, ano_corrente, ano_anterior)

##################
margem_bruta = calculando_margem_bruta(lista_de_empresas, dre_trimestral_ano_corrente, dre_trimestral_ano_anterior, dre_anual, trimestres)
print('Margem Bruta:')
print(margem_bruta)

margem_liquida = calculando_margem_liquida(lista_de_empresas, trimestres, dre_trimestral_ano_corrente, dre_trimestral_ano_anterior)
print('Margem Liquida:')
print(margem_liquida)

divida_bruta_pl, pl_ajustado = calculando_divida_bruta_patrimonio_liquido(lista_de_empresas,n_empresas, bpp_trimestral_ano_anterior)
print('Divida_bruta:')
print(divida_bruta_pl)

caixa = calculando_caixa(lista_de_empresas, bpa_trimestral_ano_anterior)
print('Caixa:')
print(caixa)

liquidez_corrente = calculando_liquidez_corrente(lista_de_empresas,n_empresas, bpa_trimestral_ano_anterior, bpp_trimestral_ano_anterior)
print('Liquidez corrente:')
print(liquidez_corrente)

ebit = calculando_ebit(lista_de_empresas, dre_trimestral_ano_anterior, dre_anual, trimestres)
print('EBIT:')
print(ebit)

roe = calculando_roe(lista_de_empresas, n_empresas, bpp_trimestral_ano_anterior, bpp_trimestral_ano_corrente, 
                     dre_trimestral_ano_anterior, dre_trimestral_ano_corrente, dre_anual, bpp_anual)
print('ROE:')
print(roe)

ebit_ano = calculando_ebit_ano(lista_de_empresas, dre_trimestral_ano_anterior, dre_anual, trimestres)
print('EBIT ANO:')
print(ebit_ano)

ebit_ativo = calculando_ebit_ativo(ebit_ano, bpa_trimestral_ano_anterior, bpa_anual, lista_de_empresas, trimestres)
print('EBIT ATIVO:')
print(ebit_ativo)

roic = calculando_roic(ebit_ano, pl_ajustado, divida_bruta_pl, caixa)
print('ROIC:')
print(roic)
print('O tempo de execução desse programa foi de %s segundos---' % (time.time() - start_time))

