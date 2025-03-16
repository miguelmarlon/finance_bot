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

def obter_trimestres(lista_cvm_ativos):
    # Essa função está com um erro quando busco dados de anos diferentes
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
    primeiro_ano = trimestres[0].split('-')[0]
    primeiro_ano = int(primeiro_ano)
    ano_anterior = primeiro_ano - 1
    
    if primeiro_trimestre == 'T4':
        extrair_demonstrativos_trimestrais(lista_cvm_ativos, str(primeiro_ano))
        extrair_demonstrativos_anuais(lista_cvm_ativos, str(primeiro_ano))
        
    else:
        extrair_demonstrativos_trimestrais(lista_cvm_ativos, str(primeiro_ano))
        extrair_demonstrativos_trimestrais(lista_cvm_ativos, str(ano_anterior))
        extrair_demonstrativos_anuais(lista_cvm_ativos, str(ano_anterior))
        
    return trimestres

def processando_arquivos():
    caminho = os.getcwd()
    arquivos = os.listdir(caminho)

    ano_corrente = 2024
    ano_anterior = 2023

    padroes = {
        "anual": r"Demonstrativos Anual Empresa \d+_\d{4}\.xlsx",
        "trimestral_corrente": f"Demonstrativos Trimestrais Empresa \d+_{ano_corrente}\.xlsx",
        "trimestral_anterior": f"Demonstrativos Trimestrais Empresa \d+_{ano_anterior}\.xlsx",
    }

    grupos_de_arquivos = filtrar_e_organizar_arquivos(arquivos, padroes)

    trimestral_ano_corrente = "trimestral_corrente"  # Altere para o grupo que você quer processar

    # Verificar se o grupo existe no dicionário
    if trimestral_ano_corrente in grupos_de_arquivos:
        arquivos_trimestral_ano_corrente = grupos_de_arquivos[trimestral_ano_corrente]
        print(f"Processando grupo: {trimestral_ano_corrente}")
        lista_de_empresas, n_empresas, dre_trimestral_ano_corrente, bpa_trimestral_ano_corrente, bpp_trimestral_ano_corrente = criando_tabelas_dre_bpa_bpp(arquivos_trimestral_ano_corrente)
        print(f"Arquivos processados no grupo '{trimestral_ano_corrente}': {arquivos_trimestral_ano_corrente}")
    else:
        print(f"O grupo '{trimestral_ano_corrente}' não foi encontrado!")

    trimestral_ano_anterior = "trimestral_anterior"  # Altere para o grupo que você quer processar

    # Verificar se o grupo existe no dicionário
    if trimestral_ano_anterior in grupos_de_arquivos:
        arquivos_trimestral_ano_anterior = grupos_de_arquivos[trimestral_ano_anterior]
        print(f"Processando grupo: {trimestral_ano_anterior}")
        lista_de_empresas, n_empresas, dre_trimestral_ano_anterior, bpa_trimestral_ano_anterior, bpp_trimestral_ano_anterior = criando_tabelas_dre_bpa_bpp(arquivos_trimestral_ano_anterior)
        print(f"Arquivos processados no grupo '{trimestral_ano_anterior}': {arquivos_trimestral_ano_anterior}")
    else:
        print(f"O grupo '{trimestral_ano_anterior}' não foi encontrado!")

    anual = "anual"  # Altere para o grupo que você quer processar

    # Verificar se o grupo existe no dicionário
    if anual in grupos_de_arquivos:
        arquivos_anual = grupos_de_arquivos[anual]
        print(f"Processando grupo: {anual}")
        lista_de_empresas, n_empresas, dre_anual = criando_tabelas_dre_bpa_bpp(arquivos_anual)
        print(f"Arquivos processados no grupo '{anual}': {arquivos_anual}")
    else:
        print(f"O grupo '{anual}' não foi encontrado!")
    return lista_de_empresas, n_empresas, dre_anual, dre_trimestral_ano_anterior, bpa_trimestral_ano_anterior, bpp_trimestral_ano_anterior, dre_trimestral_ano_corrente, bpa_trimestral_ano_corrente, bpp_trimestral_ano_corrente

def extrair_demonstrativos_trimestrais(lista_cvm_ativos, ano):
    #recebe a lista de ativos para pesquisa
    start_time = time.time()
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
    print('O tempo de execução desse programa foi de %s segundos---' % (time.time() - start_time))

def extrair_demonstrativos_anuais(lista_cvm_ativos, ano):
    start_time = time.time()
    demonstrativos = ['DRE']
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
    print('O tempo de execução desse programa foi de %s segundos---' % (time.time() - start_time))

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

def calculando_margem_bruta(lista_de_empresas, n_empresas, dre):
    margem_bruta = pd.DataFrame()
    for i in range(n_empresas):
        calculo_margem = pd.Series((dre.loc[lista_de_empresas[i],:].loc['Resultado Bruto'].iloc[-1])/(dre.loc[lista_de_empresas[i],:].loc['Receita de Venda de Bens e/ou Serviços'].iloc[-1]))

        margem_bruta = pd.concat([margem_bruta, calculo_margem], axis=1)
    margem_bruta.columns = lista_de_empresas
    return margem_bruta

def calculando_margem_liquida(lista_de_empresas, n_empresas, dre):
    margem_liquida = pd.DataFrame()
    for i in range(0, n_empresas):
        calculo_margem_liq = pd.Series((dre.loc[lista_de_empresas[i],:].loc['Lucro/Prejuízo Consolidado do Período'].iloc[-1])/(dre.loc[lista_de_empresas[i],:].loc['Receita de Venda de Bens e/ou Serviços'].iloc[-1]))
        margem_liquida = pd.concat([margem_liquida, calculo_margem_liq], axis=1)
    margem_liquida.columns = lista_de_empresas
    return margem_liquida

def calculando_divida_bruta_patrimonio_liquido(lista_de_empresas, n_empresas, bpp):
    pl_ajustado = pd.DataFrame()
    divida_bruta = pd.DataFrame()
    for i in range(0, n_empresas):
        empresa = lista_de_empresas[i]
        try:
            dados_empresa = bpp[bpp["DENOM_CIA"] == empresa]
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
    divida_bruta_pl
    return divida_bruta_pl, pl_ajustado

def calculando_caixa(lista_de_empresas, bpa):

    caixa = pd.DataFrame()
    df_caixa = pd.DataFrame()
    bpa = bpa.set_index("DENOM_CIA")
    for empresa in lista_de_empresas:
        
        if empresa in bpa.index:
            filtro = (bpa.index == empresa) & (bpa['DS_CONTA'].isin(['Caixa e Equivalentes de Caixa', 'Aplicações Financeiras']))
            
            dados_filtrados = bpa.loc[filtro, ['DS_CONTA', 'VL_AJUSTADO', 'DT_AJUSTADO']]
            
            if not dados_filtrados.empty:
                dados_filtrados = dados_filtrados.sort_values(by='DT_REFER', ascending=False).drop_duplicates(subset=['DS_CONTA'])
            
                valores = {conta: dados_filtrados.loc[dados_filtrados['DS_CONTA'] == conta, 'VL_AJUSTADO'].iloc[0] 
                    for conta in dados_filtrados['DS_CONTA'].unique()}
                
                caixa[empresa] = valores
                
            else:
                print(f"Nenhum dado encontrado para a empresa '{empresa}'.")
        else:
            print(f"A empresa '{empresa}' não foi encontrada no DataFrame 'bpa'.")

    df_caixa = pd.concat([df_caixa, caixa], axis=1)
    df_somado = df_caixa.sum().to_frame().T
    return df_somado

def calculando_liquidez_corrente(lista_de_empresas, n_empresas, bpa):
    liquidez_corrente = pd.DataFrame()
    for i in range(0, n_empresas):
        liq_corrente = pd.Series((bpa.loc[lista_de_empresas[i],:].loc['Ativo Circulante'].iloc[-1])/(bpp.loc[lista_de_empresas[i],:].loc['Passivo Circulante'].iloc[-1,3]))
        liquidez_corrente = pd.concat([liquidez_corrente, liq_corrente], axis=1)
    liquidez_corrente.columns = lista_de_empresas
    return liquidez_corrente

def calculando_ebit(lista_de_empresas, n_empresas, dre):
    # ebit_ajustado = pd.DataFrame()
    # for i in range(0, n_empresas):
    #     ebit_ajustado_ = pd.Series(dre.loc[lista_de_empresas[i],:].loc['Receita de Venda de Bens e/ou Serviços'].iloc[-1] +
    #                                 dre.loc[lista_de_empresas[i],:].loc['Custo dos Bens e/ou Serviços Vendidos'].iloc[-1]+
    #                                 dre.loc[lista_de_empresas[i],:].loc['Despesas com Vendas'].iloc[-1]+
    #                                 dre.loc[lista_de_empresas[i],:].loc['Despesas Gerais e Administrativas'].iloc[-1])
    #     ebit_ajustado = pd.concat([ebit_ajustado, ebit_ajustado_], axis=1)
    # ebit_ajustado.columns = lista_de_empresas
    # #return ebit_ajustado

    dre = dre.set_index("DENOM_CIA")
    ebit_ajustado = pd.DataFrame()
    ebit = pd.DataFrame()
    for empresa in lista_de_empresas:
        
        if empresa in dre.index:
            filtro = (dre.index == empresa) & (dre['DS_CONTA'].isin(['Receita de Venda de Bens e/ou Serviços', 'Custo dos Bens e/ou Serviços Vendidos',
                                                                     'Despesas com Vendas', 'Despesas Gerais e Administrativas']))
            
            dados_filtrados = dre.loc[filtro, ['DS_CONTA', 'VL_AJUSTADO', 'DT_AJUSTADO']]
            
            if not dados_filtrados.empty:
                dados_filtrados = dados_filtrados.sort_values(by='DT_AJUSTADO', ascending=False).drop_duplicates(subset=['DS_CONTA'])
            
                valores = {conta: dados_filtrados.loc[dados_filtrados['DS_CONTA'] == conta, 'VL_AJUSTADO'].iloc[0] 
                    for conta in dados_filtrados['DS_CONTA'].unique()}
                
                ebit[empresa] = valores
                
            else:
                print(f"Nenhum dado encontrado para a empresa '{empresa}'.")
        else:
            print(f"A empresa '{empresa}' não foi encontrada no DataFrame 'bpa'.")

    ebit_ajustado = pd.concat([ebit_ajustado, ebit], axis=1)
    print(ebit_ajustado)
    df_somado = ebit_ajustado.sum().to_frame().T
    return df_somado

def calculando_roe(lista_de_empresas, n_empresas, bpp_trimestral_ano_corrente, dre_trimestral_ano_anterior):

    dados_trimestrais = []
    for empresa in lista_de_empresas:
        dre_empresa = dre_trimestral_ano_anterior[dre_trimestral_ano_anterior["DENOM_CIA"] == empresa]
        resultados_empresa = {"Empresa": empresa}

        for trimestre, (inicio, fim) in {
            "1T23": ("2023-01-01", "2023-03-31"),
            "2T23": ("2023-04-01", "2023-06-30"),
            "3T23": ("2023-07-01", "2023-09-30")
        }.items():
            filtro = (
                (dre_empresa["DT_INI_EXERC"] == inicio) & 
            (dre_empresa["DT_FIM_EXERC"] == fim) &
            (dre_empresa["DS_CONTA"] == "Atribuído a Sócios da Empresa Controladora")
            )
            resultados_empresa[trimestre] = dre_empresa.loc[filtro, "VL_AJUSTADO"].sum()
        dados_trimestrais.append(resultados_empresa)

    df_trimestres = pd.DataFrame(dados_trimestrais)

    dados_anuais = []

    for empresa in lista_de_empresas:
        dre_empresa = dre_anual[dre_anual["DENOM_CIA"] == empresa]
        resultados_empresa = {"Empresa": empresa}

        for trimestre, (inicio, fim) in {
            "anual": ("2023-01-01", "2023-12-31")
        }.items():
            filtro = (
                (dre_empresa["DT_INI_EXERC"] == inicio) & 
            (dre_empresa["DT_FIM_EXERC"] == fim) &
            (dre_empresa["DS_CONTA"] == "Atribuído a Sócios da Empresa Controladora")
            )
            resultados_empresa[trimestre] = dre_empresa.loc[filtro, "VL_AJUSTADO"].sum()
        dados_anuais.append(resultados_empresa)

    df_anual = pd.DataFrame(dados_anuais)

    df_combinado_trimestres = pd.merge(df_trimestres, df_anual, on="Empresa")    

    df_combinado_trimestres["4T23"] = (
        df_combinado_trimestres["anual"] 
        - df_combinado_trimestres["1T23"] 
        - df_combinado_trimestres["2T23"] 
        - df_combinado_trimestres["3T23"]
    )

    #print(df_combinado_trimestres)
    #calculo ROE = 4 últimos trimestres / pl_ajustado

    divida_bruta_pl, pl_ajustado = calculando_divida_bruta_patrimonio_liquido(lista_de_empresas, n_empresas, bpp_trimestral_ano_corrente)
    #print(pl_ajustado)

    df_combinado_trimestres["Soma_Trimestres"] = df_combinado_trimestres[["1T23", "2T23", "3T23", "4T23"]].sum(axis=1)
    pl_ajustado_transposto = pl_ajustado.T.reset_index()
    pl_ajustado_transposto.columns = ["Empresa", "Valor"]
    df_final = pd.merge(df_combinado_trimestres, pl_ajustado_transposto, left_on="Empresa", right_on="Empresa")
    df_final["roe"] = df_final["Soma_Trimestres"] / df_final["Valor"]
    return df_final

def calculando_liquidez_trimestral(lista_de_empresas, dre_trimestral_ano_anterior ,ano, dt_ini, dt_fim):
    #função complementar a função calculando_ebit_ano
    receita_liq_trimestral = pd.DataFrame(columns=["Empresa", "Receita Líquida"])
    contas_desejadas = [
        "Receita de Venda de Bens e/ou Serviços",
        "Custo dos Bens e/ou Serviços Vendidos",
        "Despesas com Vendas",
        "Despesas Gerais e Administrativas"
    ]
    
    for empresa in lista_de_empresas:
        filtro_trim = pd.Series(
            (dre_trimestral_ano_anterior["DT_INI_EXERC"] == f"{ano}-{dt_ini}") &
            (dre_trimestral_ano_anterior["DT_FIM_EXERC"] == f"{ano}-{dt_fim}") &
            (dre_trimestral_ano_anterior["ORDEM_EXERC"] == "ÚLTIMO") &
            (dre_trimestral_ano_anterior["DS_CONTA"].isin(contas_desejadas)) &
            (dre_trimestral_ano_anterior["DENOM_CIA"] == empresa)
        )
        
        dados_trimestral = dre_trimestral_ano_anterior.loc[filtro_trim, ["DS_CONTA", "VL_AJUSTADO"]]
        
        receita_liq = dados_trimestral['VL_AJUSTADO'].sum()

        # Adicionando ao DataFrame final
        receita_liq_trimestral = pd.concat([receita_liq_trimestral, pd.DataFrame([[empresa, receita_liq]], columns=["Empresa", "Receita Líquida"])], ignore_index=True)
    return receita_liq_trimestral

def calculando_liquidez_anual(lista_de_empresas, dre_anual, ano):
    #função complementar a função calculando_ebit_ano
    receita_liq_anual= pd.DataFrame(columns=["Empresa", "Receita Líquida"])
    contas_desejadas = [
        "Receita de Venda de Bens e/ou Serviços",
        "Custo dos Bens e/ou Serviços Vendidos",
        "Despesas com Vendas",
        "Despesas Gerais e Administrativas"
    ]
    
    for empresa in lista_de_empresas:
        filtro_anual = pd.Series(
            (dre_anual["DT_INI_EXERC"] == "2023-01-01") &
            (dre_anual["DT_FIM_EXERC"] == "2023-12-31") &
            (dre_anual["ORDEM_EXERC"] == "ÚLTIMO") &
            (dre_anual["DS_CONTA"].isin(contas_desejadas)) &
            (dre_anual["DENOM_CIA"] == empresa)
        )
        dados_anual = dre_anual.loc[filtro_anual, ["DS_CONTA", "VL_AJUSTADO"]]
        
        receita_liq = dados_anual['VL_AJUSTADO'].sum()

        # Adicionando ao DataFrame final
        receita_liq_anual = pd.concat([receita_liq_anual, pd.DataFrame([[empresa, receita_liq]], columns=["Empresa", "Receita Líquida"])], ignore_index=True)
    
    return receita_liq_anual

def calculando_ebit_ano(lista_de_empresas, dre_trimestral_ano_anterior, dre_anual, trimestres):
    
    # EBIT ANO
    # É A SOMA DA RECEITA LIQUIDA DOS ÚLTIMOS 4 TRIMESTRES
    #estou em março 2025, portanto devo considerar os 4 trimestres do ano anterior
    for trimestre in trimestres:
        ano, trim = trimestre.split("-")
        if trim != 'T4':
            if trim == 'T1':
                dt_ini = '01-01'
                dt_fim = '03-31'
                liq_t1 = calculando_liquidez_trimestral(lista_de_empresas, dre_trimestral_ano_anterior, ano, dt_ini, dt_fim)
                
            if trim == 'T2':
                dt_ini = '04-01'
                dt_fim = '06-30'
                liq_t2 = calculando_liquidez_trimestral(lista_de_empresas, dre_trimestral_ano_anterior, ano, dt_ini, dt_fim)
                
            if trim == 'T3':
                dt_ini = '07-01'
                dt_fim = '09-30'
                liq_t3 = calculando_liquidez_trimestral(lista_de_empresas, dre_trimestral_ano_anterior, ano, dt_ini, dt_fim)
                
        else:
            liq_ano = calculando_liquidez_anual(lista_de_empresas, dre_anual, ano)
            
   
    liq_t3 = liq_t3.set_index("Empresa")
    liq_ano = liq_ano.set_index("Empresa")
    liq_t4 = liq_ano - liq_t3
    
    liq_t1 = liq_t1.set_index("Empresa")
    liq_t2 = liq_t2.set_index("Empresa")
    
    ebit_ano = liq_t1 + liq_t2 + liq_t3 + liq_t4
    
    ebit_ano = ebit_ano.T.reset_index(drop=True)
    return ebit_ano      
  
def calculando_ebit_ativo(ebit_ano, bpa, lista_de_empresas):
    
    bpa = bpa.set_index("DENOM_CIA")
    
    ebit_ativo = pd.DataFrame()
    for empresa in lista_de_empresas:
        if empresa in bpa.index:
            filtro = (bpa.index == empresa) & (bpa['DS_CONTA'] == 'Ativo Total')
            
            dados_filtrados = bpa.loc[filtro, 'VL_AJUSTADO']
            
            if not dados_filtrados.empty:
                ativo_total = dados_filtrados.iloc[-1]  # Pegar o último valor
                ebit_ativo[empresa] = [ativo_total]
            else:
                print(f"Nenhum dado encontrado para 'Ativo Total' da empresa '{empresa}'.")
                continue
        else:
            print(f"A empresa '{empresa}' não foi encontrada no DataFrame 'bpa'.")
    ebit_ativo.columns = lista_de_empresas
    ebit_ativo_final = ebit_ano/ebit_ativo

    # ebit_ativo_long = ebit_ativo.melt(var_name="Empresa", value_name="ebit_ativo")
    # df_merged = pd.merge(ebit_ano, ebit_ativo_long, on="Empresa", how="inner")
    # df_merged["EBIT/Ativo"] = df_merged["ebit_ano"] / df_merged["ebit_ativo"]
    # df_merged = df_merged.drop(columns=["ebit_ano", "ebit_ativo"])
    #df_merged = df_merged.T.set_index('Empresa')
    return ebit_ativo_final

def calculando_roic(ebit_ano, pl_ajustado, divida_bruta_pl_df, caixa_ajustado):
    roic = ebit_ano/(pl_ajustado+divida_bruta_pl_df+caixa_ajustado)
    return roic

# #testando o código
lista_cvm_ativos = ['20494', '20605', '20982']
#trimestres = obter_trimestres(lista_cvm_ativos) 
lista_de_empresas, n_empresas, dre_anual, dre_trimestral_ano_anterior, bpa_trimestral_ano_anterior, bpp_trimestral_ano_anterior, dre_trimestral_ano_corrente, bpa_trimestral_ano_corrente, bpp_trimestral_ano_corrente = processando_arquivos()

##################
# extrair_demonstrativos_trimestrais(lista_cvm_ativos, 2024)
# extrair_demonstrativos_trimestrais(lista_cvm_ativos, 2023)
# extrair_demonstrativos_anuais(lista_cvm_ativos, 2023)

margem_bruta = calculando_margem_bruta(lista_de_empresas, n_empresas, dre_trimestral_ano_corrente)
print('Margem Bruta:')
print(margem_bruta)

# margem_liquida = calculando_margem_liquida(lista_de_empresas,n_empresas, dre_trimestral_ano_corrente)
# print('Margem Liquida:')
# print(margem_liquida)

# divida_bruta_pl, pl_ajustado = calculando_divida_bruta_patrimonio_liquido(lista_de_empresas,n_empresas, bpp_trimestral_ano_corrente)
# print('Divida_bruta:')
# print(divida_bruta_pl)

# caixa = calculando_caixa(lista_de_empresas, bpa_trimestral_ano_anterior)
# print('Caixa:')
# print(caixa)

# liquidez_corrente = calculando_liquidez_corrente(lista_de_empresas,n_empresas, bpa_trimestral_ano_corrente)
# print('Liquidez corrente:')
# print(liquidez_corrente)

# ebit = calculando_ebit(lista_de_empresas, n_empresas, dre_trimestral_ano_corrente)
# print('EBIT:')
# print(ebit)

# roe = calculando_roe(lista_de_empresas, n_empresas, bpp_trimestral_ano_corrente, dre_trimestral_ano_anterior)
# print('ROE:')
# print(roe['roe'])

# ebit_ano = calculando_ebit_ano(lista_de_empresas, dre_trimestral_ano_anterior, dre_anual, trimestres)
# print('EBIT ANO:')
# print(ebit_ano)

# ebit_ativo = calculando_ebit_ativo(ebit_ano, bpa_trimestral_ano_corrente, lista_de_empresas)
# print('EBIT ATIVO:')
# print(ebit_ativo)

# roic = calculando_roic(ebit_ano, pl_ajustado, divida_bruta_pl, caixa)
# print('ROIC:')
# print(roic)


