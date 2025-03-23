import os
import sys
from dotenv import load_dotenv
from crewai import Agent, Task, Crew, Process
from crewai_tools import SerperDevTool
from datetime import date
from langchain_community.chat_models import ChatOllama
import torch
from datetime import datetime


print("GPU em uso:", torch.cuda.get_device_name(0) if torch.cuda.is_available() else "Nenhuma GPU detectada")
device = 'cuda' if torch.cuda.is_available() else 'cpu'
load_dotenv(override=True)

cripto = 'Weg S.A.' #ativo a ser pesquisada
data = date.today()
# Configure o encoding para UTF-8
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

llm = ChatOllama(model='ollama/llama3.1:8b', device = device)

tool = SerperDevTool(
    n_results=10,  # Optional: Number of results to return (default: 10)
    save_file=False,  # Optional: Save results to file (default: False)
    search_type="search"  # Optional: Type of search - "search" or "news" (default: "search")
)

results = tool._run(search_query=f"dados atualizados fundamentalistas e técnicos da empresa {cripto} para o dia {data}")

analista_fundamentalista = Agent(
    role="Analista Financeiro Fundamentalista",
    goal=f"Analisar os fundamentos da empresa {cripto}",
#     backstory="""Você é um analista financeiro Senior especializado em análise fundamentalista.
# Seu trabalho é avaliar o valor intrínseco das criptomoedas.""",
    backstory="""Você é um analista financeiro Senior especializado em análise fundamentalista.
Seu trabalho é avaliar o valor intrínseco das empresas com base em seus balanços financeiros.""",
    verbose=True,
    allow_delegation=False,
    llm=llm,
    tools=[tool]
)

analista_tecnico = Agent(
    role="Analista Técnico de Ações",
    goal=f"Analisar os padrões gráficos e indicadores técnicos de {cripto}",
    backstory="""Você é um analista técnico Senior especializado em análise de gráficos.
    Seu trabalho é identificar padrões de preço e volume, além de utilizar indicadores técnicos.""",
    verbose=True,
    allow_delegation=True,
    llm=llm,
    tools=[tool],  # Use the tool property
)

supervisor = Agent(
    role="Supervisor de Análises",
    goal="Revisar as análises dos outros agentes e produzir um relatório final coeso e bem estruturado",
    backstory="""Você é um analista financeiro chefe. Seu papel é revisar os relatórios dos analistas
    fundamentalista e técnico, identificar inconsistências e gerar um relatório consolidado para decisão.""",
    verbose=True,
    allow_delegation=False,
    llm=llm,
)

# Configure as tarefas
tarefa_fundamentalista = Task(
                                # description="""Conduza uma análise fundamentalista completa da criptomoeda {cripto}.
                                # Avalie os balanços financeiros, demonstrações de resultados, fluxo de caixa""",
                                # expected_output="Relatório completo com avaliação dos fundamentos financeiros",
                                # agent=analista_fundamentalista,
                                description=f"""Conduza uma análise fundamentalista completa da empresa {cripto}.
                                """,
                                expected_output="Relatório completo com avaliação dos fundamentos financeiros",
                                agent=analista_fundamentalista,
                                )
tarefa_tecnica = Task(
                                description=f"""Conduza uma análise técnica completa da empresa {cripto}.
                                Avalie os padrões gráficos, indicadores técnicos (como médias móveis, RSI, MACD)
                                e identifique uma possível contação para entrada e saida""",
                                expected_output=f"Relatório completo com avaliação técnica da {cripto}",
                                agent=analista_tecnico,
                            )

tarefa_supervisor = Task(
    description="""Revise os relatórios de análise fundamentalista e técnica.
    Identifique inconsistências, erros e pontos de melhoria. Produza um relatório consolidado para decisão.""",
    expected_output="Relatório final consolidado com insights acionáveis.",
    agent=supervisor,
)

# Configure a equipe
equipe = Crew(
    agents=[analista_fundamentalista, analista_tecnico, supervisor],
    tasks=[tarefa_fundamentalista, tarefa_tecnica, tarefa_supervisor],
    verbose=True, # Ajuste para 1 ou 2 para níveis diferentes de log
    process=Process.sequential, # Execute as tarefas em sequência
)

# Inicie a execução
resultado = equipe.kickoff()

print("######################")
print(resultado)

# Salvar o resultado em um arquivo
with open('resultado.md', 'w', encoding='utf-8') as f:
    f.write(str(resultado))
