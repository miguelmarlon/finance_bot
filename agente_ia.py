import json
from dotenv import load_dotenv
from langchain_community.chat_models import ChatOllama
import torch
print("GPU disponível:", torch.cuda.is_available())
print("GPU em uso:", torch.cuda.get_device_name(0) if torch.cuda.is_available() else "Nenhuma GPU detectada")


def send_message(prompt, sistema = "", json_format = False):
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    llm = ChatOllama(model='deepseek-r1:8b', device = device)

    formato = "text"
    if json_format:
        formato = "json_object"

    mensagens = []
    if sistema:
        mensagens.append({"role": "system", "content": sistema})
    mensagens.append({"role": "user", "content": prompt})


    response = llm.invoke(mensagens)
    
    if json_format:
        try:
            return json.loads(response.content)  # Converte a resposta JSON para um dicionário Python
        except json.JSONDecodeError as e:
            print("Erro ao decodificar JSON:", e)
            return {"error": "Resposta inválida", "raw_response": response.content}
    return response.content
    
# formato_json = True
# sistema = "Seu nome é Miguel"
# mensagem = """
# Bom dia, qual seu nome? Responda exclusivamente no formato JSON.
# """

# # Example usage:
# response = send_message(mensagem, sistema, formato_json)
# print(response)   
    
# resposta = send_message("Qual é a capital do Brasil?")
# print(resposta)

# llm = ChatOllama(model='deepseek-r1:8b', device = device)
# # chain = llm | StrOutputParser()


# resposta = llm.invoke("Qual é a capital do Brasil?")
# print(resposta.content)