from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime, timedelta
import requests
import json

class TwitterHook(HttpHook):
    """
    Classe que interage com a API do Twitter, utilizando Airflow's HttpHook como base.
    Realiza buscas por tweets recentes com base em uma query e intervalo de datas especificados.
    """
    
    def __init__(self, start_date, end_date, search_query, conn_id=None):
        """
        Inicializa o hook com as informações necessárias para a busca de tweets.
         
        :param start_date: Data de início para a busca de tweets.
        :param end_date: Data de término para a busca de tweets.
        :param search_query: Query de busca que será utilizada para filtrar os tweets.
        :param conn_id: ID de conexão do Airflow para a API do Twitter (opcional).
        """
        self.start_date = start_date
        self.end_date = end_date
        self.search_query = search_query
        self.conn_id = conn_id or "twitter_connections_default"
        
        # Inicializa a classe pai HttpHook com o ID da conexão
        super().__init__(http_conn_id=self.conn_id)

    def create_url(self):
        """
        Cria a URL que será usada para fazer a requisição à API do Twitter.

        :return: URL formatada para a requisição de busca de tweets recentes.
        """
        FORMAT_DATE = "%Y-%m-%dT%H:%M:%S.00Z"
        # Formato de data requerido pela API do Twitter
        end_time = self.end_date  # Data de término formatada
        start_time = self.start_date  # Data de início formatada

        # Campos que serão retornados na resposta da API
        tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
        user_fields = "expansions=author_id&user.fields=id,name,username,created_at"
        query = self.search_query

        # Monta a URL completa com os parâmetros de busca
        url_raw = f"{self.base_url}/2/tweets/search/recent?query={query}&{tweet_fields}&{user_fields}&start_time={start_time}&end_time={end_time}"

        return url_raw

    def create_connection(self, url, session):
        """
        Cria e prepara a requisição para a API do Twitter.
        :param url: URL a ser usada para a requisição.
        :param session: Sessão HTTP para a requisição.
        :return: Resposta da API após a execução da requisição.
        """

        # Realiza a requisição HTTP GET
        get_response = requests.Request("GET", url)

        # Prepara a requisição com a sessão fornecida
        prep = session.prepare_request(get_response)

        # Loga a URL que está sendo utilizada
        self.log.info(f"URL: {url}")

        # Executa e verifica a requisição
        return self.run_and_check(session, prep, {})

    def create_paginate(self, raw_url, session):
        """
        Realiza a paginação dos resultados da API do Twitter para obter todos os tweets.

        :param raw_url: URL base para a requisição.
        :param session: Sessão HTTP para a requisição.
        :return: Lista com todas as respostas da API, paginadas.
        """
        
        # Obtém a primeira resposta da API
        response = self.create_connection(raw_url, session)
        list_request = []
        first_json_response = response.json()
        list_request.append(first_json_response)

        # Continua fazendo requisições enquanto houver um 'next_token' nos metadados da resposta

        interaction = 1
        while 'next_token' in first_json_response.get('meta', {}) and interaction < 100:
            next_token = first_json_response['meta']['next_token']
            nxt_url = f"{raw_url}&next_token={next_token}"
            nxt_response = self.create_connection(nxt_url, session)
            nxt_json_responses = nxt_response.json()
            list_request.append(nxt_json_responses)
            interaction += 1
    
        return list_request

    def run(self):
        """
        Executa o processo de busca de tweets, incluindo a paginação dos resultados.
        """
        
        # Obtém uma sessão HTTP a partir da conexão configurada
        session = self.get_conn()
        
        # Cria a URL para a requisição
        url = self.create_url()
        
        # Realiza a paginação para obter todos os tweets e retorna os resultados
        return self.create_paginate(url, session)

if __name__ == "__main__":
    
    end_time= datetime.now()
    start_time = (datetime.now() + timedelta(-1)).date()

    query = "data engineer"

    for pg in TwitterHook(start_time, end_time, query).run():
        print(json.dumps(pg, indent=8, sort_keys=True))