# Twitter Data Extractor with Apache Airflow and Spark
Este projeto trata-se de um extrator de dados do Twitter, utilizando Apache Airflow para orquestração das tarefas e Spark para transformação e carregamento de dados. Os dados extraídos são processados em diferentes camadas de um Data Lake, com foco em tratamento e wrangling para uso final em Power BI.

## Descrição do Projeto
O fluxo de dados segue as seguintes etapas:

1. Extração: Utilizando operadores e hooks do Apache Airflow, os dados são extraídos de uma base de dados fake que simula o Twitter. Nesta etapa, os dados crus são salvos no Data Lake na camada Bronze em formato JSON, sem passar por nenhum processo de transformação.

2. Transformação: Em uma segunda task, são carregados apenas os dados necessários, contendo informações dos tweets e usuários. Esses dados são processados e salvos na camada Silver, onde começam a ser limpos e organizados.

3. Wrangling e Carregamento: Os dados são trabalhados utilizando Spark, aplicando técnicas de wrangling e preparação para serem consumidos pelo Power BI. Nesta fase, os dados são transformados e otimizados para análises mais avançadas.

Tecnologias Utilizadas
* Python 3.9
* Apache Airflow: Versão 2.3.2
* Apache Spark: Versão 3.1.3
* Hadoop: Versão 3.1.3
* Instalação do Projeto

## Instalação do projeto

Para rodar o projeto em sua máquina local, siga os passos abaixo:

### 1. Clone o Repositório
```xbash
git clone https://github.com/seu-usuario/seu-projeto.git
cd seu-projeto
```

2. Instale e Configure o Ambiente Virtual e Airflow
Após clonar o repositório, você precisará instalar o Airflow e configurar o ambiente virtual:

1. Limpe o terminal (opcional):

```bash
Ctrl + L
```

2. Crie e acesse uma nova pasta para o projeto:

```bash

mkdir airflowalura
cd airflowalura
```

3. Crie o ambiente virtual usando Python 3.9:

```bash
python3.9 -m venv venv
```

4. Ative o ambiente virtual:

```bash
source venv/bin/activate
```

## 3. Instale as Dependências
Agora, dentro do ambiente virtual, instale as dependências do projeto (Airflow, Spark, etc.) de acordo com o arquivo requirements.txt fornecido no repositório.

```bash

pip install -r requirements.txt
```

## 4. Execute o Projeto
Agora que tudo está configurado, você pode iniciar o Airflow e agendar as tarefas para a extração, transformação e carregamento dos dados do Twitter fake.



# AIRFLOW_AUTO_EXTRACTOR

Esse projeto foi desenvolvido como treinamento para testar os conhecimentos em airflow, nesse projeto realizei a extração de uma api faker do twitter alguns json que foram adicionado a um datalake realizando a exração dos dados do processo de ETL
Nesse projeto apreendi mais profundamente como usar detalhes fundamentais como Operator, Dags e Tasks além de que pude ter conhecimento como configurar conexões diretamente no Airflow para facilitar o processo.

Abaixo alguns prints de telas do airflow funcionado com extração de dados diários da api fake do twitter.

![image](https://github.com/user-attachments/assets/edba1a96-86a3-459b-ba4c-801ab9d9dbff)

![image](https://github.com/user-attachments/assets/6d1b7d43-b6c4-4f5c-93c4-2a785c86fa25)

![image](https://github.com/user-attachments/assets/e8db50cf-a5c2-455c-a788-9da3fbdd378d)

![image](https://github.com/user-attachments/assets/2d716d05-ca13-4ea2-86a3-253aae3725b3)

Esse projeto é a parte 2 da formação em Airflow para data engineer do curso alura.
