

1) Aqui temos um exemplo simples de um código com opção de ter um log para acompanhamentos, ter um salvamento em csv e uma analise basica das colunas.

import pandas as pd
import os
import numpy as np
import warnings
from loguru import logger
import sys
import shutil
import ydata_profiling
import random

# Ocultar mensagens FutureWarning
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", message="DataFrame is highly fragmented.")


arquivo_log = '//xxxx/xxxx/xxxx/xxxx/arquivo_log.log'
arquivo_csv = "xxxx.csv"
# Define o nível global do loguru
logger.remove() 
logger.add(sys.stdout, level='INFO', format="{time:YYYY-MM-DD HH:mm:ss} - {level} - {message}")
logger.add(arquivo_log, level='DEBUG', format="{time:YYYY-MM-DD HH:mm:ss} - {level} - {message}", mode="w")
logger.info(f"Arquivo log em: {arquivo_log}")



try: # CODIGO PRINCIPAL...

    ###############################################################################
    ############################   SAVE CSV #######################################
    ###############################################################################
    
    # real_append.to_pickle()
    # Write CSV
    os.chdir(
        r"\\xxx\xxx\csv_arquivos"
    )  # change directory to save in the right folder

    logger.warning("...Dados SENDO copiados agora no CSV! AGUARDE...")
    logger.info('salvando CSV em: {diretorio}', diretorio=os.getcwd())
    xxxx.to_csv(f"{arquivo_csv}", encoding="utf-8", index=False, sep=";")
    # real_append.to_pickle('xxxx.pkl')

    ###############################################################################
    ######################        SELECT TABLE        #############################
    ###############################################################################

    # Suponha que 'Valor' seja a coluna que você deseja converter para números decimais
   xxxxxx['Valor'] = pd.to_numeric(xxxxx['Valor'], errors='coerce')

    # Obtém uma amostra aleatória de 10% dos dados
    amostra_10porcento = real_append.sample(frac=0.10, random_state=42)  # Use um valor de semente (random_state) para reproducibilidade

    # Gere o relatório em HTML para a amostra
    nome_arquivo = "xxxxx_sample_10.html"
    profile = ydata_profiling.ProfileReport(amostra_10porcento)

    # Salve o relatório no diretório atual de trabalho
    profile.to_file(nome_arquivo)

    # Obtém o diretório atual de trabalho
    diretorio_atual = os.getcwd()

    # Imprime o diretório atual
    logger.warning(f"Informação geral sobre os dados: {diretorio_atual} no arquivo relatorio_perfil_{nome_arquivo}.html ")

    logger.warning("FINALIZADO COM SUCESSO")
    logger.stop()

except Exception as e:
    # Em caso de erro, mova o arquivo existente para a pasta de backup
    
    caminho_csv = r"\\xxxx\xxxxx\csv_arquivos"
    arquivo_existente = fr'{caminho_csv}\{arquivo_csv}'
    pasta_backup = fr'{caminho_csv}\backup_quando_erro'

    # Crie a pasta de backup se ela ainda não existir
    if not os.path.exists(pasta_backup):
        os.makedirs(pasta_backup)

    # Mova o arquivo para a pasta de backup
    shutil.move(arquivo_existente, os.path.join(pasta_backup, f'{arquivo_csv}'))

    logger.error(f"Houve erro no processo!, backup salvo em: {pasta_backup}, verificar processo ETL")


2)Aqui é um exemplo simples de importação de uma base de dados para que seja utilizada no SQL:

    ###############################################################################
    ####### WRITE TABLE TO SQL SERVER #############################################
    ###############################################################################
    logger.warning("...Dados SENDO copiados agora no SQL! AGUARDE...")
    logger.info('salvando SQL em: {diretorio}', diretorio=tabela_sql)

    # Calcula o tamanho máximo de caracteres em cada coluna
    tamanho_maximo = dim_empresa.apply(lambda coluna: coluna.astype(str).str.len().max())

    # Exibe o tamanho máximo de caracteres em cada coluna na tabela
    logger.debug(tamanho_maximo)

    # SQL injection
    connection_string = (
    "Driver={SQL Server};"
    f"Server=XXXXXXXXXXX;"
    "UID=XXXXXXXXXXXXXXX;"
    "PWD=XXXXXXXXXXXXXX;"
    f"Database={Banco_sql[:-4]};"
    )
    quoted = urllib.parse.quote_plus(connection_string)
    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={quoted}')
    with engine.connect() as cnn:
        dim_empresa.to_sql(tabela_sql.split('.')[-1],con=cnn, if_exists='replace', index=False, 
                        dtype={'EMPRESA_ID': INTEGER,
                                'EMPRESA': VARCHAR (50),
                                'ATUACAO': VARCHAR (20),
                                'AREA_DE_NEGOCIO': VARCHAR (20),
                                'GRUPO': VARCHAR (20),
                                'METODO_CONSOLIDACAO': VARCHAR (20),
                                'PERC_PARTIC': DECIMAL (18,2),
                                'PAIS': VARCHAR (20),
                                'MOEDA_ID': VARCHAR (3),
                                'CONTINENTE': VARCHAR (15),
                                'STATUS': VARCHAR (10)})


Outro exemplo de importação para base de dados SQL:

    ###############################################################################
    ###################### WRITE TABLE TO SQL SERVER ##############################
    ###############################################################################

    logger.warning("...Dados SENDO copiados agora no SQL! AGUARDE...")
    logger.info('salvando SQL em: {diretorio}', diretorio=tabela_sql)


    # Calcula o tamanho máximo de caracteres em cada coluna
    tamanho_maximo = df_final.apply(lambda coluna: coluna.astype(str).str.len().max())

    # Exibe o tamanho máximo de caracteres em cada coluna na tabela
    logger.debug(tamanho_maximo)

    Delete_ano = f"DELETE FROM {tabela_sql.split('.')[-1]};"

    Alter_table_varchar = f"""
    ALTER TABLE {tabela_sql.split('.')[-1]} 
      DROP CONSTRAINT PK_XXXXXXXXXXXXXXX;
    ALTER TABLE {tabela_sql.split('.')[-1]} 
    DROP COLUMN Id
    """

    Alter_table_specific = f"""
    ALTER TABLE {tabela_sql.split('.')[-1]} 
    ADD Id INT IDENTITY(1,1);
    ALTER TABLE {tabela_sql.split('.')[-1]} 
    ADD CONSTRAINT PK_XXXXXXXXXXXXXXX PRIMARY KEY (Id)
    """

    Bulk_insert = f"""
    BULK INSERT {tabela_sql.split('.')[-1]}
    FROM '\\\\XXXXXX\\csv_arquivos\\{arquivo_csv}'
    WITH (
        FORMAT = 'CSV',
        FIRSTROW = 2,
        FIELDTERMINATOR = ';',
        CODEPAGE = '65001',
        DATAFILETYPE = 'char'
    )
    """

    connection_string = (
    "Driver={SQL Server};"
    f"Server=XXXXXXXXXXX;"
    "UID=XXXXXXXX;"
    "PWD=XXXXXXXXXX;"
    f"Database={Banco_sql[:-4]};"
    )

    quoted = urllib.parse.quote_plus(connection_string)
    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={quoted}')

    with engine.connect() as cnn:

        # Executar comando DELETE das linhas em referência
        cnn.execute(Delete_ano)

        # Alterar colunas para tipo varchar e derrubar a PK antes de inserir dados
        cnn.execute(Alter_table_varchar)
        
        # Executar comando BULK INSERT
        cnn.execute(Bulk_insert)
        
        # Alterar colunas para os tipos desejados e refazer a PK ao final
        cnn.execute(Alter_table_specific)

3)Caso se queira gerar por exemplo um executável podemos sempre usar o #usar auto-py-to-exe para gerar executável via código de comando
