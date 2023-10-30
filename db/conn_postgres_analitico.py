# pip install psycopg2-binary
import psycopg2
from psycopg2 import Error

def conn():
    # Tente estabelecer a conexão
    try:
        # Configurar os detalhes da conexão
        connection = psycopg2.connect(
            database="compescal",
            user="postgres",
            password="example",
            host="127.0.0.1",  # Host do banco de dados
            port="5432"  # Porta padrão do PostgreSQL
        )

    except Error as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None
    
    return connection

def select(table):
    try:
        connection = conn()
        cursor = connection.cursor()
        #table='teste'
        select_query = f"SELECT * FROM {table}"
        cursor.execute(select_query)
        result = cursor.fetchall()

        return result

    except Error as e:
        print(f"Erro ao ler dados: {e}")
    
    finally:
        if 'cursor' in locals():
            cursor.close()
        if connection.closed == 0:
            connection.close()

 

def insert(df, table):

    try:
        connection = conn()
        cursor = connection.cursor()
        
        registros = df.to_records(index=False)
        placeholders = ", ".join(["%s" for _ in registros[0]])
        insert_query = f"INSERT INTO {table} VALUES ({placeholders})"
        
        cursor.executemany(insert_query, registros)
        connection.commit()
        print(f"{len(registros)} registros inseridos com sucesso na tabela {table}.")
    except Error as e:
        print(f"Erro ao inserir dados: {e}")

    finally:
        if 'cursor' in locals():
            cursor.close()
        if connection.closed == 0:
            connection.close()

def delete(table):
    try:
        connection = conn()
        cursor = connection.cursor()
        delete_query = f"DELETE FROM {table}"
        cursor.execute(delete_query)
        connection.commit()
    except Error as e:
        print(f"Erro ao excluir dados: {e}")         
    finally:
        if 'cursor' in locals():
            cursor.close()
        if connection.closed == 0:
            connection.close()


# Função para obter as colunas de uma tabela
def obter_colunas(connection, tabela):
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = %s", (tabela,))
        colunas = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return colunas
    except Error as e:
        print(f"Erro ao obter colunas da tabela: {e}")
        return []

# Função para obter a coluna de chave primária de uma tabela
def obter_coluna_chave_primaria(connection, tabela):
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT column_name FROM information_schema.key_column_usage WHERE table_name = %s", (tabela,))
        chave_primaria = cursor.fetchone()
        cursor.close()
        if chave_primaria:
            return chave_primaria[0]
        else:
            return None
    except Error as e:
        print(f"Erro ao obter a coluna de chave primária da tabela: {e}")
        return None

# Função para criar uma operação upsert
def upsert(tabela, dados):
    
    connection = conn()

    # colunas = obter_colunas(connection, tabela)
    
    chave_primaria = obter_coluna_chave_primaria(connection, tabela)

    if not chave_primaria:
        print("A tabela não tem uma coluna de chave primária. A operação upsert não é possível.")
        return

    try:
        cursor = connection.cursor()

        for i in range(len(dados)):
            # Montar a parte do INSERT com os valores
            colunas = list(dados[i].keys())
            colunas = ", ".join(colunas)
            valores = ", ".join(["%s" for _ in dados[i].values()])
            insert_query = f"INSERT INTO {tabela} ({colunas}) VALUES ({valores})"

            # Montar a parte do CONFLICT com a chave primária
            conflict_query = f"ON CONFLICT ({chave_primaria}) DO UPDATE SET "

            # Montar a parte do SET com os valores a serem atualizados
            for coluna, valor in dados[i].items():
                conflict_query += f"{coluna} = EXCLUDED.{coluna}, "
            conflict_query = conflict_query[:-2]  # Remover a última vírgula e espaço

            # Combinar as partes para criar a consulta final
            upsert_query = insert_query + conflict_query

            # Executar a consulta
            cursor.execute(upsert_query, tuple(dados[i].values()))
            connection.commit()

    except Error as e:
        print(f"Erro na operação upsert: {e}")

    finally:
        if 'cursor' in locals():
            cursor.close()
        if connection.closed == 0:
            connection.close()