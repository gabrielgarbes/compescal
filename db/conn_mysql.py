import mysql.connector

def conn():
    # Tente estabelecer a conexão
    try:
        connection = mysql.connector.connect(
            host='127.0.0.1',
            port='3306',
            user='root',
            password='example',
            database='company'
        )
        if connection.is_connected():
            print("Conexão ao MySQL bem-sucedida.")
            
        # Faça o que você precisa fazer com o banco de dados aqui

    except mysql.connector.Error as e:
        print(f"Erro ao conectar ao MySQL: {e}")
    
    return connection

    # finally:
    #     # Certifique-se de fechar a conexão, independentemente do resultado
    #     if 'connection' in locals() and connection.is_connected():
    #         connection.close()
    #         print("Conexão ao MySQL fechada.")

def select():
    try:
        # Estabeleça uma conexão com o banco de dados
        connection = conn()

        if connection.is_connected():
            print("Conexão ao MySQL bem-sucedida.")

            # Crie um cursor para executar consultas
            cursor = connection.cursor()

            # Execute uma consulta SELECT
            query = "SELECT * FROM employee"
            cursor.execute(query)

            # Recupere os resultados da consulta
            results = cursor.fetchall()

            return results

    except mysql.connector.Error as e:
        print(f"Erro ao conectar ao MySQL: {e}")

    finally:
        # Certifique-se de fechar o cursor e a conexão
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()
            print("Conexão ao MySQL fechada.")

def insert(df):
    try:
        # Estabeleça uma conexão com o banco de dados
        connection = conn()

        if connection.is_connected():
            print("Conexão ao MySQL bem-sucedida.")

            # Crie um cursor para executar comandos SQL
            cursor = connection.cursor()

            # Itere sobre as linhas do DataFrame e insira os dados na tabela
            for index, row in df.iterrows():
                query = """INSERT INTO dre (
                            custo_mao_obra_direta,
                            saldo_inicial,
                            saldo_final,
                            mes_referencia,
                            valor_mes
                            ) VALUES (%s,%s,%s,%s,%s)"""
                values = (row['custo_mao_obra_direta'], 
                          row['saldo_inicial'],
                          row['saldo_final'],
                          row['mes_referencia'],
                          row['valor_mes'])
                cursor.execute(query, values)

            # Confirme a transação para efetivar as inserções
            connection.commit()
            print("Inserção de dados concluída com sucesso.")

    except mysql.connector.Error as e:
        print(f"Erro ao conectar ao MySQL: {e}")

    finally:
        # Certifique-se de fechar o cursor e a conexão
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()
            print("Conexão ao MySQL fechada.")
conn = psycopg2.connect(
    database="compescal",
    user="postgres",
    password="example",
    host="localhost",  # Host do banco de dados
    port="5432"  # Porta padrão do PostgreSQL
)
def delete():
    try:
        # Estabeleça uma conexão com o banco de dados
        connection = conn()

        if connection.is_connected():
            print("Conexão ao MySQL bem-sucedida.")

            # Crie um cursor para executar consultas
            cursor = connection.cursor()

            # Execute uma consulta SELECT
            query = "delete from dre"
            cursor.execute(query)
            connection.commit()


    except mysql.connector.Error as e:
        print(f"Erro ao conectar ao MySQL: {e}")

    finally:
        # Certifique-se de fechar o cursor e a conexão
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()
            print("Conexão ao MySQL fechada.")            