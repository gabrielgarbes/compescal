import sys
from pathlib import Path
file = Path(__file__).resolve()
root = file.parents[2]
sys.path.append(str(root))

from _util.decorator.log import log_etl_process


def extract(table):
    from db.conn_postgres_operacao import select
    list_data, columns = select(table)
    return list_data, columns

def transform(list_data,columns):
    from datetime import datetime
    import pandas as pd

    df = pd.DataFrame(list_data, columns=columns)
    df.at[3, 'idade'] = 90
    df.at[3, 'data_atualizacao'] = datetime.now()

    return df

def load(df):
    from db.conn_postgres_operacao import upsert
    dados_dict = df.to_dict(orient='records')
    upsert('teste', dados_dict)


@log_etl_process('etl_log.csv')
def tabela_modelo():
    
    list_data, columns = extract('teste')
    df = transform(list_data,columns)
    load(df)

    records = df.values.tolist()
    return records

if __name__ == '__main__':
    tabela_modelo()
    

