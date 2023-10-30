from log import log_etl_process
import time

@log_etl_process('etl_log.csv')
def my_etl_process():
    # Simule sua carga de ETL aqui
    records = ['record1', 'record2', 'record3']
    time.sleep(2)
    return records

if __name__ == "__main__":
    my_etl_process()