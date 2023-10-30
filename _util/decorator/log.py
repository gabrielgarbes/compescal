import csv
import functools
from datetime import datetime

# Função para criar uma linha de registro
def create_log_entry(start_time, end_time, num_records, table_name):
    duration = int((end_time - start_time).total_seconds())
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return ["data_inicio_carga", "data_fim_carga", "tempo_carga", "quantidade_registros", "data_execucao", "tabela_rotina"], [start_time, end_time, duration, num_records, current_time, table_name]

# Função decorator para registrar informações em um arquivo CSV
def log_etl_process(csv_filename):
    def decorator(etl_func):
        @functools.wraps(etl_func)
        def wrapper(*args, **kwargs):
            start_time = datetime.now()
            result = etl_func(*args, **kwargs)
            end_time = datetime.now()
            num_records = len(result) if isinstance(result, list) else 0

            header, log_entry = create_log_entry(start_time, end_time, num_records, etl_func.__name__)

            # Verifica se o arquivo já existe para não adicionar o cabeçalho novamente
            file_exists = False
            try:
                with open("logs/" + csv_filename, 'r') as log_file:
                    file_exists = True
            except FileNotFoundError:
                file_exists = False

            # Escreve o log no arquivo CSV, adicionando o cabeçalho se necessário
            with open("logs/" + csv_filename, mode='a', newline='') as log_file:
                log_writer = csv.writer(log_file)
                if not file_exists:
                    log_writer.writerow(header)
                log_writer.writerow(log_entry)

            return result
        return wrapper
    return decorator

