import pandas as pd
import db.conn_mysql as conn_mysql
import os

def formata_mes(data_original):
    from datetime import datetime

    data_parts = data_original.split('/')  # Divide a data em partes
    mes = data_parts[0]
    ano = data_parts[1]

    # Adiciona um zero à esquerda se o mês tiver apenas um dígito
    if len(mes) == 1:
        mes = '0' + mes

    data_formatada = f'{ano}-{mes}-01'
    return data_formatada

arr = []

arquivos = os.listdir()

# Agora, você pode imprimir a lista de arquivos
for arquivo in arquivos:
    if 'DRE' in arquivo:
        print(arquivo)

        with open(arquivo, 'r', encoding='iso-8859-1') as file:
            
            mes = arquivo.split(' ')[3].replace('.csv','').split('.')[0]
            ano = arquivo.split(' ')[3].replace('.csv','').split('.')[1]

            mes_referencia = f'{ano}-{mes}-01'
            item = 0
            for line in file:
                line = line.replace('"','').replace('=','').replace('\n','')
                
                if 'OUTROS CUSTOS' in line:
                    break
                
                if item == 1: 
                    descricao = line.split(';')[1].strip()
                    saldo_inicial = line.split(';')[2].replace(',','.')
                    valor_mes = line.split(';')[3].replace(',','.')
                    saldo_final = line.split(';')[4].replace(',','.')

                    arr.append([descricao, saldo_inicial, saldo_final, mes_referencia, valor_mes])
                
                if 'CUSTO DE MÃO-DE-OBRA DIRETA' in line:
                    item = 1

df = pd.DataFrame(arr, columns=['custo_mao_obra_direta',
                            'saldo_inicial',
                            'saldo_final',
                            'mes_referencia',
                            'valor_mes'])

df.drop(0, axis=0, inplace=True)
conn_mysql.delete()
conn_mysql.insert(df)