import re
from dotenv import load_dotenv
from connection import query_postgresql
import csv

load_dotenv()

query = """
    SELECT DISTINCT 
        fc."valorTotal",
        fc.objeto,
        fc."dataCelebracao",
        fc.objetivo 
    FROM
        orcamento.fato_contrato fc
    JOIN orcamento.fato_saldo fs2 ON
        fc.codigo = fs2."Contrato"
    WHERE
        (fs2."Item Patrimonial" = '5707'
            OR fs2."Item Patrimonial" = '5711'
            OR fs2."Item Patrimonial" = '5713')
        AND fs2."Contrato" <> '00000000';
"""

def getDate(text):
    regex = r'\b(\d{1,2})\s+de\s+([JANEIRO|FEVEREIRO|MARÇO|ABRIL|MAIO|JUNHO|JULHO|AGOSTO|SETEMBRO|OUTUBRO|NOVEMBRO|DEZEMBRO]+)\s+de\s+(\d{4})\b|\b(\d{1,2}/\d{1,2}/\d{4})\b'
    dates = re.findall(regex, text, re.IGNORECASE)
    
    datas_extraidas = []
    for data in dates:
        if data[0] and data[1] and data[2]:
            datas_extraidas.append(f"{data[0]} de {data[1]} de {data[2]}")
        elif data[3]:
            datas_extraidas.append(data[3])
    return datas_extraidas

def main():
    results = query_postgresql(query, 1000)

    data_results = []
    if results:
        for row in results:
            try:
                dates_extracted = getDate(row[3])
                if dates_extracted:
                    for date in dates_extracted:
                        data_results.append((date, *row))
            except Exception as e:
                print(f"Erro ao extrair data de {row[3]}: {e}")
    
    # Escrevendo no arquivo CSV
    with open('dates.csv', 'w', newline='', encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(['Data Extraída', 'Valor Total', 'Objeto', 'Data de Celebração', 'Objetivo'])  # Cabeçalhos do CSV
        writer.writerows(data_results)

if __name__ == "__main__":
    main()
