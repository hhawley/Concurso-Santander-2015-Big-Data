#!/usr/bin/env python

__author__ = 'Hector Hawley Herrera'
__descripcion__ = 'Agarra los datos creados en Clientes_FECHA y los separa por antiguedad'

import numpy as np 
import pandas as pd 
import zipfile as z
import sqlite3

is_test = True

if is_test:
	dirName = 'Test '
	separator = ','
	dates = {'2014': ['SEP', 'AUG', 'JUL', 'JUN', 'MAY', 'APR', 'MAR', 'FEB', 'JAN'],
			'2013': ['DEC', 'NOV', 'OCT', 'SEP', 'AUG', 'JUL', 'JUN', 'MAY', 'APR', 'MAR', 'FEB', 'JAN'],
			'2012': ['DEC', 'NOV', 'OCT']}

else:
# Elegi el a~no del 2014 de los meses Diciembre a Julio porque esos eran los meses que ibamos a predecir.
	dirName = 'Real '
	separator = ';'
	dates = {'2014': ['DEC', 'NOV', 'OCT','SEP', 'AUG', 'JUL']}
	# dates = {'2014': ['NOV', 'OCT', 'SEP', 'AUG', 'JUL', 'JUN', 'MAY', 'APR', 'MAR', 'FEB', 'JAN'],
	# 		'2013': ['DEC', 'NOV']}


conn = sqlite3.connect('CLIENTES.db')
c - conn.cursor()

columns= ['FECHA', 'ID_CLIENTE', 'ANTIGUEDAD', 'CUADRANTE', 'MOV90_CTA',
	'SALDO_ACTENOPAQ', 'SALDO_ACTEPAQ', 'SALDO_ACCD', 'SALDO_ACAHPAQ', 
	'SALDO_ACAHNOPAQ', 'SALDO_ACAD', 'PP_MONTO', 'PR_MONTO', 'PH_MONTO',
	'PC_MONTO', 'SALDO_ADPF', 'SALDO_APFE', 'SALDO_FONDO_MONEDA1', 'SALDO_FONDO_MONEDA2',
	'SALDO_ATIT']

dataHeaders = pd.read_excel('Informacion para el Analisis//DICCIONARIO_DATOS_CLIENTE.xlsx')
zip_file = z.ZipFile(dirName + 'Data//CLIENTES.zip', 'r')
dec_chunks = pd.read_csv(zip_file.open('CLIENTES_DEC2014.txt'),
 						usecols = columns, names=dataHeaders["VARIABLE"], sep=separator)

print len(dec_chunks)

for year, months in dates.items():
	for month in months:
		print year, month
		month_df = pd.read_csv(zip_file.open('CLIENTES_' + month + year + '.txt'), 
							usecols = columns, names=dataHeaders["VARIABLE"], sep=separator)
		month_df.loc[month_df['ID_CLIENTE'].isin(dec_chunks['ID_CLIENTE'])].to_sql(
			'scrambled_data', if_exists='append', con=conn)

if not is_test:
	dec_chunks.to_csv(dirName + 'Data//scrambled_data.txt', index=False, header=False, mode='a')

month_df = None
dec_chunks = None

scrambled_data = pd.read_sql(sql='SELECT DISTINCT ID_CLIENTE FROM scrambled_data', con=conn)

max_count = 6
for row in scrambled_data.iterrows():

	temp_df = pd.read_sql(sql='SELECT * FROM scrambled_data WHERE ID_CLIENTE=\'' + row[1][0] + '\';', con=conn)

	count = len(values)
	values = values.sort_values(by=['FECHA'])

	if count > max_count:
		values = values.head(max_count)
		count = max_count

	values.to_csv(dirName + 'Data//' + str(count)  + 'MONTHS.txt', index=False, mode='a', header = False)

	values = None


	