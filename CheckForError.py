#!/usr/bin/env python

__author__ = 'Hector Hawley Herrera'
__descripcion__ = 'Revisa el error de la prediccion por volumen promedio'

import numpy as np 
import pandas as pd
import zipfile as z
import matplotlib.pyplot as plt
import math

columns_real=[ 'FECHA', 'ID_CLIENTE', 'ANTIGUEDAD', 'CUADRANTE', 'MOV90_CTA',
	'SALDO_ACTENOPAQ', 'SALDO_ACTEPAQ', 'SALDO_ACCD', 'SALDO_ACAHPAQ', 
	'SALDO_ACAHNOPAQ', 'SALDO_ACAD', 'PP_MONTO', 'PR_MONTO', 'PH_MONTO',
	'PC_MONTO', 'SALDO_ADPF', 'SALDO_APFE', 'SALDO_FONDO_MONEDA1', 'SALDO_FONDO_MONEDA2',
	'SALDO_ATIT']
use_cols=['FECHA', 'ID_CLIENTE', 'MOV90_CTA',
		'SALDO_ACTENOPAQ', 'SALDO_ACTEPAQ', 'SALDO_ACCD', 'SALDO_ACAHPAQ', 
		'SALDO_ACAHNOPAQ', 'SALDO_ACAD', 'PP_MONTO', 'PR_MONTO', 'PH_MONTO',
		'PC_MONTO', 'SALDO_ADPF', 'SALDO_APFE', 'SALDO_FONDO_MONEDA1', 'SALDO_FONDO_MONEDA2',
		'SALDO_ATIT']

dataHeaders = pd.read_excel('Informacion para el Analisis//DICCIONARIO_DATOS_CLIENTE.xlsx')

zip_file = z.ZipFile('Test Data//CLIENTES.zip', 'r')
real_data = pd.read_csv(zip_file.open('CLIENTES_OCT2014.txt'), names = dataHeaders["VARIABLE"], usecols=use_cols)
real_data = real_data.append(
				pd.read_csv(zip_file.open('CLIENTES_NOV2014.txt'), names = dataHeaders["VARIABLE"], usecols=use_cols))
real_data = real_data.append(
				pd.read_csv(zip_file.open('CLIENTES_DEC2014.txt'), names = dataHeaders["VARIABLE"], usecols=use_cols))
predicted_months = pd.read_csv('Test Data//predicted_values.csv', error_bad_lines=False)

predicted_months = predicted_months.dropna()
predicted_months['ID_CLIENTE'] = predicted_months['ID_CLIENTE'].astype(float)
predicted_months['MOV90_CTA'] = predicted_months['MOV90_CTA'].astype(float)

clientes_id = real_data.groupby('ID_CLIENTE')

vol_prom_df = pd.DataFrame(columns=['REAL', 'PRED'])
mov_df = pd.DataFrame(columns=['REAL', 'PRED'])



i = 0
errorDf01 = np.zeros(0)

errorDf02 = np.zeros(0)
errorDf03 = np.zeros(0)
type_col = 'MOV90_CTA'

for id, cliente in clientes_id:
	cliente = cliente.tail(6)

	# Agarramos el valor real y predicho de diciembre junto con los datos de los otros meses
	#cliente_datos = cliente.head(5)
	cliente_real_months = cliente.tail(3)
	cliente_pred_months = predicted_months[predicted_months['ID_CLIENTE'] == id]
	cliente_pred_months = cliente_pred_months.tail(3)
	cliente_pred_months = cliente_pred_months.sort_values(by=['FECHA'])

	if cliente_pred_months.empty:
		continue
	elif len(cliente_pred_months) != len(cliente_real_months):
		continue

	# print cliente_pred_months
	# print cliente_real_months

	# res_sqr = np.append(res_sqr, pow(cliente_real_months[type_col].values.astype(float)[0]- real_mean), 2)
	errorDf01 = np.append(errorDf01, pow(cliente_real_months[type_col].values.astype(float)[0] - cliente_pred_months[type_col].values.astype(float)[0], 2))
	errorDf02 = np.append(errorDf02, pow(cliente_real_months[type_col].values.astype(float)[1] - cliente_pred_months[type_col].values.astype(float)[1], 2))
	errorDf03 = np.append(errorDf03, pow(cliente_real_months[type_col].values.astype(float)[2] - cliente_pred_months[type_col].values.astype(float)[2], 2))
	
	# total = 0
	# for index, mes in cliente_datos.iterrows():
	# 	total = total + \
	# 			mes['SALDO_ACTENOPAQ'] + mes['SALDO_ACTEPAQ'] + \
	# 			mes['SALDO_ACCD'] + mes['SALDO_ACAHPAQ'] + \
	# 			mes['SALDO_ACAHNOPAQ'] + mes['SALDO_ACAD'] + \
	# 			mes['PP_MONTO'] + mes['PR_MONTO'] + \
	# 			mes['PH_MONTO'] + mes['PC_MONTO'] + \
	# 			mes['SALDO_ADPF'] + mes['SALDO_APFE'] + \
	# 			mes['SALDO_FONDO_MONEDA1'] + mes['SALDO_FONDO_MONEDA2'] + \
	# 			mes['SALDO_ATIT']

	# total_pred = 0
	# for index, dec in cliente_pred_months.iterrows():
	# 	total_pred = total + \
	# 			dec['SALDO_ACTENOPAQ'] + dec['SALDO_ACTEPAQ'] + \
	# 			dec['SALDO_ACCD'] + dec['SALDO_ACAHPAQ'] + \
	# 			dec['SALDO_ACAHNOPAQ'] + dec['SALDO_ACAD'] + \
	# 			dec['PP_MONTO'] + dec['PR_MONTO'] + \
	# 			dec['PH_MONTO'] + dec['PC_MONTO'] + \
	# 			dec['SALDO_ADPF'] + dec['SALDO_APFE'] + \
	# 			dec['SALDO_FONDO_MONEDA1'] + dec['SALDO_FONDO_MONEDA2'] + \
	# 			dec['SALDO_ATIT']

	# total_real = 0
	# for index, dec in cliente_real_months.iterrows():
	# 	total_real = total + \
	# 			dec['SALDO_ACTENOPAQ'] + dec['SALDO_ACTEPAQ'] + \
	# 			dec['SALDO_ACCD'] + dec['SALDO_ACAHPAQ'] + \
	# 			dec['SALDO_ACAHNOPAQ'] + dec['SALDO_ACAD'] + \
	# 			dec['PP_MONTO'] + dec['PR_MONTO'] + \
	# 			dec['PH_MONTO'] + dec['PC_MONTO'] + \
	# 			dec['SALDO_ADPF'] + dec['SALDO_APFE'] + \
	# 			dec['SALDO_FONDO_MONEDA1'] + dec['SALDO_FONDO_MONEDA2'] + \
	# 			dec['SALDO_ATIT']

	# v = int(cliente_real_months['ANTIGUEDAD'].values.astype(float))
	# antiguedad = min(v, 6)

	# volumen_promedio_real = total_real / antiguedad
	# volumen_promedio_pred = total_pred / antiguedad

	# cuadrante = cliente_real_months['CUADRANTE'].values.astype(str)
	# ref_importe = 7

	# if cuadrante == 'S1' or cuadrante == 'S2' or \
	# 	cuadrante == 'S3' or cuadrante == 'A1' or \
	# 	cuadrante == 'A2' or cuadrante == 'A3':
	# 	ref_importe = 143
	# elif cuadrante == 'B1' or cuadrante == 'B2' or \
	# 	cuadrante == 'B3':
	# 	ref_importe = 36

	# imp = pd.DataFrame([[volumen_promedio_real < ref_importe, volumen_promedio_pred < ref_importe]], 
	# 	index=[0], columns=['REAL', 'PRED'])
	# vol_prom_df = vol_prom_df.append(imp, ignore_index=True)

	# if not volumen_promedio_real < ref_importe:
	# 	rr = cliente_real_months['MOV90_CTA'].values.astype(float) < 3.0
	# 	rp = cliente_pred_months['MOV90_CTA'].values.astype(float) < 3.0
	# 	imp = pd.DataFrame([[rr[0], rp[0]]], index=[0], columns=['REAL', 'PRED'])
	# 	mov_df = mov_df.append(imp, ignore_index=True)

print len(errorDf01)

res_sqr = 4839532.02833052
r1 = np.sum(errorDf01)
r2 = np.sum(errorDf02) 
r3 = np.sum(errorDf03) 

r1 = 1 - r1 / res_sqr
r2 = 1 - r2 / res_sqr
r3 = 1 - r3 / res_sqr

print r1
print r2
print r3

# summary_real = vol_prom_df.groupby('REAL').size()
# summary_pred = vol_prom_df.groupby('PRED').size()

# print 'Por Vol Prom:'
# print summary_real
# print summary_pred


# summary_real = mov_df.groupby('REAL').size()
# summary_pred = mov_df.groupby('PRED').size()

# print 'Por MOV90_CTA:'
# print summary_real
# print summary_pred