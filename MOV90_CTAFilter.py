#Existia una condicion en el concurso donde si los movimientos promedios en los ultimos 6 o menos 
#(en caso de ser un usuario nuevo) meses era menor a cierta cantidad
#se consideraba que el usuario era inactivo.

# En este script se pretende agarrar esos usuarios y ver si no eran activos

import pandas as pd
import numpy as np
import os.path

# Los archivos no tienen los nombres de las columnas, no necesarios pero facilita la lectura
columns= ['FECHA', 'ID_CLIENTE', 'ANTIGUEDAD', 'CUADRANTE', 'MOV90_CTA',
	'SALDO_ACTENOPAQ', 'SALDO_ACTEPAQ', 'SALDO_ACCD', 'SALDO_ACAHPAQ', 
	'SALDO_ACAHNOPAQ', 'SALDO_ACAD', 'PP_MONTO', 'PR_MONTO', 'PH_MONTO',
	'PC_MONTO', 'SALDO_ADPF', 'SALDO_APFE', 'SALDO_FONDO_MONEDA1', 'SALDO_FONDO_MONEDA2',
	'SALDO_ATIT']

users = pd.read_csv('Test Data//13MONTHS.txt', names = columns)

max_count = int(len(users) * 0.02307)
print len(users)
print max_count
current_count = 0

pd.DataFrame(columns = columns).to_csv('Test_data.csv', index=False)

for id, user in users.groupby('ID_CLIENTE'):
	if current_count >= max_count:
		break

	user_copy = user.copy()

	p = user[user['FECHA'] == '01/12/2014']
	if p['MOV90_CTA'].values.astype(int) < 3:
		user_copy.to_csv('Test_data.csv', index=False, header=False, mode='a')
		current_count = current_count + 1
		continue

	user_ultimos_6_meses = user.tail(6)

	total = 0
	for index, mes in user_ultimos_6_meses.iterrows():
		total = total + \
				mes['SALDO_ACTENOPAQ'] + mes['SALDO_ACTEPAQ'] + \
				mes['SALDO_ACCD'] + mes['SALDO_ACAHPAQ'] + \
				mes['SALDO_ACAHNOPAQ'] + mes['SALDO_ACAD'] + \
				mes['PP_MONTO'] + mes['PR_MONTO'] + \
				mes['PH_MONTO'] + mes['PC_MONTO'] + \
				mes['SALDO_ADPF'] + mes['SALDO_APFE'] + \
				mes['SALDO_FONDO_MONEDA1'] + mes['SALDO_FONDO_MONEDA2'] + \
				mes['SALDO_ATIT']

	v = user.tail(1)['ANTIGUEDAD'].values.astype(int)
	antiguedad = min(v, 6)

	volumen_promedio = total / antiguedad

	cuadrante = user.tail(1)['CUADRANTE'].values.astype(str)
	ref_importe = 7
	if cuadrante == 'S1' or cuadrante == 'S2' or \
		cuadrante == 'S3' or cuadrante == 'A1' or \
		cuadrante == 'A2' or cuadrante == 'A3':
		ref_importe = 143
	elif cuadrante == 'B1' or cuadrante == 'B2' or \
		cuadrante == 'B3':
		ref_importe = 36

	if volumen_promedio < ref_importe:
		user_copy.to_csv('Test_data.csv', index=False, header=False, mode='a');
		current_count = current_count + 1
