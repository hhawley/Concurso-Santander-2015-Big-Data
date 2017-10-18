#!/usr/bin/env python

# Script que predice el comportamiento del cliente en los proximos meses. 
# Se usa una muestra para entrenar al algoritmo. Se consideran valores como la fecha, y algunos valores
# constantes del cliente (que no pueden cambiar de ninguna manera) como los que se predicen los comportamientos.
# Los demas valores, por ejemplo MOV90_CTA, son los valores a predecir. 

import sys
import os.path
import pandas as pd
import numpy as np
import zipfile as z
from sklearn.kernel_ridge import KernelRidge
from sklearn.grid_search import GridSearchCV
from multiprocessing import Pool

# Global variables rbf
num_months = 0
clf = KernelRidge(alpha = 0.1, kernel = 'rbf')
to_save = 'Test Data//predicted_values'

def Time_To_Int(dateTimeStr):
	month = int(dateTimeStr[3:5]); #print month
	year = int(dateTimeStr[6:]); #print year
	return (month - 1) + (12 * (year - 2012))

def Int_To_Time(number):
	date = '01/'
	num = int(number)
	if num >= 36:
		num = num - 31
		date = date + str(num).zfill(2) + '/2015'
	elif 36 > num and num >= 24:
		num = num - 23
		date = date + str(num).zfill(2) + '/2014'
	elif 24 > num and num >= 12:
		num = num - 11
		date = date + str(num).zfill(2) + '/2013'

	return date


# Agarro las columnas que son constantes y las que me interesan predecir \
# dependiendo del cliente y entreno al algoritmo (clf, KernelRidge) 
def train(train_Values):
	train_Values = [[ 
			Time_To_Int(cliente_mes['FECHA']), 
			cliente_mes['MOV90_CTA'], 
			cliente_mes['SALDO_ACTENOPAQ'], cliente_mes['SALDO_ACTEPAQ'], 
			cliente_mes['SALDO_ACCD'], cliente_mes['SALDO_ACAHPAQ'], 
			cliente_mes['SALDO_ACAHNOPAQ'], cliente_mes['SALDO_ACAD'], 
			cliente_mes['PP_MONTO'], cliente_mes['PR_MONTO'], cliente_mes['PH_MONTO'], 
			cliente_mes['PC_MONTO'], cliente_mes['SALDO_ADPF'], cliente_mes['SALDO_APFE'], 
			cliente_mes['SALDO_FONDO_MONEDA1'], cliente_mes['SALDO_FONDO_MONEDA2'], 
			cliente_mes['SALDO_ATIT'] 
	] for index, cliente_mes in train_Values.iterrows()]

	train_Values = np.array(train_Values)

	train_Points = train_Values[:, 0].reshape(-1, 1)
	train_Values = train_Values[:, 1:]

	clf.fit(train_Points, train_Values)

	train_Values = None
	train_Points = None

# Predecir si el usuario seguira usando los servicios en 3 meses.
def predict_user(kernel, num_months, cliente, id, months_to_predict = 3):

		cliente = cliente.tail(num_months)

		# Agarramos los valores a predecir ya que todos no son necesarios
		cliente_values = [[ 
			Time_To_Int(cliente_mes['FECHA'])
			,cliente_mes['MOV90_CTA'], 
			cliente_mes['SALDO_ACTENOPAQ'], cliente_mes['SALDO_ACTEPAQ'], 
			cliente_mes['SALDO_ACCD'], cliente_mes['SALDO_ACAHPAQ'], 
			cliente_mes['SALDO_ACAHNOPAQ'], cliente_mes['SALDO_ACAD'], 
			cliente_mes['PP_MONTO'], cliente_mes['PR_MONTO'], cliente_mes['PH_MONTO'], 
			cliente_mes['PC_MONTO'], cliente_mes['SALDO_ADPF'], cliente_mes['SALDO_APFE'], 
			cliente_mes['SALDO_FONDO_MONEDA1'], cliente_mes['SALDO_FONDO_MONEDA2'], 
			cliente_mes['SALDO_ATIT'] 
		] for index, cliente_mes in cliente.iterrows()]

		# Limpiamos memoria
		cliente = None

		cliente_values.sort(key = lambda tup: tup[0])
		cliente_values = np.array(cliente_values)

		cliente_trainingPoints = cliente_values[:, 0].reshape(-1, 1)
		cliente_trainingValues = cliente_values[:, 1:]

		# Los valores a predecir seran los mismos que prueba pero agregados los meses a predecir
		cliente_predictPoints = np.arange (
							cliente_trainingPoints[0], 
							cliente_trainingPoints[len(cliente_trainingPoints) - 1] + months_to_predict + 1
						).reshape(-1, 1)

		cliente_values = None

		kernel.fit(cliente_trainingPoints, cliente_trainingValues)

		# Todo esto solo para acomodar los datos para poder guardarlos
		cliente_predictValues = kernel.predict(cliente_predictPoints)
		len_pred_values = len(cliente_predictValues[:, 0])
		cliente_predictValues = cliente_predictValues[len_pred_values - months_to_predict:len_pred_values, :]
		
		# Append ids
		temp_array = np.zeros(months_to_predict).reshape(-1,1)
		temp_array.fill(id)
		cliente_predictValues = np.append(cliente_predictValues, temp_array, axis=1)

		# Append dates
		cliente_predictPoints = cliente_predictPoints[len(cliente_predictPoints) - months_to_predict:len(cliente_predictPoints)]
		cliente_predictPoints = np.array([Int_To_Time(x) for x in cliente_predictPoints]).reshape(-1, 1)
		cliente_predictValues = np.append(cliente_predictValues, cliente_predictPoints, axis=1)
		temp_array = None

		clf.score()

		return pd.DataFrame(cliente_predictValues, 
			#index=[0], 
			columns=['MOV90_CTA',
			'SALDO_ACTENOPAQ', 'SALDO_ACTEPAQ', 'SALDO_ACCD', 'SALDO_ACAHPAQ', 
			'SALDO_ACAHNOPAQ', 'SALDO_ACAD', 'PP_MONTO', 'PR_MONTO', 'PH_MONTO', 
			'PC_MONTO', 'SALDO_ADPF', 'SALDO_APFE', 'SALDO_FONDO_MONEDA1', 'SALDO_FONDO_MONEDA2', \
			'SALDO_ATIT', 'ID_CLIENTE', 'FECHA'])

def chunkProcess(chunk):
	ids = chunk.groupby('ID_CLIENTE')
	num = np.random.randint(100)
	for id, data in ids:
		predict_user(clf, num_months, data, id).to_csv(to_save + '.csv', index=False, header=False, mode='a')

def predict(pool, dataChunks):
	if not os.path.isfile(to_save + '.csv'):
		pd.DataFrame(
				columns=['MOV90_CTA',
				'SALDO_ACTENOPAQ', 'SALDO_ACTEPAQ', 'SALDO_ACCD', 'SALDO_ACAHPAQ', 
				'SALDO_ACAHNOPAQ', 'SALDO_ACAD', 'PP_MONTO', 'PR_MONTO', 'PH_MONTO', 
				'PC_MONTO', 'SALDO_ADPF', 'SALDO_APFE', 'SALDO_FONDO_MONEDA1', 'SALDO_FONDO_MONEDA2', \
				'SALDO_ATIT', 'ID_CLIENTE', 'FECHA']).to_csv(to_save + '.csv', index=False)

	for chunk in dataChunks:
		chunkProcess(chunk)
	# pool.map(chunkProcess, dataChunks)
	# pool.close()

def main():
	path_data = ''
	#num_months = 0
	try:
		path_data = str(sys.argv[1])
	except:
		print 'Ninguna direccion a la informacion fue escrita.'
		return 0

	try:
		num_months = int(sys.argv[2])
	except:
		print 'Cuantos meses?'
		return 1

	try:
		to_save = str(sys.argv[3])
	except:
		i=0

	columns=['FECHA', 'ID_CLIENTE', 'MOV90_CTA', 'ANTIGUEDAD', 'CUADRANTE', 
		'SALDO_ACTENOPAQ', 'SALDO_ACTEPAQ', 'SALDO_ACCD', 'SALDO_ACAHPAQ', 
		'SALDO_ACAHNOPAQ', 'SALDO_ACAD', 'PP_MONTO', 'PR_MONTO', 'PH_MONTO',
		'PC_MONTO', 'SALDO_ADPF', 'SALDO_APFE', 'SALDO_FONDO_MONEDA1', 'SALDO_FONDO_MONEDA2',
		'SALDO_ATIT']

	dataHeaders = pd.read_excel('Informacion para el Analisis//DICCIONARIO_DATOS_CLIENTE.xlsx')

	if path_data.endswith('.zip'):
		zip_file = z.ZipFile(path_data)

		str_path_txt = path_data.split('\\')
		lenn = len(str_path_txt)
		str_path_txt = str_path_txt[lenn - 1].split('.')[0]
		print str_path_txt
		dataChunks = pd.read_csv(zip_file.open(str_path_txt + '.txt'), usecols=columns, chunksize= num_months * 1000, names=columns)
	elif path_data.endswith('.txt') or path_data.endswith('.csv'):
		dataChunks = pd.read_csv(path_data, usecols=columns, chunksize= num_months * 1000, names=columns)
	else:
		print 'File is the wrong format: (needs .txt, .csv or .zip)'
		return 3

	print 'Initializing prediction...'
	pool = Pool(1)
	print 'Doing training...'
	#train(pd.read_csv('Test_data.csv'))
	print 'Parallel running... now'
	predict(pool, dataChunks)
	print 'Done'

if __name__ == "__main__":
	main()