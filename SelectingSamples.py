#	Author: Hector Hawley Herrera
#	Descripcion: Script para la eleccion de las muestras para tener una muestra poblacional decente y buscarlo
# 	cada mes

# Librerias necesarias
import pandas as pd
import numpy as np
import os.path
import urllib2
import zipfile as z

# Variables para web
base_url = 'http://www.santanderneoschallenge.com/data/CLIENTES_{}{}.zip'

# Para ver el progreso en la seleccion de data
def printCompleted(i, length, current):
	print(str(100.0*i / length) + '% Completado')
	print("Hasta ahora: " + str(current))


# Variables para leer el archivo de texto principalmente.
meses = ['DEC', 'NOV', 'OCT', 'SEP', 'AUG', 'JUL', 'JUN', 'MAY', 'APR', 'MAR', 'FEB', 'JAN']
years = ['2014', '2013', '2012']

# Variables para la seleccion de los datos
DEC2014_chunksize = 1000
totalMiembros = 0
totalMaxMiembros = 3000
miembrosPorChunk = 25
l = 2342 # Longitud aproximada de chunks que se formaran en diciembre 2014
gruposDeseados = 120
aproxPorcentaje = gruposDeseados * 100.0 / l # en porcentaje

# Los archivos no tienen los nombres de las columnas, no necesarios pero facilita la lectura
dataHeaders = pd.read_excel('Informacion para el Analisis/DICCIONARIO_DATOS_CLIENTE.xlsx')

# Si las muestras de enero existen solo abrelo
if os.path.isfile("Result//CLIENTES_DEC2014.txt"):
	miembrosSelectos = pd.read_csv("Result//CLIENTES_DEC2014.txt", names=dataHeaders["VARIABLE"])
else:
	miembrosSelectos = pd.DataFrame(columns = dataHeaders["VARIABLE"])

for year in years:
	for mes in meses:

		print('En: ' + mes + year)
		pathFile = 'Downloads//CLIENTES_' + mes + year + '.zip'

		if not os.path.isfile(pathFile):
			connect = urllib2.urlopen(base_url.format(mes, year))
			data = connect.read()
			with open(pathFile, "wb") as code:
				code.write(data)


		# El archivo se descarga como zip
		zip_archive = z.ZipFile(pathFile, 'r')


		# Para crear y filtrar las muestras en Enero
		if mes == 'DEC' and year == '2014':
			chunks = pd.read_csv(zip_archive.open('CLIENTES_' + mes + year + ".txt"), 	\
				 names=dataHeaders["VARIABLE"], 										\
				 sep=';', 																\
				 chunksize= DEC2014_chunksize)

			i = 0
			for chunk in chunks:
				chance =  100 * np.random.rand()
				if chance <= aproxPorcentaje:
					miembrosSelectos = miembrosSelectos.append(chunk.sample(miembrosPorChunk))

					totalMiembros = totalMiembros + miembrosPorChunk
					if totalMiembros >= totalMaxMiembros:
						break 

				printCompleted(i, l, totalMiembros)
				i = i + 1

			miembrosSelectos.to_csv("Result//CLIENTES_DEC2014.txt", 	\
				index=False, 											\
				header=False, 											\
				mode='a')
			try:
				zip_archive_write = z.ZipFile("Result//CLIENTES.zip", 'w', z.ZIP_DEFLATED)
				zip_archive_write.write('Result/CLIENTES_DEC2014.txt')
			finally:
				zip_archive_write.close()


		elif mes != 'DEC' or year != '2014':
			print len(miembrosSelectos)

			# No existe informacion de Diciembre, Noviembre y Octubre del 2012
			if year == '2012' and (mes != 'DEC' and mes != 'NOV' and mes != 'OCT'): 
				continue

			chunks = None
			chunks = pd.read_csv(zip_archive.open('CLIENTES_' + mes + year + ".txt"), 	\
				names=dataHeaders["VARIABLE"], 											\
				sep=';',																\
				chunksize=50000)

			for chunk in chunks:	 
				chunk['ID_CLIENTE'] = pd.to_numeric(chunk['ID_CLIENTE'], errors='coerce')
				chunk.loc[chunk['ID_CLIENTE'].isin(miembrosSelectos['ID_CLIENTE'])].to_csv('Result/CLIENTES_' + mes + year + '.txt', 	\
					index=False, 																										\
					header=False, \
					mode='a')
			try:
				zip_archive_write = z.ZipFile('Result//CLIENTES.zip', 'w', z.ZIP_DEFLATED)
				zip_archive_write.write('Result/CLIENTES_' + mes + year + '.txt')
			finally:
				zip_archive_write.close()