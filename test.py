"""
This script unzips packages and read archives inside of they.
It count the archives, introducing data in a relacional database.
Author: Rueda Caballero Luis Mario
2021-09-21

"""
from getpass import getpass
import xlrd
import zipfile
import mysql.connector
import os

def connection(): #No genero entrada a la función con parámetros porque se me pide crear la BD.
	
	#En este bloque se realiza la conexión con el entorno de mysql y la creación de la base de datos, cada que se abre
	#una conexión se cierra y se borra la variable de almacenamiento.
	try:
		connection_bd = mysql.connector.connect(host="localhost",user="root",password="")
		mycursor = connection_bd.cursor()
		mycursor.execute("CREATE DATABASE yokmak")
	except mysql.connector.Error as err:
		if err.errno==errorcode.ER_ACCESS_DENIED_ERROR:
			print("Pasa algo malo con la contraseña o el usuario")
		elif err.errno == errorcode.ER_BAD_DB_ERROR:
			print("No existe la base de datos")
		else:
			print(err)
	mycursor.close()
	del mycursor
	#########################################################################

	try:
		#En este otro bloque realizamos la conexión con la base de datos previamente creada.
		connection_bd = mysql.connector.connect(host="localhost",user="root",password="",database="yokmak")
		mycursor = connection_bd.cursor()
		mycursor.execute("CREATE TABLE principal_data (id INT(50) NOT NULL PRIMARY KEY AUTO_INCREMENT,panel_number VARCHAR(20) NOT NULL,job_number VARCHAR(20) NOT NULL,job_name VARCHAR(50) NOT NULL,seal BOOLEAN NOT NULL,type VARCHAR(20) NOT NULL,modbus_id INT(20) NOT NULL)")
		mycursor.execute("CREATE TABLE second_data (id INT(30) NOT NULL PRIMARY KEY AUTO_INCREMENT, id_FK INT(10), serial_number VARCHAR(30), meter_no INT(10))")
	except mysql.connector.Error as err:
		if err.errno==errorcode.ER_ACCESS_DENIED_ERROR:
			print("Pasa algo malo con la contraseña o el usuario")
		elif err.errno == errorcode.ER_BAD_DB_ERROR:
			print("No existe la base de datos")
		else:
			print(err)
	mycursor.close()
	del mycursor
	###########################################################################
	
#Esta función descomprime el zip, OJO: solo funciona con archivos con extensión zip.
def decompress():
	dirarchive=os.getcwd()
	with zipfile.ZipFile('Yok-Mak.zip', 'r') as zip_ref: #Se debe colocar el nombre del archivo .zip
		zip_ref.extractall(dirarchive+"\\"+'newdir') #Aquí abajo se coloca la ruta del archivo .zip
################################################################################


"""
extract() es la función más importante, los archivos de donde se pide sacar los datos son archivos de Excel,
no podía separarlos con comas en un .csv porque las celdas combinadas o mal distribuidas eran muy díficil de 
de "acomodar". Pensé en una solución rápida, el Excel es una plantilla, los valores no o raramente van a cambiar
de posición por lo que mapeé las coordenadas de las celdas y así es como extraigo sus datos. Posteriormente,
inserto en la base de datos los archivos sacados de los Excel mapeados.
"""
def extract():
	#Panel number(2,3), Job number(3,3), Seal(2,9), type(27,1), 
	#modbusid(32,2), serialnumber(49...,2) y meterno(49...,1)
	dirarchive=os.getcwd()+"\\"+'newdir'
	listarchives=os.listdir(dirarchive)
	indice_principaltable=1

	for archives in listarchives:
		actual=xlrd.open_workbook(dirarchive+"\\"+archives)
		sheet = actual.sheet_by_index(0)
		#Mapeando los datos
		panel_numberextract=sheet.cell_value(2, 3)
		job_numberextract=sheet.cell_value(3, 3)
		job_nameextract=sheet.cell_value(4, 3)
		if len(sheet.cell_value(2, 9)) == 0:
			sealextract=0
		else:
			sealextract=1
		taipeextract=sheet.cell_value(27,1)
		modbusid=sheet.cell_value(32,2)

		try:
			connection_bd = mysql.connector.connect(host="localhost",user="root",password="",database="yokmak")
			mycursor = connection_bd.cursor()
			#Introducción de datos en la primera tabla
			input_data="INSERT INTO principal_data (panel_number,job_number,job_name,seal,type,modbus_id) VALUES ('%s','%s','%s','%s','%s','%s')"%(panel_numberextract,job_numberextract,job_nameextract,sealextract,taipeextract,modbusid)
			mycursor.execute(input_data)
			connection_bd.commit()
		except mysql.connector.Error as err:
			if err.errno==errorcode.ER_ACCESS_DENIED_ERROR:
				print("Pasa algo malo con la contraseña o el usuario")
			elif err.errno == errorcode.ER_BAD_DB_ERROR:
				print("No existe la base de datos")
			else:
				print(err)
		mycursor.close()
		del mycursor

		i=49 #No. de columna donde empiezan los SN's.

		try:
			#Cada que necesito una conexión a la bd la abro y la cierro en el lugar por motivos de seguridad y optmización.
			connection_bd = mysql.connector.connect(host="localhost",user="root",password="",database="yokmak")
			mycursor = connection_bd.cursor()

			#Introducción de datos en la segunda tabla
			while(len(sheet.cell_value(i,2))!=0):
				serialnumber=sheet.cell_value(i,2)
				meterno=sheet.cell_value(i,1)
				i=i+1
				input_data="INSERT INTO second_data (id_FK,serial_number,meter_no) VALUES ('%s','%s','%s')"%(indice_principaltable,serialnumber,meterno)
				mycursor.execute(input_data)
				connection_bd.commit()
			indice_principaltable=indice_principaltable+1
		except mysql.connector.Error as err:
			if err.errno==errorcode.ER_ACCESS_DENIED_ERROR:
				print("Pasa algo malo con la contraseña o el usuario")
			elif err.errno == errorcode.ER_BAD_DB_ERROR:
				print("No existe la base de datos")
			else:
				print(err)

		mycursor.close()
		del mycursor


if __name__ == '__main__':		
	connection()
	decompress()
	extract()

