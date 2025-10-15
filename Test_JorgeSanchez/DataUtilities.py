from omegaconf import OmegaConf
import pandas as pd
import os
import sys
from datetime import datetime

#Carga el archivo de configuraciones
def load_config(pais: str = "Guatemala", env: str = "dev"):
    
    global  csvPath, abreviatura, factorConversion, moneda, unidadReducida, processedPath
    try:
        if env == "dev":
            conf = OmegaConf.load("config/Dev.yaml")
        else:
            conf = OmegaConf.load("config/PRD.yaml")

        csvPath = conf.globalPath
        fecha_inicio = conf.fecha_inicio
        fecha_fin = conf.fecha_fin
        abreviatura = conf[pais].abreviatura
        factorConversion = conf[pais].Factor_conversion_ST
        moneda = conf[pais].moneda
        unidadReducida = conf[pais].unidad_reducida
        processedPath = conf[pais].processedPath

    except ValueError:
        print("Error al procesar el archivo de configuración.")

# validando las fechas encontradas en el archivo de configuracion
def date_validate():
    
    global fecha_inicio, fecha_fin

    validate = False
    try:
        # Solicitar fechas por consola
        fecha_inicio_str = input("Ingresa la fecha de inicio (YYYY-MM-DD): ")
        fecha_fin_str = input("Ingresa la fecha de fin (YYYY-MM-DD): ")

        fecha_inicio = datetime.strptime(fecha_inicio_str, "%Y-%m-%d").date()
        fecha_fin = datetime.strptime(fecha_fin_str, "%Y-%m-%d").date()
        
        # Validar que el rango sea correcto
        if fecha_fin < fecha_inicio:
            print("Error: la fecha de fin no puede ser anterior a la de inicio.")
        else:
            print("Rango correcto:", fecha_inicio,fecha_fin) 
            validate = True
            
    except ValueError:
        print("Formato incorrecto en archivo de configuración. Usa el formato YYYY-MM-DD.")

    return validate

#Validando los datos de cada columna    
def data_validation(df):

    columnas = df.columns
    for col in columnas:
        cantidad_nulos = df[col].isna().sum()
        if cantidad_nulos > 0:
            print(f"Columna '{col}' tiene {cantidad_nulos} valores nulos o vacíos.")
        else:
            print(f"Columna '{col}' no tiene valores nulos.")

        
def data_process():

    global fecha_inicio, fecha_fin, abreviatura, factorConversion
    try:
        
        df = pd.read_csv(csvPath)

        #obteniendo la información en el rango de fechas y pais seleccionado
        dfResult = df[(df["pais"] == abreviatura) & (df["fecha_proceso"].between(int(fecha_inicio.strftime('%Y%m%d')), int(fecha_fin.strftime('%Y%m%d'))))]

        #Validando los datos
        data_validation(dfResult)
        
        #obteniendo particiones con base a la fecha
        lstPartition = dfResult["fecha_proceso"].unique()

        #Filtrando tipos de entrega validos
        dfResult = dfResult[dfResult["tipo_entrega"].isin(["ZPRE", "ZVE1","Z04","Z05"])]

        #agregando columnas por cada tipo de entrega encontrada
        lstTipoEntrega = dfResult["tipo_entrega"].unique()
        for col in lstTipoEntrega:
            dfResult[col] = None
            dfResult.loc[dfResult["tipo_entrega"] == col, col] = "X"

        #Actualizando cantidades a cajas
        dfResult.loc[dfResult['unidad'] == 'ST', 'cantidad'] = dfResult.loc[dfResult['unidad'] == 'ST', 'cantidad'] / factorConversion
        #Actualizando unidad de medida
        dfResult.loc[dfResult['unidad'] == 'ST', 'unidad'] = unidadReducida #(CS)


        
        #Creando archivo por particion
        for partition in lstPartition:
        
            dfPartition = dfResult[dfResult["fecha_proceso"] == partition]
        
            fileName = str(partition) + ".csv"
            fullPath = os.path.join(processedPath, fileName)
        
            dfPartition.to_csv(fullPath, index=False)

            print("Guardando arcvhivo: ", fullPath)

    except ValueError:
        print("Error al procesar los datos")
    