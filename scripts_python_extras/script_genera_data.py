import pandas as pd
import os

# os.environ['VENTAS_ERP_DIR'] = "/tamales_inc/ventas_mensuales_tamales_inc/mx/20200801/csv/"
# os.environ['PATH_TEINVENTO'] = "/teinvento_inc/ventas_reportadas_mercado_tamales/mx/20200801/fact_table"
# os.environ['PATH_TEINVENTO_DATALAKE'] = "/teinvento_inc/ventas_reportadas_mercado_tamales/mx"


# Generar datos por año y mes de acuerdo a la estructura del data lake para ventas tamales inc


def filtrarVentasERP(fullPath):
    suffix = "ventas_mensuales"
    fileName = (fullPath.split("/")[-1]).lower()
    if fileName.startswith(suffix):
        return fullPath

def agregarIdMes(mesName):
    dictionarioMes = {  "jan":1,
                        "feb":2,
                        "mar":3,
                        "apr":4,
                        "may":5,
                        "jun":6,
                        "jul":7,
                        "aug":8,
                        "sep":9,
                        "oct":10,
                        "nov":11,
                        "dec":12
                     }
    
    return dictionarioMes[mesName.lower()]

mypath = os.getenv('VENTAS_ERP_DIR')

f = []
for (dirpath, dirnames, filenames) in os.walk(mypath):
    if len(filenames) > 0:
        files = list(map(lambda x: os.path.join(dirpath,x),filenames))
        f.extend(files)


    
ventasFiles = list(filter(filtrarVentasERP,f))

dfList = []
columnsName= ["anio","mes","pais","categoria_calorica","sabor","origen","codigo_por_sabor","producto","total_venta"]
for file in ventasFiles:
    df = pd.read_csv(file,names=columnsName, index_col=False)
    dfList.append(df)

dataFrameVentas = pd.concat(dfList)

anios = list(dataFrameVentas["anio"].value_counts().index)
meses = list(dataFrameVentas["mes"].value_counts().index)


for anio in anios:
    for mes in meses:
        mesNum = agregarIdMes(mes)
        if  mesNum < 10:
            mesNum = '0'+str(mesNum)
        path= f'/home/alejandro/Documentos/projects/OPI/tamales_inc/ventas_mensuales_tamales_inc/mx/{anio}{mesNum}'
        if not os.path.exists(path):
            print("*"*100)
            print(path)
            os.mkdir(path)
            print("*"*100)
        df = dataFrameVentas[(dataFrameVentas["anio"] ==  anio) & (dataFrameVentas["mes"] ==  mes)]
        df.to_csv(os.path.join(path,"ventas_mensuales.csv"),index=False)



# Generar datos por año y mes de acuerdo a la estructura del data lake para teInvento.inc
def filtrarTeInventoCsv(fullPath):
    prefix = ".csv"
    fileName = (fullPath.split("/")[-1]).lower()
    if fileName.endswith(prefix):
        return fullPath
    
pathTeInvento = os.getenv('PATH_TEINVENTO')

f = []
for (dirpath, dirnames, filenames) in os.walk(pathTeInvento):
    if len(filenames) > 0:
        files = list(map(lambda x: os.path.join(dirpath,x),filenames))
        f.extend(files)

teInventoFiles = list(filter(filtrarTeInventoCsv,f))

dfList = []
columnsName= ["year","month","sales","id_region","id_product"]
for file in teInventoFiles:
    df = pd.read_csv(file,names=columnsName, index_col=False)
    dfList.append(df)

dataFrameVentasTeinvento = pd.concat(dfList)

anios = list(dataFrameVentasTeinvento["year"].value_counts().index)
meses = list(dataFrameVentasTeinvento["month"].value_counts().index)

pathTeInvento = os.getenv('PATH_TEINVENTO_DATALAKE') 

for anio in anios:
    for mes in meses:
        mesNum = agregarIdMes(mes)
        if  mesNum < 10:
            mesNum = '0'+str(mesNum)
        path= f'{pathTeInvento}/{anio}{mesNum}'
        if os.path.exists(path):
            print("*"*100)
            print(path)
            os.mkdir(path)
            print("*"*100)
        df = dataFrameVentasTeinvento[(dataFrameVentasTeinvento["year"] ==  anio) & (dataFrameVentasTeinvento["month"] ==  mes)]
        df.to_csv(os.path.join(path,"ventas_mensuales_teinventi.csv"),index=False)