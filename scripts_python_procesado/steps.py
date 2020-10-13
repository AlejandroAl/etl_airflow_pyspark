import numpy as np
import pandas as pd
import os
import time
import pandas as pd
import sqlalchemy
import os
import psycopg2


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


def obtenerNombreMes(identificadorMes):
    dictionarioMes = {  1:"Jan",
                        2:"Feb",
                        3:"Mar",
                        4:"Apr",
                        5:"May",
                        6:"Jun",
                        7:"Jul",
                        8:"Aug",
                        9:"Sep",
                        10:"Oct",
                        11:"Nov",
                        12:"Dec"
                     }
    return dictionarioMes[identificadorMes]

from datetime import timedelta,datetime
def obtenerMesPrevio(fechaActualAAAAMM):
    today = datetime.strptime(fechaActualAAAAMM, "%Y%m")
    lastMonth = today - timedelta(days=1)
    return lastMonth.strftime("%Y%m")

def filtrarCsv(fullPath):
    prefix = ".csv"
    fileName = (fullPath.split("/")[-1]).lower()
    if fileName.endswith(prefix):
        return fullPath

def obtenerDataFramePorRuta(path,fechaActualAAAAMM="", nombresColumnas=[]):
    f = []
    for (dirpath, dirnames, filenames) in os.walk(path):
        if fechaActualAAAAMM != "":
            if (len(filenames) > 0) & (str(dirpath).endswith(fechaActualAAAAMM)):
                print(dirpath)
                files = list(map(lambda x: os.path.join(dirpath,x),filenames))
                f.extend(files)
        else:
            if (len(filenames) > 0):
                print(dirpath)
                files = list(map(lambda x: os.path.join(dirpath,x),filenames))
                f.extend(files)

    dfList = []
    f = list(filter(filtrarCsv,f))
    for file in f:
        if len(nombresColumnas) > 0:
            df = pd.read_csv(file, index_col=False,names=nombresColumnas)
        else:
            df = pd.read_csv(file, index_col=False)
        dfList.append(df)

    df_r = pd.concat(dfList)
    return df_r

def aplicaMetricas(dfVentas):
    sales = dfVentas["sales"] 
    venta_acumulada =  dfVentas["venta_acumulada"]
    salesPrev =  dfVentas["salesPrev"]
    
    print("*"*10)
    print(sales)
    print(venta_acumulada)
    print(salesPrev)
    
    
    dfVentas["venta_acumulada"] = sales + venta_acumulada
    value = (sales - salesPrev)  / salesPrev
    print(value)
    print("*"*10)
    porcentaje = value if np.isnan(value) or np.isinf(value) else int(round(value*100))
    
    dfVentas["procentaje_diferencia_mes_anterior"] = porcentaje
    return dfVentas


def calculaDif_acumulados(dfVentas, mesPrevio):
    
    if mesPrevio == '':
        dfVentas["venta_acumulada"] = dfVentas["sales"]
        dfVentas["procentaje_diferencia_mes_anterior"] = np.nan
        dfVentas["salesPrev"] = 0
    else:
        con = obtenerConexionPostgres()
        idTiempo = dfVentas["idTiempo"][0] - 1
        dfVentasPrevio = pd.read_sql(f""" SELECT * FROM ventas WHERE "idTiempo" = {idTiempo} """, con).rename(columns={"sales":"salesPrev"})
        dfVentas = dfVentas.merge(dfVentasPrevio[["id_region","id_product","salesPrev","venta_acumulada"]], on=["id_region","id_product"],how="left")
        dfVentas = dfVentas.apply(aplicaMetricas,axis=1)
        con.close()
        
    return dfVentas    


def generarMetricasPorMes(fechaActualAAAAMM):

    fechaActualAAAAMM = obtenerMesPrevio(fechaActualAAAAMM)
    
    path_ventas = os.getenv('VENTAS_ERP_DIR')
    path_teinvento = os.getenv("PATH_TEINVENTO")
    
    dataFrameVentas = obtenerDataFramePorRuta(path_ventas,fechaActualAAAAMM)
    dataFrameTeinvento = obtenerDataFramePorRuta(path_teinvento,fechaActualAAAAMM)
    
    con = obtenerConexionPostgres()
    dfProductos = pd.read_sql("SELECT p.* FROM public.producto_dim p", con)
    dfRegiones = pd.read_sql("SELECT r.* FROM public.region_dim r", con)
    dfTiempo = pd.read_sql("SELECT t.* FROM public.tiempo_dim t", con)
    
    
    dataFrameVentas = dataFrameVentas.merge(dfProductos[["id_product","sabor","nombreProducto"]], left_on=["sabor","producto"], right_on=["sabor","nombreProducto"], how="left")
    dataFrameVentas = dataFrameVentas.merge(dfRegiones, left_on=["lugar_venta","origen"], right_on=["country","region"], how="left")
    dataFrameVentas.rename(columns={"anio":"year","mes":"month"},inplace=True)
#     dataFrameVentas = dataFrameVentas.groupby(by=["anio","mes","id_region","id_product"]).sum("total_venta").reset_index()    

    dataFrameVentas.rename(columns={"anio":"year","mes":"month", "total_venta":"sales"},inplace=True)
    
    dataFrameVentas = dataFrameVentas[["year","month", "sales", "id_region", "id_product"]]
    
    dataFrameSales = pd.concat([dataFrameTeinvento, dataFrameVentas])
    
    dataFrameSales = dataFrameSales.groupby(by=["year","month","id_region","id_product"])["sales"].sum().reset_index()

    dataFrameSales = dataFrameSales.merge(dfTiempo, left_on=["year","month"], right_on=["anio","mes"]).drop(["anio","mes","mesNum","year","month"], axis=1)

    mesPrevio = ""
    if not fechaActualAAAAMM.endswith("01"):
        mesPrevio = obtenerMesPrevio(fechaActualAAAAMM)
    
    dataFrameSales = calculaDif_acumulados(dataFrameSales,mesPrevio).drop(["salesPrev"],axis=1)
    
    dataFrameSales.to_sql("ventas", con,if_exists='append', index=False)
    
    con.close()

def obtenerDataFramePorRuta(path,fechaActualAAAAMM="", nombresColumnas=[]):
    f = []
    for (dirpath, dirnames, filenames) in os.walk(path):
        if fechaActualAAAAMM != "":
            if (len(filenames) > 0) & (str(dirpath).endswith(fechaActualAAAAMM)):
                print(dirpath)
                files = list(map(lambda x: os.path.join(dirpath,x),filenames))
                f.extend(files)
        else:
            if (len(filenames) > 0):
                print(dirpath)
                files = list(map(lambda x: os.path.join(dirpath,x),filenames))
                f.extend(files)

    dfList = []
    f = list(filter(filtrarCsv,f))
    for file in f:
        if len(nombresColumnas) > 0:
            df = pd.read_csv(file, index_col=False,names=nombresColumnas)
        else:
            df = pd.read_csv(file, index_col=False)
        dfList.append(df)

    df_r = pd.concat(dfList)
    return df_r

def obtenerConexionPostgres():
    usuario_postgres = os.getenv('USUARIO_POSTGRES')
    contrasena_postgres = os.getenv('CONTRASENA_POSTGRES')
    base_datos_postgres = os.getenv('NOMBRE_BASEDATOS_POSTGRES')
    host_postgres = os.getenv('HOST_POSTGRES')
    
    cadena_conexion = f"postgresql://{usuario_postgres}:{contrasena_postgres}@{host_postgres}/{base_datos_postgres}"
    engine = sqlalchemy.create_engine(cadena_conexion)
    
    con = engine.connect()
    
    return con


def getSabor_nombreProducto(s):
    producto = s["product"]
    if s["producer"] == 'Tamales Inc':
        if producto != None:
            sabor = str(producto).split(" ")[-1]
            nombreProducto = str(producto).replace(sabor,"").strip()
            s["sabor"] = (sabor).replace("-","")
            s["nombreProducto"] = nombreProducto
        else:
            s["sabor"] = None
            s["nombreProducto"] = None
    else:
        s["sabor"] = None
        s["nombreProducto"] = None
    
    return s

def agregarDimProducto():
    path_dim_producto = os.getenv('PATH_DIM_PRODUCTO')
    nombresColumnas = ["id_product","calorie_category","product","product_brand","producer"]
    dataframeproducto = obtenerDataFramePorRuta(path=path_dim_producto, nombresColumnas=nombresColumnas)    
    dataframeproducto = dataframeproducto.apply(getSabor_nombreProducto, axis=1)
    con = obtenerConexionPostgres()
    dataframeproducto.to_sql("producto_dim", con,if_exists='replace', index=False)
    con.close()



def agregarDimRegion():
    path_dim_region = os.getenv('PATH_DIM_REGION')
    nombresColumnas = ["id_region","country","region"]
    dataframeregion = obtenerDataFramePorRuta(path=path_dim_region, nombresColumnas=nombresColumnas)    
    con = obtenerConexionPostgres()
    dataframeregion.to_sql("region_dim", con,if_exists='replace', index=False)
    con.close()

def agregarDimTiempo(inicio,fin):
    inicio = inicio
    fin = fin
    
    mesesRango = list(range(1,13))
    aniosRango = list(range(inicio,fin+1))
    
    identificadores = []
    anios = []
    meses = []
    nombreMeses = []
    identificador = 0
    
    for anio in aniosRango:
        for mes in mesesRango:
            identificador += 1
            identificadores.append(identificador)
            anios.append(anio)
            meses.append(mes)
            nombreMeses.append(obtenerNombreMes(mes))
        
    dimTiempo = pd.DataFrame({'idTiempo': identificadores,
                        'anio': anios,
                        'mesNum': meses,
                        'mes': nombreMeses},
                        index=identificadores)
    
    con = obtenerConexionPostgres()
    dimTiempo.to_sql("tiempo_dim", con,if_exists='replace', index=False)
    
    
    con.close()



def crearBaseDatos():
    usuario_postgres = os.getenv('USUARIO_POSTGRES')
    contrasena_postgres = os.getenv('CONTRASENA_POSTGRES')
    base_datos_postgres = os.getenv('NOMBRE_BASEDATOS_POSTGRES')
    host_postgres = os.getenv('HOST_POSTGRES')

    conn = psycopg2.connect(host=host_postgres, port = 5432, user=usuario_postgres, password=contrasena_postgres)
    conn.autocommit = True
    cursor = conn.cursor()
    sql = f" SELECT 1 AS result FROM pg_database WHERE datname='{base_datos_postgres}' "
    cursor.execute(sql)
    value = cursor.fetchone()
    if not value:
        sql = f'''CREATE database {base_datos_postgres}'''
        cursor.execute(sql)
    cursor.close()
    conn.close()