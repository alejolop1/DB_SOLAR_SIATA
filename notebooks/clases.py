import pandas as pd


#CLASE PARA ELIMINAR COLUMNA CALIDAD(SIATA) Y VOLVER DATE_TIME LA COLUMNA DE FECHA
class ProcesarDataFrame:
    def __init__(self, df):
        self.df = df

    def eliminar_cal(self):
        # Eliminar la columna "Calidad"
        self.df = self.df.drop(columns=["Calidad"], errors='ignore')
        return self.df

    def date_time(self):
        # Convertir la columna "fecha_hora" a tipo datetime
        self.df["fecha_hora"] = pd.to_datetime(self.df["fecha_hora"], errors='coerce')
        return self.df

#CLASE PARA UNIR LAS VARIABLES METEOROLOGICAS DE LA ESTACIÓN
class UnirDataFrames:
    def __init__(self, dfs):
        # Lista de DataFrames
        self.dfs = dfs
        self.df_final = None

    def unir_variables(self):
        # Unir los DataFrames de la lista usando 'fecha_hora' como clave
        if len(self.dfs) > 1:
            self.df_final = self.dfs[0]
            for df in self.dfs[1:]:
                self.df_final = pd.merge(self.df_final, df, on='fecha_hora')
        else:
            self.df_final = self.dfs[0]
        return self.df_final

#RENOMBRAR COLUMNAS, PARA COINCIDAD CON TABLAS DE MYSQL
def name_columns(df):
    df.columns = name_sql
    return df

#FUNCION PARA UNIR LAS ESTACIONES EN UN DF
def unir_estaciones(df1, df2):
    df_final = pd.concat([df1, df2], axis=0, ignore_index=True)
    return df_final

#LEER CSV DE LAS ESTACIONES
hum_rio = pd.read_csv("data/rionegro_julio/estacion_data_humedad_199__20240701_20240731.csv")
tem_rio = pd.read_csv("data/rionegro_julio/estacion_data_temperatura_199__20240701_20240731.csv")
pre_rio = pd.read_csv("data/rionegro_julio/estacion_data_presion_199__20240701_20240731.csv")
vie_rio = pd.read_csv("data/rionegro_julio/estacion_data_vientos_199__20240701_20240731.csv")
#presp_rio = pd.read_csv("data/rionegro_julio/estacion_data_precipitacion_199__20240701_20240731.csv")

hum_pie_bl = pd.read_csv("data/piedras_blancas_julio/estacion_data_humedad_207__20240701_20240731.csv")
tem_pie_bl = pd.read_csv("data/piedras_blancas_julio/estacion_data_temperatura_207__20240701_20240731.csv")
pre_pie_bl = pd.read_csv("data/piedras_blancas_julio/estacion_data_presion_207__20240701_20240731.csv")
vie_pie_bl = pd.read_csv("data/piedras_blancas_julio/estacion_data_vientos_207__20240701_20240731.csv")
#presp_pie_bl = pd.read_csv("data/piedras_blancas_julio/estacion_data_precipitacion_207__20240701_20240731.csv")


#LLAMAR LOS METODOS
dfs_rione = [hum_rio,tem_rio, pre_rio, vie_rio]
dfs_rio_procesados = []
for i in dfs_rione:
    procesador = ProcesarDataFrame(i)
    df = procesador.eliminar_cal()
    df = procesador.date_time()
    dfs_rio_procesados.append(df)

dfs_pie_bl = [tem_pie_bl,hum_pie_bl,pre_pie_bl,vie_pie_bl]
dfs_pbl_procesados = []
for k in dfs_pie_bl:
    procesador = ProcesarDataFrame(k)
    dfk = procesador.eliminar_cal()
    dfk = procesador.date_time()
    dfs_pbl_procesados.append(dfk)

#
est_rionegro = UnirDataFrames(dfs_rio_procesados)
est_piedras_blancas = UnirDataFrames(dfs_pbl_procesados)
df_rionegro = est_rionegro.unir_variables()
df_piedras_blancas = est_piedras_blancas.unir_variables()

df_rionegro["nombre_estacion"] = 'rionegro'
df_piedras_blancas["nombre_estacion"] = 'piedras_blancas'

name_sql = ["fecha_medicion","temperatura","humedad", "presion", "viento_velocidad_prom_", 
            "viento_velocidad_max", "viento_direccion_prom", "viento_direccion_max", "nombre_estacion"]

df_rionegro = name_columns(df_rionegro)
df_piedras_blancas = name_columns(df_piedras_blancas)

print(unir_estaciones(df_rionegro, df_piedras_blancas))