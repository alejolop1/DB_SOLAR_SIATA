import pandas as pd

class DataProcessor:
    def __init__(self):
        self.df = None  # Inicialmente no hay DataFrame
    
    def cargar_df(self, df):
        """
        Carga un DataFrame inicial para ser procesado.
        """
        self.df = df
        print("DataFrame cargado.")
    
    def eliminar_cal(self):
        """
        Elimina todas las columnas excepto las dos primeras.
        """
        if self.df is not None:
            self.df = self.df["Calidad"]
            print("Columnas eliminadas.")
        else:
            print("Error: No se ha cargado ningún DataFrame.")
    
    def date_time(self):
        """
        Convierte la columna 'fecha_hora' a formato datetime.
        """
        if self.df is not None and 'fecha_hora' in self.df.columns:
            self.df["fecha_hora"] = pd.to_datetime(self.df["fecha_hora"])
            print("Columna 'fecha_hora' convertida a datetime.")
        else:
            print("Error: No se ha cargado ningún DataFrame o falta la columna 'fecha_hora'.")
    
    def unir_varios_variables(self, dfs):
        """
        Une el DataFrame actual con una lista de DataFrames en la columna 'fecha_hora'.
        """
        if self.df is not None and 'fecha_hora' in self.df.columns:
            for df in dfs:
                if 'fecha_hora' in df.columns:
                    self.df = pd.merge(self.df, df, on='fecha_hora')
                    print("DataFrame unido en 'fecha_hora'.")
                else:
                    print("Error: El DataFrame proporcionado no contiene la columna 'fecha_hora'.")
        else:
            print("Error: Asegúrate de que 'fecha_hora' esté en el DataFrame principal.")
    
    # def unir_varios_estaciones(self, dfs):
    #     """
    #     Concatena el DataFrame actual con una lista de DataFrames horizontalmente.
    #     """
    #     if self.df is not None:
    #         self.df = pd.concat([self.df] + dfs, axis=1)
    #         print("Estaciones unidas por columnas.")
    #     else:
    #         print("Error: No se ha procesado correctamente el DataFrame.")
    
    def obtener_df(self):
        """
        Retorna el DataFrame procesado.
        """
        return self.df
    

hum = pd.read_csv("data/rionegro_julio/estacion_data_humedad_199__20240701_20240731.csv")
tem = pd.read_csv("data/rionegro_julio/estacion_data_temperatura_199__20240701_20240731.csv")
pre = pd.read_csv("data/rionegro_julio/estacion_data_presion_199__20240701_20240731.csv")
vie = pd.read_csv("data/rionegro_julio/estacion_data_viento_199__20240701_20240731.csv")
presp = pd.read_csv("data/rionegro_julio/estacion_data_precipitacion_199__20240701_20240731.csv")

procesador = DataProcessor


procesador.cargar(hum)

procesador.eliminar_cal()
procesador.date_time()

df_f = procesador.unir_varios_variables([tem, pre, vie, presp])
print(df_f)
