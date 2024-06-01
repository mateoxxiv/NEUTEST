import pandas as pd
import numpy as np
import json
import time

def medir_tiempo(funcion):
    def wrapper(*args, **kwargs):
        inicio = time.time()
        resultado = funcion(*args, **kwargs)
        fin = time.time()
        tiempo_transcurrido = fin - inicio
        print("Tiempo de ejecución de '{}': {} segundos".format(funcion.__name__, tiempo_transcurrido))
        return resultado
    return wrapper


class items_factura:
    
    def __init__(self):
        # Lectura de los archivos CSV
        self.consumption = pd.read_csv('./files/consumption.csv')
        self.consumption.rename(columns={'value': 'consumption_value'}, inplace=True)
                                
        self.injection = pd.read_csv('./files/injection.csv')
        self.injection.rename(columns={'value': 'injection_value'}, inplace=True)

        self.records = pd.read_csv('./files/records.csv')
        self.records['record_timestamp'] = pd.to_datetime(self.records['record_timestamp'])

        self.services = pd.read_csv('./files/services.csv')
        self.tariffs = pd.read_csv('./files/tariffs.csv')

        self.xm_data_hourly_per_agent = pd.read_csv('./files/xm_data_hourly_per_agent.csv')
        self.xm_data_hourly_per_agent.rename(columns={'value': 'hour_value'}, inplace=True)
        self.xm_data_hourly_per_agent['record_timestamp'] = pd.to_datetime(self.xm_data_hourly_per_agent['record_timestamp'])

    
    def get_items(self, service: int = np.nan):
        # Separamos los items para cada cliente
        if not np.isnan(service):
            # Combinar registros
            join1 = pd.merge(self.records, self.consumption, how='left', on='id_record')
            join2 = pd.merge(join1, self.injection, how='left', on='id_record')
            join3 = pd.merge(join2, self.services, how='left', on='id_service')

            # Procesar los diferentes niveles de voltaje por separado
            join3_2_3 = join3[join3['voltage_level'].isin([2, 3])]
            join3_2_3 = pd.merge(join3_2_3, self.tariffs, how='left', on=['id_market','voltage_level'])

            join3_other = join3[~join3['voltage_level'].isin([2, 3])]
            join3_other = pd.merge(join3_other, self.tariffs, how='left', on=['id_market', 'cdi', 'voltage_level'])

            # Unir resultados
            join3 = pd.concat([join3_2_3, join3_other])
            join3 = join3[join3['id_service'] == service]

            
            
            join3['record_timestamp'] = pd.to_datetime(join3['record_timestamp'])

            # Sumatioria de value_consumption y value_injection por horas
            group = join3.groupby(['record_timestamp']).agg({
                'consumption_value':'sum',
                'injection_value':'sum',}).reset_index()
            
            # Agregar la tarifa por cada hora
            group = pd.merge(group, self.xm_data_hourly_per_agent, on='record_timestamp', how='left')

            # Hallar el valor acumulado de consumption e injection
            group['cumu_consumption_value'] = group['consumption_value'].cumsum()
            group['cumu_injection_value'] = group['injection_value'].cumsum()

            group['ee2'] = np.where(
                group['cumu_injection_value'] <= join3['consumption_value'].sum(),
                0,
                np.where(
                    group['cumu_injection_value'].shift(1) <= join3['consumption_value'].sum(),
                    group['cumu_injection_value'] - join3['consumption_value'].sum(),
                    group['injection_value']
                )
            ) * group['hour_value']

            # Calculate the required values
            ea_sum = join3['consumption_value'].sum() * join3['CU'].unique()[0]
            ec_sum = join3['injection_value'].sum() * join3['C'].unique()[0]
            ee1_sum = (np.minimum(join3['consumption_value'].sum(), join3['injection_value'].sum()))*-join3['CU'].unique()[0]
            ee2_sum = (group.ee2.sum())

            response = {
                'id': service,
                'data': {
                    'ea': ea_sum,
                    'ec': ec_sum,
                    'ee1': ee1_sum,
                    'ee2': - ee2_sum
                }
            }

            return json.dumps(response, indent=4)
        else:
            return 'No especifico'

# Instanciar la clase items_factura
calc = items_factura()

# Llamar al método get_items decorado para medir el tiempo de ejecución
@medir_tiempo
def calcular_factura():
    return calc.get_items(2478)

# Llamar a la función decorada
resultado = calcular_factura()
print(resultado)