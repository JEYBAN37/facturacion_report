# This is a sample Python script.
import csv
from pathlib import Path

import pandas as pd


# Press Mayús+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
COBERTURA_ATENCION = [
    "CONSULTA DE PRIMERA VEZ POR MEDICINA GENERAL RIAS",
    "CONSULTA DE PRIMERA VEZ POR MEDICINA GENERAL",
    "CONSULTA DE PRIMERA VEZ POR ESPECIALISTA EN PEDIATRIA",
    "CONSULTA DE CONTROL O DE SEGUIMIENTO POR ESPECIALISTA EN PEDIATRIA",
    "INTERCONSULTA POR ESPECIALISTA EN PEDIATRIA",
    "ATENCION VISITA DOMICILIARIA POR MEDICINA GENERAL",
]

COBERTURA_SALUD_BUCAL = [
"CONSULTA DE CONTROL O DE SEGUIMIENTO  POR ODONTOLOGIA GENERAL RIAS",
"CONSULTA DE PRIMERA VEZ POR ODONTOLOGIA GENERAL RIAS",
"CONSULTA DE PRIMERA VEZ POR ODONTOLOGIA GENERAL",
"CONSULTA DE PRIMERA VEZ POR ODONTOLOGIA GENERAL",
"CONSULTA DE CONTROL O DE SEGUIMIENTO  POR ODONTOLOGIA GENERAL",
"CONSULTA DE URGENCIAS POR ODONTOLOGIA GENERAL",
"CONSULTA DE PRIMERA VEZ POR ODONTOLOGIA EMBARAZO RIAS",
"EDUCACION INDIVIDUAL EN SALUD POR ODONTOLOGIA",

]

TAMIZAJE_CUELLO = [
    "TOMA NO QUIRURGICA DE MUESTRA O TEJIDO VAGINAL PARA ESTUDIO CITOLOGICO RIAS",
    "ESTUDIO ANATOMOPATOLOGICO BASICO EN CITOLOGIA CERVICOVAGINAL MANUAL"]

TAMIZAJE_COLON = ["SANGRE OCULTA EN MATERIA FECAL GUAYACO O EQUIVALENTE"]
def detect_and_read(path: Path):
    # 1) detect delimiter
    sample = path.read_bytes()[:8192].decode('utf-8', errors='replace')
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[',',';','\t','|'])
        sep = dialect.delimiter
    except Exception:
        sep = ','

    print(f"Detected delimiter: {sep!r}")

    # 2) try tolerant read with python engine (no low_memory) and warn on bad lines
    try:
        df = pd.read_csv(path, sep=sep, engine='python', on_bad_lines='warn', encoding='utf-8')
        print("Read OK (initial attempt). Rows:", len(df))
        return df
    except Exception as e:
        print("Initial read failed:", e)

    # 3) determine expected field count from header (using detected sep)
    with path.open('r', encoding='utf-8', errors='replace') as f:
        first_line = f.readline().rstrip('\n\r')
    expected = len(first_line.split(sep))
    print("Expected fields (from header):", expected)

    # 4) scan file to find bad lines (counts != expected)
    bad = []
    with path.open('r', encoding='utf-8', errors='replace') as f:
        reader = csv.reader(f, delimiter=sep)
        for i, row in enumerate(reader, start=1):
            if len(row) != expected:
                bad.append((i, len(row), row))
    print("Problematic lines found:", len(bad))
    if bad:
        for ln, cnt, _ in bad[:5]:
            print("line", ln, "fields:", cnt)

    # 5) simple auto-repair: if row has more fields, merge extras into last column
    cleaned = path.with_name(path.stem + '_cleaned' + path.suffix)
    with path.open('r', encoding='utf-8', errors='replace') as fin, cleaned.open('w', encoding='utf-8', newline='') as fout:
        reader = csv.reader(fin, delimiter=sep)
        writer = csv.writer(fout, delimiter=sep)
        for row in reader:
            if len(row) > expected:
                row = row[:expected-1] + [sep.join(row[expected-1:])]
            elif len(row) < expected:
                row = row + [''] * (expected - len(row))
            writer.writerow(row)

    print("Wrote cleaned file to", cleaned)
    df = pd.read_csv(cleaned, sep=sep, engine='python', encoding='utf-8')
    print("Read cleaned file. Rows:", len(df))
    return df

def unificacion_facturacion():

    #df_2020_01 = pd.read_csv('Facturacion/2020/2020-1.csv')
    df_2020_01 = detect_and_read(Path(r'Facturacion/2020/2020-1.csv'))
    df_2020_02 = pd.read_excel('Facturacion/2020/2020-2-1.xlsx')
    df_2020_03 = pd.read_excel('Facturacion/2020/2020-2-2.xlsx')

    df_facturacion_consolidado = pd.concat(
    [df_2020_01,df_2020_02,df_2020_03], ignore_index=True)
    print(df_facturacion_consolidado.head())

    df_facturacion_consolidado.to_csv('Facturacion/2020/Facturacion_Consolidado_2020.csv', index=False)

def unificacion_produccion():
    df_produccion = pd.read_excel('Produccion/Informe_Produccion.xlsx')
    df_produccion_adicional_uno = pd.read_excel('Produccion/1.xlsx')
    df_produccion_adicional_dos = pd.read_excel('Produccion/2.xlsx')
    df_produccion_adicional_tres = pd.read_excel('Produccion/3.xlsx')
    df_produccion_adicional_cuatro = pd.read_excel('Produccion/4.xlsx')

    df_produccion_consolidado = pd.concat([df_produccion, df_produccion_adicional_uno, df_produccion_adicional_dos,
                                           df_produccion_adicional_tres,df_produccion_adicional_cuatro], ignore_index=True)
    print(df_produccion_consolidado.head())

    df_produccion_consolidado.to_csv('Produccion/Produccion_Consolidado.csv', index=False)



def calcular_denominadores(df,binsc = None, labelsc= None, genero=None):
    poblacion_denominador = df.drop_duplicates('Identificacion')
    if genero is not None:
        poblacion_denominador = poblacion_denominador[poblacion_denominador['Sexo'] == genero]
    # Normalize Edad to a numeric value (extract first number if string contains 'años' or similar)
    poblacion_denominador.loc[:, 'Edad'] = poblacion_denominador['Edad'].astype(str).str.extract(r"(\d+)")[0]
    poblacion_denominador.loc[:, 'Edad'] = pd.to_numeric(poblacion_denominador['Edad'], errors='coerce')

    # If no bins/labels provided, create a single 'Total' age_group so downstream code still works
    if not binsc or not labelsc:
        poblacion_denominador.loc[:, 'age_group'] = 'Total'
        return poblacion_denominador.groupby('age_group').size().reset_index(name='count')

    poblacion_denominador.loc[:, 'age_group'] = pd.cut(poblacion_denominador['Edad'], bins=binsc, labels=labelsc,
                                                include_lowest=True)

    return poblacion_denominador.groupby('age_group').size().reset_index(name='count')


def encontrados_con_ebs():

    df_facturacion = pd.read_csv('Facturacion/2020/Facturacion_Consolidado_2020.csv')

    df_filtrado = df_facturacion[
        ['Identificacion', 'Servicio', 'CodProce', 'programa', 'CodDiag', 'Fecha_Servicio', 'Edad','Sexo']]

    # Normalize Edad to numeric early so range filters work correctly
    df_filtrado.loc[:, 'Edad'] = df_filtrado['Edad'].astype(str).str.extract(r"(\d+)")[0]
    df_filtrado.loc[:, 'Edad'] = pd.to_numeric(df_filtrado['Edad'], errors='coerce')

    bins_curso_vida = [0, 5, 11, 17, 28, 59, 150]
    labels_curso_vida = ['0-5', '6-11', '12-17', '18-28', '29-59', '60+']

    # Use proper bin edges for age-range bins
    bins_cuello_uterino = [25, 65]
    labels_cuello_uterino = ['25-65']

    bins_mama = [50, 69]
    labels_mama = ['50-69']

    cobertura_atencion, _ = valores_por_servicio(df_filtrado, COBERTURA_ATENCION, bins=bins_curso_vida, labels=labels_curso_vida)
    cobertura_personas , _ = valores_por_servicio(df_filtrado, COBERTURA_SALUD_BUCAL, bins=bins_curso_vida, labels=labels_curso_vida)
    cobertura_agudeza_visual, _ = valores_por_servicio(df_filtrado, None,890201 ,bins=bins_curso_vida, labels=labels_curso_vida) #"890201"
    tamizadas_cancer_cuello_uterino, _ = valores_por_servicio(df_filtrado, TAMIZAJE_CUELLO,None, rango_edad=(25, 65), sexo='F', bins=bins_cuello_uterino, labels=labels_cuello_uterino)
    tamizaje_cancer_mama, _  = valores_por_servicio(df_filtrado, None, None, rango_edad=(50, 69), sexo='F', bins=bins_mama, labels=labels_mama)
    tamizajes_cancer_colon, _  = valores_por_servicio(df_filtrado, TAMIZAJE_COLON, rango_edad=(50, 75), bins=[50, 75], labels=['50-75'])
    tamizaje_prostata, _  = valores_por_servicio(df_filtrado, ["ANTIGENO ESPECIFICO DE PROSTATA SEMIAUTOMATIZADO O AUTOMATIZADO","COPROSCOPICO"],  rango_edad=(50, 150), sexo='M', bins=[50, 150], labels=['50-150'])
    cobertura_placa, _  = valores_por_servicio(df_filtrado, ["CONTROL DE PLACA DENTAL RIAS"], bins=bins_curso_vida, labels=labels_curso_vida)

    df_denominador_gestantes = df_filtrado[df_filtrado['programa'].isin(["RUTA MATERNO PERINATAL","Deteccion de Alteraciones del Embarazo","Atencion del Parto"])]


    gestantes_temprana, _  = valores_por_servicio(df_denominador_gestantes, sexo='F',codigo_diag=["Z340","Z350"],bins=[13, 50], labels=['13-50'])
    tamizaje_gestantes_vih, df_vih  = valores_por_servicio(df_denominador_gestantes, servicio=["VIRUS DE INMUNODEFICIENCIA HUMANA 1 Y 2 ANTICUERPOS","PRUEBA RAPIDA VIH 1 Y 2"], sexo='F',bins=[13, 50], labels=['13-50'])
    tamizaje_getantes_sifilis, df_sifilis = valores_por_servicio(df_denominador_gestantes, servicio=["PRUEBA RAPIDA TREPONEMA PALLIDUM","PRUEBA NO TREPONEMICA MANUAL","SEROLOGIA PRUEBA NO TREPONEMICA RIAS",""], sexo='F',bins=[13, 50], labels=['13-50'])
    tamizaje_gestantes_hepatitis, df_hepatitis = valores_por_servicio(df_denominador_gestantes, servicio=["PRUEBA RAPIDA ANTIGENO DE SUPERFICIE AG HBS",""], sexo='F',bins=[13, 50], labels=['13-50'])
    gestantes_suministros, _ = valores_por_servicio(df_denominador_gestantes,bins=[13, 50], labels=['13-50'])


    # Obtener identificaciones únicas del denominador (gestantes)
    ids_denominador = set(df_denominador_gestantes['Identificacion'].dropna().astype(str).unique())

    # Obtener identificaciones únicas para cada tamizaje (los df_* vienen del segundo valor devuelto por valores_por_servicio)
    ids_vih = set(df_vih['Identificacion'].dropna().astype(str).unique()) if 'df_vih' in locals() and df_vih is not None else set()
    ids_sifilis = set(df_sifilis['Identificacion'].dropna().astype(str).unique()) if 'df_sifilis' in locals() and df_sifilis is not None else set()
    ids_hepatitis = set(df_hepatitis['Identificacion'].dropna().astype(str).unique()) if 'df_hepatitis' in locals() and df_hepatitis is not None else set()

    # Intersecciones con el denominador (solo consideramos los IDs que están en el grupo de gestantes)
    ids_vih_in_den = ids_vih & ids_denominador
    ids_sifilis_in_den = ids_sifilis & ids_denominador
    ids_hepatitis_in_den = ids_hepatitis & ids_denominador

    # Conteos
    count_vih = len(ids_vih_in_den)
    count_sifilis = len(ids_sifilis_in_den)
    count_hepatitis = len(ids_hepatitis_in_den)
    count_any = len((ids_vih | ids_sifilis | ids_hepatitis) & ids_denominador)
    count_all_three = len((ids_vih & ids_sifilis & ids_hepatitis) & ids_denominador)

    # Opcional: porcentaje sobre el denominador total de gestantes
    total_gestantes = len(ids_denominador)
    pct_vih = round(count_vih / total_gestantes * 100, 2) if total_gestantes > 0 else 0
    pct_sifilis = round(count_sifilis / total_gestantes * 100, 2) if total_gestantes > 0 else 0
    pct_hepatitis = round(count_hepatitis / total_gestantes * 100, 2) if total_gestantes > 0 else 0
    pct_any = round(count_any / total_gestantes * 100, 2) if total_gestantes > 0 else 0
    pct_all_three = round(count_all_three / total_gestantes * 100, 2) if total_gestantes > 0 else 0

    # Mostrar resultados
    print("Cobertura de con atención por enfermería, medicina general o especializada en pediatría o medicina familiar de acuerdo con el esquema definido para el curso de vida:")
    print(cobertura_atencion[['age_group', 'porcentaje_cobertura']])
    print("Cobertura de personas con valoración de la salud bucal de acuerdo con el esquema definido para el curso de vida:")
    print(cobertura_personas[['age_group', 'porcentaje_cobertura']])
    print("Proporción de personas con tamizaje de agudeza visual de acuerdo con el esquema definido para el curso de vida:")
    print(cobertura_agudeza_visual[['age_group', 'porcentaje_cobertura']])
    print("Proporción de mujeres entre 25 y 65 años tamizadas para cáncer de cuello uterino con cualquier prueba de tamización:")
    print(tamizadas_cancer_cuello_uterino[['porcentaje_cobertura']])
    print("Proporción de mujeres entre 50 y 69 años tamizadas para cáncer de mama con mamografía en los últimos dos años:")
    print(tamizaje_cancer_mama[['porcentaje_cobertura']])
    print("Proporción de personas entre 50 y 75 años tamizadas para cáncer de colon y recto (sangre oculta en materia fecal con inmunoquímica, según lo definido en el esquema):")
    print(tamizajes_cancer_colon[['porcentaje_cobertura']])
    print("Proporción de hombres mayores de 50 años con tamizaje de oportunidad para cáncer de próstata (Antígenos Sanguíneos Prostáticos (PSA) y tacto rectal combinado):")
    print(tamizaje_prostata[['porcentaje_cobertura']])
    print("Cobertura de control de placa bacteriana de acuerdo con el esquema para el curso de vida:")
    print(cobertura_placa[['porcentaje_cobertura']])
    print("Proporción de gestantes con captación temprana al control prenatal")
    print(gestantes_temprana[['porcentaje_cobertura']])

    print(f"Proporción de gestantes con suministro de micronutrientes: {total_gestantes}")
    print(f"Proporción de gestantes con tamizaje para VIH: {count_vih} ({pct_vih}%)")
    print(f"Proporción de gestantes con tamizaje para Sífilis : {count_sifilis} ({pct_sifilis}%)")
    print(f"Proporción de gestantes con tamizaje Hepatitis B: {count_hepatitis} ({pct_hepatitis}%)")
    print(f"Proporción de gestantes con tamizaje para VIH, Sífilis y Hepatitis B: {count_all_three} ({pct_all_three}%)")

    # Si quieres, también devolver un pequeño DataFrame resumen
    resumen_gestantes = pd.DataFrame([
        {"indicador": "total_gestantes", "valor": total_gestantes},
        {"indicador": "vih", "valor": count_vih},
        {"indicador": "sifilis", "valor": count_sifilis},
        {"indicador": "hepatitis", "valor": count_hepatitis},
        {"indicador": "al_menos_una", "valor": count_any},
        {"indicador": "tres_pruebas", "valor": count_all_three}
    ])





def valores_por_servicio (df, servicio=None, codigo_proce=None, rango_edad=None, sexo=None, bins=None, labels=None, codigo_diag=None):
    # Use the provided bins/labels when calculating denominators so merges align
    denominadores = calcular_denominadores(df, bins, labels, sexo)
    df_cobertura_atencion = df.copy()


    if servicio is not None:
        df_cobertura_atencion.loc[:, 'Servicio'] = df_cobertura_atencion['Servicio'].astype(str).str.strip()

        df_cobertura_atencion = df_cobertura_atencion[
            df_cobertura_atencion['Servicio'].isin(servicio)
        ]

    if codigo_proce is not None:
        # support single value or list of procedure codes
        if isinstance(codigo_proce, (list, tuple, set)):
            df_cobertura_atencion = df_cobertura_atencion[df_cobertura_atencion['CodProce'].isin(codigo_proce)]
        else:
            df_cobertura_atencion = df_cobertura_atencion[df_cobertura_atencion['CodProce'] == codigo_proce]

    if sexo is not None:
        df_cobertura_atencion = df_cobertura_atencion[
            df_cobertura_atencion['Sexo'] == sexo
        ]

    if rango_edad is not None:
        # Ensure Edad is numeric before between
        df_cobertura_atencion = df_cobertura_atencion[
            df_cobertura_atencion['Edad'].between(rango_edad[0], rango_edad[1])
        ]

    if codigo_diag is not None:
        # support list or single diagnosis codes
        if isinstance(codigo_diag, (list, tuple, set)):
            df_cobertura_atencion = df_cobertura_atencion[df_cobertura_atencion['CodDiag'].isin(codigo_diag)]
        else:
            df_cobertura_atencion = df_cobertura_atencion[df_cobertura_atencion['CodDiag'] == codigo_diag]

    df_cobertura_atencion = df_cobertura_atencion.drop_duplicates(['Identificacion', 'Servicio'])

    df_cobertura = calcular_denominadores(df_cobertura_atencion, bins, labels, sexo)

    resultados = pd.merge(denominadores, df_cobertura, on='age_group', how='left', suffixes=('_total', '_cobertura'))
    resultados['count_total'] = resultados['count_total'].fillna(0).astype(int)
    resultados['count_cobertura'] = resultados['count_cobertura'].fillna(0).astype(int)
    resultados['porcentaje_cobertura'] = resultados.apply(
        lambda r: (r['count_cobertura'] / r['count_total'] * 100) if r['count_total'] > 0 else 0,
        axis=1
    ).round(2)

    return resultados,df_cobertura_atencion




# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    #unificacion_facturacion()
    encontrados_con_ebs()
    #unificacion_produccion()



# See PyCharm help at https://www.jetbrains.com/help/pycharm/
