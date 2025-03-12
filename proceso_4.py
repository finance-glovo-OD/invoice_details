import streamlit as st
import pandas as pd
import tempfile
from google.oauth2 import service_account
import gspread
from utils import read_sheet, write_sheet

# Implementar caching para las funciones que leen datos de Google Sheets
@st.cache_data(ttl=60)  # Cachea los datos por 60 segundos
def get_sheet_data(_creds, spreadsheet_id, range_name):
    return read_sheet(_creds, spreadsheet_id, range_name)

def run():
    st.header("Proceso 3 - Generación de reportes de facturación (Modo Test)")

    # Paso 1: Cargar las credenciales de la service account
    st.subheader("Sube el archivo de credenciales de la Service Account (formato JSON)")
    creds_file = st.file_uploader("Selecciona el archivo de credenciales", type=["json"])
    creds = None

    if creds_file is not None:
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(creds_file.read())
            temp_file_path = temp_file.name
        
        try:
            creds = service_account.Credentials.from_service_account_file(
                temp_file_path,
                scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            )
            gs = gspread.authorize(creds)
            st.success("Conexión a Google Sheets exitosa. Las credenciales son válidas.")
        except Exception as e:
            st.error(f"Error al conectar con Google Sheets: {e}")
            return

    # Continuar solo si las credenciales son válidas
    if creds:
        # Paso 2: Solicitar el ID del Spreadsheet y los nombres de las hojas
        spreadsheet_id = st.text_input("Introduce el ID del Google Sheet", "")
        details_sheet = st.text_input("Introduce el nombre de la hoja 'Details'", "ES_details")
        refunds_sheet = st.text_input("Introduce el nombre de la hoja 'Refunds'", "ES_refunds")
        discounts_sheet = st.text_input("Introduce el nombre de la hoja 'Discounts'", "ES_discounts")
        glossary_sheet = st.text_input("Introduce el nombre de la hoja 'Glosario'", "Glosario")

        if st.button("Ejecutar Proceso"):
            try:
                # Leer los datos de la hoja 'Tabla' usando caching
                try:
                    # Leer los datos de la hoja 'Tabla' desde cache
                    tabla_df = get_sheet_data(creds, spreadsheet_id, 'Tabla!A3:H')

                    # Mostrar los nombres de las columnas y la cantidad de columnas para verificación
                    st.write("Nombres de columnas originales:")
                    st.write(tabla_df.columns)
                    st.write(f"El DataFrame tiene {len(tabla_df.columns)} columnas.")

                    # Si los nombres de columna no son los esperados, reasignar nombres genéricos
                    if len(tabla_df.columns) == 8:  # Si tiene 8 columnas
                        tabla_df.columns = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']
                    elif len(tabla_df.columns) == 7:  # Si tiene 7 columnas
                        tabla_df.columns = ['A', 'B', 'C', 'D', 'E', 'F', 'G']  # Ajustar a 7 columnas
                    else:
                        st.error(f"El número de columnas ({len(tabla_df.columns)}) no coincide con los valores esperados.")
                        return

                    st.write("Vista previa de la tabla (primeras 10 filas):")
                    st.write(tabla_df.head(10))

                    # Continuar el procesamiento utilizando los nuevos nombres de columna
                    for index, row in tabla_df.iterrows():
                        filter_value = row['A']  # CIF en la columna A
                        sam_mails = row['C']     # Email en la columna C
                        mail_value1 = row['D']   # Email en la columna D
                        mail_value2 = row['E']   # Email en la columna E
                        name_value = row['B']    # Nombre en la columna H (si tiene 8 columnas)

                        st.write(f"Procesando CIF: {filter_value}, Nombre: {name_value}")

                except Exception as e:
                    st.error(f"Error durante el proceso: {e}")

                # Simular el procesamiento de cada fila
                for index, row in tabla_df.iterrows():
                    filter_value = row['A']  # CIF en la columna A
                    sam_mails = row['C']     # Email en la columna C
                    mail_value1 = row['D']   # Email en la columna D
                    mail_value2 = row['E']   # Email en la columna E
                    name_value = row['B']    # Nombre en la columna H (si tiene 8 columnas)

                    st.write(f"Procesando CIF: {filter_value}, Nombre: {name_value}")

                    # Simular la lectura y filtrado de los detalles usando caching
                    details_df = get_sheet_data(creds, spreadsheet_id, f'{details_sheet}!A:Z')
                    filtered_details = details_df[details_df.iloc[:, 0] == filter_value]

                    if not filtered_details.empty:
                        st.write(f"Detalles filtrados para CIF {filter_value}:")
                        st.write(filtered_details.head(5))  # Mostrar los primeros 5 resultados filtrados

                        # Simular la escritura de una nueva hoja (mock)
                        new_spreadsheet_id = 'MOCK_SPREADSHEET_ID'  # Esto es un ID falso
                        st.write(f"Se crearía un nuevo Spreadsheet con ID: {new_spreadsheet_id}")

                        # Simular el filtrado de refunds usando caching
                        refunds_df = get_sheet_data(creds, spreadsheet_id, f'{refunds_sheet}!A:Z')
                        filtered_refunds = refunds_df[refunds_df.iloc[:, 0] == filter_value]
                        if not filtered_refunds.empty:
                            st.write(f"Reembolsos filtrados para CIF {filter_value}")
                            st.write(filtered_refunds.head(5))

                        # Simular el filtrado de discounts usando caching
                        discounts_df = get_sheet_data(creds, spreadsheet_id, f'{discounts_sheet}!A:Z')
                        filtered_discounts = discounts_df[discounts_df.iloc[:, 0] == filter_value]
                        if not filtered_discounts.empty:
                            st.write(f"Descuentos filtrados para CIF {filter_value}")
                            st.write(filtered_discounts.head(5))

                        # Simular la copia del glosario usando caching
                        glossary_df = get_sheet_data(creds, spreadsheet_id, f'{glossary_sheet}!A:Z')
                        st.write("Glosario copiado al nuevo Spreadsheet (simulado)")

                        # Simular el envío de correo (mock)
                        st.write(f"Se enviaría un correo a: {sam_mails}, {mail_value1}, {mail_value2}")
                        st.write(f"Enlace al nuevo documento: https://docs.google.com/spreadsheets/d/{new_spreadsheet_id}/edit")

                st.success("Proceso completado en modo test. No se han enviado correos ni creado documentos reales.")
            
            except Exception as e:
                st.error(f"Error durante el proceso: {e}")