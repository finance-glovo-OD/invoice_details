import streamlit as st
import gspread
from google.oauth2 import service_account
import tempfile

def run():
    st.header("test")
    
    # Paso 1: Cargar las credenciales de la service account
    st.subheader("Sube el archivo de credenciales de la Service Account (formato JSON)")
    creds_file = st.file_uploader("Selecciona el archivo de credenciales", type=["json"])
    creds = None
    
    if creds_file is not None:
        # Guardar el archivo subido temporalmente para luego usarlo
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(creds_file.read())
            temp_file_path = temp_file.name
        
        # Usar el archivo temporal para obtener las credenciales
        try:
            creds = service_account.Credentials.from_service_account_file(temp_file_path, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
            
            # Intentar conectarse al Google Sheets para verificar la autenticación
            gs = gspread.authorize(creds)
            st.success("Conexión a Google Sheets exitosa. Las credenciales son válidas.")
        except Exception as e:
            st.error(f"Error al conectar con Google Sheets: {e}")
            return  # Detener la ejecución si no se pueden validar las credenciales

    # Continuar solo si las credenciales han sido cargadas correctamente
    if creds:
        # Paso 2: Obtener el ID del Google Sheet y el nombre de la pestaña
        spreadsheet_id = st.text_input("Introduce el ID del Google Sheet")
        sheet_name = st.text_input("Introduce el nombre de la pestaña", value="ES Data 2024 September")

        if st.button("Ejecutar Proceso 2"):
            try:
                # Autorizar gspread y acceder a la hoja de cálculo
                gs = gspread.authorize(creds)
                worksheet = gs.open_by_key(spreadsheet_id).worksheet(sheet_name)

                # Escribir "Hola" en la celda A1
                worksheet.update_acell('A1', 'Hola')
                
                # Verificar si la escritura fue exitosa
                written_value = worksheet.acell('A1').value
                if written_value == 'Hola':
                    st.success("Escritura exitosa: 'Hola' fue escrito en la celda A1.")
                else:
                    st.error("No se pudo escribir 'Hola' en la celda A1.")
                    
            except Exception as e:
                st.error(f"Error al intentar escribir en Google Sheets: {e}")