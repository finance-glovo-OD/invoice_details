import streamlit as st
import pandas as pd
import tempfile
from google.oauth2 import service_account
import gspread
import time
from utils import read_sheet, gmail_authenticate, send_email, es_email_valido
import io
import datetime

@st.cache_data(ttl=60)
def get_sheet_data(_creds, spreadsheet_id, range_name, retries=3):
    """Reads data from a Google Sheets sheet with retries in case of temporary failures."""
    attempt = 0
    while attempt < retries:
        try:
            return read_sheet(_creds, spreadsheet_id, range_name)
        except Exception as e:
            if "503" in str(e):  # Detect if the error is 503 (temporary failure)
                attempt += 1
                st.warning(f"Error 503, retrying ({attempt}/{retries})...")
                time.sleep(2 ** attempt)  # Wait 2, 4, 8... seconds before retrying
            else:
                raise e  # Re-raise the error if it is not a 503 error
    st.error("Persistent error: Unable to retrieve data after multiple attempts.")
    return None  # Return None if all attempts fail

def run():
    st.header("Process 2 - Billing Report Generation")

    # Step 1: Load the Service Account credentials for Google Sheets
    st.subheader("Upload the Service Account credentials file for Google Sheets (JSON format)")
    sheets_creds_file = st.file_uploader("Select the Service Account credentials file", type=["json"])
    service_creds = None

    if sheets_creds_file is not None:
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(sheets_creds_file.read())
            temp_file_path = temp_file.name
        
        try:
            service_creds = service_account.Credentials.from_service_account_file(
                temp_file_path,
                scopes=[
                    "https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"
                ]
            )
            gs = gspread.authorize(service_creds)
            st.success("Connection to Google Sheets successful. The Service Account credentials are valid.")
        except Exception as e:
            st.error(f"Error connecting to Google Sheets: {e}")
            return

    # Step 2: Authenticate Gmail using OAuth2.0
    st.subheader("Upload the OAuth credentials file for Gmail (JSON format)")
    gmail_creds_file = st.file_uploader("Select the OAuth credentials file for Gmail", type=["json"])
    gmail_creds = None

    if gmail_creds_file is not None:
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(gmail_creds_file.read())
            temp_file_path = temp_file.name
        
        try:
            gmail_creds = gmail_authenticate(temp_file_path)
            st.success("Successfully authenticated with Gmail.")
        except Exception as e:
            st.error(f"Error authenticating with Gmail: {e}")
            return

    # Mode Selection: Test or Production
    st.subheader("Select Email Sending Mode")
    modo_envio = st.radio(
        "Select execution mode:",
        ("Test", "Production")
    )

    has_fixed_pricing = st.checkbox("Partner has fixed pricing")

    country_option = st.selectbox(
    "Country",
    ("Spain (default)", "Italy", "Portugal", "Bulgaria", "Poland"),
)

    # Obtain actual date
    current_date = datetime.datetime.now().strftime('%Y-%m-%d')

    # Fields to edit the email subject and body
    st.subheader("Configure the email message")

    # Change the text in brackets according to the selected mode
    if modo_envio == "Test":
        default_subject = "[TEST] Glovo On-Demand Billing Detail: {filter_value} - {invoice_numbers}"
    else:
        default_subject = "[NO RESPONSE] Glovo On-Demand Billing Detail: {filter_value} - {invoice_numbers}"
    
    # Read the email body from a file in the folder processes/instructions/
    with open('processes/emails/email_body_process3.txt', 'r') as file:
        default_body = file.read()

    # Text box for the email subject (editable but `filter_value` is mandatory)
    user_subject = st.text_input("Email subject (include {filter_value} and {invoice_numbers} in the correct place)", default_subject)

    # Text area for the email body (editable but `{spreadsheet_url}` is mandatory)
    user_body = st.text_area("Message body (include {spreadsheet_url} and {invoice_numbers} in the correct place)", default_body, height=400)

    # Continue only if the credentials are valid
    if service_creds and gmail_creds:
        # Request the Spreadsheet ID and sheet names
        spreadsheet_id = st.text_input("Enter the Google Sheet ID", "")
        details_sheet = st.text_input("Enter the name of the 'Details' sheet", "ES_details")
        refunds_sheet = st.text_input("Enter the name of the 'Refunds' sheet", "ES_refunds")
        # if country_option == 'Spain (default)': discounts_sheet = st.text_input("Enter the name of the 'Discounts' sheet", "ES_discounts")
        # glossary_sheet = st.text_input("Enter the name of the 'Glossary' sheet", "Glosario")

        if st.button("Execute Process"):
            try:
                # Leer los datos de la hoja 'Tabla' usando caching
                try:
                    tabla_df = get_sheet_data(service_creds, spreadsheet_id, 'Tabla!A3:H')

                    # Display the column names and the number of columns for verification
                    # st.write("Original column names:")
                    # st.write(tabla_df.columns)
                    # st.write(f"The DataFrame has {len(tabla_df.columns)} columns.")

                    # If columns have duplicate names or are None, reassign generic names
                    if tabla_df.columns.isnull().any() or len(set(tabla_df.columns)) != len(tabla_df.columns):
                        # st.warning("Duplicate or invalid column names detected. Reassigning generic names.")
                        tabla_df.columns = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']

                    st.write("Table preview (first 10 rows):")
                    st.write(tabla_df.head(10))

                    # Read the Glossary from the main sheet
                    # glosario_df = get_sheet_data(service_creds, spreadsheet_id, f'{glossary_sheet}!A1:B22')

                    # Store the links of each created sheet
                    resumen_data = []
                    # Store failed emails
                    email_fails = []

                    # Continue processing using the new column names
                    for index, row in tabla_df.iterrows():
                        
                        filter_value = row['A']  # CIF in column A
                        sam_mails = row['C']     # Email in column C
                        mail_value1 = row['D']   # Email in column D
                        mail_value2 = row['E']   # Email in column E
                        name_value = row['B']    # Name in column B
                        invoice_numbers = row['H']  # Invoice Number in column H

                        st.write(f"Processing CIF: {filter_value}, Name: {name_value}")

                        # Add retry mechanism for each CIF
                        retry_attempts = 3  # Maximum number of retries per CIF
                        # Create a new real spreadsheet in Google Sheets
                        new_spreadsheet = gs.create(f'Billing Detail - {filter_value} - {invoice_numbers}')
                        new_spreadsheet.sheet1.update_title('Orders')
                        new_spreadsheet_id = new_spreadsheet.id
                        spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{new_spreadsheet_id}/edit"
                        st.write(f"A new Spreadsheet has been created with ID: {new_spreadsheet_id}")

                        for attempt in range(retry_attempts):
                            valid_emails = []
                            try:
                                # Validate emails for "Production" mode
                                if modo_envio == "Production":
                                    for email in [sam_mails, mail_value1, mail_value2]:
                                        if email and es_email_valido(email):
                                            valid_emails.append(email)
                                        else:
                                            # Log empty or invalid emails in the email_fails list
                                            if email:
                                                email_fails.append([filter_value, email])
                                            else:
                                                email_fails.append([filter_value, "Empty email"])

                                # Share the file according to the execution mode
                                if modo_envio == "Test":
                                    test_emails = ['bernat.pavon@glovoapp.com','santiago.aducho@glovoapp.com']
                                    for email in test_emails:
                                        new_spreadsheet.share(email, perm_type='user', role='writer')
                                        time.sleep(1)  # 1-second pause between each share request to avoid quota limit
                                    new_spreadsheet.share(None, perm_type='anyone', role='writer')    
                                else:
                                    # Share the file with valid emails and add finance.ondemand@glovoapp.com as a writer
                                    for email in valid_emails:
                                        new_spreadsheet.share(email, perm_type='user', role='writer')
                                        time.sleep(1)  # 1-second pause between each share request
                                    # Add finance.ondemand@glovoapp.com as an editor for each sheet in "Production" mode
                                    new_spreadsheet.share('finance.ondemand@glovoapp.com', perm_type='user', role='writer')
                                    # Make the file accessible to anyone with the link in "Viewer" mode
                                    new_spreadsheet.share(None, perm_type='anyone', role='writer')

                                # **Add the CIF and link to resumen_data**
                                resumen_data.append([filter_value, spreadsheet_url])

                                # Copy the details to the newly created sheet
                                details_df = get_sheet_data(service_creds, spreadsheet_id, f'{details_sheet}!A:Z')
                                filtered_details = details_df[details_df.iloc[:, 0] == filter_value]
                                if not filtered_details.empty:
                                    new_sheet = new_spreadsheet.sheet1
                                    new_sheet.update([filtered_details.columns.values.tolist()] + filtered_details.values.tolist())

                                # Add and copy Refunds
                                refunds_df = get_sheet_data(service_creds, spreadsheet_id, f'{refunds_sheet}!A:Z')
                                filtered_refunds = refunds_df[refunds_df.iloc[:, 0] == filter_value]
                                if not filtered_refunds.empty:
                                    refund_sheet = new_spreadsheet.add_worksheet(title='Reembolsos', rows='100', cols='20')
                                    refund_sheet.update([filtered_refunds.columns.values.tolist()] + filtered_refunds.values.tolist())

                                # Add and copy Discounts
                                # if country_option == 'Spain (default)':
                                #     discounts_df = get_sheet_data(service_creds, spreadsheet_id, f'{discounts_sheet}!A:Z')
                                #     filtered_discounts = discounts_df[discounts_df.iloc[:, 0] == filter_value]
                                #     if not filtered_discounts.empty:
                                #         discount_sheet = new_spreadsheet.add_worksheet(title='Descuentos', rows='100', cols='20')
                                #         discount_sheet.update([filtered_discounts.columns.values.tolist()] + filtered_discounts.values.tolist())

                                # Create and copy the Glossary from the main sheet
                                # glossary_sheet = new_spreadsheet.add_worksheet(title='Glosario', rows='20', cols='10')
                                # glossary_sheet.update([glosario_df.columns.values.tolist()] + glosario_df.values.tolist())

                                # Get the ID of the 'Orders' sheet
                                sheet_id_ordenes = new_spreadsheet.sheet1._properties['sheetId']

                                # Condition to detect the file type and proceed
                                num_columns = len(filtered_details.columns)

                                if num_columns == 16:  # If it has 16 columns (index 15), create the pivot table and the 'Summary Table' sheet

                                    # Create the new 'Summary Table' sheet
                                    summary_sheet = new_spreadsheet.add_worksheet(title='Tabla Resumen', rows='100', cols='20')

                                    # Create the body of the Pivot Table request for files with 16 columns
                                    pivot_table_body = {
                                        "requests": [
                                            {
                                                "updateCells": {
                                                    "range": {
                                                        "sheetId": summary_sheet._properties['sheetId'],  # Sheet ID 'Tabla Resumen'
                                                        "startRowIndex": 0,
                                                        "startColumnIndex": 0
                                                    },
                                                    "rows": [
                                                        {
                                                            "values": [
                                                                {
                                                                    "pivotTable": {
                                                                        "source": {
                                                                            "sheetId": sheet_id_ordenes,  # Sheet ID 'Ordenes'
                                                                            "startRowIndex": 0,  # Adjust if there are headers
                                                                            "startColumnIndex": 0,  # Initiate first column
                                                                            "endColumnIndex": 16  # Adjust number of columns
                                                                        },
                                                                        "rows": [
                                                                            {
                                                                                "sourceColumnOffset": 10,  # Column for the rows of the pivot table
                                                                                "showTotals": True,
                                                                                "sortOrder": "ASCENDING"
                                                                            }
                                                                        ],
                                                                        "values": [
                                                                            {
                                                                                "summarizeFunction": "COUNTA",
                                                                                "sourceColumnOffset": 1  # Column for the summed values in the pivot table
                                                                            }
                                                                        ],
                                                                        "columns": [
                                                                            {
                                                                                "sourceColumnOffset": 11,  # Column for the columns of the pivot table
                                                                                "showTotals": True,
                                                                                "sortOrder": "ASCENDING"
                                                                            }
                                                                        ],
                                                                        "filterSpecs": [
                                                                            {
                                                                                "filterCriteria": {
                                                                                    "condition": {
                                                                                        "type": "TEXT_EQ",  # Exact filter for "YES"
                                                                                        "values": [
                                                                                            {
                                                                                                "userEnteredValue": "SI"  # Filter only the "SI" values
                                                                                            }
                                                                                        ]
                                                                                    },
                                                                                    "visibleValues": ["SI", "NO"]  # Display both options in the value filter
                                                                                },
                                                                                "columnOffsetIndex": 15  # Column 15 to apply the filter
                                                                            }
                                                                        ]
                                                                    }
                                                                }
                                                            ]
                                                        }
                                                    ],
                                                    "fields": "pivotTable"
                                                }
                                            }
                                        ]
                                    }

                                    # Execute batchUpdate to create the pivot table with the adjusted conditional filter
                                    new_spreadsheet.batch_update(pivot_table_body)

                                    # Read the instructions from the file in the folder processes/instructions/
                                    with open('processes/instructions/instructions_process3.txt', 'r') as file:
                                        instructions = file.readlines()

                                    # Add the instructions to column H in the 'Summary Table' sheet
                                    summary_sheet.update(f'H1:H{len(instructions)}', [[instruction.strip()] for instruction in instructions])

                                # If there was no error, break the retry loop
                                break

                            except Exception as e:
                                st.error(f"Error processing sheets for {filter_value} (Attempt {attempt + 1}/{retry_attempts}): {e}")
                                if attempt < retry_attempts - 1:
                                    st.write("Retrying...")
                                    time.sleep(5)  # Wait 5 seconds before retrying
                                else:
                                    st.error(f"Persistent error processing sheets for {filter_value}. Continuing with the next CIF.")
                                    continue  # Skip to the next CIF after all retry attempts fail

                        # Process the subject and body of the email with placeholders replaced
                        subject = user_subject.format(modo_envio=modo_envio, filter_value=row['A'], invoice_numbers=invoice_numbers)
                        body = user_body.format(spreadsheet_url=spreadsheet_url, filter_value=row['A'], invoice_numbers=invoice_numbers)

                                                # Paso 1: Leer el archivo HTML
                        if country_option == 'Spain (default)': 
                            email_template = 'processes/templates/email_template_3.html' if has_fixed_pricing else 'processes/templates/email_template_5.html'
                        elif country_option == 'Portugal':
                            email_template = 'processes/templates/email_template_PT.html'
                        elif country_option == 'Italy':
                            email_template = 'processes/templates/email_template_IT.html'
                        elif country_option == 'Bulgaria':
                            email_template = 'processes/templates/email_template_BG.html'
                        else:
                            email_template = 'processes/templates/email_template_PL.html'

                        with open(email_template, 'r') as file:
                            email_template = file.read()

                        # Paso 2: Reemplazar los placeholders dinámicos en el HTML con los valores correspondientes
                        email_body_html = email_template.format(
                            invoice_numbers=invoice_numbers,
                            filter_value=filter_value,
                            spreadsheet_url=spreadsheet_url,
                            form_link="https://customer.nuapay.com/signup/sign-up-to-glovoapp-spain-platform/?lid=aevi5d4znymn",
                            tutorial_link="https://drive.google.com/file/d/1SiJEWhou3SkeV018tMWnLS6vktKLCnN8/view?usp=sharing&lid=zxsxm8snusc5"
                        )

                        # Choose recipient based on the selected mode
                        if modo_envio == "Test":
                            destinatario = "santiago.aducho@glovoapp.com"
                        else:
                            destinatario = ", ".join(valid_emails)  # Join valid emails for "Production"

                        # Check if there is at least one valid recipient before sending the email
                        if destinatario:
                            try:
                                # Send Email
                                send_email(
                                    gmail_creds,
                                    destinatario,
                                    subject,
                                    email_body_html,
                                    is_html=True  
                                )
                                st.write(f"Email sent to {destinatario} with the link to the document: {spreadsheet_url}")
                            except Exception as e:
                                st.error(f"Error sending email to {destinatario}: {e}")
                        else:
                            st.error("There are no valid emails to send.")

                except Exception as e:
                    st.error(f"Error during the process: {e}")

                # Create final sheet in the end of the process
                if resumen_data:
                    resumen_spreadsheet = gs.create(f'Summary of Generated Sheets {current_date}')
                    resumen_sheet = resumen_spreadsheet.sheet1
                    resumen_sheet.update_title('Resumen')

                    # Write data in the summary sheet
                    resumen_headers = ["CIF", "Enlace al Google Sheet"]
                    resumen_sheet.update([resumen_headers] + resumen_data)

                    # Share summary data with the specified emails.
                    emails_to_share = ['bernat.pavon@glovoapp.com']
                    for email in emails_to_share:
                        resumen_spreadsheet.share(email, perm_type='user', role='writer')
                        time.sleep(1)  # 1-second pause between each share request to avoid quota limit

                    # Display the link to the summary sheet
                    resumen_spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{resumen_spreadsheet.id}/edit"
                    st.write(f"Summary sheet created: {resumen_spreadsheet_url}")

                    # Convert the data list into a pandas DataFrame
                    df_resumen = pd.DataFrame(resumen_data, columns=["CIF", "Enlace al Google Sheet"])

                    # Create an in-memory buffer for the Excel file
                    excel_buffer = io.BytesIO()

                    # Save the DataFrame to the Excel buffer
                    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                        df_resumen.to_excel(writer, index=False, sheet_name='Resumen')
                            # Add the "Failed Emails" tab if there are errors
                        if email_fails:
                            email_fails_df = pd.DataFrame(email_fails, columns=["CIF", "Failed Emails"])
                            # Write the failed emails to a separate sheet
                            email_fails_df.to_excel(writer, index=False, sheet_name='Failed Emails')

                    # Insert a download button in Streamlit
                    st.download_button(
                        label="Download Summary as Excel",
                        data=excel_buffer.getvalue(),
                        file_name = f"billing_summary_{current_date}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                    # Add the "Failed Emails" tab if there are errors
                    if email_fails:
                        email_fail_sheet = resumen_spreadsheet.add_worksheet(title='Failed Emails', rows='100', cols='20')

                        # Write the data to the "Failed Emails" tab
                        email_fail_headers = ["CIF", "Failed Emails"]
                        email_fail_sheet.update([email_fail_headers] + email_fails)
                    
                st.success(f"Process completed in {modo_envio} mode. Real documents have been created and emails sent.")
            
            except Exception as e:
                st.error(f"Error during the process: {e}")
