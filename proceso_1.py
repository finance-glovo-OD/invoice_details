import streamlit as st
import pandas as pd
import re
import numpy as np
from google.oauth2 import service_account
import gspread
from googleapiclient.discovery import build
import tempfile
from utils import read_sheet, write_sheet

def run():
    st.header("Process 1 - Verification and Payment Update")

    # Step 1: Upload Service Account Credentials
    st.subheader("Upload the Service Account Credentials (JSON format)")
    creds_file = st.file_uploader("Select the credentials file", type=["json"])
    creds = None
    
    if creds_file is not None:
        # Save the uploaded file temporarily to use it
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(creds_file.read())
            temp_file_path = temp_file.name
        
        # Use the temporary file to load the credentials
        try:
            creds = service_account.Credentials.from_service_account_file(
                temp_file_path, 
                scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            )
            
            # Test connection to Google Sheets to verify authentication
            gs = gspread.authorize(creds)
            st.success("Successfully connected to Google Sheets. Credentials are valid.")
        except Exception as e:
            st.error(f"Error connecting to Google Sheets: {e}")
            return  # Stop execution if credentials are not valid

    # Continue if credentials were successfully loaded
    if creds:
        # Step 2: Upload the Excel file
        st.subheader("Upload the Excel file")
        uploaded_file = st.file_uploader("Select the Excel file", type=["xlsx"])
        
        if uploaded_file is not None:
            try:
                # Read the Excel file starting from row 5 (pandas index 4)
                df = pd.read_excel(uploaded_file, skiprows=4)

                # Preview the first 10 rows of the uploaded Excel file
                st.write("Preview of the uploaded file (first 10 rows):")
                st.write(df.head(10))

                # Check if the required columns exist
                if 'Description' not in df.columns or 'Flow amount' not in df.columns:
                    st.error("The columns 'Description' and 'Flow amount' are required but not found in the Excel file.")
                    return

                # Step 3: Extract invoice numbers from the Excel file
                results = []

                # Iterate over the rows of the DataFrame
                for index, row in df.iterrows():
                    description = row['Description']
                    flow_amount = row['Flow amount']

                    # Find all numbers starting with '22' and are 10 digits long, and invoices starting with 'ES-FVR' and 15 characters long
                    matches_22 = re.findall(r'\b22\d{8}\b', str(description))
                    matches_fvr = re.findall(r'ES-FVR\d{10}', str(description))

                    # Combine both matches
                    matches = list(set(matches_22 + matches_fvr))

                    # Append results with their corresponding flow amount
                    if matches:
                        results.append({'Invoice Numbers': matches, 'Total Amount': flow_amount})

                # Convert the results to a DataFrame
                extracted_df = pd.DataFrame(results)

                if extracted_df.empty:
                    st.warning("No valid invoice numbers were found in the Excel file.")
                else:
                    # Show a preview of the extracted invoice numbers
                    st.write("Extracted Invoice Numbers (first 10 rows):")
                    st.write(extracted_df.head(10))

                # Step 4: Update Google Sheets
                st.subheader("Update Google Sheets")
                spreadsheet_id = st.text_input("Enter the Google Sheet ID")
                sheet_name = st.text_input("Enter the sheet name", value="ES Data 2024 September")

                if st.button("Execute Process"):
                    try:
                        # Authorize gspread
                        gs = gspread.authorize(creds)
                        worksheet = gs.open_by_key(spreadsheet_id).worksheet(sheet_name)

                        # Read the current data from Google Sheets (starting from A2 to skip the header)
                        sheet_df = read_sheet(creds, spreadsheet_id, f"{sheet_name}!A2:I")

                        # Strip spaces from the column headers
                        sheet_df.columns = sheet_df.columns.str.strip()

                        # Convert data types to the correct format
                        sheet_df['Invoice Number'] = sheet_df['Invoice Number'].astype(str)
                        sheet_df['Balance'] = sheet_df['Balance'].replace({',': ''}, regex=True).replace('', np.nan).astype(float)

                        # Lists to store matches and non-matches
                        no_matches = []
                        matches_paid = []
                        matches_partial = []

                        # Tolerance for amount comparison
                        tolerance = 0.01

                        # Iterate over the extracted invoice numbers and check them against the Google Sheets
                        for _, row in extracted_df.iterrows():
                            invoice_numbers = row['Invoice Numbers']
                            total_amount = row['Total Amount']

                            # Find matching rows in the Google Sheet
                            matching_rows = sheet_df[sheet_df['Invoice Number'].isin(invoice_numbers)]

                            if matching_rows.empty:
                                no_matches.append(f"Invoice Numbers: {invoice_numbers}, Total Amount: {total_amount}")
                            else:
                                # Sum the balances of the matching rows
                                matching_amount_sum = matching_rows['Balance'].sum()

                                # Check if the total amount matches, with tolerance
                                if abs(matching_amount_sum - total_amount) <= tolerance:
                                    matches_paid.append(f"Invoice Numbers: {invoice_numbers}, Total Amount: {total_amount} - Marked as 'Paid'")
                                    # Update each row as 'Paid'
                                    for matching_index in matching_rows.index:
                                        gsheet_row = matching_index + 2  # +2 because sheet_df starts from A2
                                        worksheet.update_cell(gsheet_row, 9, 'Paid')
                                else:
                                    matches_partial.append(f"Invoice Numbers: {invoice_numbers}, Total Amount: {total_amount} - Marked as 'Paid but not completed'")
                                    # Update each row as 'Paid but not completed'
                                    for matching_index in matching_rows.index:
                                        gsheet_row = matching_index + 2
                                        worksheet.update_cell(gsheet_row, 9, 'Paid but not completed')

                        # Show the results of matches and non-matches
                        st.write("No matches found for the following Invoice Numbers:")
                        st.write(no_matches)
                        st.write("The following Invoice Numbers were marked as 'Paid':")
                        st.write(matches_paid)
                        st.write("The following Invoice Numbers were marked as 'Paid but not completed':")
                        st.write(matches_partial)

                        st.success("Process completed. Payments have been updated in Google Sheets.")

                    except Exception as e:
                        st.error(f"Error updating Google Sheets: {e}")

            except Exception as e:
                st.error(f"Error reading the Excel file: {e}")
