import streamlit as st
from processes import proceso_1, proceso_2, proceso_3

# Configure the Streamlit interface
st.set_page_config(page_title="Automation Dashboard", layout="wide")

# Main page
st.title("Automation Dashboard")

# Create a selector to choose the process
selected_tab = st.selectbox("Select a process:", [
    "Process 1 - Payment Verification and Update", 
    "Process 2 - Billing Report Generation", 
])

# Add the corresponding content according to the selected process
if selected_tab == "Process 1 - Payment Verification and Update":
    proceso_1.run()

elif selected_tab == "Process 2 - Billing Report Generation":
    proceso_3.run()  # Run process 2