import streamlit as st
import pandas as pd
import xml.etree.ElementTree as ET 
from datetime import datetime 

# Title and caption for the app
st.title("The Streamlit ETL App 🗂️")

st.caption("""
         With this app, you will be able to Extract, Transform, and Load the following file types:
         \n1. CSV
         \n2. JSON
         \n3. XML
         \nPS: You can upload multiple files.
         """)

# File uploader to accept multiple files
uploaded_files = st.file_uploader("Choose a file", accept_multiple_files=True)

# Function to extract data from files based on their type
def extract(file_to_extract):
    if file_to_extract.name.split(".")[-1] == "csv": 
        extracted_data = pd.read_csv(file_to_extract)
    elif file_to_extract.name.split(".")[-1] == 'json':
        extracted_data = pd.read_json(file_to_extract, lines=True)
    elif file_to_extract.name.split(".")[-1] == 'xml':
        extracted_data = pd.read_xml(file_to_extract)
    return extracted_data

# Initialize an empty list to hold dataframes
dataframes = []

# Process the uploaded files
if uploaded_files:
    for file in uploaded_files:
        file.seek(0)
        df = extract(file)
        dataframes.append(df)

    # Merge dataframes if there is more than one
    if len(dataframes) >= 1:
        merged_df = pd.concat(dataframes, ignore_index=True, join='outer')

    # Options to remove duplicates and null values
    remove_duplicates = st.selectbox("Remove duplicate values?", ["No", "Yes"])
    remove_nulls = st.selectbox("Remove null values in the dataset?", ["Yes", "No"])

    if remove_duplicates == "Yes":
        merged_df.drop_duplicates(inplace=True)

    if remove_nulls == "Yes":
        merged_df.dropna(how="all", inplace=True)

    # Option to show the result
    show_result = st.checkbox("Show Result", value=True)

    if show_result:
        st.write(merged_df)

    # Provide a download button for the cleaned data
    csv = merged_df.to_csv().encode("utf-8")
    st.download_button(label="Download cleaned data as CSV",
                       data=csv,
                       file_name="cleaned_data.csv",
                       mime="text/csv")
