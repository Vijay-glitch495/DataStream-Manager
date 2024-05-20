{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "b51f35d1",
   "metadata": {},
   "outputs": [],
   "source": [
    "#pip install streamlit"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "31b79b77",
   "metadata": {},
   "outputs": [],
   "source": [
    "import streamlit as st\n",
    "import pandas as pd"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "id": "57401bcf",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "2024-05-18 07:49:52.448 \n",
      "  \u001b[33m\u001b[1mWarning:\u001b[0m to view this Streamlit app on a browser, run it with the following\n",
      "  command:\n",
      "\n",
      "    streamlit run /Users/vijaykumarreddybommireddy/anaconda3/lib/python3.11/site-packages/ipykernel_launcher.py [ARGUMENTS]\n"
     ]
    },
    {
     "data": {
      "text/plain": [
       "DeltaGenerator()"
      ]
     },
     "execution_count": 4,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "st.title(\"The Streamlit ETL App 🗂️ \")\n",
    "\n",
    "st.caption(\"\"\"\n",
    "         With this app you will be able to Extract, Transform and Load Below File Types:\n",
    "         \\n1.CSV\n",
    "         \\n2.JSON\n",
    "         \\n3.XML\n",
    "         \\nps: You can upload multiple files.\"\"\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "id": "617e39cf",
   "metadata": {},
   "outputs": [],
   "source": [
    "uploaded_files = st.file_uploader(\"Choose a file\", accept_multiple_files=True)\n",
    "\n",
    "# let's create a function to check the file types and read them accordingly.\n",
    "\n",
    "def extract(file_to_extract):\n",
    "    if file_to_extract.name.split(\".\")[-1] == \"csv\": \n",
    "        extracted_data = pd.read_csv(file_to_extract)\n",
    "\n",
    "    elif file_to_extract.name.split(\".\")[-1] == 'json':\n",
    "         extracted_data = pd.read_json(file_to_extract, lines=True)\n",
    "\n",
    "    elif file_to_extract.name.split(\".\")[-1] == 'xml':\n",
    "         extracted_data = pd.read_xml(file_to_extract)\n",
    "         \n",
    "    return extracted_data\n",
    "\n",
    "# create an empty list which will be used to merge the files.\n",
    "\n",
    "dataframes = []"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "b357b4fc",
   "metadata": {},
   "outputs": [],
   "source": [
    "if uploaded_files:\n",
    "    for file in uploaded_files:\n",
    "        file.seek(0)\n",
    "        df = extract(file)\n",
    "        dataframes.append(df)\n",
    "\n",
    "    if len(dataframes) >= 1:\n",
    "        merged_df = pd.concat(dataframes, ignore_index=True, join='outer')\n",
    "\n",
    "    remove_duplicates = st.selectbox(\"Remove duplicate values ?\", [\"No\", \"Yes\"])\n",
    "    remove_nulls = st.selectbox(\"Remove null values in the dataset ?\", [\"Yes\", \"No\"])\n",
    "\n",
    "    if remove_duplicates == \"Yes\":\n",
    "        merged_df.drop_duplicates(inplace=True)\n",
    "\n",
    "    if remove_nulls == \"Yes\":\n",
    "        merged_df.dropna(how=\"all\", inplace=True)\n",
    "\n",
    "    \n",
    "    show_result = st.checkbox(\"Show Result\", value=True)\n",
    "\n",
    "    if show_result:\n",
    "        st.write(merged_df)\n",
    "\n",
    "    csv = merged_df.to_csv().encode(\"utf-8\")\n",
    "\n",
    "    st.download_button(label=\"Download cleaned data as csv\",\n",
    "                       data=csv,\n",
    "                       file_name=\"cleaned_data.csv\",\n",
    "                       mime=\"text/csv\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "id": "2b887c22",
   "metadata": {},
   "outputs": [],
   "source": [
    "import streamlit as st\n",
    "import pandas as pd\n",
    "import xml.etree.ElementTree as ET \n",
    "from datetime import datetime \n",
    "\n",
    "st.title(\"The Streamlit ETL App 🗂️ \")\n",
    "\n",
    "st.caption(\"\"\"\n",
    "         With this app you will be able to Extract, Transform and Load Below File Types:\n",
    "         \\n1.CSV\n",
    "         \\n2.JSON\n",
    "         \\n3.XML\n",
    "         \\nps: You can upload multiple files.\"\"\")\n",
    "\n",
    "uploaded_files = st.file_uploader(\"Choose a file\", accept_multiple_files=True)\n",
    "\n",
    "     \n",
    "def extract(file_to_extract):\n",
    "    if file_to_extract.name.split(\".\")[-1] == \"csv\": \n",
    "        extracted_data = pd.read_csv(file_to_extract)\n",
    "\n",
    "    elif file_to_extract.name.split(\".\")[-1] == 'json':\n",
    "         extracted_data = pd.read_json(file_to_extract, lines=True)\n",
    "\n",
    "    elif file_to_extract.name.split(\".\")[-1] == 'xml':\n",
    "         extracted_data = pd.read_xml(file_to_extract)\n",
    "         \n",
    "    return extracted_data\n",
    "\n",
    "dataframes = []\n",
    "\n",
    "\n",
    "if uploaded_files:\n",
    "    for file in uploaded_files:\n",
    "        file.seek(0)\n",
    "        df = extract(file)\n",
    "        dataframes.append(df)\n",
    "\n",
    "    if len(dataframes) >= 1:\n",
    "        merged_df = pd.concat(dataframes, ignore_index=True, join='outer')\n",
    "\n",
    "    remove_duplicates = st.selectbox(\"Remove duplicate values ?\", [\"No\", \"Yes\"])\n",
    "    remove_nulls = st.selectbox(\"Remove null values in the dataset ?\", [\"Yes\", \"No\"])\n",
    "\n",
    "    if remove_duplicates == \"Yes\":\n",
    "        merged_df.drop_duplicates(inplace=True)\n",
    "\n",
    "    if remove_nulls == \"Yes\":\n",
    "        merged_df.dropna(how=\"all\", inplace=True)\n",
    "\n",
    "    \n",
    "    show_result = st.checkbox(\"Show Result\", value=True)\n",
    "\n",
    "    if show_result:\n",
    "        st.write(merged_df)\n",
    "\n",
    "    csv = merged_df.to_csv().encode(\"utf-8\")\n",
    "\n",
    "    st.download_button(label=\"Download cleaned data as csv\",\n",
    "                       data=csv,\n",
    "                       file_name=\"cleaned_data.csv\",\n",
    "                       mime=\"text/csv\")\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "f47f5b91",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.11.5"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
