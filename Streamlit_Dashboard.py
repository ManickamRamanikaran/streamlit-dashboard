import streamlit as st
import pandas as pd
import sqlparse
import re
from io import StringIO
import tempfile
import os

def load_mapping_file(uploaded_file):
    """Load and process the mapping Excel file"""
    try:
        df = pd.read_excel(uploaded_file)
        required_columns = ['table_name', 'column_name', 'old_name', 'new_name']
        if not all(col in df.columns for col in required_columns):
            st.error("Excel file must contain columns: table_name, column_name, old_name, new_name")
            return None
        return df
    except Exception as e:
        st.error(f"Error reading Excel file: {str(e)}")
        return None

def load_sql_file(uploaded_file):
    """Load the SQL file content"""
    try:
        content = uploaded_file.getvalue().decode('utf-8')
        return content
    except Exception as e:
        st.error(f"Error reading SQL file: {str(e)}")
        return None

def update_sql_with_mappings(sql_content, mapping_df):
    """Update SQL content based on mapping dataframe"""
    updated_sql = sql_content
    changes_made = []

    # Sort by length of old_name in descending order to replace longer strings first
    mapping_df = mapping_df.assign(name_length=mapping_df['old_name'].str.len())
    mapping_df = mapping_df.sort_values('name_length', ascending=False)

    for _, row in mapping_df.iterrows():
        old_name = str(row['old_name']).strip()
        new_name = str(row['new_name']).strip()
        if old_name and new_name and old_name != new_name:
            pattern = r'\b' + re.escape(old_name) + r'\b'
            count = len(re.findall(pattern, updated_sql, re.IGNORECASE))
            if count > 0:
                updated_sql = re.sub(pattern, new_name, updated_sql, flags=re.IGNORECASE)
                changes_made.append({
                    'old': old_name,
                    'new': new_name,
                    'occurrences': count
                })

    return updated_sql, changes_made
def main():
    st.title("SQL Update Dashboard")
    st.write("Upload your mapping Excel file and SQL file to process changes")

    # File uploaders
    mapping_file = st.file_uploader("Upload Mapping Excel File", type=['xlsx', 'xls'])
    sql_file = st.file_uploader("Upload SQL File", type=['sql'])

    if not mapping_file or not sql_file:
        st.info("Please upload both a mapping Excel file and a SQL file to begin.")
        return

    # Load files
    mapping_df = load_mapping_file(mapping_file)
    sql_content = load_sql_file(sql_file)

    if mapping_df is not None and sql_content is not None:
        st.subheader("Mapping Preview")
        st.dataframe(mapping_df)

        if st.button("Process SQL Updates"):
            updated_sql, changes = update_sql_with_mappings(sql_content, mapping_df)

            st.subheader("Changes Made")
            if changes:
                changes_df = pd.DataFrame(changes)
                st.dataframe(changes_df)
            else:
                st.write("No changes were necessary.")

            st.subheader("SQL Preview")
            st.text_area("Updated SQL", updated_sql, height=300)

            st.download_button(
                label="Download Updated SQL",
                data=updated_sql,
                file_name="updated_sql.sql",
                mime="text/plain"
            )

if __name__ == "__main__":
    main()