import streamlit as st
from db_engine import execute_query
from sample_data import load_sample_tables

# Initialize session state
if 'tables' not in st.session_state:
    st.session_state.tables = load_sample_tables()

st.title("🗃️ Mini RDBMS Web App")

st.markdown("""
Type a SQL-like query (e.g.,  
- `SELECT * FROM students`,  
- `INSERT INTO students VALUES (4, 'Ankit', 92)`)  
Supported commands: `SELECT`, `INSERT`, `DELETE`
""")

query = st.text_area("Enter your SQL query:")

if st.button("Execute"):
    if query.strip():
        result = execute_query(query, st.session_state.tables)
        if isinstance(result, str):
            st.warning(result)
        else:
            st.dataframe(result)
    else:
        st.info("Please enter a query to execute.")
import pandas as pd

def load_sample_tables():
    students = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Sanskriti", "Ravi", "Priya"],
        "marks": [95, 88, 91]
    })

    courses = pd.DataFrame({
        "code": ["CS101", "MA102", "PH103"],
        "title": ["Intro to CS", "Calculus", "Physics"],
        "sid": [1, 2, 1]
    })

    return {
        "students": students,
        "courses": courses
    }
