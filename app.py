import streamlit as st
import snowflake.connector
import os

from dotenv import load_dotenv
load_dotenv()

user=os.environ["SNOWFLAKE_USER"],
password=os.environ["SNOWFLAKE_PASSWORD"],
account=os.environ["SNOWFLAKE_ACCOUNT"],
database=os.environ["SNOWFLAKE_DATABASE"],
schema=os.environ["SNOWFLAKE_SCHEMA"],
warehouse=os.environ["SNOWFLAKE_WAREHOUSE"]

st.title("Warranty Validity Checker")

# User input
warranty_id = st.text_input("Enter Warranty ID")

# Connect to Snowflake
if warranty_id:
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema
        )
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM WARRANTY WHERE WARRANTY_ID = '{warranty_id}'")
        result = cursor.fetchone()

        if result:
            st.success("✅ Warranty is valid!")
            st.write(result)
        else:
            st.error("❌ Invalid Warranty ID")

    except Exception as e:
        st.error(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()
