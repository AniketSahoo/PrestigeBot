import streamlit as st
import boto3
from PIL import Image, ImageEnhance, ImageFilter
import os
import re
import dotenv
import snowflake.connector
import pandas as pd
import io
import logging
from snowflake.snowpark import Session
import gzip

def load_secrets_from_stage(session, stage_name, file_name):
    file = session.file.get(stage_name + "/" + file_name, "./")[0]
    with gzip.open(file.name, 'rb') as f:
        content = f.read().decode()
        dotenv.load_dotenv(dotenv_path=io.StringIO(content))

def create_snowflake_session():
    connection_parameters = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA")
    }
    session = Session.builder.configs(connection_parameters).create()
    return session

def connect_textract(aws_access_key_id,aws_secret_access_key,aws_region):
    textract_client = boto3.client(
        'textract',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_region=aws_region
    )
    return textract_client

def connect_snowflake(user,password,account,warehouse,database,schema):
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )
    cursor = conn.cursor()
    return conn, cursor

def process_image(uploaded_file):
    
    image = Image.open(uploaded_file).convert("RGB")
    image = image.filter(ImageFilter.SHARPEN)
    image = ImageEnhance.Contrast(image).enhance(2)
    st.image(image, caption="Uploaded Image", use_container_width=True)

    return image

def extract_text(image,aws_access_key_id,aws_secret_access_key,aws_region):    

    image_bytes = io.BytesIO()
    image.save(image_bytes, format="JPEG")
    image_bytes.seek(0)

    textract_client = connect_textract(aws_access_key_id,aws_secret_access_key,aws_region)
    response = textract_client.detect_document_text(Document={'Bytes': image_bytes.read()})

    extracted_text = ""
    for item in response['Blocks']:
        if item['BlockType'] == 'LINE':
            extracted_text += item['Text'] + "\n"

    # st.write("Extracted Text:")
    # st.text(extracted_text)

    return extracted_text

def check_warranty_validity(extracted_text,cursor):
    warranty_id = None
    warranty_match = re.search(r"\b\d{6}\b", extracted_text)
    if warranty_match:
        warranty_id = warranty_match.group()
        st.success(f"🔍 Detected Warranty ID: {warranty_id}")

        cursor.execute("SELECT * FROM WARRANTY WHERE ID = %s", (warranty_id,))
        result = cursor.fetchone()
        columns = [desc[0] for desc in cursor.description]

        if result:
            df = pd.DataFrame([result], columns=columns)
            st.success("✅ Warranty is valid!")
            st.dataframe(df)
        else:
            st.error("❌ Invalid Warranty ID.")

    else:
        st.warning("⚠️ Couldn't find a valid 6-digit warranty ID in the image.")
    return warranty_id

def check_product_validity(extracted_text,cursor):
    model = None
    model_match = re.search(r"model\s*[:\-]?\s*(\S.+)", extracted_text, re.IGNORECASE)
    model = model_match.group(1).strip() if model_match else None

    if model:
        st.success(f"🛠️ Detected Model: {model}")

        cursor.execute("SELECT * FROM PRODUCT WHERE NAME ILIKE %s", (model,))
        result = cursor.fetchone()
        columns = [desc[0] for desc in cursor.description]  

        if result:
            df = pd.DataFrame([result], columns=columns)
            st.success("✅ Product is valid!")
            st.dataframe(df)
        else:
            st.error("❌ Invalid Product.")
    else:
        st.warning("⚠️ Couldn't find Product in the text.")
    return model

def main():

    st.title("Warranty Validity Checker")

    session = create_snowflake_session()
    try:
        load_secrets_from_stage(session, "@prestige_bot_secrets", "prestige_secrets.env.gz")
    except Exception as e:
        st.error(f"Failed to load secrets: {e}")
        st.stop()

    # AWS credentials
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_DEFAULT_REGION")

    # Snowflake Credentials
    user=os.getenv("SNOWFLAKE_USER")
    password=os.getenv("SNOWFLAKE_PASSWORD")
    account=os.getenv("SNOWFLAKE_ACCOUNT")
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE")
    database=os.getenv("SNOWFLAKE_DATABASE")
    schema=os.getenv("SNOWFLAKE_SCHEMA")

    uploaded_file = st.file_uploader("Upload Invoice Image (to extract Warranty ID)", type=["png", "jpg", "jpeg"])    

    if uploaded_file is not None:
        image = process_image(uploaded_file)
        with st.spinner("🔍 Extracting text..."):
            extracted_text = extract_text(image,aws_access_key_id,aws_secret_access_key,aws_region)
        conn, cursor = connect_snowflake(user,password,account,warehouse,database,schema)
        try:
            warranty_id = check_warranty_validity(extracted_text,cursor)
            model = check_product_validity(extracted_text,cursor)
        finally:
            cursor.close()
            conn.close()
    else:
        # st.info("Please upload you Warranty card.")
        logging.info("Waiting on Upload.")

if __name__ == "__main__":
    main()