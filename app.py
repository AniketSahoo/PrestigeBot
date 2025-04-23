import streamlit as st
import boto3
from PIL import Image, ImageEnhance, ImageFilter
import os
import re
from dotenv import load_dotenv
import snowflake.connector
import pandas as pd

load_dotenv()

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

def connect_textract(aws_access_key_id,aws_secret_access_key,region_name):
    textract_client = boto3.client(
        'textract',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
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
    return cursor

def process_image(uploaded_file):
    
    image = Image.open(uploaded_file).convert("RGB")
    image = image.filter(ImageFilter.SHARPEN)
    image = ImageEnhance.Contrast(image).enhance(2)
    st.image(image, caption="Uploaded Image", use_container_width=True)

    return image


def extract_text(image):    

    temp_image_path = "temp_image.jpg"
    image.save(temp_image_path)

    textract_client = connect_textract(aws_access_key_id,aws_secret_access_key,aws_region)

    with open(temp_image_path, 'rb') as img_file:
        response = textract_client.detect_document_text(Document={'Bytes': img_file.read()})

    os.remove(temp_image_path)

    extracted_text = ""
    for item in response['Blocks']:
        if item['BlockType'] == 'LINE':
            extracted_text += item['Text'] + "\n"

    # st.write("Extracted Text:")
    # st.text(extracted_text)

    return extracted_text

def check_warranty_validity(extracted_text):
    cursor = connect_snowflake(user,password,account,warehouse,database,schema)
    warranty_match = re.search(r"\b\d{6}\b", extracted_text)
    if warranty_match:
        warranty_id = warranty_match.group()
        st.success(f"🔍 Detected Warranty ID: {warranty_id}")

        # Query Snowflake for warranty validity
        cursor.execute(f"SELECT * FROM WARRANTY WHERE ID = '{warranty_id}'")
        result = cursor.fetchone()
        columns = [desc[0] for desc in cursor.description]  # Get column names

        if result:
            df = pd.DataFrame([result], columns=columns)
            st.success("✅ Warranty is valid!")
            st.dataframe(df)
        else:
            st.error("❌ Invalid Warranty ID.")

    else:
        st.warning("⚠️ Couldn't find a valid 6-digit warranty ID in the image.")

def check_product_validity(extracted_text):
    cursor = connect_snowflake(user,password,account,warehouse,database,schema)
    model_match = re.search(r"Model\s*:\s*(.+)", extracted_text)
    model = model_match.group(1).strip() if model_match else None

    if model:
        st.success(f"🛠️ Detected Model: {model}")

        cursor.execute(f"SELECT * FROM PRODUCT WHERE NAME ILIKE '{model}'")
        result = cursor.fetchone()
        columns = [desc[0] for desc in cursor.description]  # Get column names

        if result:
            df = pd.DataFrame([result], columns=columns)
            st.success("✅ Product is valid!")
            st.dataframe(df)
        else:
            st.error("❌ Invalid Product.")
    else:
        st.warning("⚠️ Couldn't find Product in the text.")

def main():

    st.title("Warranty Validity Checker")

    uploaded_file = st.file_uploader("Upload Invoice Image (to extract Warranty ID)", type=["png", "jpg", "jpeg"])    

    if uploaded_file is not None:
        image = process_image(uploaded_file)
        extracted_text = extract_text(image)
        check_warranty_validity(extracted_text)
        check_product_validity(extracted_text)
    else:
        # st.info("Please upload you Warranty card.")
        print("Waiting on Upload.")

if __name__ == "__main__":
    main()