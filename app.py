import os
import pandas as pd
from flask import Flask, render_template
from azure.storage.blob import BlobServiceClient
import requests

# Azure Blob Storage credentials
storage_account_name = 'customclassify2070'
container_name = 'customer-reviews'
blob_csv_name = 'customer_reviews.csv'  # CSV file name
blob_xlsx_name = 'customer_reviews.xlsx'  # Excel file name
connect_str = 'DefaultEndpointsProtocol=https;AccountName=customclassify2070;AccountKey=0NwVUOleJqmw7rTo6//s5vHKn3C/90BjtEHavM4+4KJOvstuvQ8SlfajYGI/wHDz2jmjkMyRdIbI+AStukhs7g==;EndpointSuffix=core.windows.net'

# Azure Cognitive Services Text Analytics credentials
text_analytics_endpoint = 'https://textalanysis01.cognitiveservices.azure.com/'  
text_analytics_key = '57e64cd52b944c489e79950721db38a7'  

app = Flask(__name__)

def download_blob_file(blob_name):
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    download_file_path = os.path.join(os.getcwd(), blob_name)

    with open(download_file_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())

    return download_file_path

# Function to call Azure Text Analytics API for sentiment analysis
def sentiment_analysis(review_text):
    url = f"{text_analytics_endpoint}/text/analytics/v3.0/sentiment"
    headers = {
        "Ocp-Apim-Subscription-Key": text_analytics_key,
        "Content-Type": "application/json"
    }
    documents = {"documents": [{"id": "1", "language": "en", "text": review_text}]}
    
    try:
        response = requests.post(url, headers=headers, json=documents)

        if response.status_code == 200:
            result = response.json()
            return result["documents"][0]["sentiment"]
        else:
            raise Exception(f"Error calling Azure Text Analytics API: {response.text}")

    except Exception as e:
        raise Exception(f"An error occurred while calling Azure Text Analytics API: {str(e)}")

# Perform sentiment analysis on the reviews in the specified files
def perform_sentiment_analysis():
    # List of potential blob names
    blob_names = [blob_csv_name, blob_xlsx_name]
    df = None  # Initialize DataFrame

    for blob_name in blob_names:
        try:
            # Download the file from Blob Storage
            file_path = download_blob_file(blob_name)

            # Load the file based on its type
            if blob_name.endswith('.csv'):
                df = pd.read_csv(file_path)  # Use read_csv for CSV files
            elif blob_name.endswith('.xlsx'):
                df = pd.read_excel(file_path)  # Use read_excel for Excel files

            # Make column names lowercase to avoid case sensitivity issues
            df.columns = df.columns.str.lower()

            # Check if 'review' column exists
            if 'review' not in df.columns:
                raise Exception('File must contain a "review" column')

            # Apply sentiment analysis on the 'review' column
            df['sentiment'] = df['review'].apply(sentiment_analysis)

            return df  # Return the DataFrame with results

        except Exception as e:
            print(f"An error occurred while processing {blob_name}: {e}")

    return df  # Return None if no DataFrame was created

@app.route('/')
def index():
    # Assume df is your DataFrame containing sentiment analysis results
    df = perform_sentiment_analysis()  # Get the sentiment analysis results
    if df is not None:
        return render_template('index.html', reviews=df[['review', 'sentiment']].to_dict(orient='records'))
    else:
        return "No reviews available or an error occurred."

if __name__ == '__main__':
    app.run(debug=True)
