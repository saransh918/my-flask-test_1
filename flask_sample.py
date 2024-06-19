from flask import Flask, render_template, redirect, url_for
from azure.storage.blob import BlobServiceClient
import os

app = Flask(__name__)

# Azure Blob Storage credentials
AZURE_STORAGE_CONNECTION_STRING = 'DefaultEndpointsProtocol=https;AccountName=didq2024;AccountKey=ntayQbzAzlGjFbJYzdswvYkT/SUty3y2YOnLUjwLtMzWRV+VIEJL0JrU3UFya2HkJYxuz/RsHXO4+ASt7n99iQ==;EndpointSuffix=core.windows.net'
AZURE_CONTAINER_NAME = 'didqcontainer'
AZURE_BLOB_NAME = 'DI_DQ.txt'

def read_blob():
    # Create a BlobServiceClient object using the connection string
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)

    # Get a container client
    container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)

    # Get a blob client
    blob_client = container_client.get_blob_client(AZURE_BLOB_NAME)

    # Download the blob's contents
    blob_content = blob_client.download_blob().readall()

    # Convert the blob content to a string
    return blob_content.decode('utf-8')

@app.route('/')
def home():
    return render_template('Home.html')

@app.route('/read_blob', methods=['GET', 'POST'])
def read_blob_route():
    content = read_blob()
    return render_template('Result.html', content=content)

@app.route('/back')
def back():
    return redirect(url_for('home'))

if __name__ == '__main__':
    app.run(debug=True)
