from flask import Flask, render_template, request
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
import os
import pandas as pd
from io import StringIO

app = Flask(__name__)
app.secret_key = 'testing'

# Get the Azure ADLS account URL from environment variables
AZURE_ADLS_ACCOUNT_URL = os.getenv('AZURE_ADLS_ACCOUNT_URL')
print(AZURE_ADLS_ACCOUNT_URL)


def get_latest_adls_file(container_name, directory_path, file_pattern):
    try:
        # Authenticate and create DataLakeServiceClient
        credential = DefaultAzureCredential()
        data_lake_service_client = DataLakeServiceClient(account_url=AZURE_ADLS_ACCOUNT_URL, credential=credential)
        file_system_client = data_lake_service_client.get_file_system_client(container_name)
        #directory_client = file_system_client.get_directory_client(directory_path)

        # List files and get the latest one based on last modified time and pattern
        #paths = directory_client.get_paths()
        paths = file_system_client.get_paths(path=directory_path)
        print(paths)
        latest_file = None
        latest_time = None

        for path in paths:
            print(path)
            if not path.is_directory and re.match(file_pattern, path.name):
                if latest_time is None or path.last_modified > latest_time:
                    latest_time = path.last_modified
                    latest_file = path.name

        if latest_file:
            return latest_file
        else:
            return "No files found matching the specified pattern in the specified path."

    except Exception as e:
        return f"Error accessing ADLS Gen2: {str(e)}"


def read_adls_file(container_name, file_path, num_rows=None):
    try:
        credential = DefaultAzureCredential()
        data_lake_service_client = DataLakeServiceClient(account_url=AZURE_ADLS_ACCOUNT_URL, credential=credential)
        file_system_client = data_lake_service_client.get_file_system_client(container_name)
        file_client = file_system_client.get_file_client(file_path)
        download = file_client.download_file()
        downloaded_bytes = download.readall()
        content = downloaded_bytes.decode('utf-8')

        if num_rows:
            # Read limited number of rows
            limited_content = "\n".join(content.split('\n')[:num_rows])
        else:
            # Read all rows
            limited_content = content

        return limited_content
    except Exception as e:
        return f"Error accessing ADLS Gen2: {str(e)}"

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/read_file', methods=['POST'])
def read_file():
    file = request.form['file_name']
    container_name = "didqsynapsefilesystem"
    directory_path = "EMPLOYEE/"
    file_name = get_latest_adls_file(container_name, directory_path, file)
    print(file_name)
    if "Error" in file_name:
        return file_name

    content = read_adls_file(container_name, file_name)
    if "Error" in content:
        return content

    # Convert the content to a DataFrame
    data = StringIO(content)
    df = pd.read_csv(data)

    # Convert the DataFrame to CSV for display
    output = df.to_csv(index=False)

    return render_template('result.html', content=output)

if __name__ == '__main__':
    app.run(debug=True)