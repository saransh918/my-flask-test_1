from io import StringIO
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceNotFoundError
import csv
import os
import pandas as pd
import glob
import datetime
import sys
import configparser
import numpy as np
from flask import Flask, render_template, request, flash, redirect, url_for
import matplotlib.pyplot as plt
import io
import base64
import re
pd.options.mode.chained_assignment = None
import operator
import threading
from dateutil.relativedelta import relativedelta
from azure.storage.blob import BlobServiceClient
#from azure.core.exceptions import ResourceNotFoundError
#import logging

app = Flask(__name__)
#app.secret_key = 'testing'
initialized = False

AZURE_ADLS_ACCOUNT_URL = os.getenv('AZURE_ADLS_ACCOUNT_URL')
app.secret_key = os.getenv('SECRET_KEY')

#One time Run
def initialize_app():
    global initialized
    if not initialized:
        try:
            data_lake_service_client = get_data_lake_service_client()
            container_name = 'metadata'
            folder1_path = 'METADATA'
            folder2_path = 'ERROR_FILES'
            file_system_client = data_lake_service_client.get_file_system_client('metadata')
            create_file_system_if_not_exists(file_system_client)
            directory_client1 = file_system_client.get_directory_client(folder1_path)
            create_directory_if_not_exists(directory_client1)
            directory_client2 = file_system_client.get_directory_client(folder2_path)
            create_directory_if_not_exists(directory_client2)
            initialized = True
            return "Initialization activities completed successfully"
        except Exception as e:
            #logging.error(f"Error during initialization: {str(e)}")
            return "Error during initialization: {str(e)}"
        
        
def get_data_lake_service_client():
    account_url = os.getenv('AZURE_ADLS_ACCOUNT_URL')
    if not account_url:
        raise ValueError("AZURE_ADLS_ACCOUNT_URL environment variable is not set")
    credential = DefaultAzureCredential()
    return DataLakeServiceClient(account_url=account_url, credential=credential)
    
def list_containers():
    try:
        data_lake_service_client = get_data_lake_service_client()
        containers = data_lake_service_client.list_file_systems()
        container_list = [container.name for container in containers]
        return container_list
    except Exception as e:
        logging.error(f"Error listing containers: {str(e)}")
        return []

def read_adls_file(container_name, file_path, num_rows=None):
    try:
        #credential = DefaultAzureCredential()
        data_lake_service_client = get_data_lake_service_client()
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

# This method will return list of projects and a dictionary with project as key and files under the project as values.
def project_and_files():
    project_files = {}
    met_file = "METADATA/metadata.csv"
    metadata = read_adls_file('metadata', met_file, num_rows=None)
    if "Error" in metadata:
        return metadata
    data = StringIO(metadata)
    df = pd.read_csv(data, delimiter='|')
    containers = df['CONTAINER'].sort_values().unique().tolist()
    for container in containers:
        df1 = df.loc[df['CONTAINER'] == container, ['PREFIX']]
        files = df1['PREFIX'].sort_values().unique().tolist()
        project_files[container] = files
    containers.append('')
    return containers, project_files

def check_file_exists(container_name, file_path):
    data_lake_service_client = get_data_lake_service_client()
    file_system_client = data_lake_service_client.get_file_system_client(container_name)
    file_client = file_system_client.get_file_client(file_path)

    try:
        file_client.get_file_properties()
        return True
    except ResourceNotFoundError:
        return False

# Renders Home page
@app.route('/', methods=['GET', 'POST'])
def Home():
    if not initialized:
        initialize_app()
    return render_template('Home.html')


# Decisions based on selection in Home page.
@app.route('/action', methods=['GET', 'POST'])
def action():
    button = request.form['submit_button']
    if button == 'ADD NEW FILE':
        return redirect(url_for('f_add_new'))
    if button == 'FILE DETAILS':
        return redirect(url_for('f_details'))
    if button == 'VALIDATE FILE':
        return redirect(url_for('f_validate'))
    if button == 'HOME':
        return redirect(url_for('Home'))
    if button == 'ADD RULE':
        return redirect(url_for('f_pre_rules'))
    if button == 'UPDATE':
        return redirect(url_for('f_upd_met'))


@app.route('/f_validate', methods=['GET', 'POST'])
def f_validate():
    containers, project_files = project_and_files()
    return render_template('FileValidate.html', categories=containers, subcategories=project_files)


@app.route('/f_add_new', methods=['GET', 'POST'])
def f_add_new():
    frequencies = ['', 'Daily', 'Weekly', 'Monthly', 'Quarterly', 'Bi-Weekly', 'Bi-Monthly', 'Half-Yearly', 'Yearly']
    containers, project_files = project_and_files()
    container_list = list_containers()
    return render_template('AddFile.html', containers=container_list, frequencies=frequencies)


@app.route('/f_details', methods=['GET', 'POST'])
def f_details():
    containers, project_files = project_and_files()
    return render_template('FileDetails.html', categories=containers, subcategories=project_files)  # AddPredefinedRule


@app.route('/f_pre_rules', methods=['GET', 'POST'])
def f_pre_rules():
    containers, project_files = project_and_files()
    return render_template('AddRules.html', subcategories=project_files)


@app.route('/f_upd_met', methods=['GET', 'POST'])
def f_upd_met():
    containers, project_files = project_and_files()
    frequencies = ['', 'Daily', 'Weekly', 'Monthly', 'Quarterly', 'Bi-Weekly', 'Bi-Monthly', 'Half-Yearly', 'Yearly']
    return render_template('UpdateFile.html', categories=containers, subcategories=project_files, frequencies=frequencies)


def determine_delimiter(content):
    sniffer = csv.Sniffer()
    return sniffer.sniff(content).delimiter


# This method reads sample file and extracts details such as delimiter, extension, etc.
def read_header(container_name, file_path):
    file_name = os.path.basename(file_path)
    directory = os.path.dirname(file_path)
    ext = os.path.splitext(file_name)[1]
    content = read_adls_file(container_name, file_path, num_rows=5)
    delimiter = determine_delimiter(content)
    data = StringIO(content)
    df = pd.read_csv(data, delimiter=delimiter)
    header = delimiter.join(df.columns)
    return ext, directory, header, delimiter


# This method saves the error records in an error file
def save_error(file, invalid_rows):
    current_date = datetime.date.today().strftime('%Y-%m-%d')
    directory_path = 'ERROR_FILES/' + file
    data_lake_service_client = get_data_lake_service_client()
    file_system_client = data_lake_service_client.get_file_system_client('metadata')
    directory_client = file_system_client.get_directory_client(directory_path)
    met_file = "METADATA/metadata.csv"
    metadata = read_adls_file('metadata', met_file, num_rows=None)
    if "Error" in metadata:
        return metadata
    data = StringIO(metadata)
    met = pd.read_csv(data, delimiter='|')
    hd_df = met.loc[(met['PREFIX'] == file), ['HEADER']]
    err_header = hd_df.iloc[0]['HEADER']
    err_header = err_header.split(',') + ['ERROR']
    invalid_rows.columns = err_header
    out = invalid_rows.head(100)
    file_name = file + '_' + str(current_date) + '.txt'
    file_client = directory_client.get_file_client(file_name)
    file_client.upload_data(out, overwrite=True)
    return "File info saved"


# This method saves the metadata of new file
def save_header(file, out):
    directory_path = 'METADATA/'
    data_lake_service_client = get_data_lake_service_client()
    file_system_client = data_lake_service_client.get_file_system_client('metadata')
    directory_client = file_system_client.get_directory_client(directory_path)
    file_client = directory_client.get_file_client(file)
    try:
        # Try to download the existing file
        download = file_client.download_file()
        existing_data = download.readall().decode('utf-8')
        df = pd.read_csv(pd.compat.StringIO(existing_data))
    except ResourceNotFoundError:
        # If the file does not exist, create a new DataFrame
        df = pd.DataFrame()
        
    new_row_df = pd.DataFrame([out.split('|')], columns=df.columns if not df.empty else None)
    # Append the new row to the existing DataFrame
    df = pd.concat([df, new_row_df], ignore_index=True)
    
    # Save the updated DataFrame back to the file in ADLS Gen2
    csv_data = df.to_csv(index=False)
    file_client.upload_data(csv_data, overwrite=True)
    return "File info saved"


# Route to update new file details
@app.route('/new_file', methods=['GET', 'POST'])
def new_file():
    button = request.form['submit_button']
    response = request.form.get('response')
    created = datetime.date.today()
    updated = ''
    if button == 'HOME':
        return redirect(url_for('Home'))
    if response == 'OK':
        met_file = "METADATA/metadata.csv"
        metadata = read_adls_file('metadata', met_file, num_rows=None)
        if "Error" in metadata:
            return metadata
        data = StringIO(metadata)
        df = pd.read_csv(data, delimiter='|')
        container = request.form['container']
        #container_list = list_containers()
        #if container not in container_list
        category = request.form['category']
        prefix = request.form['prefix']
        frequency = request.form['frequency']
        csv_file_path = request.form['csv_file_path']
        if not check_file_exists(container, csv_file_path):
            message = "File {} within container {} does not exist. Please enter complete path to the file including file name".format(csv_file_path, container)
            flash(message)
            return redirect(url_for('f_add_new'))
        date_format = request.form['date_format']
        auto = 'AutomationCheck' in request.form
        notification = 'NotificationCheck' in request.form
        emails = request.form.get('Email', '')
        desc = request.form['desc']
        row_exists = df.loc[(df['CONTAINER'] == container) & (df['PREFIX'] == prefix)].shape[0] > 0
        if auto:
            automate = 'Y'
        else:
            automate = 'N'
        if notification:
            notify = 'Y'
        else:
            notify = 'N'
        if not container or not category or not prefix or not csv_file_path:
            message = "container, Category, Prefix and sample file path are necessary to create a new file"
            flash(message)
            return redirect(url_for('f_add_new'))
        elif row_exists:
            message = "File {} for {} is already present. Please choose a different prefix".format(prefix, container)
            flash(message)
            return redirect(url_for('f_add_new'))
        elif notify == 'Y' and (emails == '' or emails == 'Emails'):
            message = "If you have opted for notifications, please enter at least one email address"
            flash(message)
            return redirect(url_for('f_add_new'))
        else:
            ext, directory, header, delimiter = read_header(container, csv_file_path)
            extension = ext
            path = directory
            delim = delimiter
            head = header
            new_row = [container, category, extension, prefix, frequency, path, head, delim, desc,
                       date_format, automate, notify, emails, created, updated]
            df.loc[len(df)] = new_row
            updated_content = df.to_csv(index=False, sep='|')
            data_lake_service_client = get_data_lake_service_client()
            file_system_client = data_lake_service_client.get_file_system_client('metadata')
            upload_file = file_system_client.get_file_client(met_file)
            upload_file.upload_data(updated_content, overwrite=True)
            message = "File {} for container {} is saved successfully".format(prefix, container)
            flash(message)
            return redirect(url_for('f_add_new'))


# This method gives container details
def project_details(container):
    df = pd.read_csv('metadata.csv', sep='|')
    filtered_df = df.loc[df['CONTAINER'] == container, ['PREFIX', 'FREQUENCY', 'DIRECTORY', 'DESCRIPTION']]
    return filtered_df


# This route is for displaying selected file details
@app.route('/file_details', methods=['GET', 'POST'])
def file_details():
    button = request.form['submit_button']
    if button == 'HOME':
        return redirect(url_for('Home'))
    if button == 'VALIDATE FILE':
        return redirect(url_for('f_validate'))
    if button == 'VIEW DETAILS':
        container = request.form['category']
        file = request.form['subcategory']
        met_file = "METADATA/metadata.csv"
        metadata = read_adls_file('metadata', met_file, num_rows=None)
        if "Error" in metadata:
            return metadata
        data = StringIO(metadata)
        df = pd.read_csv(data, delimiter='|')
        filtered_df = df.loc[
            (df['CONTAINER'] == container) & (df['PREFIX'] == file), ['CONTAINER', 'PREFIX', 'FREQUENCY', 'DIRECTORY',
                                                                  'DESCRIPTION', 'DATE_FORMAT']]
        freq = filtered_df.iloc[0]['FREQUENCY']
        directory = filtered_df.iloc[0]['DIRECTORY']
        desc = filtered_df.iloc[0]['DESCRIPTION']
        dt = filtered_df.iloc[0]['DATE_FORMAT']
        dt_frmt = str(dt)
        detail_str = 'This file belongs to the container: ' + container + '.' + '\n' + 'It arrives ' + freq + ' at the following location: ' + directory + '.' + '\n' + desc + '.' + '\n' + 'Date fields have the following format: ' + dt_frmt + '.'
        detail_str = detail_str.split('\n')
        inf_file = "METADATA/information.csv"
        info = read_adls_file('metadata', inf_file, num_rows=None)
        if "Error" in info:
            return info
        data_info = StringIO(info)
        df2 = pd.read_csv(data_info, delimiter='|')
        filtered_df2 = df2.loc[
            (df2['CONTAINER'] == container) & (df2['PREFIX'] == file), ['CONTAINER', 'PREFIX', 'FILE_NAME', 'DATE', 'COUNT',
                                                                    'STATUS', 'REASON']]
        if filtered_df2.empty:
            latest_str = 'This is a new file.' + '\n' + 'Please validate the file at least once to view last processed details.'
            latest_str = latest_str.split('\n')
        else:
            df_sorted = filtered_df2.sort_values('DATE', ascending=False)
            name = df_sorted.iloc[0]['FILE_NAME']
            date = df_sorted.iloc[0]['DATE']
            cnt = str(df_sorted.iloc[0]['COUNT'])
            status = df_sorted.iloc[0]['STATUS']
            reason = df_sorted.iloc[0]['REASON']
            latest_str = 'The latest file was: ' + name + '. This was processed on: ' + date + '. It had ' + cnt + ' records.' + '\n' + 'The status was: ' + status + ' due to following reason: ' + reason
            latest_str = latest_str.split('\n')
        today = datetime.date.today()
        one_month_back_exact = today - relativedelta(months=1)
        one_month_back = pd.to_datetime(one_month_back_exact)
        met = df2[['CONTAINER', 'FILE_NAME', 'DATE', 'STATUS', 'PREFIX']]
        # met = pd.read_csv('information.csv', sep='|', usecols=['PROJECT', 'FILE_NAME', 'DATE', 'STATUS', 'PREFIX'])
        project_fil = met[met['CONTAINER'] == container]
        project_fil['DATE'] = pd.to_datetime(project_fil['DATE'])
        one_month_df = project_fil[project_fil['DATE'] > one_month_back]
        one_month_df.sort_values('DATE')
        one_month_dedup_df = one_month_df.drop_duplicates(subset=['FILE_NAME'], keep='first')
        data = one_month_dedup_df.groupby(['PREFIX', 'STATUS']).size().unstack(fill_value=0)
        return render_template('ViewDetails.html', details=detail_str, latest=latest_str, container=container, file=file)


# This method generates file count vs date graph
def count_vs_date(container, file):
    # df = pd.read_csv('information.csv', sep='|')
    inf_file = "METADATA/information.csv"
    info = read_adls_file('metadata', inf_file, num_rows=None)
    if "Error" in info:
        return info
    data_info = StringIO(info)
    df = pd.read_csv(data_info, delimiter='|')
    filtered_df = df.loc[(df['CONTAINER'] == container) &
                         (df['PREFIX'] == file) &
                         (df['STATUS'] == 'VALID'),
                         ['CONTAINER', 'PREFIX', 'DATE', 'COUNT', 'STATUS']]
    df_sorted = filtered_df.sort_values('DATE', ascending=False).head(6)
    df_desc = df_sorted.sort_values('DATE')
    fig, ax = plt.subplots(figsize=(4, 4))
    ax.scatter(df_desc['DATE'], df_desc['COUNT'])
    ax.set_title('Counts vs Date', fontsize=10)
    ax.set_xlabel('DATE')
    ax.set_ylabel('COUNT')
    img = io.BytesIO()
    plt.savefig(img, format='png', bbox_inches='tight')
    img.seek(0)
    img_data = base64.b64encode(img.getvalue()).decode('utf-8')
    html = '<img src="data:image/png;base64,{}">'.format(img_data)
    return img, html


def check_tuple(tuple):
    empty_string_index = None
    all_empty = all(element == "" for element in tuple)
    if all_empty:
        flag = 'AE'
        eflag = ''
    else:
        flag = ''
        for i, element in enumerate(tuple):
            if element == "":
                empty_string_index = i
                break
        if empty_string_index == 0:
            eflag = 'EC'
        elif empty_string_index == 1:
            eflag = 'NC'
        elif empty_string_index == 2:
            eflag = 'NV'
        else:
            eflag = ''
    return flag, eflag


def extract_largest_number(s):
    numbers = re.findall('\d+', s)
    if numbers:
        return max(map(int, numbers))
    else:
        return 0


@app.route('/rules', methods=['GET', 'POST'])
def rules():
    button = request.form['submit_button']
    response = request.form.get('response')
    if button == 'HOME':
        return redirect(url_for('Home'))
    if response == 'OK':
        file = request.form['subcategory']
        rule_file = "METADATA/rules.csv"
        rules = read_adls_file('metadata', rule_file, num_rows=None)
        if "Error" in rules:
            return rules
        rule_info = StringIO(rules)
        df = pd.read_csv(rule_info, delimiter='|')
        # df = pd.read_csv('rules.csv', sep='|')
        df2 = df.loc[df['FILE_NAME'] == file, ['COLUMNS', 'OPERATOR', 'VALUES']]
        df3 = df.loc[df['FILE_NAME'] == file, ['RULE']]
        c1 = request.form['column1']
        condition1 = request.form['condition1']
        textbox1 = request.form['textbox1']
        tuple1 = (c1, condition1, textbox1)
        flag1, eflag1 = check_tuple(tuple1)

        c2 = request.form['column2']
        condition2 = request.form['condition2']
        textbox2 = request.form['textbox2']
        tuple2 = (c2, condition2, textbox2)
        flag2, eflag2 = check_tuple(tuple2)

        c3 = request.form['column3']
        condition3 = request.form['condition3']
        textbox3 = request.form['textbox3']
        tuple3 = (c3, condition3, textbox3)
        flag3, eflag3 = check_tuple(tuple3)

        c4 = request.form['column4']
        condition4 = request.form['condition4']
        textbox4 = request.form['textbox4']
        tuple4 = (c4, condition4, textbox4)
        flag4, eflag4 = check_tuple(tuple4)

        if flag1 == 'AE' and flag2 == 'AE' and flag3 == 'AE' and flag4 == 'AE':
            flash('You have not selected any rule. Please select at least 1 rule!')
            return redirect(url_for('f_pre_rules'))
        elif eflag1 == 'EC' or eflag2 == 'EC' or eflag3 == 'EC' or eflag4 == 'EC':
            flash('You have not selected any column for one of the rules!')
            return redirect(url_for('f_pre_rules'))
        elif eflag1 == 'NC' or eflag2 == 'NC' or eflag3 == 'NC' or eflag4 == 'NC':
            flash('You have not selected any condition for one of the rules!')
            return redirect(url_for('f_pre_rules'))
        elif eflag1 == 'NV' or eflag2 == 'NV' or eflag3 == 'NV' or eflag4 == 'NV':
            flash('You have not selected any values for one of the rules!')
            return redirect(url_for('f_pre_rules'))
        else:
            df3['numeric_col'] = df3['RULE'].apply(extract_largest_number)
            if df3['numeric_col'].empty:
                max_num = 1
            else:
                max_num = df3['numeric_col'].max() + 1
            if flag1 == '' and eflag1 == '':
                if df2.isin([c1, condition1, textbox1]).all(axis=1).any():
                    flash('First rule created is already present for this file!')
                    return redirect(url_for('f_pre_rules'))
                else:
                    out = file + '|' + 'RULE' + str(max_num) + '|' + c1 + '|' + condition1 + '|' + textbox1
                    save_header('rules.csv', out)
                max_num = max_num + 1
            if flag2 == '' and eflag2 == '':
                if df2.isin([c2, condition2, textbox2]).all(axis=1).any():
                    flash('Second rule created is already present for this file!')
                    return redirect(url_for('f_pre_rules'))
                else:
                    out = file + '|' + 'RULE' + str(max_num) + '|' + c2 + '|' + condition2 + '|' + textbox2
                    save_header('rules.csv', out)
                max_num += 1
            if flag3 == '' and eflag3 == '':
                if df2.isin([c3, condition3, textbox3]).all(axis=1).any():
                    flash('Third rule created is already present for this file!')
                    return redirect(url_for('f_pre_rules'))
                else:
                    out = file + '|' + 'RULE' + str(max_num) + '|' + c3 + '|' + condition3 + '|' + textbox3
                    save_header('rules.csv', out)
                max_num += 1
            if flag4 == '' and eflag4 == '':
                if df2.isin([c4, condition4, textbox4]).all(axis=1).any():
                    flash('Fourth rule created is already present for this file!')
                    return redirect(url_for('f_pre_rules'))
                else:
                    out = file + '|' + 'RULE' + str(max_num) + '|' + c4 + '|' + condition4 + '|' + textbox4
                    save_header('rules.csv', out)
        flash('Rules Saved Successfully!')
        return redirect(url_for('f_pre_rules'))


def process_chunk(chunk, prefix, columns, comparison_operator, values, oprtr):
    column_list = columns.split(',')
    met_file = "METADATA/metadata.csv"
    metadata = read_adls_file('metadata', met_file, num_rows=None)
    if "Error" in metadata:
        return metadata
    data = StringIO(metadata)
    met = pd.read_csv(data, sep='|', usecols=['PREFIX', 'DATE_FORMAT', 'HEADER'])
    # dt_df = met.loc[(met['PREFIX'] == prefix), ['DATE_FORMAT']]
    dt_df = met[['DATE_FORMAT']][met['PREFIX'] == prefix]
    dt_frmt = dt_df.iloc[0]['DATE_FORMAT']
    if comparison_operator == 'between':
        low, high = values.split(',')
        if low.isnumeric() and high.isnumeric():
            for column in column_list:
                if int(chunk[column].min()) >= int(low) and int(chunk[column].max()) <= int(high):
                    pass
                else:
                    invalid_rows = chunk[(chunk[column] < int(low)) | (chunk[column] > int(high))]
                    error = "Range Error: Column '{0}' between {1} and {2}".format(column, low, high)
                    invalid_rows.loc[:, 'ERROR'] = error
                    save_error(prefix, invalid_rows)
        elif isinstance(low, datetime.date) and isinstance(high, datetime.date):
            for column in column_list:
                if chunk[column].min() >= low and chunk[column].max() <= high:
                    pass
                else:
                    invalid_rows = chunk[(chunk[column].min() < low) | (chunk[column].max() > high)]
                    error = "Range Error: Column '{0}' between {1} and {2}".format(column, low, high)
                    invalid_rows.loc[:, 'ERROR'] = error
                    save_error(prefix, invalid_rows)
    elif comparison_operator == 'contains':
        for column in column_list:
            if chunk[column].str.contains(values).all():
                pass
            else:
                invalid_rows = chunk[~(chunk[column].str.contains(values).all())]
                error = "String Error: Column '{0}' Does not contain '{1}'".format(column, values)
                invalid_rows.loc[:, 'ERROR'] = error
                save_error(prefix, invalid_rows)
    elif comparison_operator == 'numeric fields':
        for column in column_list:
            df_num = chunk.replace('', np.nan).dropna()
            invalid_rows = df_num[pd.to_numeric(df_num[column], errors='coerce').isnull()]
            if invalid_rows.empty:
                pass
            else:
                error = "Number Error: Column '{0}' contains non numeric value".format(column)
                invalid_rows.loc[:, 'ERROR'] = error
                save_error(prefix, invalid_rows)
    elif comparison_operator == 'date fields':
        for column in column_list:
            df_dt = chunk.dropna(subset=[column])
            invalid_rows = df_dt[pd.to_datetime(df_dt[column], format=dt_frmt, errors='coerce').isnull()]
            if invalid_rows.empty:
                pass
            else:
                error = "Date Error: Column '{0}' contains Non-Date values".format(column)
                invalid_rows.loc[:, 'ERROR'] = error
                save_error(prefix, invalid_rows)
    elif comparison_operator == 'not null fields':
        for column in column_list:
            df_null = chunk[column].isnull()
            invalid_rows = chunk.loc[df_null]
            if invalid_rows.empty:
                pass
            else:
                error = "Null Error: Column '{0}' has null values".format(column)
                invalid_rows.loc[:, 'ERROR'] = error
                save_error(prefix, invalid_rows)
    elif comparison_operator == 'primary key':
        for column in column_list:
            df_key = chunk[column].duplicated()
            invalid_rows = chunk.loc[df_key]
            if invalid_rows.empty:
                pass
            else:
                error = "Key Error: Column '{0}' has duplicate values".format(column)
                invalid_rows.loc[:, 'ERROR'] = error
                save_error(prefix, invalid_rows)
    else:
        for column in column_list:
            is_all = comparison_operator(chunk[column], int(values))
            if not is_all.all():
                invalid_rows = chunk[~(comparison_operator(chunk[column], int(values)))]
                error = "Comparison Error : Column '{0}' '{1}' '{2}'".format(column, oprtr, values)
                invalid_rows.loc[:, 'ERROR'] = error
                save_error(prefix, invalid_rows)
            else:
                pass
    return "Done"


def validate_rule(latest_file, file):
    rule_file = "METADATA/rules.csv"
    rules = read_adls_file('metadata', rule_file, num_rows=None)
    if "Error" in rules:
        return rules
    rule_info = StringIO(rules)
    df = pd.read_csv(rule_info, sep='|')
    df2 = df.loc[df['FILE_NAME'] == file, ['COLUMNS', 'OPERATOR', 'VALUES']]
    dict = {}
    if df2.empty:
        dict['no_rules'] = 'Y'
        return dict
    else:
        chunk_size = 50000
        current_directory = os.getcwd()
        # didqsynapsefilesystem
        # err_file_dir = current_directory + '\\Error_files\\' + file + '\\'
        err_file_dir = 'ERROR_FILES/' + file + '/'
        current_date = datetime.date.today().strftime('%Y-%m-%d')
        file_name = file + '_' + str(current_date) + '.txt'
        matched_file = glob.glob(os.path.join(err_file_dir, file_name))
        if matched_file:
            if os.path.exists(matched_file[0]):
                os.remove(matched_file[0])
        for i in range(len(df2)):
            columns = df2.iloc[i]['COLUMNS']
            oprtr = df2.iloc[i]['OPERATOR']
            values = df2.iloc[i]['VALUES']
            if oprtr == '=':
                op = operator.eq
            elif oprtr == '<=':
                op = operator.le
            elif oprtr == '>=':
                op = operator.ge
            elif oprtr == 'between':
                op = 'between'
            elif oprtr == 'contains':
                op = 'contains'
            elif oprtr == 'not null fields':
                op = 'not null fields'
            elif oprtr == 'numeric fields':
                op = 'numeric fields'
            elif oprtr == 'primary key':
                op = 'primary key'
            elif oprtr == 'date fields':
                op = 'date fields'
            data = read_adls_file(container_name, latest_file, num_rows=None)
            if "Error" in data:
                return data
            complete_data = StringIO(data)
            for chunk in pd.read_csv(complete_data, chunksize=chunk_size, na_values=['""', '']):
                thread = threading.Thread(target=process_chunk, args=(chunk, file, columns, op, values, oprtr))
                thread.start()
                thread.join()
        matched_file = glob.glob(os.path.join(err_file_dir, file_name))
        if not matched_file:
            return dict
        else:
            dict['Error File'] = matched_file
            return dict


@app.route('/file_validate', methods=['GET', 'POST'])
def file_validate():
    button = request.form['submit_button']
    if button == 'HOME':
        return redirect(url_for('Home'))
    elif button == 'VALIDATE':
        container = request.form['category']
        file = request.form['subcategory']
        met_file = "METADATA/metadata.csv"
        metadata = read_adls_file(container_name, met_file, num_rows=None)
        if "Error" in metadata:
            return metadata
        data = StringIO(metadata)
        df = pd.read_csv(data, sep='|', usecols=['CONTAINER', 'PREFIX', 'HEADER', 'DIRECTORY', 'DELIMITER', 'TYPE'])
        new_df = df.loc[
            (df['CONTAINER'] == container) & (df['PREFIX'] == file), ['CONTAINER', 'HEADER', 'DIRECTORY', 'DELIMITER',
                                                                  'TYPE']]
        container = new_df.iloc[0]['CONTAINER']
        heading = new_df.iloc[0]['HEADER']
        directory = new_df.iloc[0]['DIRECTORY']
        delim = new_df.iloc[0]['DELIMITER']
        type = new_df.iloc[0]['TYPE']
        all_files = glob.glob(os.path.join(directory + '/' + file + '*'))
        latest_file = sorted(all_files, key=os.path.getmtime)[-1]
        file_name = os.path.basename(latest_file)
        time_stamp = os.path.getmtime(latest_file)
        dt_object = datetime.datetime.fromtimestamp(time_stamp)
        time_string = dt_object.strftime("%Y-%m-%d")
        processed_on = str(datetime.date.today())
        inf_file = "METADATA/information.csv"
        info = read_adls_file('metadata', inf_file, num_rows=None)
        if "Error" in info:
            return info
        data_info = StringIO(info)
        df1 = pd.read_csv(data_info, sep='|', usecols=['FILE_NAME', 'DATE', 'STATUS'])
        fil_df = df1.loc[(df1['FILE_NAME'] == file_name) & (df1['STATUS'] == 'VALID'), ['DATE', 'STATUS']]
        if fil_df.empty:
            ext, directory, header, delimiter, count = read_header(container, latest_file)
            # structure = check_file_structure(latest_file, delimiter)
            reason = ""
            val_dict = {}
            if ext != type:
                val_dict['ext'] = 'N'
            if header != heading:
                val_dict['header'] = 'N'
            if delimiter != delim:
                val_dict['delimiter'] = 'N'
            # if structure != 0:
            #    val_dict['structure'] = 'N'
            if len(val_dict) != 0:
                status1 = 'INVALID'
                if 'ext' in val_dict:
                    reason += "Invalid Extension!"
                if 'header' in val_dict:
                    reason += "Invalid Header!"
                if 'delimiter' in val_dict:
                    reason += "Invalid Delimiter!"
                # if 'structure' in val_dict:
                #    reason += "Invalid Structure!"
            else:
                status1 = 'VALID'
                reason += "File structure is correct! "
                rule_dict = validate_rule(latest_file, file)
                if len(rule_dict) != 0:
                    if 'no_rules' in rule_dict:
                        status2 = 'VALID'
                        reason += "No rules are present for this file!"
                    else:
                        status2 = 'INVALID'
                        error_file = rule_dict['Error File']
                        error_file_name = str(error_file[0])
                        error_file_name = error_file_name.replace("\\", "/")
                        reason += 'File has errors, check error file: ' + str(error_file_name)
                else:
                    status2 = 'VALID'
                    reason += "File satisfies all rules!"
            if status1 == 'VALID' and status2 == 'VALID':
                status = "VALID"
            else:
                status = "INVALID"
            entry = container + '|' + file + '|' + file_name + '|' + processed_on + '|' + count + '|' + status + '|' + reason
            save_header('information.csv', entry)
            if status == "INVALID":
                message = "File is Invalid because {}".format(reason)
            elif status == "VALID":
                message = "File is Valid because {}".format(reason)
            flash(message)
            return redirect(url_for('f_validate'))
        else:
            date = fil_df.iloc[0]['DATE']
            message = 'File: ' + file_name + ' was already validated on ' + date + ' and status was VALID'
        flash(message)
        return redirect(url_for('f_validate'))


@app.route('/update_file', methods=['GET', 'POST'])
def update_file():
    button = request.form['submit_button']
    response = request.form.get('response')
    if button == 'HOME':
        return redirect(url_for('Home'))
    if response == 'OK':
        updated_data = {}
        met_file = "METADATA/metadata.csv"
        metadata = read_adls_file('metadata', met_file, num_rows=None)
        if "Error" in metadata:
            return metadata
        data = StringIO(metadata)
        df = pd.read_csv(data, sep='|')
        today = datetime.date.today()
        container = request.form['category']
        prefix = request.form['subcategory']
        search_keys = {'CONTAINER': container, 'PREFIX': prefix}
        freq = request.form['frequency']
        path = request.form['csv_file_path']
        dt_frmt = request.form['date_format']
        auto = 'AutomationCheck' in request.form
        notification = 'NotificationCheck' in request.form
        emails = request.form.get('Email', '')
        desc = request.form['desc']
        if not freq and not path and not dt_frmt and not auto and not notification and not desc:
            message = "Please select at least one field to update"
            flash(message)
            return redirect(url_for('f_upd_met'))
        elif notification and (emails == '' or emails == 'Emails'):
            message = "Please enter at least one email to enable notifications"
            flash(message)
            return redirect(url_for('f_upd_met'))
        else:
            if freq:
                updated_data['FREQUENCY'] = freq
            if path:
                updated_data['DIRECTORY'] = path
            if dt_frmt:
                updated_data['DATE_FORMAT'] = dt_frmt
            if auto:
                updated_data['AUTOMATE'] = 'Y'
            if notification:
                updated_data['NOTIFY'] = 'Y'
                updated_data['EMAIL'] = emails
            if desc:
                updated_data['DESCRIPTION'] = desc
            updated_data['UPDATED'] = today
            filter_condition = ((df['CONTAINER'] == search_keys['CONTAINER']) & (df['PREFIX'] == search_keys['PREFIX']))
            # index_to_update = df.index[filter_condition]
            for column, value in updated_data.items():
                df.loc[filter_condition, column] = value
            # df.to_csv('metadata.csv', index=False, sep='|')
            updated_content = df.to_csv(index=False, sep='|')
            data_lake_service_client = get_data_lake_service_client()
            file_system_client = data_lake_service_client.get_file_system_client('metadata')
            upload_file = file_system_client.get_file_client(met_file)
            upload_file.upload_data(updated_content, overwrite=True)
            message = "Metadata for file {0} in container {1} is updated successfully!".format(prefix, container)
            flash(message)
            return redirect(url_for('f_upd_met'))
            
if __name__ == "__main__":
    app.run()
    app.debug = True