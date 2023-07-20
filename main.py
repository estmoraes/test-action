import requests
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from datetime import timedelta
import json
import logging
import warnings
warnings.filterwarnings('ignore')

class DataProcessor:
    def __init__(self, host, user, password, database, api_url):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.api_url = api_url
        self.logger = logging.getLogger('DataProcessor')
        self.logger.setLevel(logging.INFO)
        
    # Create a FileHandler and set the file path for the log
        file_handler = logging.FileHandler('status.log')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def calculate_statistics(self, data):
        baseline = data['ExecutionTime'].mean()
        std = data['ExecutionTime'].std()
        upper_limit = baseline + (std * 1.5)
        lower_limit = baseline - (std * 1.5)
        return baseline, std, upper_limit, lower_limit

    def calculate_statistics_for_latest_time(self, df, latest_time):
        start_time = latest_time - timedelta(days=1)
        end_time = latest_time

        filtered_data = df[(df['Datetime'] >= start_time) & (df['Datetime'] <= latest_time)]

        if len(filtered_data) >= 2:
            baseline, std, upper_limit, lower_limit = self.calculate_statistics(filtered_data)
        else:
            baseline, std, upper_limit, lower_limit = None, None, None, None

        return baseline, std, upper_limit, lower_limit

    def create_db_connection(self):
        connection = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )
        return connection

    def fetch_data_from_db(self, connection):
        query = "SELECT * FROM testAnalytics"
        df = pd.read_sql(query, connection)
        return df

    def process_data(self):
        self.logger.info('Starting data processing...')

        connection = self.create_db_connection()
        df_fetch = self.fetch_data_from_db(connection)
        connection.close()

        response = requests.get(self.api_url)
        if response.status_code == 200:
            data = response.json()
            df_new = pd.DataFrame(data)
            df_new['Datetime'] = pd.to_datetime(df_new['Datetime'])
            df_new = df_new.reindex(columns=['Datetime', 'ProcessName', 'ExecutionTime'])
            df_combined = pd.concat([df_fetch, df_new[~df_new['Datetime'].isin(df_fetch['Datetime'])]], ignore_index=True)
        else:
            self.logger.error("Failed to retrieve data from the API.")
            return

        last_calculation_time = df_combined['Datetime'].min()
        
        for index, row in df_combined.iterrows():
            latest_time = row['Datetime']
            if latest_time > last_calculation_time:
                baseline, std, upper_bound, lower_bound = self.calculate_statistics_for_latest_time(df_combined, latest_time)

                # Update data terbaru dengan hasil kalkulasi
                df_combined.at[index, 'Baseline'] = baseline
                df_combined.at[index, 'StdDev'] = std
                df_combined.at[index, 'UpperBounds'] = upper_bound
                df_combined.at[index, 'LowerBounds'] = lower_bound

                # Update last_calculation_time dengan waktu terbaru
                last_calculation_time = latest_time

        # Inisialisasi kolom 'IsAnomaly' dengan nilai 0 untuk semua data
        df_combined['IsAnomaly'] = 0

        # Periksa kondisi dan tandai data yang merupakan anomaly dengan nilai 1
        df_combined.loc[df_combined['ExecutionTime'] > df_combined['UpperBounds'], 'IsAnomaly'] = 1
        
        self.logger.info('Data processing completed successfully.')

        # Membuat koneksi ke database
        connection = self.create_db_connection()
        engine = create_engine(f'mysql+pymysql://{self.user}:{self.password}@{self.host}/{self.database}')
        df_combined.to_sql('testAnalytics', con=engine, if_exists='replace', index=False)
        connection.close()

        self.logger.info('Data has been successfully sent to Databases.')

def load_config(file_path):
    with open(file_path) as f:
        return json.load(f)

if __name__ == '__main__':
    # Load database credentials from config file
    config = load_config('config.json')

    # Set up logging configuration
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Replace the URL with the actual API URL
    api_url = 'http://103.82.92.229:5000/generate-data'

    # Create data processor instance with credentials from config
    data_processor = DataProcessor(
        config['host'],
        config['user'],
        config['password'],
        config['database'],
        api_url
    )

    # Process data
    data_processor.process_data()
