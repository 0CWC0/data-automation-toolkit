import pandas as pd
import os

class FileHandler:
    @staticmethod
    def read_csv(file_path):
        """ Reads a CSV file and returns a DataFrame """  
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The file {file_path} does not exist.")
        return pd.read_csv(file_path)

    @staticmethod
    def write_csv(data_frame, file_path):
        """ Writes a DataFrame to a CSV file """  
        data_frame.to_csv(file_path, index=False)

    @staticmethod
    def read_excel(file_path):
        """ Reads an Excel file and returns a DataFrame """  
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The file {file_path} does not exist.")
        return pd.read_excel(file_path)

    @staticmethod
    def write_excel(data_frame, file_path):
        """ Writes a DataFrame to an Excel file """  
        data_frame.to_excel(file_path, index=False)