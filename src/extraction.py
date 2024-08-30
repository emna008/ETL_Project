import json

file_path="C:/Users/SYB ASUS/PycharmProjects/ETL_Project/data/input/Candidate_Sample_Data 1.json"

def extract_data(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data

