import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))



from src.extraction import extract_data
from src.transformation import transform_data
from src.loading import upsert_data, calculate_aggregates, insert_facts
from src.loading import save_transformed_data


def run_etl():
    raw_data = extract_data("C:/Users/SYB ASUS/PycharmProjects/ETL_Project/data/input/Candidate_Sample_Data 1.json")

    transformed_data = transform_data(raw_data)

    save_transformed_data(transformed_data, "C:/Users/SYB ASUS/PycharmProjects/ETL_Project/data/output/transformed_data.json")

    upsert_data(transformed_data)

    aggregates = calculate_aggregates(transformed_data)
    insert_facts(aggregates)


if __name__ == "__main__":
    run_etl()
