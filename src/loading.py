import psycopg2
from datetime import datetime
import json
def upsert_data(transformed_data):
    conn = psycopg2.connect(
        dbname="candidates_db",
        user="postgres",
        password="12345678",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()

    # Upsert dans DimCandidates
    candidate = transformed_data['candidate']
    cur.execute(
        """
        INSERT INTO DimCandidates (candidateID, name, email, phone, address)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (candidateID) 
        DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email, phone = EXCLUDED.phone, address = EXCLUDED.address
        """,
        [candidate['candidateID'], candidate['name'], candidate['email'], candidate['phone'], candidate['address']]
    )

    # Upsert dans DimSkills
    for skill in transformed_data['skills']:
        cur.execute(
            """
            INSERT INTO DimSkills (skillID, candidateID, skillName)
            VALUES (%s, %s, %s)
            ON CONFLICT (skillID)
            DO UPDATE SET skillName = EXCLUDED.skillName
            """,
            [skill['skillID'], skill['candidateID'], skill['skillName']]
        )

    # Upsert dans DimWorkHistory
    for work in transformed_data['work_histories']:
        cur.execute(
            """
            INSERT INTO DimWorkHistory (workHistoryID, candidateID, companyName, position, startDate, endDate)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (workHistoryID)
            DO UPDATE SET companyName = EXCLUDED.companyName, position = EXCLUDED.position, startDate = EXCLUDED.startDate, endDate = EXCLUDED.endDate
            """,
            [work['workHistoryID'], work['candidateID'], work['companyName'], work['position'], work['startDate'], work['endDate']]
        )

    # Upsert dans DimEducation
    for edu in transformed_data['educations']:
        cur.execute(
            """
            INSERT INTO DimEducation (educationID, candidateID, school, degree, startDate, endDate)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (educationID)
            DO UPDATE SET school = EXCLUDED.school, degree = EXCLUDED.degree, startDate = EXCLUDED.startDate, endDate = EXCLUDED.endDate
            """,
            [edu['educationID'], edu['candidateID'], edu['school'], edu['degree'], edu['startDate'], edu['endDate']]
        )

    conn.commit()
    cur.close()
    conn.close()

def calculate_aggregates(transformed_data):
    total_experience_years = 0
    for work in transformed_data['work_histories']:
        start_date = datetime.strptime(work['startDate'], '%Y-%m-%d %H:%M')
        end_date = datetime.strptime(work['endDate'], '%Y-%m-%d %H:%M')
        duration_years = (end_date - start_date).days / 365.25
        total_experience_years += duration_years

    education_duration_years = 0
    for edu in transformed_data['educations']:
        start_date = datetime.strptime(edu['startDate'], '%Y-%m-%d %H:%M')
        end_date = datetime.strptime(edu['endDate'], '%Y-%m-%d %H:%M')
        duration_years = (end_date - start_date).days / 365.25
        education_duration_years += duration_years

    skills_count = len(transformed_data['skills'])

    return {
        'candidateID': transformed_data['candidate']['candidateID'],
        'totalExperienceYears': total_experience_years,
        'educationDurationYears': education_duration_years,
        'skillsCount': skills_count
    }

def insert_facts(aggregates):
    conn = psycopg2.connect(
        dbname="candidates_db",
        user="postgres",
        password="12345678",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO FactCandidateData (candidateID, totalExperienceYears, educationDurationYears, skillsCount)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (candidateID) 
        DO UPDATE SET totalExperienceYears = EXCLUDED.totalExperienceYears, educationDurationYears = EXCLUDED.educationDurationYears, skillsCount = EXCLUDED.skillsCount
        """,
        [aggregates['candidateID'], aggregates['totalExperienceYears'], aggregates['educationDurationYears'],
         aggregates['skillsCount']]
    )


    conn.commit()
    cur.close()
    conn.close()



output_path='C:/Users/SYB ASUS/PycharmProjects/ETL_Project/data/output/transformed_data.json'
def save_transformed_data(transformed_data, output_path):

    with open(output_path, 'w') as f:
        json.dump(transformed_data, f, indent=4)
    print(f"Transformed data saved to {output_path}")
