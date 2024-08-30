from datetime import datetime
def transform_data(raw_data):

    candidate_id = raw_data['CandidateID']
    name = raw_data['firstName'] + " " + raw_data['lastName']
    email = raw_data.get('email', None)
    phone = raw_data.get('PhoneNumber', [None])[0]

    # Formater l'adresse
    address_parts = [
        raw_data['address'].get('address1', ''),
        raw_data['address'].get('city', ''),
        raw_data['address'].get('countryName', '')
    ]
    address = ', '.join(filter(None, address_parts))

    # Transformation des compétences
    skills = []
    for skill in raw_data['primarySkills']['data']:
        skills.append({
            'skillID': skill['id'],
            'candidateID': candidate_id,
            'skillName': skill['name']
        })

    # Transformation de l'historique de travail
    work_histories = []
    for work in raw_data['workHistories']['data']:
        work_histories.append({
            'workHistoryID': work['id'],
            'candidateID': candidate_id,
            'companyName': work['CompanyName'],
            'position': work['Position'],
            'startDate': datetime.fromtimestamp(work['StartDate'] / 1000).strftime('%Y-%m-%d %H:%M'),
            'endDate': datetime.fromtimestamp(work['EndDate'] / 1000).strftime('%Y-%m-%d %H:%M') if work[
                'EndDate'] else None
        })

    # Transformation des éducations
    educations = []
    for edu in raw_data['educations']['data']:
        educations.append({
            'educationID': edu['id'],
            'candidateID': candidate_id,
            'school': edu['school'],
            'degree': edu['degree'],
            'startDate': datetime.fromtimestamp(edu['StartDate'] / 1000).strftime('%Y-%m-%d %H:%M'),
            'endDate': datetime.fromtimestamp(edu['EndDate'] / 1000).strftime('%Y-%m-%d %H:%M') if edu[
                'EndDate'] else None
        })

    return {
        'candidate': {
            'candidateID': candidate_id,
            'name': name,
            'email': email,
            'phone': phone,
            'address': address
        },
        'skills': skills,
        'work_histories': work_histories,
        'educations': educations
    }
