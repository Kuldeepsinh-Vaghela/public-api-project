import requests
import json
import pandas as pd



def get_tender_data(url,country):
    response = requests.get(url)
    if response.status_code == 200:
        all_tenders = []
        data = response.json()
        tender_list = data['data']
        for tender in tender_list:
            tender_details = {}
            tender_details['id'] = tender.get('id',None)
            tender_details['date'] = tender.get('date',None)
            tender_details['deadline_date'] = tender.get('deadline_date', None)
            tender_details['title'] = tender.get('title', None)
            tender_details['category'] = tender.get('category', None)
            tender_details['phase'] = tender.get('phase_en', None)
            tender_details['awarded_value_in_euros'] = tender.get('awarded_value_eur', None)
            tender_details['tender_country'] = country
            all_tenders.append(tender_details)


        return all_tenders
    else:
        return None

# getting data for procurements in Hungary, Poland, Romania, Spain, Ukraine
url_list = {'https://tenders.guru/api/hu/tenders': 'Hungary',
    'https://tenders.guru/api/pl/tenders': 'Poland',
    'https://tenders.guru/api/ro/tenders': 'Romania',
    'https://tenders.guru/api/es/tenders': 'Spain',
    'https://tenders.guru/api/ua/tenders': 'Ukraine'}
merged_data_list = []
merged_data_df = pd.DataFrame()
for url, country in url_list.items():
    data = get_tender_data(url,country)
    df = pd.DataFrame(data)
    print(df)
    merged_data_df = pd.concat([merged_data_df, df])



merged_data_df.to_csv('merged_data_csv.csv', index=False)


