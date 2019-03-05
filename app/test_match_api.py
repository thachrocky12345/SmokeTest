import concurrent.futures
import time
import asyncio
from datetime import datetime
import requests
from lib.log import getLogger, LoggingSetup, DEBUG, INFO, ERROR


from datetime import datetime
import csv
import json
# URL  = 'https://matalgoapi-dev-ev2-b-track.mc-track-csase-dev-ev2-use0-b.p.azurewebsites.net/getResults'
URL = 'http://40.121.207.138:80/getResults'
POST_DATA_TEMPLATE = {"SUPPLIER_ID":"313415",
"SUPPLIER_NAME":"PokerCentral, LLC",
"SUPPLIER_STREET_ADDRESS_1":"3960 HOWARD HUGHES PKWY STE 500 ",
"SUPPLIER_CITY":"LAS VEGAS,NV,US",
"SUPPLIER_ZIP":"",
"COUNTRY":"US",
"CORRELATION_ID":"587693d3-8bd9-42e8-a4e8-608ccfd3d232",
"SUPPLIER_STATE":"NV"}

# Get 1000 datas

log = getLogger("Test5000")
setup_log = LoggingSetup(name="Test5000", console_level=INFO, file_level=ERROR, daily_file=False)
setup_log.init_logging()


def get_1000_data():
    headers = ['RawOrganizationKey',
               'CorrelationID',
               'EIN',
               'TaxCode',
               'IDFromNetwork',
               'IDFromBuyer',
               'DunsNumber',
               'ComplianceVendorID',
               'CompanyNumber',
               'OrganizationName',
               'CurrentAlternativeLegalName',
               'Branch',
               'RegisteredBusinessName',
               'Address1',
               'Address2',
               'City',
               'State',
               'Zip',
               'Country',
               'CompanyGeoLatLong',
               'PhoneNumber',
               'FaxNumber',
               'CompanyURL',
               'DateOfCompanyRegistration',
               'DateOfStartingOperation',
               'TypeOfOwnership',
               'ContactPersonName',
               'ContactPersonEmail',
               'ContactPersonPhone',
               'ContactPersonName2',
               'ContactPersonEmail2',
               'ContactPersonPhone2',
               'CompanyBusinessDescription',
               'IndustryDescription',
               'IndustryCode',
               'BusinessClassification',
               'NumberOfEmployees',
               'Revenue',
               'Certifications',
               'CertificateOfInsurance',
               'W9',
               'F1099',
               'BankingStatements',
               'FinancialStatements',
               'ISOCertificates',
               'SOC1Type2',
               'IncorporationDocument',
               'JurisdictionCode',
               'NormalisedName',
               'CompanyType',
               'Nonprofit',
               'CurrentStatus',
               'DissolutionDate',
               'BusinessNumber',
               'CurrentAlternativeLegalNameLanguage',
               'HomeJurisdictiontext',
               'NativeCompanyNumber',
               'PreviousName',
               'AlternativeNames',
               'RetrievedAt',
               'RegistryUrl',
               'RestrictedForMarketing',
               'RegisteredAddressInFull',
               'PrimaryUserID',
               'CreatedDate',
               'CreatedBy',
               'CreatedByKey',
               'ModifiedDate',
               'ModifiedBy',
               'ModifiedByKey',
               'StreetAddress',
               'Address3',
               'SpendPurchaseAmount',
               'PriorityLevel',
               'ParentCompanyName',
               'ParentCompanyAddress',
               'ParentCompanyAddress1',
               'ParentCompanyAddress2',
               'ParentCompanyAddress3',
               'ParentCompanyCity',
               'ParentCompanyState',
               'ParentCompanyCountry',
               'ParentCompanyZip',
               'TIN',
               'VAT',
               'RegistrationNumber',
               'StreetAddress2',
               'City2',
               'State2',
               'Country2',
               'Zip2']
    data = []
    with open(u'./ashish_2000.csv', 'r', encoding="utf8") as csvfile:
        reader = csv.DictReader(csvfile, fieldnames=headers)
        count = 0
        for row in reader:

            if count >= 1:
                record = {"SUPPLIER_ID": 1111111,
                          "SUPPLIER_NAME": row['OrganizationName'] if row['OrganizationName'] else row['RegisteredBusinessName'],
                          "SUPPLIER_STREET_ADDRESS_1": row['Address1'] if row['Address1'] else row['StreetAddress'],
                          "SUPPLIER_CITY": row['City'],
                          "SUPPLIER_ZIP": row['Zip'],
                          "COUNTRY": row['Country'],
                          "CORRELATION_ID": row['CorrelationID'],
                          "SUPPLIER_STATE": row['State']}
                data.append([record, row['RawOrganizationKey']])
            count += 1
    return data

def blocking_func(data):
    response = requests.post(URL, data=json.dumps(data), headers={"Content-Type": "application/json"})
    result = response.json()
    return result

# async def blocking_func(n):
#     print("Start {}".format(n))
#     await asyncio.sleep(10)
#     return n**2


async def main(loop, executor):
    excel_data = []
    headers = ['RawOrganizationKey', 'SUPPLIER_NAME', 'ADDRESS', 'CITY', 'COUNTRY', 'Rank', 'MatchPercent']
    excel_data.append(headers)

    datas = get_1000_data()
    process_tasks = []
    for data, RawOrganizationKey in datas:
        log.debug(data)
        task = loop.run_in_executor(executor, blocking_func, data)
        process_tasks.append(task)
    results = await asyncio.gather(*process_tasks)
    for raw, result in zip(datas, results):
        data, RawOrganizationKey = raw
        for ret in sorted(result, key=lambda a: a.get('Rank')):
            instance_data = [RawOrganizationKey, data['SUPPLIER_NAME'], data['SUPPLIER_STREET_ADDRESS_1'], data['SUPPLIER_CITY'], data['COUNTRY'], ret['Rank'], ret['MatchPercent']]
            excel_data.append(instance_data)
    log.debug("Excel len: {}".format(len(excel_data)))
    with open(u'./result_20000.csv', 'w') as writeFile:
        writer = csv.writer(writeFile, delimiter=',', lineterminator='\n')
        writer.writerows(excel_data)

if __name__ == '__main__':
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
    loop = asyncio.get_event_loop()
    start_time = datetime.now()
    try:
        result = loop.run_until_complete(main(loop, executor))
        print(result)
    finally:
        loop.close()

    print("Run time: {}".format(datetime.now() - start_time))
