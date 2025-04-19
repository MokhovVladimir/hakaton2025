import re
import csv
from collections import defaultdict

CSV_FOLDER = "./csvs/"
MERGED = 'merged.csv'
RESULT = 'result.csv'
DELETED = 'deleted.csv'

#reg exp
status_pattern = r'(В эксплуатации|Планируется|Подготовка к эксплуатации|Выведен из эксплуатации|На обслуживании)?'
ci_code_pattern = r'[A-Za-z]{3}[ -]\d{8}'
hostname_pattern = r'$|^[A-Za-z]{3}\d-[A-Za-z]{3}-[A-Za-z]{3}'
dns_pattern = r'$|^[A-Za-z]{3}\d-[A-Za-z]{3}-[A-Za-z]{3}\.[A-Za-z]*\.[A-Za-z]*'
short_name_pattern = r'^.*$'
created_on_pattern = r'$|^.*'
updated_on_pattern = r'$|^.*'
name_pattern = r'[^|]*\|[^|]*'
id_pattren = r'[A-Za-z0-9]{8}-[A-Za-z0-9]{4}-[A-Za-z0-9]{4}-[A-Za-z0-9]{4}-[A-Za-z0-9]{12}'
type_pattern = r'^.*$'
serial_pattern = r'$|^[A-Za-z]*'
full_name_pattern = r'$|^.*'
description_pattern = r'$|^.*'
notes_pattern = r'$|^.*'
manufacturer_pattern = r'$|^.*'
model_pattern = r'$|^.*'
location_pattern = r'$|^.*'
ip_pattern = r'$|^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)'
cpu_cores_pattern = r'$|^\d+'
cpu_freq_pattern = r'$|^-?\d+\.\d+'
ram_pattern = r'^$|^\d+'
total_volume_pattern = r'^$|^\d+$'
category_pattern = r'^$|^\d+$'
user_org_pattern = r'^$|^.*$'
owner_org_pattern = r'^$|^.*$'
code_mon_pattern = r'^$|^.*$'
mount_pattern = r'^$|^(?:[Сс]тойка|[Мм]есто)\s*\d+$'


STATUS = 'status'
CI_CODE = 'ci_code'
HOSTNAME = 'hostname'
DNS = 'dns'
SHORT_NAME = 'short_name'
CREATED_ON = 'created_on'
UPDATED_ON = 'updated_on'
NAME = 'name'
ID = 'id'
TYPE = 'type'
SERIAL = 'serial'
FULL_NAME = 'full_name'
DESCRIPTION = 'description'
NOTES = 'notes'
MANUFACTURER = 'manufacturer'
MODEL = 'model'
LOCATION = 'location'
IP = 'ip'
CPU_CORES = 'cpu_cores'
CPU_FREQ = 'cpu_freq'
RAM = 'ram'
TOTAL_VOLUME = 'total_volume'
CATEGORY = 'category'
USER_ORG = 'user_org'
OWNER_ORG = 'owner_org'
CODE_MON = 'code_mon'
MOUNT = 'mount'


q = {
    STATUS: 1,
    CI_CODE: 5,
    HOSTNAME: 4,
    DNS: 4,
    SHORT_NAME: 1,
    CREATED_ON: 2,
    UPDATED_ON: 2,
    NAME: 1,
    ID: 7,
    TYPE: 3,
    SERIAL: 2,
    FULL_NAME: 1,
    DESCRIPTION: 0,
    NOTES: 1,
    MANUFACTURER: 3,
    MODEL: 0,
    LOCATION: 1,
    IP: 3,
    CPU_CORES: 2,
    CPU_FREQ: 2,
    RAM: 2,
    TOTAL_VOLUME: 2,
    CATEGORY: 2,
    USER_ORG: 3,
    OWNER_ORG: 3,
    CODE_MON: 1,
    MOUNT: 1
}


UNIQUE_CODES = [
    CI_CODE,
    DNS,
    HOSTNAME,
    ID
]

def all_regular_is_valid(row):
    return re.fullmatch(status_pattern, str(row[STATUS])) and \
        re.fullmatch(ci_code_pattern, str(row[CI_CODE])) and \
        re.fullmatch(hostname_pattern, str(row[HOSTNAME])) and \
        re.fullmatch(dns_pattern, str(row[DNS])) and \
        re.fullmatch(short_name_pattern, str(row[SHORT_NAME])) and \
        re.fullmatch(created_on_pattern, str(row[CREATED_ON])) and \
        re.fullmatch(updated_on_pattern, str(row[UPDATED_ON])) and \
        re.fullmatch(name_pattern, str(row[NAME])) and \
        re.fullmatch(id_pattren, str(row[ID])) and \
        re.fullmatch(type_pattern, str(row[TYPE])) and \
        re.fullmatch(serial_pattern, str(row[SERIAL])) and \
        re.fullmatch(full_name_pattern, str(row[FULL_NAME])) and \
        re.fullmatch(description_pattern, str(row[DESCRIPTION])) and \
        re.fullmatch(notes_pattern, str(row[NOTES])) and \
        re.fullmatch(manufacturer_pattern, str(row[MANUFACTURER])) and \
        re.fullmatch(model_pattern, str(row[MODEL])) and \
        re.fullmatch(location_pattern, str(row[LOCATION])) and \
        re.fullmatch(ip_pattern, str(row[IP])) and \
        re.fullmatch(cpu_cores_pattern, str(row[CPU_CORES])) and \
        re.fullmatch(cpu_freq_pattern, str(row[CPU_FREQ])) and \
        re.fullmatch(ram_pattern, str(row[RAM])) and \
        re.fullmatch(total_volume_pattern, str(row[TOTAL_VOLUME])) and \
        re.fullmatch(category_pattern, str(row[CATEGORY])) and \
        re.fullmatch(user_org_pattern, str(row[USER_ORG])) and \
        re.fullmatch(owner_org_pattern, str(row[OWNER_ORG])) and \
        re.fullmatch(code_mon_pattern, str(row[CODE_MON])) and \
        re.fullmatch(mount_pattern, str(row[MOUNT]))


def pick_best(rows):
        return_row = None
        max_reward = 0
        for row in rows:
            reward = get_reward(row)
            if (reward > max_reward):
                max_reward = reward
                return_row = row
        return return_row
    

def get_reward(row):
    reward = 0
    for i in range(len(fields_headers)):
        isEmpty = row[fields_headers[i]] == ""
        reward += q[fields_headers[i]] * isEmpty
    return reward


fields_headers = [ID, CREATED_ON, UPDATED_ON, NAME, CI_CODE, 
                  SHORT_NAME, FULL_NAME, DESCRIPTION, NOTES, STATUS, MANUFACTURER, 
                  SERIAL, MODEL, LOCATION, MOUNT, HOSTNAME, DNS, IP, CPU_CORES, CPU_FREQ, RAM, 
                  TOTAL_VOLUME, TYPE, CATEGORY, USER_ORG, OWNER_ORG, CODE_MON]


def do():
    csv_transformer.transform(fields_headers)
    seen = defaultdict(list)
    with open(MERGED, 'r', newline='') as merged:
        csvreader = csv.reader(merged, delimiter=',')
        for row in csvreader:
            row_dict = dict()
            for i in range(len(fields_headers)):
                row_dict[fields_headers[i]] = row[i]
            if all_regular_is_valid(row_dict):
                csv_transformer.add_to_result(etalon_headers=fields_headers, row=row)
            else:
                csv_transformer.add_to_deleted(etalon_headers=fields_headers, row=row)


    with open(RESULT, 'r', newline='') as result:
        csvreader = csv.reader(result, delimiter=',')
        for row in csvreader:
            row_dict = {fields_headers[i]: row[i] for i in range(len(fields_headers))}
            key = tuple(row_dict[k] for k in UNIQUE_CODES)
            seen[key].append(row_dict)


    for key, row_group in seen.items():
        best_row = pick_best(row_group)
        for row in row_group:
            if row != best_row:
                csv_transformer.add_to_deleted(etalon_headers=fields_headers, row=row)
