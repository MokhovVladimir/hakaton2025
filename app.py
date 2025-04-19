from fastapi import FastAPI, Request, File, UploadFile, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import os
import uvicorn
import csv
from datetime import datetime
import glob
from elasticsearch import Elasticsearch
from pathlib import Path
import pandas as pd
import re
from typing import Optional

app = FastAPI()

# Конфигурация
UPLOAD_FOLDER = "db"
ELASTICSEARCH_HOST = "http://elasticsearch:9200"
ELASTICSEARCH_INDEX = "input_db"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
CSV_FOLDER = "./db/"
MERGED = 'input.csv'
RESULT = 'result.csv'
DELETED = 'deleted.csv'


# Регулярные выражения для валидации
status_pattern = r'(В эксплуатации|Планируется|Подготовка к эксплуатации|Выведен из эксплуатации|На обслуживании)?'
ci_code_pattern = r'[A-Za-z]{3}[ -]\d{8}'
hostname_pattern = r'$|^[A-Za-z]{3}\d-[A-Za-z]{3}-[A-Za-z]{3}'
dns_pattern = r'$|^[A-Za-z]{3}\d-[A-Za-z]{3}-[A-Za-z]{3}\.[A-Za-z]\.[A-Za-z]*'
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

# Константы для имен полей
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

# Инициализация Elasticsearch с таймаутами
es = Elasticsearch([ELASTICSEARCH_HOST])

app.mount("/static", StaticFiles(directory="frontend/static"), name="static")
templates = Jinja2Templates(directory="frontend/templates")

def create_elastic_index():
    """Создает индекс в Elasticsearch с нужной структурой"""
    if es.indices.exists(index=ELASTICSEARCH_INDEX):
        es.indices.delete(index=ELASTICSEARCH_INDEX)
    
    mapping = {
        "mappings": {
            "properties": {
                "id": {"type": "integer"},
                "created_on": {"type": "date"},
                "updated_on": {"type": "date"},
                "name": {"type": "text"},
                "ci_code": {"type": "keyword"},
                "short_name": {"type": "text"},
                "full_name": {"type": "text"},
                "description": {"type": "text"},
                "notes": {"type": "text"},
                "status": {"type": "keyword"},
                "manufacturer": {"type": "keyword"},
                "serial": {"type": "keyword"},
                "model": {"type": "keyword"},
                "location": {"type": "keyword"},
                "mount": {"type": "keyword"},
                "hostname": {"type": "keyword"},
                "dns": {"type": "keyword"},
                "ip": {"type": "ip"},
                "cpu_cores": {"type": "integer"},
                "cpu_freq": {"type": "float"},
                "ram": {"type": "integer"},
                "total_volume": {"type": "integer"},
                "type": {"type": "keyword"},
                "category": {"type": "keyword"},
                "user_org": {"type": "keyword"},
                "owner_org": {"type": "keyword"},
                "code_mon": {"type": "keyword"}
            }
        }
    }
    es.indices.create(index=ELASTICSEARCH_INDEX, body=mapping)

def parse_date(date_str):
    """Преобразует строку даты в объект datetime"""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    except:
        return None

def safe_int_conversion(value):
    """Безопасное преобразование в целое число"""
    try:
        return int(value) if value else None
    except (ValueError, TypeError):
        return None

def safe_float_conversion(value):
    """Безопасное преобразование в число с плавающей точкой"""
    try:
        return float(value) if value else None
    except (ValueError, TypeError):
        return None

def import_to_elasticsearch(file_path: str, index_name: str):
    """Обновленная функция импорта с поддержкой разных индексов"""
    if not os.path.exists(file_path):
        print(f"Ошибка: файл {file_path} не найден!")
        return False

    try:
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            total_docs = 0
            
            for i, row in enumerate(reader, 1):
                try:
                    doc = {
                        "id": safe_int_conversion(row.get("id")),
                        "created_on": parse_date(row.get("created_on")),
                        "updated_on": parse_date(row.get("updated_on")),
                        "name": row.get("name", "").strip(),
                        "ci_code": row.get("ci_code", "").strip(),
                        "short_name": row.get("short_name", "").strip(),
                        "full_name": row.get("full_name", "").strip(),
                        "description": row.get("description", "").strip(),
                        "notes": row.get("notes", "").strip(),
                        "status": row.get("status", "").strip(),
                        "manufacturer": row.get("manufacturer", "").strip(),
                        "serial": row.get("serial", "").strip(),
                        "model": row.get("model", "").strip(),
                        "location": row.get("location", "").strip(),
                        "mount": row.get("mount", "").strip(),
                        "hostname": row.get("hostname", "").strip(),
                        "dns": row.get("dns", "").strip(),
                        "ip": row.get("ip", "").strip(),
                        "cpu_cores": safe_int_conversion(row.get("cpu_cores")),
                        "cpu_freq": safe_float_conversion(row.get("cpu_freq")),
                        "ram": safe_int_conversion(row.get("ram")),
                        "total_volume": safe_int_conversion(row.get("total_volume")),
                        "type": row.get("type", "").strip(),
                        "category": row.get("category", "").strip(),
                        "user_org": row.get("user_org", "").strip(),
                        "owner_org": row.get("owner_org", "").strip(),
                        "code_mon": row.get("code_mon", "").strip()
                    }
                    
                    # Для невалидных записей добавляем информацию об ошибках валидации
                    if index_name == "deleted_db":
                        doc["validation_errors"] = get_validation_errors(row)
                    
                    doc = {k: v for k, v in doc.items() if v not in (None, "")}
                    
                    if doc:
                        es.index(
                            index=index_name,
                            document=doc,
                            id=doc.get("id")  # Используем id как идентификатор документа
                        )
                        total_docs += 1
                        
                    if i % 100 == 0:
                        print(f"Обработано {i} строк | Добавлено {total_docs} документов в {index_name}")
                        
                except Exception as doc_error:
                    print(f"Ошибка в строке {i}: {doc_error}")
                    continue
            
            print(f"Импорт в {index_name} завершен. Всего строк: {i}, успешно добавлено: {total_docs}")
            
            if total_docs > 0:
                count = es.count(index=index_name)['count']
                print(f"Документов в индексе {index_name}: {count}")
            
            return True
            
    except Exception as e:
        print(f"Критическая ошибка импорта в {index_name}: {str(e)}")
        return False

def get_etalon_headers():
    """Получает заголовки из fields.csv"""
    fields_path = os.path.join(CSV_FOLDER, 'fields.csv')
    if not os.path.exists(fields_path):
        raise FileNotFoundError("Файл fields.csv не найден в папке db")
    
    with open(fields_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        return next(reader)

def merge_csv():
    """Объединяет первые 1000 строк из каждого CSV файла"""
    try:
        etalon_headers = get_etalon_headers()
        csv_files = [f for f in os.listdir(CSV_FOLDER) 
                    if f.endswith('.csv') and f != 'fields.csv']
        
        if not csv_files:
            print("Нет CSV файлов для обработки")
            return None
        
        dfs = []
        for filename in csv_files:
            file_path = os.path.join(CSV_FOLDER, filename)
            try:
                df = pd.read_csv(
                    file_path,
                    encoding='utf-8',
                    sep=',',
                    nrows=1000
                )
                
                missing_cols = set(etalon_headers) - set(df.columns)
                if missing_cols:
                    print(f"В файле {filename} отсутствуют колонки: {missing_cols}")
                
                df = df[etalon_headers]
                dfs.append(df)
                print(f"Обработан файл {filename} (строк: {len(df)})")
                
            except Exception as file_error:
                print(f"Ошибка при обработке файла {filename}: {str(file_error)}")
                continue
        
        if not dfs:
            print("Нет данных для объединения")
            return None
        
        result = pd.concat(dfs, ignore_index=True)
        merged_path = os.path.join(CSV_FOLDER, MERGED)
        result.to_csv(merged_path, index=False, encoding='utf-8', sep=',')
        print(f"Объединенный файл сохранен: {merged_path} (строк: {len(result)})")
        
        return merged_path
        
    except Exception as e:
        print(f"Критическая ошибка при объединении файлов: {str(e)}")
        return None

def add_to_result(row):
    """Добавляет строку в result.csv"""
    try:
        etalon_headers = get_etalon_headers()
        result_path = os.path.join(CSV_FOLDER, RESULT)
        
        if not os.path.exists(result_path) or os.stat(result_path).st_size == 0:
            with open(result_path, 'a', encoding='utf-8', newline='') as of:
                writer = csv.writer(of)
                writer.writerow(etalon_headers)
        
        with open(result_path, 'a', encoding='utf-8', newline='') as of:
            writer = csv.writer(of)
            writer.writerow(row)
            
        return True
    except Exception as e:
        print(f"Ошибка при добавлении в result.csv: {str(e)}")
        return False

def add_to_deleted(row):
    """Добавляет строку в deleted.csv"""
    try:
        etalon_headers = get_etalon_headers()
        deleted_path = os.path.join(CSV_FOLDER, DELETED)
        
        if not os.path.exists(deleted_path) or os.stat(deleted_path).st_size == 0:
            with open(deleted_path, 'a', encoding='utf-8', newline='') as of:
                writer = csv.writer(of)
                writer.writerow(etalon_headers)
        
        with open(deleted_path, 'a', encoding='utf-8', newline='') as of:
            writer = csv.writer(of)
            writer.writerow(row)
            
        return True
    except Exception as e:
        print(f"Ошибка при добавлении в deleted.csv: {str(e)}")
        return False

def validate_csv(input_path: str, result_path: str, deleted_path: str):
    """Проверяет input.csv и разделяет данные на valid (result.csv) и invalid (deleted.csv)"""
    try:
        etalon_headers = get_etalon_headers()
        
        with open(input_path, 'r', encoding='utf-8') as input_file, \
             open(result_path, 'w', encoding='utf-8', newline='') as result_file, \
             open(deleted_path, 'w', encoding='utf-8', newline='') as deleted_file:
            
            reader = csv.DictReader(input_file)
            result_writer = csv.DictWriter(result_file, fieldnames=etalon_headers)
            deleted_writer = csv.DictWriter(deleted_file, fieldnames=etalon_headers)
            
            result_writer.writeheader()
            deleted_writer.writeheader()
            
            valid_count = 0
            invalid_count = 0
            
            for row in reader:
                if all_regular_is_valid(row):
                    result_writer.writerow(row)
                    valid_count += 1
                else:
                    deleted_writer.writerow(row)
                    invalid_count += 1
            
            return {
                "status": "success",
                "valid_count": valid_count,
                "invalid_count": invalid_count
            }
            
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }

def all_regular_is_valid(row: dict) -> bool:
    """Проверяет строку на соответствие всем регулярным выражениям"""
    try:
        checks = [
            re.fullmatch(status_pattern, str(row.get(STATUS, ''))),
            re.fullmatch(ci_code_pattern, str(row.get(CI_CODE, ''))),
            re.fullmatch(hostname_pattern, str(row.get(HOSTNAME, ''))),
            re.fullmatch(dns_pattern, str(row.get(DNS, ''))),
            re.fullmatch(short_name_pattern, str(row.get(SHORT_NAME, ''))),
            re.fullmatch(created_on_pattern, str(row.get(CREATED_ON, ''))),
            re.fullmatch(updated_on_pattern, str(row.get(UPDATED_ON, ''))),
            re.fullmatch(name_pattern, str(row.get(NAME, ''))),
            re.fullmatch(id_pattren, str(row.get(ID, ''))),
            re.fullmatch(type_pattern, str(row.get(TYPE, ''))),
            re.fullmatch(serial_pattern, str(row.get(SERIAL, ''))),
            re.fullmatch(full_name_pattern, str(row.get(FULL_NAME, ''))),
            re.fullmatch(description_pattern, str(row.get(DESCRIPTION, ''))),
            re.fullmatch(notes_pattern, str(row.get(NOTES, ''))),
            re.fullmatch(manufacturer_pattern, str(row.get(MANUFACTURER, ''))),
            re.fullmatch(model_pattern, str(row.get(MODEL, ''))),
            re.fullmatch(location_pattern, str(row.get(LOCATION, ''))),
            re.fullmatch(ip_pattern, str(row.get(IP, ''))),
            re.fullmatch(cpu_cores_pattern, str(row.get(CPU_CORES, ''))),
            re.fullmatch(cpu_freq_pattern, str(row.get(CPU_FREQ, ''))),
            re.fullmatch(ram_pattern, str(row.get(RAM, ''))),
            re.fullmatch(total_volume_pattern, str(row.get(TOTAL_VOLUME, ''))),
            re.fullmatch(category_pattern, str(row.get(CATEGORY, ''))),
            re.fullmatch(user_org_pattern, str(row.get(USER_ORG, ''))),
            re.fullmatch(owner_org_pattern, str(row.get(OWNER_ORG, ''))),
            re.fullmatch(code_mon_pattern, str(row.get(CODE_MON, ''))),
            re.fullmatch(mount_pattern, str(row.get(MOUNT, '')))
        ]
        return all(checks)
    except Exception as e:
        print(f"Ошибка валидации строки: {e}")
        return False

def create_result_index():
    """Создает индекс result_db в Elasticsearch для валидных данных"""
    if es.indices.exists(index="result_db"):
        es.indices.delete(index="result_db")
    
    mapping = {
        "mappings": {
            "properties": {
                "id": {"type": "integer"},
                "created_on": {"type": "date"},
                "updated_on": {"type": "date"},
                "name": {"type": "text"},
                "ci_code": {"type": "keyword"},
                "short_name": {"type": "text"},
                "full_name": {"type": "text"},
                "description": {"type": "text"},
                "notes": {"type": "text"},
                "status": {"type": "keyword"},
                "manufacturer": {"type": "keyword"},
                "serial": {"type": "keyword"},
                "model": {"type": "keyword"},
                "location": {"type": "keyword"},
                "mount": {"type": "keyword"},
                "hostname": {"type": "keyword"},
                "dns": {"type": "keyword"},
                "ip": {"type": "ip"},
                "cpu_cores": {"type": "integer"},
                "cpu_freq": {"type": "float"},
                "ram": {"type": "integer"},
                "total_volume": {"type": "integer"},
                "type": {"type": "keyword"},
                "category": {"type": "keyword"},
                "user_org": {"type": "keyword"},
                "owner_org": {"type": "keyword"},
                "code_mon": {"type": "keyword"}
            }
        }
    }
    es.indices.create(index="result_db", body=mapping)

def create_deleted_index():
    """Создает индекс deleted_db в Elasticsearch для невалидных данных"""
    if es.indices.exists(index="deleted_db"):
        es.indices.delete(index="deleted_db")
    
    mapping = {
        "mappings": {
            "properties": {
                "id": {"type": "integer"},
                "created_on": {"type": "date"},
                "updated_on": {"type": "date"},
                "name": {"type": "text"},
                "ci_code": {"type": "keyword"},
                "short_name": {"type": "text"},
                "full_name": {"type": "text"},
                "description": {"type": "text"},
                "notes": {"type": "text"},
                "status": {"type": "keyword"},
                "manufacturer": {"type": "keyword"},
                "serial": {"type": "keyword"},
                "model": {"type": "keyword"},
                "location": {"type": "keyword"},
                "mount": {"type": "keyword"},
                "hostname": {"type": "keyword"},
                "dns": {"type": "keyword"},
                "ip": {"type": "ip"},
                "cpu_cores": {"type": "integer"},
                "cpu_freq": {"type": "float"},
                "ram": {"type": "integer"},
                "total_volume": {"type": "integer"},
                "type": {"type": "keyword"},
                "category": {"type": "keyword"},
                "user_org": {"type": "keyword"},
                "owner_org": {"type": "keyword"},
                "code_mon": {"type": "keyword"},
                "validation_errors": {"type": "text"}  # Дополнительное поле для ошибок валидации
            }
        }
    }
    es.indices.create(index="deleted_db", body=mapping)

def get_validation_errors(row: dict) -> str:
    """Возвращает строку с описанием ошибок валидации для невалидных записей"""
    errors = []
    
    if not re.fullmatch(status_pattern, str(row.get(STATUS, ''))):
        errors.append("Неверный формат статуса")
    
    if not re.fullmatch(ci_code_pattern, str(row.get(CI_CODE, ''))):
        errors.append("Неверный формат CI кода")
    

@app.post("/validate_csv")
async def handle_validate_csv(request: Request):
    try:
        input_path = os.path.join(CSV_FOLDER, MERGED)
        result_path = os.path.join(CSV_FOLDER, RESULT)
        deleted_path = os.path.join(CSV_FOLDER, DELETED)
        
        if not os.path.exists(input_path):
            return templates.TemplateResponse(
                "index.html",
                {
                    "request": request,
                    "error": "Файл input.csv не найден",
                    "show_alert": True
                }
            )
        
        # Создаем индексы перед валидацией
        create_result_index()
        create_deleted_index()
        
        validation_result = validate_csv(input_path, result_path, deleted_path)
        
        if validation_result["status"] == "success":
            # Импортируем валидные данные в result_db
            import_to_elasticsearch(result_path, index_name="result_db")
            
            # Импортируем невалидные данные в deleted_db
            import_to_elasticsearch(deleted_path, index_name="deleted_db")
            
            return templates.TemplateResponse(
                "index.html",
                {
                    "request": request,
                    "success": f"Обработано записей: {validation_result['valid_count']} валидных, {validation_result['invalid_count']} невалидных",
                    "show_alert": True
                }
            )
        else:
            return templates.TemplateResponse(
                "index.html",
                {
                    "request": request,
                    "error": f"Ошибка валидации: {validation_result['message']}",
                    "show_alert": True
                }
            )
            
    except Exception as e:
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "error": f"Ошибка: {str(e)}",
                "show_alert": True
            }
        )


@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/upload", response_class=HTMLResponse)
async def read_upload(request: Request, success: str = None, error: str = None):
    return templates.TemplateResponse("upload.html", {
        "request": request,
        "success": success,
        "error": error
    })

@app.post("/upload_files")
async def upload_files(request: Request, files: list[UploadFile] = File(...)):
    try:
        uploaded_files = []
        for file in files:
            file_path = os.path.join(CSV_FOLDER, file.filename)
            with open(file_path, "wb") as buffer:
                buffer.write(await file.read())
            uploaded_files.append(file.filename)
        
        return RedirectResponse(
            f"/upload?success=Файлы+{',+'.join(uploaded_files)}+успешно+загружены",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            f"/upload?error=Ошибка+загрузки:+{str(e).replace(' ', '+')}",
            status_code=303
        )

@app.post("/merge_files")
async def merge_files(request: Request):
    try:
        # Проверяем подключение к Elasticsearch
        if not es.ping():
            raise ConnectionError("Не удалось подключиться к Elasticsearch")
        
        # Объединяем файлы
        merged_file = merge_csv()
        if not merged_file:
            return RedirectResponse(
                "/upload?error=Нет+CSV+файлов+для+объединения",
                status_code=303
            )
        
        # Создаем индекс и импортируем данные
        create_elastic_index()
        if not import_to_elasticsearch(merged_file, ELASTICSEARCH_INDEX):
            raise Exception("Ошибка при импорте данных в Elasticsearch")
        
        return RedirectResponse(
            f"/upload?success=Файлы+объединены+и+загружены+в+Elasticsearch",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            f"/upload?error=Ошибка:+{str(e).replace(' ', '+')}",
            status_code=303
        )

@app.get("/view_elasticsearch-original", response_class=HTMLResponse)
async def view_elasticsearch(
    request: Request,
    query: Optional[str] = None,  # Полнотекстовый запрос
    page: int = 1,  # Номер страницы
    size: int = 50  # Количество документов на странице
):
    try:
        # Расчет смещения (offset) для пагинации
        from_ = (page - 1) * size

        # Формируем запрос к Elasticsearch
        if query:
            search_query = {
                "from": from_,
                "size": size,
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": [
                            "name",
                            "ci_code",
                            "short_name",
                            "full_name",
                            "description",
                            "notes",
                            "manufacturer",
                            "serial",
                            "model",
                            "location",
                            "mount",
                            "hostname",
                            "dns",
                            "type",
                            "category",
                            "user_org",
                            "owner_org",
                            "code_mon"
                        ]
                    }
                }
            }
        else:
            search_query = {
                "from": from_,
                "size": size,
                "query": {"match_all": {}}
            }

        # Выполняем поиск в индексе 'input_db'
        response = es.search(index=ELASTICSEARCH_INDEX, body=search_query)

        # Извлечение данных из ответа Elasticsearch
        records = [
            {
                "_id": hit["_id"],  # ID документа в Elasticsearch
                "id": hit["_source"].get("id", "N/A"),
                "name": hit["_source"].get("name", "N/A"),
                "ci_code": hit["_source"].get("ci_code", "N/A"),
                "short_name": hit["_source"].get("short_name", "N/A"),
                "full_name": hit["_source"].get("full_name", "N/A"),
                "manufacturer": hit["_source"].get("manufacturer", "N/A"),
                "serial": hit["_source"].get("serial", "N/A"),
                "location": hit["_source"].get("location", "N/A"),
                "mount": hit["_source"].get("mount", "N/A"),
                "hostname": hit["_source"].get("hostname", "N/A"),
                "dns": hit["_source"].get("dns", "N/A"),
                "ip": hit["_source"].get("ip", "N/A"),
                "type": hit["_source"].get("type", "N/A"),
                "category": hit["_source"].get("category", "N/A"),
                "user_org": hit["_source"].get("user_org", "N/A"),
                "code_mon": hit["_source"].get("code_mon", "N/A")
            }
            for hit in response["hits"]["hits"]
        ]

        # Подготовка данных для пагинации
        total_hits = response["hits"]["total"]["value"]  # Общее количество документов
        total_pages = (total_hits + size - 1) // size  # Общее количество страниц

        return templates.TemplateResponse(
            "view_elasticsearch.html",
            {
                "request": request,
                "records": records,
                "page": page,
                "size": size,
                "total_pages": total_pages,
                "total_hits": total_hits,
                "query": query  # Передаем текущий запрос обратно в шаблон
            }
        )
    except Exception as e:
        error_message = f"Ошибка при получении данных из Elasticsearch: {str(e)}"
        return templates.TemplateResponse(
            "view_elasticsearch.html",
            {"request": request, "error_message": error_message}
        )

@app.post("/validate_csv")
async def handle_validate_csv(request: Request):
    try:
        input_path = os.path.join(CSV_FOLDER, MERGED)
        result_path = os.path.join(CSV_FOLDER, RESULT)
        deleted_path = os.path.join(CSV_FOLDER, DELETED)
        
        if not os.path.exists(input_path):
            return templates.TemplateResponse(
                "index.html",
                {
                    "request": request,
                    "error": "Файл input.csv не найден",
                    "show_alert": True
                }
            )
        
        # Создаем индексы перед валидацией
        create_result_index()
        create_deleted_index()
        
        validation_result = validate_csv(input_path, result_path, deleted_path)
        
        if validation_result["status"] == "success":
            # Импортируем валидные данные в result_db
            import_to_elasticsearch(result_path, index_name="result_db")
            
            # Импортируем невалидные данные в deleted_db
            import_to_elasticsearch(deleted_path, index_name="deleted_db")
            
            return templates.TemplateResponse(
                "index.html",
                {
                    "request": request,
                    "success": f"Обработано записей: {validation_result['valid_count']} валидных, {validation_result['invalid_count']} невалидных",
                    "show_alert": True
                }
            )
        else:
            return templates.TemplateResponse(
                "index.html",
                {
                    "request": request,
                    "error": f"Ошибка валидации: {validation_result['message']}",
                    "show_alert": True
                }
            )
            
    except Exception as e:
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "error": f"Ошибка: {str(e)}",
                "show_alert": True
            }
        )

@app.get("/view_elasticsearch-result", response_class=HTMLResponse)
async def view_elasticsearch_result(
    request: Request,
    query: Optional[str] = None,  # Полнотекстовый запрос
    page: int = 1,  # Номер страницы
    size: int = 50  # Количество документов на странице
):
    try:
        # Расчет смещения (offset) для пагинации
        from_ = (page - 1) * size

        # Формируем запрос к Elasticsearch
        if query:
            search_query = {
                "from": from_,
                "size": size,
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": [
                            "name",
                            "ci_code",
                            "short_name",
                            "full_name",
                            "description",
                            "notes",
                            "manufacturer",
                            "serial",
                            "model",
                            "location",
                            "mount",
                            "hostname",
                            "dns",
                            "type",
                            "category",
                            "user_org",
                            "owner_org",
                            "code_mon"
                        ]
                    }
                }
            }
        else:
            search_query = {
                "from": from_,
                "size": size,
                "query": {"match_all": {}}
            }

        # Выполняем поиск в индексе 'result_db'
        response = es.search(index="result_db", body=search_query)

        # Извлечение данных из ответа Elasticsearch
        records = [
            {
                "_id": hit["_id"],  # ID документа в Elasticsearch
                "id": hit["_source"].get("id", "N/A"),
                "name": hit["_source"].get("name", "N/A"),
                "ci_code": hit["_source"].get("ci_code", "N/A"),
                "short_name": hit["_source"].get("short_name", "N/A"),
                "full_name": hit["_source"].get("full_name", "N/A"),
                "manufacturer": hit["_source"].get("manufacturer", "N/A"),
                "serial": hit["_source"].get("serial", "N/A"),
                "location": hit["_source"].get("location", "N/A"),
                "mount": hit["_source"].get("mount", "N/A"),
                "hostname": hit["_source"].get("hostname", "N/A"),
                "dns": hit["_source"].get("dns", "N/A"),
                "ip": hit["_source"].get("ip", "N/A"),
                "type": hit["_source"].get("type", "N/A"),
                "category": hit["_source"].get("category", "N/A"),
                "user_org": hit["_source"].get("user_org", "N/A"),
                "code_mon": hit["_source"].get("code_mon", "N/A")
            }
            for hit in response["hits"]["hits"]
        ]

        # Подготовка данных для пагинации
        total_hits = response["hits"]["total"]["value"]  # Общее количество документов
        total_pages = (total_hits + size - 1) // size  # Общее количество страниц

        return templates.TemplateResponse(
            "view_elasticsearch.html",
            {
                "request": request,
                "records": records,
                "page": page,
                "size": size,
                "total_pages": total_pages,
                "total_hits": total_hits,
                "query": query  # Передаем текущий запрос обратно в шаблон
            }
        )
    except Exception as e:
        error_message = f"Ошибка при получении данных из Elasticsearch: {str(e)}"
        return templates.TemplateResponse(
            "view_elasticsearch.html",
            {"request": request, "error_message": error_message}
        )



@app.get("/view_elasticsearch-delete", response_class=HTMLResponse)
async def view_elasticsearch_delete(
    request: Request,
    query: Optional[str] = None,  # Полнотекстовый запрос
    page: int = 1,  # Номер страницы
    size: int = 50  # Количество документов на странице
):
    try:
        # Расчет смещения (offset) для пагинации
        from_ = (page - 1) * size

        # Формируем запрос к Elasticsearch
        if query:
            search_query = {
                "from": from_,
                "size": size,
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": [
                            "name",
                            "ci_code",
                            "short_name",
                            "full_name",
                            "description",
                            "notes",
                            "manufacturer",
                            "serial",
                            "model",
                            "location",
                            "mount",
                            "hostname",
                            "dns",
                            "type",
                            "category",
                            "user_org",
                            "owner_org",
                            "code_mon"
                        ]
                    }
                }
            }
        else:
            search_query = {
                "from": from_,
                "size": size,
                "query": {"match_all": {}}
            }

        # Выполняем поиск в индексе 'deleted_db'
        response = es.search(index="deleted_db", body=search_query)

        # Извлечение данных из ответа Elasticsearch
        records = [
            {
                "_id": hit["_id"],  # ID документа в Elasticsearch
                "id": hit["_source"].get("id", "N/A"),
                "name": hit["_source"].get("name", "N/A"),
                "ci_code": hit["_source"].get("ci_code", "N/A"),
                "short_name": hit["_source"].get("short_name", "N/A"),
                "full_name": hit["_source"].get("full_name", "N/A"),
                "manufacturer": hit["_source"].get("manufacturer", "N/A"),
                "serial": hit["_source"].get("serial", "N/A"),
                "location": hit["_source"].get("location", "N/A"),
                "mount": hit["_source"].get("mount", "N/A"),
                "hostname": hit["_source"].get("hostname", "N/A"),
                "dns": hit["_source"].get("dns", "N/A"),
                "ip": hit["_source"].get("ip", "N/A"),
                "type": hit["_source"].get("type", "N/A"),
                "category": hit["_source"].get("category", "N/A"),
                "user_org": hit["_source"].get("user_org", "N/A"),
                "code_mon": hit["_source"].get("code_mon", "N/A")
            }
            for hit in response["hits"]["hits"]
        ]

        # Подготовка данных для пагинации
        total_hits = response["hits"]["total"]["value"]  # Общее количество документов
        total_pages = (total_hits + size - 1) // size  # Общее количество страниц

        return templates.TemplateResponse(
            "view_elasticsearch.html",
            {
                "request": request,
                "records": records,
                "page": page,
                "size": size,
                "total_pages": total_pages,
                "total_hits": total_hits,
                "query": query  # Передаем текущий запрос обратно в шаблон
            }
        )
    except Exception as e:
        error_message = f"Ошибка при получении данных из Elasticsearch: {str(e)}"
        return templates.TemplateResponse(
            "view_elasticsearch.html",
            {"request": request, "error_message": error_message}
        )



if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)