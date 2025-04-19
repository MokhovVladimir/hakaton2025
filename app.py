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

def import_to_elasticsearch(file_path: str):
    """Импортирует данные из CSV в Elasticsearch"""
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
                    
                    doc = {k: v for k, v in doc.items() if v not in (None, "")}
                    
                    if doc:
                        es.index(
                            index=ELASTICSEARCH_INDEX,
                            document=doc,
                            id=doc.get("id")  # Используем id как идентификатор документа
                        )
                        total_docs += 1
                        
                    if i % 100 == 0:
                        print(f"Обработано {i} строк | Добавлено {total_docs} документов")
                        
                except Exception as doc_error:
                    print(f"Ошибка в строке {i}: {doc_error}")
                    continue
            
            print(f"Импорт завершен. Всего строк: {i}, успешно добавлено: {total_docs}")
            
            if total_docs > 0:
                count = es.count(index=ELASTICSEARCH_INDEX)['count']
                print(f"Документов в индексе {ELASTICSEARCH_INDEX}: {count}")
            
            return True
            
    except Exception as e:
        print(f"Критическая ошибка импорта: {str(e)}")
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
    """Объединяет первые 500 строк из каждого CSV файла"""
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
                    nrows=500
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
        if not import_to_elasticsearch(merged_file):
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

@app.get("/view_elasticsearch", response_class=HTMLResponse)
async def view_elasticsearch(request: Request):
    try:
        # Запрос всех документов из индекса 'input_db'
        response = es.search(
            index="input_db",
            body={"query": {"match_all": {}}}
        )
        # Извлечение данных из ответа Elasticsearch
        records = [
            {
                "id": hit["_source"].get("id", "N/A"),
                "created_on": hit["_source"].get("created_on", "N/A"),
                "updated_on": hit["_source"].get("updated_on", "N/A"),
                "name": hit["_source"].get("name", "N/A"),
                "ci_code": hit["_source"].get("ci_code", "N/A"),
                "short_name": hit["_source"].get("short_name", "N/A"),
                "full_name": hit["_source"].get("full_name", "N/A"),
                "description": hit["_source"].get("description", "N/A"),
                "notes": hit["_source"].get("notes", "N/A"),
                "status": hit["_source"].get("status", "N/A"),
                "manufacturer": hit["_source"].get("manufacturer", "N/A"),
                "serial": hit["_source"].get("serial", "N/A"),
                "model": hit["_source"].get("model", "N/A"),
                "location": hit["_source"].get("location", "N/A"),
                "mount": hit["_source"].get("mount", "N/A"),
                "hostname": hit["_source"].get("hostname", "N/A"),
                "dns": hit["_source"].get("dns", "N/A"),
                "ip": hit["_source"].get("ip", "N/A"),
                "cpu_cores": hit["_source"].get("cpu_cores", "N/A"),
                "cpu_freq": hit["_source"].get("cpu_freq", "N/A"),
                "ram": hit["_source"].get("ram", "N/A"),
                "total_volume": hit["_source"].get("total_volume", "N/A"),
                "type": hit["_source"].get("type", "N/A"),
                "category": hit["_source"].get("category", "N/A"),
                "user_org": hit["_source"].get("user_org", "N/A"),
                "owner_org": hit["_source"].get("owner_org", "N/A"),
                "code_mon": hit["_source"].get("code_mon", "N/A")
            }
            for hit in response["hits"]["hits"]
        ]
        return templates.TemplateResponse(
            "view_elasticsearch.html",
            {"request": request, "records": records}
        )
    except Exception as e:
        error_message = f"Ошибка при получении данных из Elasticsearch: {str(e)}"
        return templates.TemplateResponse(
            "view_elasticsearch.html",
            {"request": request, "error_message": error_message}
        )

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)