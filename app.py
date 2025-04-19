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
import csv
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
    """Полная версия импорта данных с обработкой всех полей"""
    if not os.path.exists(file_path):
        print(f"Ошибка: файл {file_path} не найден!")
        return False

    try:
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            total_docs = 0
            
            for i, row in enumerate(reader, 1):
                try:
                    # Полное преобразование всех полей
                    doc = {
                        # Числовые поля
                        "id": int(row["id"]) if row.get("id") and row["id"].strip().isdigit() else None,
                        "cpu_cores": int(row["cpu_cores"]) if row.get("cpu_cores") and row["cpu_cores"].strip().isdigit() else None,
                        "ram": int(row["ram"]) if row.get("ram") and row["ram"].strip().isdigit() else None,
                        "total_volume": int(row["total_volume"]) if row.get("total_volume") and row["total_volume"].strip().isdigit() else None,
                        
                        # Числа с плавающей точкой
                        "cpu_freq": float(row["cpu_freq"]) if row.get("cpu_freq") and row["cpu_freq"].strip().replace('.','',1).isdigit() else None,
                        
                        # Даты
                        "created_on": parse_date(row.get("created_on")),
                        "updated_on": parse_date(row.get("updated_on")),
                        
                        # Строковые поля
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
                        "type": row.get("type", "").strip(),
                        "category": row.get("category", "").strip(),
                        "user_org": row.get("user_org", "").strip(),
                        "owner_org": row.get("owner_org", "").strip(),
                        "code_mon": row.get("code_mon", "").strip()
                    }
                    
                    # Удаляем None-значения и пустые строки
                    doc = {k: v for k, v in doc.items() if v not in (None, "")}
                    
                    # Индексация с проверкой
                    if doc:  # Только если есть данные
                        es.index(
                            index=ELASTICSEARCH_INDEX,
                            document=doc,
                            id=doc.get("id")  # Используем id как идентификатор документа
                        )
                        total_docs += 1
                    
                    # Логирование прогресса
                    if i % 100 == 0:
                        print(f"Обработано {i} строк | Добавлено {total_docs} документов")
                        print("Пример документа:", {k: v for k, v in list(doc.items())[:3]})
                
                except Exception as doc_error:
                    print(f"Ошибка в строке {i}: {doc_error}\nПроблемная строка: {row}")
                    continue
            
            # Финальная проверка
            print(f"Импорт завершен. Всего строк: {i}, успешно добавлено: {total_docs}")
            
            # Проверяем количество документов в индексе
            if total_docs > 0:
                count = es.count(index=ELASTICSEARCH_INDEX)['count']
                print(f"Документов в индексе {ELASTICSEARCH_INDEX}: {count}")
                if count != total_docs:
                    print("Предупреждение: количество добавленных документов не совпадает с ожидаемым!")
            
            return True
            
    except Exception as e:
        print(f"Критическая ошибка импорта: {str(e)}", exc_info=True)
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
    """Объединяет первые 500 строк из каждого CSV файла в папке db"""
    try:
        etalon_headers = get_etalon_headers()
        csv_files = [f for f in os.listdir(CSV_FOLDER) 
                    if f.endswith('.csv') and f != 'fields.csv']
        
        if not csv_files:
            print("Нет CSV файлов для обработки")
            return None
        
        # Используем pandas для объединения с ограничением строк
        dfs = []
        for filename in csv_files:
            file_path = os.path.join(CSV_FOLDER, filename)
            try:
                # Читаем только первые 500 строк каждого файла
                df = pd.read_csv(
                    file_path,
                    encoding='utf-8',
                    sep=',',
                    nrows=500  # Ограничение количества строк
                )
                
                # Проверяем и выравниваем заголовки
                missing_cols = set(etalon_headers) - set(df.columns)
                extra_cols = set(df.columns) - set(etalon_headers)
                
                if missing_cols:
                    print(f"В файле {filename} отсутствуют колонки: {missing_cols}")
                if extra_cols:
                    print(f"В файле {filename} есть лишние колонки: {extra_cols}")
                
                # Оставляем только нужные колонки в правильном порядке
                df = df[etalon_headers]
                dfs.append(df)
                
                print(f"Обработан файл {filename} (строк: {len(df)})")
                
            except Exception as file_error:
                print(f"Ошибка при обработке файла {filename}: {str(file_error)}")
                continue
        
        if not dfs:
            print("Нет данных для объединения")
            return None
        
        # Объединяем и сохраняем
        result = pd.concat(dfs, ignore_index=True)
        merged_path = os.path.join(CSV_FOLDER, MERGED)
        
        # Сохраняем с проверкой количества строк
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
        
        # Если файл не существует или пуст, добавляем заголовки
        if not os.path.exists(result_path) or os.stat(result_path).st_size == 0:
            with open(result_path, 'a', encoding='utf-8', newline='') as of:
                writer = csv.writer(of)
                writer.writerow(etalon_headers)
        
        # Добавляем строку
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
        
        # Если файл не существует или пуст, добавляем заголовки
        if not os.path.exists(deleted_path) or os.stat(deleted_path).st_size == 0:
            with open(deleted_path, 'a', encoding='utf-8', newline='') as of:
                writer = csv.writer(of)
                writer.writerow(etalon_headers)
        
        # Добавляем строку
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

# Поиск данных в Elasticsearch
@app.get("/view_elasticsearch", response_class=HTMLResponse)
async def view_elasticsearch(request: Request):
    try:
        # Запрос всех документов из индекса 'uploaded_files'
        response = es.search(
            index="uploaded_files",
            body={"query": {"match_all": {}}}
        )
        # Извлечение данных из ответа Elasticsearch
        records = [
            {
                "id": hit["_id"],
                "filename": hit["_source"].get("filename", "N/A"),
                "content": hit["_source"].get("content", "N/A"),
                "timestamp": hit["_source"].get("timestamp", "N/A")
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
        merged_file = merge_csv()
        if not merged_file:
            return RedirectResponse(
                "/upload?error=Нет+CSV+файлов+для+объединения",
                status_code=303
            )
        
        return RedirectResponse(
            f"/upload?success=Файлы+объединены+в+{MERGED}",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            f"/upload?error=Ошибка+объединения:+{str(e).replace(' ', '+')}",
            status_code=303
        )

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)