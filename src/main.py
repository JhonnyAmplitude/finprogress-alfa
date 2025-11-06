# src/main.py
from fastapi import FastAPI, UploadFile, File, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from pathlib import Path
from typing import Any
import tempfile

from src.services.full_statement_xml import parse_full_statement_xml
from src.utils import logger

app = FastAPI(title="XML Statement Parser API", version="0.1")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/parse-xml", response_class=JSONResponse)
async def parse_xml(file: UploadFile = File(...)) -> Any:
    """
    Загрузи XML файл (form field 'file') — вернётся JSON с операциями и meta.
    """
    filename = Path(file.filename).name if file.filename else "uploaded.xml"
    logger.info("Получен файл: %s (content_type=%s)", filename, file.content_type)

    # читаем bytes (XML можно парсить в памяти, обычно невелик)
    try:
        content = await file.read()
        if not content:
            raise ValueError("Empty file")
    except Exception as e:
        logger.exception("Ошибка чтения файла: %s", e)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Не удалось прочитать файл")

    # парсим напрямую из bytes
    try:
        result = parse_full_statement_xml(content)
    except Exception as e:
        logger.exception("Ошибка парсинга XML: %s", e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Ошибка парсинга: {e}")

    # лог-резюме
    ops_count = len(result.get("operations", []))
    parsed = result.get("meta", {}).get("trade_ops_parsed_count", 0)
    total_rows = result.get("meta", {}).get("trade_ops_raw_count", 0)
    logger.info("%s Аккаунт: (xml) операций: %s, сделок найдено/распознано: %s/%s",
                filename, ops_count, parsed, total_rows)

    return JSONResponse(content=jsonable_encoder(result))
