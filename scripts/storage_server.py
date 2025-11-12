from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.responses import JSONResponse, FileResponse
import uuid
import os
import re
from base64 import b64encode
from pathlib import Path

app = FastAPI()

STORAGE_DIR = Path("./file_storage").resolve()
STORAGE_DIR.mkdir(exist_ok=True)

def _validate_namespace(ns: str) -> str:
    if not ns:
        return "default"
    if not re.fullmatch(r"[A-Za-z0-9_\-]+", ns):
        raise HTTPException(status_code=400, detail="invalid namespace")
    return ns

@app.post("/upload_file")
async def upload_file(namespace: str = Form("default"), file: UploadFile = File(...)):
    """
    接收任意二进制文件并保存到中心服务器
    """
    file_id = str(uuid.uuid4())
    ext = Path(file.filename).suffix
    ns = _validate_namespace(namespace)
    ns_dir = STORAGE_DIR / ns
    ns_dir.mkdir(exist_ok=True)
    saved_path = ns_dir / (file_id + ext)
    try:
        with open(saved_path, "wb") as f:
            while chunk := await file.read(1024 * 1024):
                f.write(chunk)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    url = f"/get_file/{ns}/{file_id}{ext}"
    return JSONResponse({"file_path": str(saved_path), "url": url, "namespace": ns, "file_id": file_id})


@app.get("/get_file/{namespace}/{filename}")
def get_file(namespace: str, filename: str):
    ns = _validate_namespace(namespace)
    safe_name = Path(filename).name
    full_path = STORAGE_DIR / ns / safe_name
    if not full_path.exists():
        raise HTTPException(404)
    size = full_path.stat().st_size
    with open(full_path, "rb") as f:
        content_b64 = b64encode(f.read()).decode("ascii")
    return JSONResponse({"namespace": ns, "filename": safe_name, "size": size, "file_path": str(full_path), "content_base64": content_b64})


@app.delete("/delete_file/{namespace}/{filename}")
def delete_file(namespace: str, filename: str):
    ns = _validate_namespace(namespace)
    safe_name = Path(filename).name
    full_path = STORAGE_DIR / ns / safe_name
    if full_path.exists():
        full_path.unlink()
        return {"deleted": True}
    else:
        raise HTTPException(404)

@app.delete("/delete_namespace/{namespace}") 
def delete_namespace(namespace: str):
    ns = _validate_namespace(namespace) 
    ns_dir = STORAGE_DIR / ns
    if not ns_dir.is_dir():
        return {"deleted": 0}
    count = 0
    for path in ns_dir.iterdir():
        try:
            if path.is_file():
                path.unlink()
                count += 1
        except Exception:
            continue
    return {"deleted": count}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)