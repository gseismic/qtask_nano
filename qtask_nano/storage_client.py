import requests
import os
import base64
from typing import Optional, Dict, Any

class StorageClient:
    def __init__(self, server_url: str):
        self.base = server_url.rstrip("/")

    def upload_file(self, file_path: str, namespace: str = "default") -> Dict[str, Any]:
        url = self.base + "/upload_file"
        with open(file_path, "rb") as f:
            files = {"file": (os.path.basename(file_path), f)}
            data = {"namespace": namespace}
            r = requests.post(url, files=files, data=data, timeout=60)
        r.raise_for_status()
        return r.json()

    def get_file(self, namespace: str, filename: str, save_to: Optional[str] = None) -> Optional[Dict[str, Any]]:
        url = self.base + f"/get_file/{namespace}/{filename}"
        r = requests.get(url, timeout=60)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        data = r.json()
        content_b64 = data.get("content_base64")
        if save_to and content_b64:
            with open(save_to, "wb") as f:
                f.write(base64.b64decode(content_b64))
        return data

    def delete_file(self, namespace: str, filename: str) -> Dict[str, Any]:
        url = self.base + f"/delete_file/{namespace}/{filename}"
        r = requests.delete(url, timeout=30)
        if r.status_code == 404:
            return {"deleted": False}
        r.raise_for_status()
        return r.json()

    def delete_namespace(self, namespace: str) -> Dict[str, Any]:
        url = self.base + f"/delete_namespace/{namespace}"
        r = requests.delete(url, timeout=60)
        r.raise_for_status()
        return r.json()

if __name__ == "__main__":
    client = StorageClient("http://localhost:8000")
    resp = client.upload_file("query_cli.py")
    print(resp)
    
    resp  = client.delete_namespace("default")
    print(resp)
