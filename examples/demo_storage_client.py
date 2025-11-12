from qtask_nano import StorageClient


client = StorageClient("http://localhost:8000")
resp = client.upload_file("query_cli.py")
print(resp)

resp  = client.delete_namespace("default")
print(resp)
