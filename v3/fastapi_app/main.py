from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

app = FastAPI()

# 挂载前端静态文件
app.mount("/", StaticFiles(directory="frontend/dist", html=True), name="static")

@app.get("/api/health")
async def health_check():
    return {"status": "ok"}