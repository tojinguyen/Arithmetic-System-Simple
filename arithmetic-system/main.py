import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

if __name__ == "__main__":
    import uvicorn
    print("Starting FastAPI server with Uvicorn...")
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=False)

