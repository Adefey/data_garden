FROM python:3.14-alpine

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt

CMD ["uvicorn", "server.main:app", "--app-dir", ".", "--host", "0.0.0.0", "--port", "8000"]
