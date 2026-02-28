FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY browser_api.py .
COPY browser_ui.html .

EXPOSE 8081

CMD ["python", "browser_api.py", "--host", "0.0.0.0", "--port", "8081"]
