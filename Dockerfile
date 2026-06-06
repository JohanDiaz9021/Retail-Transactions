FROM python:3.12-slim

WORKDIR /app

COPY requirements-app.txt .
RUN pip install --no-cache-dir -r requirements-app.txt

COPY app/ app/
COPY start-render.sh .
RUN chmod +x start-render.sh

EXPOSE 8501

CMD ["bash", "start-render.sh"]
