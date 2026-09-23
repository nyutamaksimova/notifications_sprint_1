FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY pyproject.toml alembic.ini ./
COPY src ./src
COPY migrations ./migrations
ENV PYTHONPATH=/app/src
RUN useradd --create-home appuser
USER appuser
CMD ["uvicorn", "notification_service.main:app", "--host", "0.0.0.0", "--port", "8000"]

