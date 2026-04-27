FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir dbt-bigquery==1.7.7 functions-framework

COPY . .

RUN dbt deps || true

EXPOSE 8080

CMD ["functions-framework", "--target=run_dbt", "--port=8080"]