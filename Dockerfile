FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 BENCH_RESULTS=/results
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && useradd --uid 10001 --create-home bench \
    && mkdir /results && chown bench:bench /results
COPY --chown=bench:bench . .
USER 10001
EXPOSE 5557 8089
CMD ["locust", "-f", "locustfile.py"]
