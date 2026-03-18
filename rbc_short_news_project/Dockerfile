FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src /app/src

CMD ["/usr/bin/python3", "src/rbc_short_news_parser.py", "--hours", "24", "--rbc-max-pages", "300", "--rbc-max-items", "3000", "--rbc-fulltext-limit", "200", "--ria-max-items", "300", "--ria-fulltext-limit", "150", "--max-search-seconds", "300", "--log-level", "INFO"]
