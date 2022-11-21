FROM python:3.8-alpine

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /

COPY . .

RUN pip install -r /requirements.txt

CMD ["launch.sh"]
