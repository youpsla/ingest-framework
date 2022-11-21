FROM python:3.8-alpine

ENV AWS_EXECUTION_ENV=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH="/var/task"
ENV TZ=UTC

RUN mkdir -p /var/task

WORKDIR /var/task

COPY requirements.txt /

COPY . .

RUN pip install -r /requirements.txt
RUN cp /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

CMD ["sh", "launch.sh"]