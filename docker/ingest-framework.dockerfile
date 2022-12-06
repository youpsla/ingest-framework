FROM python:3.8-alpine

ENV AWS_EXECUTION_ENV=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH="/var/task"
ENV TZ=UTC

RUN mkdir -p /var/task

WORKDIR /var/task

COPY requirements.txt /

RUN pip install -r /requirements.txt

# Install curl to have sentry-cli.
RUN apk update
RUN apk upgrade
RUN apk add --update curl && rm -rf /var/cache/apk/*
RUN curl -sL https://sentry.io/get-cli/ | SENTRY_CLI_VERSION="2.10.0" sh

COPY . .

RUN cp /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

CMD ["sh", "launch.sh"]