FROM python:3-alpine

RUN apk update && apk upgrade && apk add jq
RUN pip install pika \
                tinydb

COPY . .

CMD python ./main.py $(jq -c . < config.json)