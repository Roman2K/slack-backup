FROM python:3.7-alpine

RUN apk update && apk add git
RUN pip3 install -e 'git+git://github.com/alexwlchan/backup-slack.git#egg=backup_slack'

ARG UID
RUN adduser -D -u $UID appuser
USER appuser

ENTRYPOINT ["backup_slack"]
