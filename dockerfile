FROM python:3.9

COPY . .
RUN pip install -r requirements.txt

RUN mkdir -p /data/statements
CMD make prefect-local
