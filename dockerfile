FROM python:3.9

COPY . .
RUN pip install poetry
RUN poetry install
RUN pip install pandas

ENV PYTHON_ENV=develop
ENV PREFECT_API_URL="http://127.0.0.1:4200/api"
RUN prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
RUN prefect server start

CMD [ "python", "./src/scripts/main.py"]
