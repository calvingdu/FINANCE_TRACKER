FROM prefecthq/prefect:2-python3.10

COPY . .
RUN pip install -r requirements.txt

ENV PYTHON_ENV=develop
ENV PYTHONPATH=.

COPY flows /opt/prefect/flows

CMD ["python", "flows/main.py"]
