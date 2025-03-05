FROM apache/airflow:2.9.0-python3.8
RUN pip install apache-airflow==2.9.0

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY ytsaurus_airflow_provider*.whl .
RUN pip install ytsaurus_airflow_provider*.whl
