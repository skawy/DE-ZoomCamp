FROM python:3.9

RUN pip install pandas

WORKDIR /app

# if you made it with pipeline scribt
COPY ingest_data.py ingest_data.py


ENTRYPOINT [ "python" , "ingest_data.py"]

