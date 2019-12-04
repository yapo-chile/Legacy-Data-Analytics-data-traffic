FROM python:3.7-slim

WORKDIR /work/

COPY resources/requirements.txt resources/requirements
COPY . .
RUN pip install -r resources/requirements.txt
ENTRYPOINT ["python", "src/main.py"]