FROM python:3.7-slim

WORKDIR /work/

COPY . .
RUN pip install -r resources/requirements.txt
ENTRYPOINT ["python", "src/main.py"]