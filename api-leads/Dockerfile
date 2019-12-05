FROM python:3.5-buster

WORKDIR /work/

COPY . .
RUN pip install -r resources/requirements.txt
ENTRYPOINT ["python", "src/main.py"]