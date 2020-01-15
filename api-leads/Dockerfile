FROM python:3.7-slim

WORKDIR /work/

COPY resources/requirements.txt resources/requirements.txt
COPY src src
COPY scripts scripts
RUN pip install -r resources/requirements.txt
ENTRYPOINT ["./scripts/run_api_leads.sh"]