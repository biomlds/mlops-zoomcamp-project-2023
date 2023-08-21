ARG VARIANT="3.10-bullseye"
FROM mcr.microsoft.com/vscode/devcontainers/python:0-${VARIANT}
USER vscode
WORKDIR /home/vscode/app
COPY requirements.txt requirements.txt
COPY predict.py predict.py
RUN pip install --upgrade pip; pip install --user --no-cache-dir  -r requirements.txt
EXPOSE 9696
# ENTRYPOINT [ "gunicorn", "--bind=0.0.0.0:9696", "predict:app" ]