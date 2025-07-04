FROM python:3.10-slim
WORKDIR /app
COPY . /app
RUN pip install pandas boto3
CMD ["python", "transform.py"]
