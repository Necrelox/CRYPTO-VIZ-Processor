FROM python:3.11-alpine
WORKDIR /
RUN apk update --no-cache
RUN apk add --no-cache gcc build-base python3-dev
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "./Source/main.py"]
