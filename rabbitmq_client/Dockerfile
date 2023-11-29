FROM python:3.10

RUN addgroup --system elyas && adduser --system --group elyas

COPY ./requirements.txt .

RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . /app

WORKDIR /app

USER elyas

CMD ["python", "main.py"]