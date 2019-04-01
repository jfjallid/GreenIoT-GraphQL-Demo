FROM python:3
RUN mkdir /app
WORKDIR /app
ADD myapp.py /app/
ADD schema.py /app/
RUN pip install -r requirements.txt

CMD["python", "/app/myapp.py"]
