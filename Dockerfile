FROM python:3.6
ADD . /code
WORKDIR /code
RUN pip install python-irodsclient
RUN pip install lxml
RUN pip install requests
RUN pip install bleach
RUN pip install pika
CMD ["python3","/code/exporterWorker.py"]