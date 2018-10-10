FROM python:3.6
ADD . /code
WORKDIR /code
RUN pip install python-irodsclient
RUN pip install lxml
RUN pip install requests
RUN mdir /tmp/idv/
ENTRYPOINT ["python3","./iRODS_to_Dataverse.py"]