FROM python:3.6

#ADD . /opt/app
WORKDIR /opt/app
 
# Python requirements
ADD requirements.txt /opt
RUN pip install -r /opt/requirements.txt

#CMD ["python3","/code/exporterWorker.py"]

# Entry point
ADD bootstrap.sh /opt
RUN chmod +x /opt/bootstrap.sh
ENTRYPOINT [ "/opt/bootstrap.sh" ]