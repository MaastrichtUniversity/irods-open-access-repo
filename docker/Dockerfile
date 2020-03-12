FROM python:3.8

WORKDIR /opt/app
 
# Python requirements
ADD requirements.txt /opt
RUN pip install -r /opt/requirements.txt

# Entry point
ADD bootstrap.sh /opt
RUN chmod +x /opt/bootstrap.sh
ENTRYPOINT [ "/opt/bootstrap.sh" ]