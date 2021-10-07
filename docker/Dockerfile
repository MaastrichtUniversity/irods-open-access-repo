FROM python:3.9

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /opt/app
 
# Python requirements
ADD ./docker/requirements.txt /opt
RUN pip install -r /opt/requirements.txt

RUN mkdir -p /opt/app/etl
COPY ./etl /opt/app/etl/

ENV PYTHONPATH /opt/app:/opt/app/etl

# Entry point
ADD ./docker/bootstrap.sh /opt
RUN chmod +x /opt/bootstrap.sh
ENTRYPOINT [ "/opt/bootstrap.sh" ]