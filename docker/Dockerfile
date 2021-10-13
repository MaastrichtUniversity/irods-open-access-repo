FROM python:3.9

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /opt/app

# Conditionally trust the custom DataHub *Dev/Test-only* Certificate Authority (CA) for iRODS-SSL-connections
ADD ssl-dev/test_only_dev_irods_dh_ca_cert.pem /tmp/test_only_dev_irods_dh_ca_cert.pem
ARG SSL_ENV
# Note: Python docker image is Debian-based. So, 'dash' as /bin/sh.
#       Strict POSIX-compliant.
RUN if [ $SSL_ENV != "acc" ] && [ $SSL_ENV != "prod" ]; then \
        echo "Adding custom DataHub iRODS-CA-certificate to the CA-rootstore (FOR DEV & TEST ONLY!)..." ; \
        cp /tmp/test_only_dev_irods_dh_ca_cert.pem /usr/local/share/ca-certificates/test_only_dev_irods_dh_ca_cert.crt ; \
        update-ca-certificates ; \
    else \
        echo "Not in dev environment: Skipping update of the CA-rootstore" ; \
    fi

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
