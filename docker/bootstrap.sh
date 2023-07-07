#!/usr/bin/env bash

 # Update /etc/hosts file with help center url pointing to proxy ip address so it can be resolved in the container (only for dev)
if [ "$ENV" == "local" ]; then
    echo "172.21.1.100 help.mdr.${ENV}.dh.unimaas.nl" >> /etc/hosts
fi

# End with exec call to preserve signals
exec python3 -u /opt/app/etl/exporterWorker.py