# irods-open-acces-repo

Export an archived iRODS collection to open access repositories.

Available repositories:
 * Dataverse.nl
 * DANS Easy
 * Zenodo
 * Figshare

## Development

For development purposes this repository is being run from `docker-dev`. The `docker-compose.yml` there is being
used to run this repository.

The `docker-compose.template.yml` available here is provided as an example. You are encouraged to keep it up to date.  

## Production

In production the the docker-compose is templated by Ansible. 
