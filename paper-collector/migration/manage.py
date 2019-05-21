#!/usr/bin/env python
from migrate.versioning.shell import main

if __name__ == '__main__':
    main(repository='../migration', url='postgresql://postgres:postgres@localhost:5432', debug='False')
