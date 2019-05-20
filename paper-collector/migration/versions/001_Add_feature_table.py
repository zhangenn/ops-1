from sqlalchemy import Table, Column, Integer, String, MetaData
from migrate import *

meta = MetaData()

features = Table('features', meta,
    Column('id', String, primary_key=True),
    Column('num_cite', Integer),
    Column('num_cited_by', Integer))

def upgrade(migrate_engine):
    # Upgrade operations go here. Don't create your own engine; bind
    # migrate_engine to your metadata
    meta.bind = migrate_engine
    features.create()


def downgrade(migrate_engine):
    # Operations to reverse the above upgrade go here.
    meta.bind = migrate_engine
    features.drop()
