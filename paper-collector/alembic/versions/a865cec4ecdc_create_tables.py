"""create tables

Revision ID: a865cec4ecdc
Revises: 
Create Date: 2019-05-26 14:51:51.745517

"""
from alembic import op
from sqlalchemy import Column, Integer, String, DateTime, JSON, ForeignKey
from sqlalchemy.orm import relationship


# revision identifiers, used by Alembic.
revision = 'a865cec4ecdc'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'papers',
        Column('id', String, primary_key=True),
        Column('version', Integer, nullable=False),
        Column('author', String, nullable=False),
        Column('title', String, nullable=False),
        Column('summary', String),
        Column('arxiv_comment', String),
        Column('tags', JSON),
        Column('published_datetime', DateTime),
        Column('updated_datetime', DateTime),
    )

    op.create_table(
        'authors',
        Column('id', String, primary_key=True),
        Column('author', String, nullable=False),
        Column('paper_id', String, ForeignKey('papers.id'))
    )


def downgrade():
    op.drop_table('papers')
    op.drop_table('authors')
