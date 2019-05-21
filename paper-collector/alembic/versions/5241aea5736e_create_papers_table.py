"""create papers table

Revision ID: 5241aea5736e
Revises:
Create Date: 2019-05-21 00:52:20.070523

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '5241aea5736e'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'papers',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('arxiv_id', sa.Integer, nullable=True),
        sa.Column('title', sa.Unicode(255), nullable=False),
        sa.Column('summary', sa.Text),
        sa.Column('arxiv_comment', sa.Text),
        sa.Column('arxiv_primary_category', sa.Unicode(255)),
        sa.Column('tags', sa.JSON),
        sa.Column('published', sa.DateTime),
        sa.Column('updated', sa.DateTime)
    )


def downgrade():
    op.drop_table('papers')
