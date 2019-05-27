"""index column

Revision ID: aace64c8ee91
Revises: a865cec4ecdc
Create Date: 2019-05-26 14:53:59.058147

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'aace64c8ee91'
down_revision = 'a865cec4ecdc'
branch_labels = None
depends_on = None


def upgrade():
    op.create_index('index_updated_datetime', 'papers', ['updated_datetime'])


def downgrade():
    op.drop_index('index_updated_datetime')
