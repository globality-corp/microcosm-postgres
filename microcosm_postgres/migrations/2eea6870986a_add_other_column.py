"""
add-other-column

Revision ID: 2eea6870986a
Revises: 322cf4f0ed51
Create Date: 2016-03-05 14:24:34.003835

"""
import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = '2eea6870986a'
down_revision = '322cf4f0ed51'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('employee', sa.Column('other', sa.String(length=255), nullable=True))


def downgrade():
    op.drop_column('employee', 'other')
