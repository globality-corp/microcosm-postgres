from microcosm.api import binding
from sqlalchemy import Column, ForeignKey, String
from sqlalchemy_utils import UUIDType

from microcosm_postgres.models import EntityMixin, Model
from microcosm_postgres.store import Store


class EmployeeIdentity(EntityMixin, Model):
    """
    An employee identity can have many employees linked to it, but is only ever created
    by a single employee.

    """
    __tablename__ = "employee_identity"

    first = Column(String(255), nullable=False)
    last = Column(String(255), nullable=False)
    created_by = Column(UUIDType, ForeignKey("employee.id", name="employee_created_by_fkey"), nullable=False)


@binding("employee_identity_store")
class EmployeeIdentityStore(Store):

    def __init__(self, graph):
        super().__init__(graph, EmployeeIdentity)
