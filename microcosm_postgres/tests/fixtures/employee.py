from microcosm.api import binding
from sqlalchemy import Column, ForeignKey, String
from sqlalchemy_utils import UUIDType

from microcosm_postgres.models import EntityMixin, Model
from microcosm_postgres.store import Store


class Employee(EntityMixin, Model):
    """
    An employee belongs to a company but does not necessarily have unique first/last names.

    """
    __tablename__ = "employee"

    first = Column(String(255), nullable=False)
    last = Column(String(255), nullable=False)
    other = Column(String(255), nullable=True)
    company_id = Column(UUIDType, ForeignKey("company.id"), nullable=False)
    employee_identity_id = Column(UUIDType, ForeignKey("employee_idenity.id", name="fk_employee_employee_identity_employee_identity_id"))

    @property
    def edges(self):
        yield (self.company_id, self.id)


class EmployeeData(EntityMixin, Model):
    """
    An employee data record containing sensitive data (accessed with a different engine).

    """
    __tablename__ = "employee_data"
    __engine__ = "secret"

    employee_id = Column(UUIDType, ForeignKey("employee.id"), nullable=False)
    password = Column(String(255), nullable=False)


@binding("employee_store")
class EmployeeStore(Store):

    def __init__(self, graph):
        super().__init__(graph, Employee)

    def search_by_company(self, company_id):
        return self.search(Employee.company_id == company_id)

    def _order_by(self, query, **kwargs):
        return query.order_by(Employee.last.asc())

    def _filter(self, query, **kwargs):
        company_id = kwargs.get("company_id")
        first = kwargs.get("first")
        if company_id is not None:
            query = query.filter(Employee.company_id == company_id)
        if first is not None:
            query = query.filter(Employee.first == first)
        return super(EmployeeStore, self)._filter(query, **kwargs)


@binding("employee_data_store")
class EmployeeDataStore(Store):

    def __init__(self, graph):
        super().__init__(graph, EmployeeData)
