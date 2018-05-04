"""
Example models and store usage.

"""
from enum import Enum, unique

from sqlalchemy import Column, ForeignKey, String
from sqlalchemy_utils import UUIDType

from microcosm.api import binding
from microcosm_postgres.models import EntityMixin, Model
from microcosm_postgres.store import Store
from microcosm_postgres.types import EnumType


@unique
class CompanyType(Enum):
    private = "private"
    public = "public"


class Company(EntityMixin, Model):
    """
    A company has a unique name.

    """
    __tablename__ = "company"

    name = Column(String(255), unique=True)
    type = Column(EnumType(CompanyType))


class Employee(EntityMixin, Model):
    """
    An employee belongs to a company but does not necessarily have unique first/last names.

    """
    __tablename__ = "employee"

    first = Column(String(255), nullable=False)
    last = Column(String(255), nullable=False)
    other = Column(String(255), nullable=True)
    company_id = Column(UUIDType, ForeignKey('company.id'), nullable=False)

    @property
    def edges(self):
        yield (self.company_id, self.id)


class CompanyStore(Store):
    pass


class EmployeeStore(Store):
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


@binding("company_store")
def configure_company_store(graph):
    return CompanyStore(graph, Company, [Company.name])


@binding("employee_store")
def configure_employee_store(graph):
    return EmployeeStore(graph, Employee)
