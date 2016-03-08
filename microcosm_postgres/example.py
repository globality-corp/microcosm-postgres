"""
Example models and store usage.

"""
from sqlalchemy import Column, ForeignKey, String
from sqlalchemy_utils import UUIDType

from microcosm.api import binding
from microcosm_postgres.models import EntityMixin, Model
from microcosm_postgres.store import Store


class Company(EntityMixin, Model):
    """
    A company has a unique name.

    """
    __tablename__ = "company"

    name = Column(String(255), unique=True)


class Employee(EntityMixin, Model):
    """
    An employee belongs to a company but does not necessarily have unique first/last names.

    """
    __tablename__ = "employee"

    first = Column(String(255), nullable=False)
    last = Column(String(255), nullable=False)
    other = Column(String(255), nullable=True)
    company_id = Column(UUIDType, ForeignKey('company.id'), nullable=False)


class CompanyStore(Store):
    pass


class EmployeeStore(Store):
    def search_by_company(self, company_id):
        return self.search(Employee.company_id == company_id)


@binding("company_store")
def configure_company_store(graph):
    return CompanyStore(graph, Company)


@binding("employee_store")
def configure_employee_store(graph):
    return EmployeeStore(graph, Employee)
