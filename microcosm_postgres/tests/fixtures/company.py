from enum import Enum, unique

from sqlalchemy import Column, String

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


@binding("company_store")
class CompanyStore(Store):

    def __init__(self, graph):
        super().__init__(graph, Company, [Company.name])
