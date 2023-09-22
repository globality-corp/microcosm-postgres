from typing import TYPE_CHECKING, ClassVar
from uuid import uuid4

from sqlalchemy import Table
from sqlalchemy.orm import mapped_column
from sqlalchemy_utils import UUIDType

from microcosm_postgres.encryption.v2.column import encryption
from microcosm_postgres.encryption.v2.encoders import IntEncoder, StringEncoder
from microcosm_postgres.encryption.v2.encryptors import AwsKmsEncryptor
from microcosm_postgres.encryption.v2.utils import members_override
from microcosm_postgres.identifiers import new_object_id
from microcosm_postgres.models import Model


class Employee(Model):
    __tablename__ = "test_encryption_employee_v2"
    if TYPE_CHECKING:
        __table__: ClassVar[Table]

    id = mapped_column(UUIDType, primary_key=True, default=uuid4)

    # Name requires beacon value for search
    name = encryption("name", AwsKmsEncryptor(), StringEncoder())
    name_encrypted = name.encrypted()
    name_unencrypted = name.unencrypted(index=True)
    name_beacon = name.beacon()

    # Salary does not require beacon value
    salary = encryption("salary", AwsKmsEncryptor(), IntEncoder())
    salary_encrypted = salary.encrypted()
    salary_unencrypted = salary.unencrypted()


def test_members_override():
    sample_dict = {
        "_internal": "should not be in result",
        "normal_field": "should remain unchanged",
        "encrypted_field": "this should not be changed if no unencrypted counterpart exists",
        "encrypted_field_unencrypted": "this should become 'encrypted_field'",
        "relation": Employee(
            id=new_object_id(),
            name="should not be in result",
            salary=100,
        ),
    }

    result = members_override(sample_dict, ["encrypted_field"])

    # Test case 1 & 2
    assert "_internal" not in result
    assert "relation" not in result

    # Test case 3 & 4
    assert "encrypted_field_unencrypted" not in result
    assert result["encrypted_field"] == "this should become 'encrypted_field'"

    # Test case 5
    assert result["normal_field"] == "should remain unchanged"


def test_members_override_missing_unencrypted():
    sample_dict = {
        "encrypted_field": "this should remain as it is if there's no unencrypted counterpart",
    }

    result = members_override(sample_dict, ["encrypted_field"])

    assert (
        result["encrypted_field"]
        == "this should remain as it is if there's no unencrypted counterpart"
    )
    assert "encrypted_field_unencrypted" not in result
