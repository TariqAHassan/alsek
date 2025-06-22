"""

    Tables

    Notes:
        * We write ALL keys into the `KeyValue` table so than when we call `scan()`
          we can see all data stored. This is even true for `Priority` records
          which are also stored in a separate table to enable fast, database-side
          sorting by `priority` values.

"""

from __future__ import annotations

from enum import Enum, unique

from sqlalchemy import (
    DOUBLE_PRECISION,
    Column,
    DateTime,
    ForeignKey,
    String,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import ENUM as PgEnum  # noqa
from sqlalchemy.orm import declarative_base

from alsek.defaults import DEFAULT_NAMESPACE
from alsek.utils.temporal import utcnow

Base = declarative_base()


@unique
class KeyValueType(Enum):
    STANDARD = "STANDARD"
    PRIORITY = "PRIORITY"
    LOCK = "LOCK"


class KeyValue(Base):
    __tablename__ = "key_value"
    __table_args__ = {"schema": DEFAULT_NAMESPACE}

    id = Column(
        String,
        primary_key=True,
    )
    value = Column(
        Text,
    )
    type = Column(
        PgEnum(KeyValueType, schema=DEFAULT_NAMESPACE),
        index=True,
        nullable=False,
    )
    created_at = Column(
        DateTime(timezone=False),
        server_default=text("(now() AT TIME ZONE 'UTC')"),
        nullable=False,
        index=True,
    )
    expires_at = Column(
        DateTime(timezone=False),
        nullable=True,
        index=True,
    )
    owner_id = Column(
        String,
        nullable=True,
        index=True,
    )

    @property
    def is_expired(self) -> bool:
        if self.expires_at:
            return self.expires_at <= utcnow()
        else:
            return False


class Priority(Base):
    __tablename__ = "priority"
    __table_args__ = {"schema": DEFAULT_NAMESPACE}

    id = Column(
        String,
        ForeignKey(f"{DEFAULT_NAMESPACE}.key_value.id", ondelete="CASCADE"),
        primary_key=True,
    )
    unique_id = Column(
        String,
        primary_key=True,
        index=True,
    )
    priority = Column(
        # Redis uses Double Precision for priority so we do the same here
        # See https://redis.io/docs/latest/commands/zadd/.
        DOUBLE_PRECISION,
        index=True,
    )
