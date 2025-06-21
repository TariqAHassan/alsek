"""

    Tables

"""

from __future__ import annotations
from enum import Enum, unique

from sqlalchemy import Column, String, DateTime, text, Text, DOUBLE_PRECISION
from sqlalchemy.dialects.postgresql import ENUM as PgEnum  # noqa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base

from alsek.utils.temporal import utcnow

Base = declarative_base()

SCHEMA_NAME: str = "alsek"


@unique
class KeyValueType(Enum):
    STANDARD = "STANDARD"
    PRIORITY = "PRIORITY"
    LOCK = "LOCK"


class KeyValue(Base):
    __tablename__ = "key_value"
    __table_args__ = {"schema": SCHEMA_NAME}

    id = Column(
        String,
        primary_key=True,
    )
    value = Column(
        Text,
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
    type = Column(
        PgEnum(KeyValueType),
        index=True,
        nullable=False,
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
    __table_args__ = {"schema": SCHEMA_NAME}

    id = Column(
        String,
        primary_key=True,
    )
    created_at = Column(
        DateTime(timezone=False),
        server_default=text("(now() AT TIME ZONE 'UTC')"),
        nullable=False,
        index=True,
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
