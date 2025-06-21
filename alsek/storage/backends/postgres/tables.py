from __future__ import annotations

from sqlalchemy import Column, String, DateTime, text, Integer
from sqlalchemy.orm import declarative_base

Base = declarative_base()

SCHEMA_NAME: str = "alsek"


class KeyValue(Base):
    __tablename__ = "key_value"
    __table_args__ = {"schema": SCHEMA_NAME}

    id = Column(String, primary_key=True)
    value = Column(String)
    created_at = Column(
        DateTime(timezone=False),
        server_default=text("(now() AT TIME ZONE 'UTC')"),
        nullable=False,
    )
    expires_at = Column(
        DateTime(timezone=False),
        nullable=True,
    )


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
    )
    unique_id = Column(
        String,
        primary_key=True,
    )
    priority = Column(Integer)


class DistributedLock(Base):
    __tablename__ = "distributed_lock"
    __table_args__ = {"schema": SCHEMA_NAME}

    id = Column(
        String,
        primary_key=True,
    )
    owner_id = Column(
        String,
        nullable=False,
    )
    created_at = Column(
        DateTime(timezone=False),
        server_default=text("(now() AT TIME ZONE 'UTC')"),
        nullable=False,
    )
    acquired_at = Column(
        DateTime(timezone=False),
        nullable=False,
    )
    expires_at = Column(
        DateTime(timezone=False),
        nullable=True,
    )
