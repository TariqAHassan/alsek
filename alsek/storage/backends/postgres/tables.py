from __future__ import annotations

from sqlalchemy import Column, String, DateTime, text, Integer, Text
from sqlalchemy.orm import declarative_base

Base = declarative_base()

SCHEMA_NAME: str = "alsek"


class KeyValue(Base):
    __tablename__ = "key_value"
    __table_args__ = {"schema": SCHEMA_NAME}

    id = Column(String, primary_key=True)
    value = Column(Text)
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
        Integer,
        index=True,
    )


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
        index=True,
    )
    created_at = Column(
        DateTime(timezone=False),
        server_default=text("(now() AT TIME ZONE 'UTC')"),
        nullable=False,
        index=True,
    )
    acquired_at = Column(
        DateTime(timezone=False),
        nullable=False,
        index=True,
    )
    expires_at = Column(
        DateTime(timezone=False),
        nullable=True,
        index=True,
    )
