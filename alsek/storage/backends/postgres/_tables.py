from __future__ import annotations

from sqlalchemy import Column, String, Float
from sqlalchemy.orm import declarative_base

Base = declarative_base()

SCHEMA_NAME: str = "alsek"


class KeyValue(Base):
    __tablename__ = "key_value"
    __table_args__ = {"schema": SCHEMA_NAME}

    name = Column(String, primary_key=True)
    value = Column(String)
    expires_at = Column(Float, nullable=True)


class Priority(Base):
    __tablename__ = "priority"
    __table_args__ = {"schema": SCHEMA_NAME}

    key = Column(String, primary_key=True)
    unique_id = Column(String, primary_key=True)
    priority = Column(Float)
