from __future__ import annotations

from sqlalchemy import Column, String, Float
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class KeyValue(Base):
    __tablename__ = "alsek_key_value"
    __table_args__ = {"schema": "alsek"}

    name = Column(String, primary_key=True)
    value = Column(String)
    expires_at = Column(Float, nullable=True)


class Priority(Base):
    __tablename__ = "alsek_priority"
    __table_args__ = {"schema": "alsek"}

    key = Column(String, primary_key=True)
    unique_id = Column(String, primary_key=True)
    priority = Column(Float)
