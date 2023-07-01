import datetime
from sqlalchemy import (
    Table,
    Column,
    Integer,
    BigInteger,
    String,
    Float,
    DateTime,
    SmallInteger,
    ForeignKey,
    Index,
    ARRAY,
    Boolean,
)
from sqlalchemy.sql.schema import MetaData

metadata = MetaData()


minutes = Table(
    'minutes', metadata,
    Column('minuteid', Integer, primary_key=True, autoincrement=True),
    Column('path', String, nullable=False),
    Column('organization', String),
    Column('documentdate', DateTime),
    Column('meetingdate_start', DateTime),
    Column('meetingdate_end', DateTime),
    Column('created', DateTime, default=datetime.datetime.utcnow),
    Column('lastmodified', DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
)

