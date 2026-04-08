from sqlalchemy import Column, Date, Integer, Numeric, String
from database import Base


class Units(Base):
    __tablename__ = "units"

    date = Column(Date, primary_key=True)
    mkp_name = Column(String, primary_key=True)

    ipl_cl = Column(Integer, nullable=False, default=0)
    ipl_pro = Column(Integer, nullable=False, default=0)
    rasuradora = Column(Integer, nullable=False, default=0)
    perfiladora = Column(Integer, nullable=False, default=0)
    repuestos = Column(Integer, nullable=False, default=0)
    exfoliante = Column(Integer, nullable=False, default=0)
    agua_de_rosas = Column(Integer, nullable=False, default=0)


class Sales(Base):
    __tablename__ = "sales"

    date = Column(Date, primary_key=True)
    mkp_name = Column(String, primary_key=True)

    ipl_cl = Column(Numeric(12, 2), nullable=False, default=0)
    ipl_pro = Column(Numeric(12, 2), nullable=False, default=0)
    rasuradora = Column(Numeric(12, 2), nullable=False, default=0)
    perfiladora = Column(Numeric(12, 2), nullable=False, default=0)
    repuestos = Column(Numeric(12, 2), nullable=False, default=0)
    exfoliante = Column(Numeric(12, 2), nullable=False, default=0)
    agua_de_rosas = Column(Numeric(12, 2), nullable=False, default=0)


class Banks(Base):
    __tablename__ = "banks"

    date = Column(Date, primary_key=True)

    bbva = Column(Numeric(12, 2), nullable=False, default=0)
    brg = Column(Numeric(12, 2), nullable=False, default=0)
    mp = Column(Numeric(12, 2), nullable=False, default=0)
    mp_liberar = Column(Numeric(12, 2), nullable=False, default=0)
    shop = Column(Numeric(12, 2), nullable=False, default=0)
    lvp = Column(Numeric(12, 2), nullable=False, default=0)
    coppel = Column(Numeric(12, 2), nullable=False, default=0)


class Expenses(Base):
    __tablename__ = "expenses"

    date = Column(Date, primary_key=True)
    concept = Column(String, primary_key=True)
    total = Column(Numeric(12, 2), nullable=False, default=0)


class AcquisitionExpense(Base):
    __tablename__ = "acquisition_expense"

    date = Column(Date, primary_key=True)

    Amazon = Column(Numeric(12, 2), nullable=False, default=0)
    Mercado_Libre = Column(Numeric(12, 2), nullable=False, default=0)
    Facebook = Column(Numeric(12, 2), nullable=False, default=0)
    Tiktok = Column(Numeric(12, 2), nullable=False, default=0)
    Google = Column(Numeric(12, 2), nullable=False, default=0)
    UGC_y_Colab = Column(Numeric(12, 2), nullable=False, default=0)
    Otros = Column(Numeric(12, 2), nullable=False, default=0)