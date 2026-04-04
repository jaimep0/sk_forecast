from database import SessionLocal
from models import Units


def read_units() -> None:
    session = SessionLocal()

    try:
        rows = session.query(Units).all()

        for row in rows:
            print(
                row.date,
                row.mkp_name,
                row.ipl_cl,
                row.ipl_pro,
                row.rasuradora,
                row.perfiladora,
                row.repuestos,
                row.exfoliante,
                row.agua_de_rosas,
            )
    finally:
        session.close()


if __name__ == "__main__":
    read_units()