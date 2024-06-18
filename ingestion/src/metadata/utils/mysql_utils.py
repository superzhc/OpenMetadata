from sqlalchemy import text

SQL_QUERY_VERSION = "SELECT VERSION()"


def get_version(session):
    version = session.execute(text(SQL_QUERY_VERSION)).scalar()
    return int(version.split(".")[0])


if __name__ == "__main__":
    from sqlalchemy import (
        create_engine,
    )
    from sqlalchemy.orm import sessionmaker

    engine = create_engine("mysql+pymysql://root:123456@127.0.0.1:3306")
    Session = sessionmaker(bind=engine)
    with Session() as session:
        # print(type(get_version(session)))
        print(get_version(session))
