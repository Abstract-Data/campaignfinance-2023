# from sqlmodel import create_engine
# from op import OnePasswordItem
#
# """ Database connection notes:
# - Must use psycopg2-binary for postgresql
# - Must use snowflake.sqlalchemy for snowflake
# """
#
#
# def create_connection():
#     return create_engine(
#         OnePasswordItem(
#             name="Database - Local Postgres")
#         .database_url
#         .get_secret_value(),
#         pool_size=200,
#         echo=True
#     )
#
#
# local_postgres_engine = create_connection()
#
# # SessionLocal: sessionmaker = sessionmaker(
# #     autocommit=False, autoflush=False, bind=engine
# # )
# #
# #
# # class Base(DeclarativeBase):
# #     pass
