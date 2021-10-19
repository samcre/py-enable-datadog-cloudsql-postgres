#!/usr/bin/env python3
"""
Script to configure Datadog on CloudSQL
"""

__author__ = "Samuel Crespo"
__version__ = "0.1.0"
__license__ = "MIT"

import argparse
import random
import string
from os import environ
import psycopg2
from logzero import logger

DD_USER = 'datadog'
DD_SCHEMA = 'datadog'
DD_EXTENSION = 'pg_stat_statements'
DD_FUNCTION = f"""
CREATE OR REPLACE FUNCTION {DD_SCHEMA}.explain_statement (
   l_query text,
   out explain JSON
)
RETURNS SETOF JSON AS
$$
BEGIN
   RETURN QUERY EXECUTE 'EXPLAIN (FORMAT JSON) ' || l_query;
END;
$$
LANGUAGE 'plpgsql'
RETURNS NULL ON NULL INPUT
SECURITY DEFINER;"""


def create_common_psql_flags(parser):
    parser.add_argument(
        "-h",
        "--host",
        action="store",
        default=environ.get("PGHOST") or "localhost",
        help="database server host (default: \"localhost\")"
    )
    parser.add_argument(
        "-p",
        "--port",
        action="store",
        default=environ.get("PGPORT") or "5432",
        help="database server port (default: \"5432\")"
    )
    parser.add_argument(
        "-U",
        "--username",
        action="store",
        default=environ.get("PGUSER") or "postgres",
        help="database user name (default: \"postgres\")"
    )
    parser.add_argument(
        "-W",
        "--password",
        action="store",
        default=environ.get("PGPASSWORD") or parser.get_default("username"),
        help=f"database user password "
        f"(default: \"{parser.get_default('username')}\")"
    )
    parser.add_argument(
        "-d",
        "--dbname",
        action="store",
        default=environ.get("PGDATABASE") or parser.get_default("username"),
        help=f"database name to connect to "
        f"(default: \"{parser.get_default('username')}\")"
    )


def create_datadog_function(conn):
    try:
        logger.debug(
            f'creating datadog function '
            f'on {conn.get_dsn_parameters()["dbname"]}')
        cur = conn.cursor()
        cur.execute(DD_FUNCTION)
    except Exception as e:
        logger.error(e)
    finally:
        conn.commit()
        cur.close()


def psql_create_extension(
        conn,
        extension):
    try:
        logger.debug(
            f'enabling extension on {conn.get_dsn_parameters()["dbname"]}')
        cur = conn.cursor()
        cur.execute(f'CREATE EXTENSION IF NOT EXISTS {extension};')
    except Exception as e:
        logger.error(e)
    finally:
        conn.commit()
        cur.close()


def psql_create_schemas(
        conn,
        schemas):
    try:
        logger.debug(
            f'creating schemas on {conn.get_dsn_parameters()["dbname"]}')
        cur = conn.cursor()
        for schema in schemas:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS {schema};')
    except Exception as e:
        logger.error(e)
    finally:
        conn.commit()
        cur.close()


def psql_grant_on_schema_to_user(
        conn,
        schemas,
        grants,
        user):
    try:
        cur = conn.cursor()
        for grant in grants:
            for schema in schemas:
                logger.debug(
                    f'granting permissions on schema {schema} '
                    f'on {conn.get_dsn_parameters()["dbname"]}')
                cur.execute(f"GRANT {grant} ON SCHEMA {schema} TO {user};")
    except Exception as e:
        logger.error(e)
    finally:
        conn.commit()
        cur.close()


def psql_grant_roles_to_user(
        conn,
        user,
        roles):
    try:
        cur = conn.cursor()
        for role in roles:
            cur.execute(f"GRANT {role} TO {user};")
    except Exception as e:
        logger.error(e)
    finally:
        conn.commit()
        cur.close()


def psql_create_user(
        conn,
        user,
        password=None):
    if password is None:
        password = ''.join(
            random.SystemRandom().choice(
                string.ascii_letters + string.digits) for _ in range(16))
    try:
        cur = conn.cursor()
        cur.execute(
            f"CREATE USER {user} WITH password '{password}';"
        )
        print(f"Created user {user} with password \'{password}\'")
    except Exception as e:
        logger.error(e)
    finally:
        conn.commit()
        cur.close()


def get_all_databases(conn):
    dbs = []
    try:
        cur = conn.cursor()
        cur.execute("SELECT datname "
                    "FROM pg_database "
                    "WHERE datname NOT LIKE 'template%' "
                    "AND datname != 'cloudsqladmin' "
                    "AND datname != 'postgres';")
        dbs = ([db[0] for db in cur.fetchall()])
    except Exception as e:
        logger.error(e)
    finally:
        conn.commit()
        cur.close()
    return dbs


def install_on(
        conn):
    try:
        psql_create_schemas(conn, schemas=[DD_SCHEMA])
        psql_create_extension(conn, extension=DD_EXTENSION)
        psql_grant_on_schema_to_user(
            conn, schemas=[DD_SCHEMA, "public"],
            grants=["USAGE"], user=DD_USER
        )
        create_datadog_function(conn)
    except Exception as e:
        logger.error(e)
    finally:
        conn.commit()


def install_datadog(opts):
    try:
        conn = psycopg2.connect(
            host=opts.host, port=opts.port, dbname=opts.dbname,
            user=opts.username, password=opts.password)
        psql_create_user(conn, user=DD_USER, password=opts.dd_password)
        psql_grant_roles_to_user(conn, user=DD_USER, roles=["pg_monitor"])
        if opts.all_databases:
            for database in get_all_databases(conn):
                install_on(psycopg2.connect(
                    host=opts.host, port=opts.port, dbname=database,
                    user=opts.username, password=opts.password))
        else:
            install_on(conn)
    except Exception as e:
        logger.error(e)
    finally:
        conn.commit()
        conn.close()


def remove_datadog(opts):
    logger.error("Function not implemented yet")


def main(opts):
    if opts.remove:
        remove_datadog(opts)
    else:
        install_datadog(opts)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--help", action="help", help="Show this help message")
    create_common_psql_flags(parser)

    action_group = parser.add_mutually_exclusive_group()
    action_group.add_argument(
        "-I",
        "--install",
        action="store_true",
        default=True,
        help="Configures Datadog on database(s) (default: \"true\")"
    )

    action_group.add_argument(
        "-R",
        "--remove",
        action="store_true",
        default=False,
        help="Removes Datadog config on database(s) (default: \"false\")"
    )
    parser.add_argument(
        "-a",
        "--all-databases",
        action="store_true",
        help="""wether if apply to all databases, """
             """or just the one set in --dbname"""
    )
    parser.add_argument(
        "--dd-password",
        action="store",
        help="Password for Datadog user on database. "
        "If not configured, a random password will be created."
    )

    args = parser.parse_args()
    main(args)
