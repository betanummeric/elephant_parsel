import re
import sys
from logging import Logger
from typing import Union, Tuple, Callable, Any

import psycopg2
import psycopg2.extras
from psycopg2.extras import DictCursor
from psycopg2.pool import ThreadedConnectionPool


def __backported_split_sql(sql):
    """Split *sql* on a single ``%s`` placeholder.

    Split on the %s, perform %% replacement and return pre, post lists of
    snippets.
    """
    curr = pre = []
    post = []
    tokens = re.split(br'(%.)', sql)
    for token in tokens:
        if len(token) != 2 or token[:1] != b'%':
            curr.append(token)
            continue

        if token[1:] == b's':
            if curr is pre:
                curr = post
            else:
                raise ValueError(
                    "the query contains more than one '%s' placeholder")
        elif token[1:] == b'%':
            curr.append(b'%')
        else:
            raise ValueError("unsupported format character: '%s'"
                             % token[1:].decode('ascii', 'replace'))

    if curr is pre:
        raise ValueError("the query doesn't contain any '%s' placeholder")

    return pre, post


def __backported_paginate(seq, page_size):
    """Consume an iterable and return it in chunks.

    Every chunk is at most `page_size`. Never return an empty chunk.
    """
    page = []
    it = iter(seq)
    while True:
        try:
            for i in range(page_size):
                page.append(next(it))
            yield page
            page = []
        except StopIteration:
            if page:
                yield page
            return


def _backported_execute_values(cur, sql, argslist, template=None, page_size=100, fetch=False):
    # taken from psycopg2 2.9.3
    from psycopg2.sql import Composable
    import psycopg2.extensions as _ext
    if isinstance(sql, Composable):
        sql = sql.as_string(cur)

    # we can't just use sql % vals because vals is bytes: if sql is bytes
    # there will be some decoding error because of stupid codec used, and Py3
    # doesn't implement % on bytes.
    if not isinstance(sql, bytes):
        sql = sql.encode(_ext.encodings[cur.connection.encoding])
    pre, post = __backported_split_sql(sql)

    result = [] if fetch else None
    for page in __backported_paginate(argslist, page_size=page_size):
        if template is None:
            template = b'(' + b','.join([b'%s'] * len(page[0])) + b')'
        parts = pre[:]
        for args in page:
            parts.append(cur.mogrify(template, args))
            parts.append(b',')
        parts[-1:] = post
        cur.execute(b''.join(parts))
        if fetch:
            result.extend(cur.fetchall())

    return result


class PostgresDBException(BaseException):
    pass


def _format_rows(rows: list, map_row, column) -> list:
    if column:
        return [row[column] for row in rows]
    if map_row:
        return [map_row(**row) for row in rows]
    return rows


class PostgresTransaction:
    def __init__(self, db):
        self._db = db
        self.connection = None
        self.transaction = None
        self.cursor = None

    def __enter__(self):
        if self.connection is not None:
            raise PostgresDBException('connection already in use')
        self.connection = self._db._pool.getconn()
        try:
            self.transaction = self.connection.__enter__()
            try:
                self.cursor = self.transaction.cursor(cursor_factory=DictCursor)
            except BaseException:
                self.transaction.__exit__(*sys.exc_info())
                raise
        except BaseException:
            self._db._pool.putconn(self.connection)
            self.connection = None
            raise
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.transaction.__exit__(exc_type, exc_val, exc_tb)
        finally:
            self.transaction = None
            self._db._pool.putconn(self.connection)
            self.connection = None

    def execute(self, statement, arguments=None):
        self.cursor.execute(statement, arguments)

    def query_one(self, statement, arguments=None, map_row=None, column: str = None):
        if arguments is None:
            arguments = dict()
        self.cursor.execute(statement, arguments)
        row = self.cursor.fetchone()
        if row is None:
            return None
        if column:
            return row[column]
        if map_row:
            return map_row(**row)
        return row

    def query_all(self, statement, arguments=None, map_row=None, column: str = None):
        if arguments is None:
            arguments = dict()
        self.cursor.execute(statement, arguments)
        rows = self.cursor.fetchall()
        return _format_rows(rows, map_row, column)

    def execute_values(self, statement, arguments=None, template=None, map_row=None, column: str = None):
        if arguments is None:
            arguments = dict()
        rows = _backported_execute_values(self.cursor, statement, arguments, template, fetch=True)
        return _format_rows(rows, map_row, column)


class PostgresDB:
    _dict_cursor_factory = psycopg2.extras.DictCursor

    def __init__(self, connection_config: dict, logger: Logger, connect=True, register_hstore=False):
        """
        :param connection_config: attributes host, port, user, dbname, password, minconn, maxconn
        """
        self.log = logger
        self.config = connection_config
        self.config['minconn'] = max(self.config.get('minconn', None) or 1, 1)
        self.config['maxconn'] = max(self.config.get('maxconn', None) or 1, 1)
        self.register_hstore = register_hstore
        self._pool = None
        if connect:
            self.login()

    def censored_config(self):
        censored_config = dict(self.config)
        if 'password' in censored_config:
            censored_config['password'] = 'censored'
        return censored_config

    def login(self):
        try:
            self.log.debug(f'opening database connection pool {self.censored_config()}')
            self._pool = ThreadedConnectionPool(**self.config)
            if self.register_hstore:
                conn = self._pool.getconn()
                psycopg2.extras.register_hstore(conn, globally=True)
                self._pool.putconn(conn)
            self.log.debug(f'opened database connection pool {self.censored_config()}')
        except Exception as e:
            raise PostgresDBException(f'login to database failed: {self.censored_config()}\n'
                                      f'exception={str(e)}', e)

    def logout(self):
        if self._pool:
            self.log.debug(f'closing database connection pool {self.censored_config()}')
            self._pool.closeall()
            self._pool = None

    def __del__(self):
        self.logout()

    def transaction(self):
        return PostgresTransaction(self)

    def _attempt_transaction_twice(self, use_transaction: Callable[[PostgresTransaction], Any]):
        try:
            with self.transaction() as transaction:
                return use_transaction(transaction)
        except psycopg2.InterfaceError as error:
            # in case the database connection does not work, login and try again
            # the codes are defined in https://www.postgresql.org/docs/current/errcodes-appendix.html#ERRCODES-TABLE
            self.log.warning(
                f'database connection {self.censored_config()} failed with InterfaceError'
                f' pgcode={error.pgcode} pgerror={error.pgerror} diag={error.diag},'
                f' trying again with a new connection...')
            with self.transaction() as transaction:
                return use_transaction(transaction)

    def query_one(self, statement: str, arguments: Union[Tuple, dict], map_row=None, column: str = None):
        def use_transaction(transaction):
            return transaction.query_one(statement, arguments, map_row, column)

        return self._attempt_transaction_twice(use_transaction)

    def query_all(self, statement: str, arguments: Union[Tuple, dict], map_row=None, column: str = None):
        def use_transaction(transaction):
            return transaction.query_all(statement, arguments, map_row, column)

        return self._attempt_transaction_twice(use_transaction)

    def execute_values(self, statement: str, arguments: list, template: str = None, map_row=None, column: str = None):
        """
        Like query_all, but with a list of arguments:
        statement must contain a single %s placeholder. args must be a list of sequences or mappings.
        template can be a string with placeholders, which will be used for each item.
        see https://www.psycopg.org/docs/extras.html#psycopg2.extras.execute_values
        """

        def use_transaction(transaction):
            return transaction.execute_values(statement, arguments, template, map_row, column)

        return self._attempt_transaction_twice(use_transaction)

    def execute(self, statement: str, arguments=None, use_transaction: bool = True):
        if use_transaction:
            def _use_transaction(transaction):
                return transaction.execute(statement, arguments)

            return self._attempt_transaction_twice(_use_transaction)

        connection = self._pool.getconn()
        try:
            connection.set_session(autocommit=True)
            try:
                return connection.cursor(cursor_factory=DictCursor).execute(statement, arguments)
            finally:
                connection.set_session(autocommit=False)
        finally:
            self._pool.putconn(connection)
