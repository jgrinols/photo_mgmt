"""container module for DbConnectionPool"""
from __future__ import annotations
from contextlib import asynccontextmanager
from typing import Tuple

from aiomysql import DictCursor,Connection,create_pool

class DbConnectionPool():
    """Provides a context manager compatible connection to the given database"""
    instance: DbConnectionPool = None

    def __init__(self, pool):
        self.__pool = pool

    @staticmethod
    def get() -> DbConnectionPool:
        """gets the DbConnectionPool singleton"""
        if not DbConnectionPool.instance:
            raise RuntimeError("DbConnectionPool is not initialized")
        return DbConnectionPool.instance

    @staticmethod
    async def initialize(host: str, port: int, user: str, pw: str):
        """creates a new DbConnectionPool instance"""
        DbConnectionPool.instance = DbConnectionPool(
            await create_pool(host=host,port=port,user=user,password=pw,db="mysql")
        )

    def __enter__(self):
        return self

    def __exit__(self, expt_type, expt_value, traceback):
        self.__pool.close()
        self.__pool.wait_closed()

    @asynccontextmanager
    async def acquire_connection(self, **kwargs) -> Connection:
        """gets a connection from the pool"""
        try:
            conn = await self.__pool.acquire()
            if "db" in kwargs:
                await conn.select_db(kwargs["db"])
            yield conn

        finally:
            self.__pool.release(conn)

    @asynccontextmanager
    async def acquire_dict_cursor(self, **kwargs) -> Tuple[DictCursor,Connection]:
        """Gets a dictionary cursor and its connection. [async, contextmanager]"""
        conn = await self.__pool.acquire()
        if "db" in kwargs:
            await conn.select_db(kwargs["db"])
        cur = await conn.cursor(DictCursor)
        try:
            yield (cur,conn)

        finally:
            await cur.close()
            await conn.ensure_closed()
            self.__pool.release(conn)

    async def clear(self):
        """Close all free connecctions in the pool."""
        await self.__pool.clear()

    def terminate(self):
        """Immediately closes all connections associated with the pool"""
        self.__pool.terminate()
