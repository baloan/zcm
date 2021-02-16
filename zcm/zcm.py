#!/usr/bin/env python3
# encoding: utf8

# Copyright (c) 2016 Andreas
# See LICENSE for details.

""" ZODB context manager """

# from BTrees.IOBTree import IOBTree
# from BTrees.OOBTree import OOBTree
# from persistent import Persistent
# from persistent.list import PersistentList as PList
# from persistent.mapping import PersistentMapping as PDict

from ZEO.ClientStorage import ClientStorage
from ZODB import DB
from ZODB.FileStorage import FileStorage


class ZDatabase():
    """ Provides a ZODB database context manager """

    def __init__(self, uri, **kwargs):
        self.storage = create_storage(uri)
        self.db = DB(self.storage, **kwargs)

    def __enter__(self):
        return self.db

    def __exit__(self, exc_type, exc_value, traceback):
        self.db.close()
        return False


class ZConnection():
    """ Provides a ZODB connection with auto-abort (default).
    Provides a tuple of connection and root object:
        with ZConnection(db) as (cx, root):
            root.one = "ok"
    ZConnection implements a connection context manager.
    Transaction context managers in contrast do auto-commit:
        a) with db.transaction() as connection, or
        b) with cx.transaction_manager as transaction, or
        c) with transaction.manager as transaction  (for the thread-local transaction manager)
    See also http://www.zodb.org/en/latest/guide/transactions-and-threading.html
    """
    def __init__(self, db, auto_commit=False, transaction_manager=None):
        self.db = db
        self.auto_commit = auto_commit
        self.transaction_manager = transaction_manager
        self.cx = None

    def __enter__(self):
        if self.transaction_manager:
            self.cx = self.db.open(self.transaction_manager)
        else:
            self.cx = self.db.open()
        return self.cx, self.cx.root()

    def __exit__(self, exc_type, exc_value, traceback):
        if self.auto_commit:
            self.cx.transaction_manager.commit()
        self.cx.close()
        return False


def create_storage(uri):
    """ supported URIs
    file://e:/workspaces/zeo/bots.fs
    zeo://localhost:8001
    e:/workspaces/zeo/bots.fs
    @see https://en.wikipedia.org/wiki/Uniform_Resource_Identifier
    """
    if uri.startswith("file://"):
        storage = FileStorage(uri[7:])
    elif uri.startswith("zeo://"):
        addr, port = uri[6:].split(":")
        # addr_ = addr.encode("ASCII")
        storage = ClientStorage((addr, int(port)))
    else:
        storage = FileStorage(uri)
    return storage


def database(uri):
    """ convenience function for single thread, return one connection from the pool """
    storage = create_storage(uri)
    db = DB(storage)
    return db


def connection(db):
    """ Convenience function for multi thread, returns
    connection, transaction manager and root
    """
    cx = db.open()
    return cx, cx.root()
