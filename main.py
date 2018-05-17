import datetime
import errno
import os
import sqlite3
from sqlite3 import Cursor, Connection

import redis

from data import PROVIDERS, DIRECTORIES


def mkdir(path: str) -> None:
    try:
        os.mkdir(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST:
            if os.path.isdir(path):
                return
            else:
                print(path + " exists and is not a directory")
        raise


def detail_key_for_name(name: str) -> str:
    return name + ":provider-detail"


def search_state_key_for_name(name: str) -> str:
    return name + ":last-search"


def ambiguous_key_for_name(name: str) -> str:
    return name + ":provider-ambiguous"


def listing_key_for_name(name: str) -> str:
    return name + ":provider-list"


def network_key_for_name(name: str) -> str:
    return name + ":provider-network"


class Archive:
    def __init__(self):
        self._r = redis.StrictRedis()
        self._now = datetime.datetime.utcnow()

    def archive_name(self, ident: str, description: str) -> str:
        return ".".join([
            "MH",
            ident,
            description,
            str(self._now.month),
            str(self._now.day),
            str(self._now.year)
        ]).lower()

    @staticmethod
    def _create_db(file_name: str) -> Connection:
        full_name = file_name + ".db"
        already_exists = os.path.exists(full_name)
        conn: Connection = sqlite3.connect(full_name)

        if already_exists:
            return conn

        c: Cursor = conn.cursor()
        c.execute(
            '''CREATE TABLE data (key text NOT NULL, value text NOT NULL)''')
        c.execute('''CREATE UNIQUE INDEX idx_data_key ON data (key)''')
        conn.commit()
        return conn

    def _store_hash(self, key: str, pid: str, desc: str):
        if not self._r.exists(key):
            print("Skipping " + key)
            return

        file_name = "./archive/{}/{}".format(pid, self.archive_name(pid, desc))
        with Archive._create_db(file_name) as conn:
            print("Would store hash " + key)

    def _process_provider(self, pid: str, name: str) -> None:
        print("Processing provider " + name)
        mkdir("./archive/" + pid)
        self._store_hash(listing_key_for_name(pid), pid, "listing")
        self._store_hash(detail_key_for_name(pid), pid, "detail")
        self._store_hash(ambiguous_key_for_name(pid), pid, "ambiguous")
        self._store_hash(network_key_for_name(pid), pid, "network")

    def _process_directory(self, did: str, name: str) -> None:
        print("Processing directory " + name)
        mkdir("./archive/" + did)
        self._store_hash(listing_key_for_name(did), did, "listing")
        self._store_hash(detail_key_for_name(did), did, "detail")
        self._store_hash(ambiguous_key_for_name(did), did, "ambiguous")

    def archive(self) -> None:
        mkdir("./archive")
        for key, value in PROVIDERS.items():
            pid = value['id']
            name = value['name']
            self._process_provider(pid, name)
        for key, value in DIRECTORIES.items():
            pid = value['id']
            name = value['name']
            self._process_directory(pid, name)


if __name__ == "__main__":
    Archive().archive()
