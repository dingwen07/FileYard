import os
import sqlite3
from datetime import datetime
import sys
import configparser

unixtime = lambda: int(datetime.now().timestamp())


class FileYard(object):
    def __init__(self, database: str) -> None:
        self.db = sqlite3.connect(database=database)
        self.db_cursor = self.db.cursor()
        if not os.path.exists(database):
            self.db_cursor.execute('''CREATE TABLE "media" (
                                        "id"                INTEGER NOT NULL UNIQUE,
                                        "type"              TEXT NOT NULL,
                                        "label"             INTEGER,
                                        "verify_time"       INTEGER NOT NULL,
                                        "verify_interval"   INTEGER NOT NULL,
                                        "status"            INTEGER NOT NULL DEFAULT 0,
                                        "meta"              TEXT,
                                        PRIMARY KEY("id" AUTOINCREMENT)
                                    );''')
            self.db_cursor.execute('''CREATE TABLE "directory" (
                                        "id"        INTEGER NOT NULL UNIQUE,
                                        "name"      TEXT NOT NULL,
                                        "parent_id" INTEGER NOT NULL,
                                        "status"    INTEGER NOT NULL DEFAULT 0,
                                        "meta"      TEXT,
                                        PRIMARY KEY("id" AUTOINCREMENT),
                                        FOREIGN KEY("parent_id") REFERENCES "directory"("id")
                                    );''')
            self.db_cursor.execute(
                '''INSERT INTO "main"."directory" ("id", "name", "parent_id") VALUES (0, 'root', 0);'''
            )
            self.db_cursor.execute('''CREATE TABLE "file" (
                                        "id"            INTEGER NOT NULL UNIQUE,
                                        "name"          TEXT NOT NULL,
                                        "checksum"      TEXT NOT NULL,
                                        "directory_id"  INTEGER NOT NULL,
                                        "status"        INTEGER NOT NULL DEFAULT 0,
                                        "meta"          TEXT,
                                        FOREIGN KEY("directory_id") REFERENCES "directory"("id"),
                                        PRIMARY KEY("id" AUTOINCREMENT)
                                    );''')
            self.db_cursor.execute(
                '''INSERT INTO "main"."file" ("id", "checksum", "name", "directory_id", "meta") VALUES (0, 'null', 'null', 0, '{"checksum_algorithm": "md5"}');'''
            )
            self.db_cursor.execute('''CREATE TABLE "connection" (
                                        "id"            INTEGER NOT NULL UNIQUE,
                                        "media_id"      INTEGER NOT NULL,
                                        "directory_id"  INTEGER NOT NULL,
                                        "status"        INTEGER NOT NULL DEFAULT 0,
                                        "meta"          TEXT,
                                        FOREIGN KEY("directory_id") REFERENCES "directory"("id"),
                                        FOREIGN KEY("media_id") REFERENCES "media"("id"),
                                        PRIMARY KEY("id" AUTOINCREMENT)
                                    );''')
            self._save_data()

    def add_media(self,
                  label: str = '',
                  type: str = 'hdd',
                  verify_interval: int = 2592000) -> int:
        self.db_cursor.execute(
            '''INSERT INTO "main"."media" ("type", "label", "verify_time", "verify_interval") VALUES (?, ?, ?, ?);''',
            type, label, unixtime(), verify_interval)
        media_id = self.db_cursor.lastrowid
        self._save_data()
        return media_id

    def add_directory(self, name: str, parent_id: int) -> int:
        self.db_cursor.execute(
            '''INSERT INTO "main"."directory" ("name", "parent_id") VALUES (?, ?);''',
            str(name), int(parent_id))
        directory_id = self.db_cursor.lastrowid
        self._save_data()
        return directory_id

    def add_file(self, name: str, checksum: str, directory_id: int) -> int:
        self.db_cursor.execute(
            '''INSERT INTO "main"."file" ("name", "checksum", "directory_id") VALUES (?, ?, ?);''',
            str(name), str(checksum), int(directory_id))
        file_id = self.db_cursor.lastrowid
        self._save_data()
        return file_id

    def add_connection(self, media_id: int, directory_id: int = 0) -> int:
        self.db_cursor.execute(
            '''INSERT INTO "main"."connection" ("media_id", "directory_id") VALUES (?, ?);''',
            int(media_id), int(directory_id))

    def _save_data(self):
        self.db.commit()

    @staticmethod
    def connect(database: str) -> 'FileYard':
        return FileYard(database)


if __name__ == "__main__":
    y = FileYard.connect('file_yard.db')
    print(y)