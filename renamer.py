import sqlite3
import sys
import os
from pathlib import Path

def main():
    if sys.argv[1] == "--help":
        print(
            "Run with python renamer.py db_file_path puid new_extension_without_period_sign."
        )
    else:
        db_path = sys.argv[1]
        puid = sys.argv[2]
        new_suffix = sys.argv[3]

        ROOTPATH = Path(db_path).parent.parent
        DB_QUERY_GET_FILES = f"SELECT relative_path, uuid FROM Files WHERE puid = '{puid}' AND warning = 'Extension mismatch';"

        connection = sqlite3.connect(db_path)
        cursor = connection.cursor()

        cursor.execute(DB_QUERY_GET_FILES)
        rows = cursor.fetchall()
        for row in rows:
            rel_path = row[0].replace("\\", "/")
            absolute_path: Path = ROOTPATH / rel_path
            try:
                absolute_path.rename(str(absolute_path) + "." + new_suffix)
            except FileNotFoundError:
                print(f"Could not find file: {absolute_path}", flush=True)
            except Exception as e:
                print(f"Unable to rename {absolute_path}: {e}", flush=True)
            else:
                new_rel_path = row[0] + "." + new_suffix
                update_query = f"UPDATE Files SET relative_path = '{new_rel_path}', warning = 'Corrected extension mismatch' WHERE Files.uuid = '{row[1]}'"
                try:
                    cursor.execute(update_query)
                except Exception as e:
                    print(f"Unable to update db for {new_rel_path}: {e}", flush=True)

        connection.commit()
        connection.close()


if __name__ == "__main__":
    main()
