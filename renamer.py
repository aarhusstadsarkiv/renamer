import sqlite3
import sys
import os
from pathlib import Path


if __name__ == "__main__":
    if sys.argv[1] == "--help":
        print("Run with python renamer.py db_file_path puid new_extension.")
    else:
        db_path = sys.argv[1]
        puid = sys.argv[2]
        new_suffix = sys.argv[3]
        
        ROOTPATH = Path(db_path).parent.parent
        
        DB_QUERY_GET_FILES = f"SELECT relative_path, uuid FROM Files WHERE puid = '{puid}' AND warning = 'Extension mismatch';"
        
        connection = sqlite3.connect(db_path)
        cursor = connection.cursor()

        rows = cursor.execute(DB_QUERY_GET_FILES)

        for row in rows:
            rel_path = row[0].replace("\\", "/")
            absolute_path: Path = ROOTPATH / rel_path
            try:
                absolute_path.rename(str(absolute_path) + "." + new_suffix)
                new_rel_path = row[0] + "." + new_suffix
                update_query = f"UPDATE Files SET relative_path = '{new_rel_path}', warning = 'Corrected extension mismatch' WHERE Files.uuid = '{row[1]}'"
                cursor.execute(update_query)
                connection.commit()
            except FileNotFoundError:
                print("Could not find the file with path: " + str(absolute_path))
        connection.close()
        
