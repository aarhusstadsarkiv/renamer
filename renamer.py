from datetime import datetime
import sqlite3
from pathlib import Path
import argparse
import json
import xmltodict
import xml.etree.ElementTree as ET


# handles the parsing of arguments from user input
def argParser(args) -> argparse.Namespace:

    arg_parser: argparse.ArgumentParser = argparse.ArgumentParser(
        prog="renamer",
        description="cli tool to help with renaming original ingested files "
        "with missing or wrong extension",
        usage="%(prog)s [options] path puid suffix",
        epilog="",
    )

    arg_parser.add_argument(
        "Path", metavar="path", type=str, help="the path to the database"
    )

    arg_parser.add_argument(
        "Puid",
        metavar="puid",
        type=str,
        help="the puid of the files wich should have their extension updated",
    )

    arg_parser.add_argument(
        "Suffix",
        metavar="suffix",
        type=str,
        help="the suffix to be appended to the files",
    )

    arg_parser.add_argument(
        "--dryrun",
        action="store_true",
        help="finds and copies the mismatched files to a new folder"
        "and adds the recommended file extension to the copies. "
        "Overrrides normal functionality, only requires an path to the database",
    )

    return arg_parser.parse_args(args=args)


def main(args=None):
    args_parsed: argparse.Namespace = argParser(args)

    db_path = Path(args_parsed.Path)
    connection: sqlite3.Connection = sqlite3.connect(db_path)
    puid = str(args_parsed.Puid)
    new_suffix = str(args_parsed.Suffix)

    dryrun_print: bool = True

    ROOTPATH = Path(db_path).parent.parent
    DB_QUERY_GET_FILES: str = (
        f"SELECT relative_path, uuid FROM Files WHERE puid = '{puid}' "
        f"AND warning = 'Extension mismatch';"
    )

    cursor: sqlite3.Cursor = connection.cursor()
    cursor.execute(DB_QUERY_GET_FILES)
    rows: list = cursor.fetchall()
    if len(rows) > 40:
        dryrun_print = False
        print("Number of input files is greater than 40")
        print("Writing the output to txt file instead")
        timestamp: str = datetime.now.strftime("%Y_%m_%d_%H_%M")
        path_to_txt: Path = Path(ROOTPATH, ("renamer_dryrun" + timestamp))
        print("Path to the file: ", path_to_txt.__str__())

    for row in rows:
        rel_path: str = row[0].replace("\\", "/")
        absolute_path: Path = ROOTPATH / rel_path
        new_absolute_path: Path = Path(absolute_path, ("." + new_suffix))
        # if dryrun flag is set, the script skips over the remaining code and just
        # prints the old path and new path for inspection
        # either to stdout or to a file depending on volume
        if args_parsed.dryrun and dryrun_print:
            print(absolute_path, " -> ", new_absolute_path)
            continue
        elif args_parsed.dryrun:
            with open(path_to_txt, "a", encoding="utf-8") as f:  # type: ignore
                f.write(absolute_path.__str__() + " -> " + new_absolute_path.__str__())
            continue
        try:
            absolute_path.rename(new_absolute_path)
        except FileNotFoundError:
            print(f"Could not find file: {absolute_path}", flush=True)
        except Exception as e:
            print(f"Unable to rename {absolute_path}: {e}", flush=True)
        else:
            new_rel_path: str = row[0] + "." + new_suffix
            update_query: str = (
                f"UPDATE Files SET relative_path = '{new_rel_path}', "
                f"warning = 'Corrected extension mismatch' WHERE Files.uuid = '{row[1]}'"
            )
            try:
                cursor.execute(update_query)
            except Exception as e:
                print(f"Unable to update db for {new_rel_path}: {e}", flush=True)

    connection.commit()
    connection.close()


if __name__ == "__main__":
    main()
