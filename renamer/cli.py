import sqlite3
import sys
import os
from pathlib import Path
import requests
import argparse
import json
import shutil
import xmltodict
import xml.etree.ElementTree as ET


def main():
    arg_parser = argparse.ArgumentParser(
        prog="renamer",
        description="cli tool to help with renaming original ingested files with missing or wrong extension",
        usage="%(prog)s [options] path puid suffix",
        epilog="",
    )

    # the if statement is there to handle the dryrun case,
    # some duplicate code, but there does not seem to be an elegant fix
    if "--dryrun" not in sys.argv:
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
            help="finds and copies the mismatched files to a new folder and adds the recommended file extension to the copies. Overrrides normal functionality, only requires an path to the database",
        )

        arg_parser.add_argument(
            "--update_puid",
            action="store_true",
            help="updates the puid's associeted with file extensions from the relevant repositories",
        )
    else:
        arg_parser.add_argument(
            "Path", metavar="path", type=str, help="the path to the database"
        )

        arg_parser.add_argument(
            "--dryrun",
            action="store_true",
            help="finds and copies the mismatched files to a new folder and adds the recommended file extension to the copies. Overrrides normal functionality, only requires an path to the database",
        )

        arg_parser.add_argument(
            "--update_puid",
            action="store_true",
            help="updates the puid's associeted with file extensions from the relevant repositories",
        )

    args = arg_parser.parse_args()

    # checks if the extension files can be updated if the --update flag is set
    if args.update_puid:
        try:
            respons_national_arc = requests.head(
                "https://cdn.nationalarchives.gov.uk/documents/DROID_SignatureFile_V107.xml"
            )
            if respons_national_arc.status_code == 200:
                response_national_arc = requests.get(
                    "https://cdn.nationalarchives.gov.uk/documents/DROID_SignatureFile_V107.xml"
                )
                with open(
                    "national_archive.xml", "w", encoding="utf-8"
                ) as national_arc_file:
                    national_arc_file.write(response_national_arc.content)
                    national_arc_file.close()

            respons_aca = requests.get(
                "https://raw.githubusercontent.com/aarhusstadsarkiv/digiarch/master/digiarch/core/custom_sigs.json"
            )
            with open("aca_file_extension.json", "w", encoding="utf-8") as aca_file:
                data_json = respons_aca.json()
                json.dump(data_json, fp=aca_file, indent=4)
                aca_file.close()
        except Exception as e:
            print(f"Error updating extension files: {e}")

    # creates a dict overview of the puid and the file extensions associated with them
    xml_parser = ET.XMLParser(encoding="utf-8")
    tree = ET.parse("national_archive.xml", parser=xml_parser)
    xml_data = tree.getroot()
    xmlstr: str = ET.tostring(xml_data, encoding="utf-8", method="xml")
    data_dict = dict(xmltodict.parse(xmlstr))
    puid_to_extensions_dict: dict = {}

    for entry in (
        data_dict.get("ns0:FFSignatureFile")
        .get("ns0:FileFormatCollection")
        .get("ns0:FileFormat")
    ):
        puid: str = entry.get("@PUID")
        extension = entry.get("ns0:Extension")
        if isinstance(extension, str):
            puid_to_extensions_dict[puid] = extension
        elif isinstance(extension, list):
            puid_to_extensions_dict[puid] = extension[0]

    with open("aca_file_extension.json", "r", encoding="utf-8") as file:
        aca_puid_json = json.load(file)

    aca_puid_dict: dict = {}
    for entry in aca_puid_json:
        aca_puid: str = entry.get("puid")
        extension = entry.get("extension")
        if isinstance(extension, str):
            aca_puid_dict[aca_puid] = extension.replace(".", "")
        elif isinstance(extension, list):
            aca_puid_dict[aca_puid] = extension[0].replace(".", "")

    # actual functionality of the script. Split in two cases: dryrun and normal run¨
    if args.dryrun:
        db_path = args.Path
        db_parent_direct = Path(db_path).parent
        ROOTPATH = Path(db_path).parent.parent
        new_directory_absolute_path: Path = (
            db_parent_direct / "copied_files_updated_ext"
        )
        all_puid: list = []
        path_puid_dict: dict = {}

        DB_QUERY_GET_FILES = "SELECT relative_path, uuid, puid, warning FROM Files WHERE warning = 'Extension mismatch';"

        connection = sqlite3.connect(db_path)
        cursor = connection.cursor()

        cursor.execute(DB_QUERY_GET_FILES)
        rows = cursor.fetchall()

        try:
            os.mkdir(new_directory_absolute_path)
        except FileExistsError:
            # the directory could have been created during another, failed run
            pass

        # gets all relevant puid from the db
        for row in rows:
            if row[2] not in all_puid:
                all_puid.append(row[2])

        # creates a new directory for all puids
        # also makes a dict to translate between puid and path compliant puid
        for puid in all_puid:
            path_puid = puid.replace("/", "-")
            path_puid_dict[puid] = path_puid
            try:
                os.mkdir(new_directory_absolute_path / path_puid)
            except FileExistsError:
                # the directory could have been created during another, failed run
                pass

        # copies files with new extension
        for row in rows:
            rel_path = row[0].replace("\\", "/")
            puid = row[2]

            # checks where to find relevant puid
            if "aca" in puid:
                new_suffix: str = aca_puid_dict.get(puid)
            else:
                new_suffix: str = puid_to_extensions_dict.get(puid)

            # constructs the paths to the new files
            absolute_path_file: Path = ROOTPATH / rel_path
            new_filename: str = os.path.basename(absolute_path_file)
            path_puid: str = path_puid_dict.get(puid)
            new_file_absolute_path: Path = (
                new_directory_absolute_path / path_puid / new_filename
            )

            # maybe use shutil.copyfile instead, should be faster. See if it becomes bottleneck
            shutil.copy2(absolute_path_file, new_directory_absolute_path / path_puid)

            # an ugly solution to en edge case: two files can have the same name, and when we rename them
            # the renamer throws an error. It is caugth and a counter is appended to the file. If this file
            # also exists it goes back in the loop, increment i and tries again
            i = 1
            while True:
                try:
                    new_file_absolute_path.rename(
                        str(new_file_absolute_path) + "." + new_suffix
                    )
                    break
                except FileNotFoundError:
                    print(f"Could not find file: {new_file_absolute_path}", flush=True)
                    break
                except WindowsError:
                    try:
                        new_file_absolute_path.rename(
                            str(new_file_absolute_path)
                            + "("
                            + str(i)
                            + ")"
                            + "."
                            + new_suffix
                        )
                        break
                    except WindowsError:
                        i += 1
                        continue
                except Exception as e:
                    print(f"Unable to rename {new_file_absolute_path}: {e}", flush=True)
                    break

        connection.commit()
        connection.close()

    # the standart case
    else:
        db_path = args.Path
        puid = args.Puid
        new_suffix = args.Suffix

        ROOTPATH = Path(db_path).parent.parent
        DB_QUERY_GET_FILES: str = (
            f"SELECT relative_path, uuid FROM Files WHERE puid = '{puid}' "
            f"AND warning = 'Extension mismatch';"
        )

        connection = sqlite3.connect(db_path)
        cursor = connection.cursor()

        cursor.execute(DB_QUERY_GET_FILES)
        rows: list = cursor.fetchall()
        for row in rows:
            rel_path: str = row[0].replace("\\", "/")
            absolute_path: Path = ROOTPATH / rel_path
            try:
                absolute_path.rename(str(absolute_path) + "." + new_suffix)
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
