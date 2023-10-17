import argparse
import contextlib
import json
import os
import shutil
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Tuple, Union

import requests
import xmltodict
from acacore.models.file import File

from renamer.renamer_db import RenamerDB


def main():
    # Read the arguments given from the command line.
    args: argparse.Namespace = make_arg_parser()

    # checks if the extension files should be updated
    if args.update_puid:
        update_ref_files()

    # A bit of path juggling, to get all the relevant files together.
    db_path: str = args.Path
    db = RenamerDB(db_path)
    db_parent_direct: Path = Path(db_path).parent
    ROOTPATH: Path = Path(db_path).parent.parent
    new_directory_absolute_path: Path = db_parent_direct / "copied_files_updated_ext"

    # actual functionality of the script. Split based on what flags are set.
    if args.dryrun:
        dryrun(new_directory_absolute_path=new_directory_absolute_path, root_path=ROOTPATH, db=db)
    elif args.update_based_on_uuids:
        uuid_run(uuid=args.uuid, new_suffix=args.suffix, root_path=ROOTPATH, db=db)
    else:
        standard_run(puid=args.puid, new_suffix=args.suffix, root_path=ROOTPATH, db=db)


def standard_run(puid: str, new_suffix: str, root_path: Path, db: RenamerDB) -> None:
    """Run with no additional flags set.

    Simply takes the relevant files based on PUID and updates their suffix.

    Args:
    ----
        puid (str): The puid of the files t update the suffix of
        new_suffix (str): the new suffix to give the files
        root_path (Path): The root_path, based on where the files.db is located
        db (RenamerDB): The database handler.
    """
    rows: list[File] = db.get_files_with_warning_and_puid(puid)  # type: ignore
    for row in rows:
        rel_path: str = row.relative_path.replace("\\", "/")  # type: ignore
        absolute_path: Path = root_path / rel_path
        try:
            absolute_path.rename(str(absolute_path) + "." + new_suffix)
        except FileNotFoundError:
            print(f"Could not find file: {absolute_path}", flush=True)
        except Exception as e:
            print(f"Unable to rename {absolute_path}: {e}", flush=True)
        else:
            new_rel_path: str = row.relative_path + "." + new_suffix
            try:
                db.update_relative_path(new_rel_path=new_rel_path, uuid=row.uuid, new_suffix=new_suffix)
            except Exception as e:
                print(f"Unable to update db for {new_rel_path}: {e}", flush=True)


def dryrun(
    new_directory_absolute_path: Path,
    root_path: Path,
    db: RenamerDB,
) -> None:
    rows: list[File] = db.get_files_based_on_warning()
    aca_puid_dict, puid_to_extensions_dict = load_in_external_files()
    all_puid: list = []
    path_puid_dict: dict = {}

    with contextlib.suppress(FileExistsError):
        os.mkdir(new_directory_absolute_path)

    # gets all relevant puid from the db
    for row in rows:
        if row.puid not in all_puid:
            all_puid.append(row[2])

    # creates a new directory for all puids
    # also makes a dict to translate between puid and path compliant puid
    for puid in all_puid:
        path_puid = puid.replace("/", "-")
        path_puid_dict[puid] = path_puid
        with contextlib.suppress(FileExistsError):
            os.mkdir(new_directory_absolute_path / path_puid)

    # copies files with new extension
    for row in rows:
        rel_path = row.relative_path.replace("\\", "/")
        puid = row.puid

        # checks where to find relevant puid
        if "aca" in puid:
            new_suffix: str = aca_puid_dict.get(puid)  # type: ignore
        else:
            new_suffix: str = puid_to_extensions_dict.get(puid)  # type: ignore

        # constructs the paths to the new files
        absolute_path_file: Path = root_path / rel_path
        new_filename: str = os.path.basename(absolute_path_file)
        path_puid: str = path_puid_dict.get(puid)  # type: ignore
        new_file_absolute_path: Path = new_directory_absolute_path / path_puid / new_filename

        # maybe use shutil.copyfile instead, should be faster. See if it becomes bottleneck
        shutil.copy2(absolute_path_file, new_directory_absolute_path / path_puid)

        # an ugly solution to en edge case: two files can have the same name, and when we rename them
        # the renamer throws an error. It is caught and a counter is appended to the file. If this file
        # also exists it goes back in the loop, increments and tries again
        i = 1
        while True:
            try:
                new_file_absolute_path.rename(str(new_file_absolute_path) + "." + new_suffix)
                break
            except FileNotFoundError:
                print(f"Could not find file: {new_file_absolute_path}", flush=True)
                break
            except OSError:
                try:
                    new_file_absolute_path.rename(
                        str(new_file_absolute_path) + "(" + str(i) + ")" + "." + new_suffix,
                    )
                    break
                except OSError:
                    i += 1
                    continue
            except Exception as e:
                print(f"Unable to rename {new_file_absolute_path}: {e}", flush=True)
                break


def uuid_run(uuid: Union[str, list[str]], new_suffix: str, root_path: Path, db: RenamerDB) -> None:
    pass
    # functionality to be developed


def make_arg_parser() -> argparse.Namespace:
    """Return an argpaser that has intialized the arguments from sys.args."""
    arg_parser = argparse.ArgumentParser(
        prog="renamer",
        description="cli tool to help with renaming original ingested files with missing or wrong extension",
        usage="%(prog)s [options] path_to_database puid suffix",
        epilog="",
    )

    # the if statement is there to handle the dryrun case,
    # some duplicate code, but there does not seem to be an elegant fix
    if "--update_based_on_uuids" in sys.argv:
        arg_parser.add_argument(
            "Path",
            metavar="path",
            type=str,
            help="the path to the database",
        )

        arg_parser.add_argument(
            "Uuid",
            metavar="uuid",
            type=Union[str, list[str]], # TODO: This might be wrong, the parser might not be able to parse a list
            help="the uuid or a list of uuids indicating the files which should have their extension updated",
        )

        arg_parser.add_argument(
            "Suffix",
            metavar="suffix",
            type=str,
            help="the suffix to be appended to the file(s)",
        )

        arg_parser.add_argument(
            "--update_based_on_uuids",
            action="store_true",
            help="Updates files based on one or more given UUID. "
            "Overrides default functionality and takes a uuid or a list of uuid instead of a puid",
        )
    if "--dryrun" in sys.argv:
        arg_parser.add_argument(
            "Path",
            metavar="path",
            type=str,
            help="the path to the database",
        )

        arg_parser.add_argument(
            "--dryrun",
            action="store_true",
            help="finds and copies the mismatched files to a new folder and adds the recommended file "
            "extension to the copies. Overrides normal functionality, only requires an path to the database",
        )

        arg_parser.add_argument(
            "--update_puid",
            action="store_true",
            help="updates the puid's associated with file extensions from the relevant repositories",
        )

        arg_parser.add_argument(
            "--update_based_on_uuids",
            action="store_true",
            help="Updates files based on one or more given UUID. "
            "Overrides default functionality and takes a uuid or a list of uuid instead of a puid",
        )
    elif "--dryrun" not in sys.argv and "--update_based_on_uuids" not in sys.argv:
        arg_parser.add_argument(
            "Path",
            metavar="path",
            type=str,
            help="the path to the database",
        )

        arg_parser.add_argument(
            "Puid",
            metavar="puid",
            type=str,
            help="the puid of the files which should have their extension updated",
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
            help="finds and copies the mismatched files to a new folder and adds the recommended file "
            "extension to the copies. Overrides normal functionality, only requires an path to the database",
        )

        arg_parser.add_argument(
            "--update_puid",
            action="store_true",
            help="updates the puid's associated with file extensions from the relevant repositories",
        )

        arg_parser.add_argument(
            "--update_based_on_uuids",
            action="store_true",
            help="Updates files based on one or more given UUID. "
            "Overrides default functionality and takes a uuid or a list of uuid instead of a puid",
        )

    return arg_parser.parse_args()


def load_in_external_files() -> Tuple[dict, dict]:
    """Load in the external files from PRONOM and ACA's own `reference_files`.

    The scripts only loads in local copies of these files, unless we manually update with `--update_puid`.

    Returns
    -------
        Tuple[dict, dict]: A tuple, where the first is a dict of ACA's own ID's, and the other is from PRONOM.
    """
    # creates a dict overview of the puid and the file extensions associated with them
    xml_parser = ET.XMLParser(encoding="utf-8")
    tree = ET.parse(Path(__file__).parent / "national_archive.xml", parser=xml_parser)
    xml_data = tree.getroot()
    xmlstr: str = ET.tostring(xml_data, encoding="utf-8", method="xml")
    data_dict: dict[str, dict] = dict(xmltodict.parse(xmlstr))
    puid_to_extensions_dict: dict = {}

    for entry in data_dict.get("ns0:FFSignatureFile").get("ns0:FileFormatCollection").get("ns0:FileFormat"):  # type: ignore
        puid: str = entry.get("@PUID")
        extension = entry.get("ns0:Extension")
        if isinstance(extension, str):
            puid_to_extensions_dict[puid] = extension
        elif isinstance(extension, list):
            puid_to_extensions_dict[puid] = extension[0]

    with open(Path(__file__).parent / "aca_file_extension.json", encoding="utf-8") as file:
        aca_puid_json = json.load(file)

    aca_puid_dict: dict = {}
    for entry in aca_puid_json:
        aca_puid: str = entry.get("puid")
        extension = entry.get("extension")
        if isinstance(extension, str):
            aca_puid_dict[aca_puid] = extension.replace(".", "")
        elif isinstance(extension, list):
            aca_puid_dict[aca_puid] = extension[0].replace(".", "")

    return (aca_puid_dict, puid_to_extensions_dict)


def update_ref_files():
    try:
        respons_national_arc = requests.head(
            "https://cdn.nationalarchives.gov.uk/documents/DROID_SignatureFile_V107.xml",
        )
        if respons_national_arc.status_code == 200:
            response_national_arc = requests.get(
                "https://cdn.nationalarchives.gov.uk/documents/DROID_SignatureFile_V107.xml",
            )
            with open(
                Path(__file__).parent / "national_archive.xml",
                "w",
                encoding="utf-8",
            ) as national_arc_file:
                national_arc_file.write(str(response_national_arc.content))
                national_arc_file.close()
        respons_aca = requests.get(
            "https://raw.githubusercontent.com/aarhusstadsarkiv/digiarch/master/digiarch/core/custom_sigs.json",
        )
        with open(Path(__file__).parent / "aca_file_extension.json", "w", encoding="utf-8") as aca_file:
            data_json = respons_aca.json()
            json.dump(data_json, fp=aca_file, indent=4)
            aca_file.close()
    except Exception as e:
        # Lazy exception handling, but if something goes wrong here it really isn't a big deal.
        print(f"Error updating extension files: {e}")


if __name__ == "__main__":
    main()
