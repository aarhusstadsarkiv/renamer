import argparse
import contextlib
import os
import shutil
import sys
from pathlib import Path
from typing import Optional, Union

from acacore.models.file import File

from renamer.renamer_db import RenamerDB
from renamer.utils import load_in_external_files, rename_file_and_update_db, update_ref_files


def main():
    # Read the arguments given from the command line.
    args: argparse.Namespace = make_arg_parser()

    # checks if the extension files should be updated
    try:
        if args.update_puid:
            update_ref_files()
    except AttributeError:
        # The argument is not in the namespace (the list of arguments), soo we simply move on
        pass

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
        try:
            rename_file_and_update_db(file=row, root_path=root_path, new_suffix=new_suffix, db=db)
        except FileNotFoundError:
            print(f"Could not find the file located at {row.relative_path}. "
                  "The renaming of the file was stopped")
        except Exception as e:
            print(f"The renaming of the file threw the following exception: {e}. "
                  "The renaming of the file was stopped.")


def dryrun(
    new_directory_absolute_path: Path,
    root_path: Path,
    db: RenamerDB,
) -> None:
    """Run as dryrun, where the files are copies into a new dir for manual checking.

    Does NOT create a history entry in the database. This type of run is only meant as a test.

    Args:
    ----
        new_directory_absolute_path (Path): the new dir the files are copied into.
        root_path (Path): The root path of the run
        db (RenamerDB): The database where the file are to be updated.
    """
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
    """Run renamer on a single file or a list of files and updates their extensions.

    Also makes a new history entry to the db detailing what has happened for the file.

    Args:
    ----
        uuid (Union[str, list[str]]): A single uuid or a list of uuids
        new_suffix (str): The new suffix to append to the file
        root_path (Path): The root path
        db (RenamerDB): The database to be updated
    """
    # firstly, we make sure that the input is a list (makes the later logic easier)
    if isinstance(uuid, str):
        files_uuid: list[str] = [uuid]

    # we iterate over the files(s) and append the new suffix to the file stem.
    for file_uuid in files_uuid:
        # we get the row
        row: Optional[File] = db.files.select(where=f"uuid IS {file_uuid}").fetchone()
        # If the UUID is not present in the db, print it to the user and continue to next iteration
        if not row:
            print(f"Error: The UUID {file_uuid} does not exist in the database.")
            continue
        # we update the db and rename the file.
        rename_file_and_update_db(row=row, root_path=root_path, new_suffix=new_suffix, db=db)


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
            type=Path,
            help="the path to the database",
        )

        arg_parser.add_argument(
            "Uuid",
            metavar="uuid",
            type=str,
            nargs='+',
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


if __name__ == "__main__":
    main()
