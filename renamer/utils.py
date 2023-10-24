"""Handlees utilities scuh as updating from reference files and updating these."""


import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Tuple

import requests
import xmltodict
from acacore.models.file import File
from renamer_db import RenamerDB


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

def rename_file_and_update_db(row: File, root_path: Path, new_suffix: str, db: RenamerDB) -> None:
    """Append the given `File` in the database with the given extension.

    Also updates the database entry and creates a history entry.

    Args:
    ----
        row (File): The `File` object to be updated, taken from the database
        root_path (Path): The root path to the files folder
        new_suffix (str): The new suffix to be appended
        db (RenamerDB): The database (files.db)

    Raises:
    ------
        FileNotFoundError: If the file cant be found, throws this error.
        e: For all other errors in the renaming process, it jsut simply raises those errors.
    """
    rel_path: str = row.relative_path.replace("\\", "/")  # type: ignore
    absolute_path: Path = root_path / rel_path
    try:
        absolute_path.rename(str(absolute_path) + "." + new_suffix)
    except FileNotFoundError:
        raise FileNotFoundError
    except Exception as e:
        raise e
    else:
        new_rel_path: str = row.relative_path + "." + new_suffix
        try:
            db.update_relative_path(new_rel_path=new_rel_path, uuid=row.uuid, new_suffix=new_suffix)
        except Exception as e:
            print(f"Unable to update db for {new_rel_path}: {e}", flush=True)
