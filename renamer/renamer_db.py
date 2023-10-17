from os import PathLike
from pathlib import Path
from typing import Any, Union
from acacore.database.files_db import FileDB


class RenamerDB(FileDB):
    """Class for handling database calls for `renamer`."""

    def __init__(self, database: str) -> None:
        super().__init__(database=database)


    def get_files_based_on_warning(self) -> list[Any]:
        """Get a records UUID, PUID and warning from the files table, if the record has `warning = 'Extension mismatch'`.

        Returns
        -------
            list[Any]: list of records with UUID and PUID
        """
        result = self.execute("SELECT relative_path, uuid, puid, warning FROM Files WHERE warning = 'Extension mismatch';")
        return result.fetchall()

    def get_relpath_uuid(self, puid: str) -> list:
        """Get the `relative_path` and `uuid` from all records with the given `puid`.

        Args:
        ----
            puid (str): The `puid` to search for in the records.

        Returns:
        -------
            list: A list of the resulting records.
        """
        result = self.execute(f"SELECT relative_path, uuid FROM Files WHERE puid = '{puid}'"
            f"AND warning = 'Extension mismatch';")
        return result.fetchall()
    
    def update_relative_path(self, new_rel_path: Union[str, PathLike, Path], uuid: str, new_suffix: str) -> None:
        """Update the relative path for the record with the given uuid and makes a `history` record of the update.

        Args:
        ----
            new_rel_path (Union[str, PathLike, Path]): The fulle new realtive path, with stem and suffix.
            uuid (str): The `UUID` of the file which path should be updated.
            new_suffix: The new suffix for the file. Is only used to create the `history` record. 
        """
        new_suffix 
        
        self.execute(
                sql=f"UPDATE Files SET relative_path = '{new_rel_path}', "
                f"warning = 'Corrected extension mismatch' WHERE Files.uuid = '{uuid}'",
                )
        
        self.add_history(uuid=uuid,
                         operation="renamer: Changed the suffix of the file",
                         data="Changes suffix from")
        self.commit()
