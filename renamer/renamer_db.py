import datetime
from os import PathLike
from pathlib import Path
from typing import Union

import pydantic
from acacore.database.files_db import FileDB
from acacore.models.file import File


class RenamerDB(FileDB):
    """Class for handling database calls for `renamer`."""

    def __init__(self, database: Path) -> None:
        super().__init__(database=database)
        self.files = self.create_table("files", File)

    def get_files_based_on_warning(self) -> list[File]:
        """Get a records from the files table, where `warning = 'Extension mismatch'`.

        Returns
        -------
            list[File]: list of records
        """
        rows = self.files.select(where="warning = 'Extension mismatch';").fetchall()
        try:
            file_validator = pydantic.TypeAdapter(list[File])
            files = file_validator.validate_python(rows)
        except pydantic.ValidationError:
            raise pydantic.ValidationError("Failed to parse files as ArchiveFiles.")
        return files

    def get_files_with_warning_and_puid(self, puid: str) -> list[File]:
        """Get a list of `File` records with the given `puid` annd where warning = 'Extension mismatch'.

        Args:
        ----
            puid (str): The `puid` to search for in the records.

        Returns:
        -------
            list[File]: A list of the resulting records.
        """
        rows = self.files.select(where=f"puid = '{puid} AND warning = 'Extension mismatch';").fetchall()
        try:
            file_validator = pydantic.TypeAdapter(list[File])
            files = file_validator.validate_python(rows)
        except pydantic.ValidationError:
            raise pydantic.ValidationError("Failed to parse files as ArchiveFiles.")
        return files

    def update_relative_path(
        self,
        new_rel_path: Union[str, PathLike, Path],
        uuid: str,
        new_suffix: str,
        reason: str = "",
    ) -> None:
        """Update the relative path for the record with the given uuid and makes a `history` record.

        The `history` record documents the changes that the tool have made to the file.

        Args:
        ----
            new_rel_path (Union[str, PathLike, Path]): The new relative path (with stem and suffix).
            uuid (str): The `UUID` of the file which path should be updated.
            new_suffix (str): The new suffix for the file. Is only used to create the `history` record.
            reason (str): The reason given for the update. Defaults to the empty string
        """
        self.execute(
            sql=f"UPDATE Files SET relative_path = '{new_rel_path}', "
            f"warning = 'Corrected extension mismatch' WHERE Files.uuid = '{uuid}'",
        )

        self.add_history(
            uuid=uuid,
            operation="renamer: Changed the suffix of the file",
            data=f"Changes suffix to {new_suffix}",
            reason=reason,
            time=datetime.datetime.now(),  # noqa: DTZ005
        )
        self.commit()
