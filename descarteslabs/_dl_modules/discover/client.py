from abc import ABC, abstractmethod

from dataclasses import dataclass
import re
from typing import Generator, List, Optional, Union
from enum import Enum
from collections import UserDict

import warnings
import urllib

from descarteslabs.auth import Auth
from descarteslabs.config import get_settings
from ..client.services.service import Service
from ..common.discover import DiscoverGrpcClient
from ..common.proto.discover import discover_pb2

NAME_SEPARATOR = ":~/"
PATH_SEPARATOR = "/"
DISCOVER_EDITOR = "discover/role/editor"
DISCOVER_VIEWER = "discover/role/viewer"
STORAGE_EDITOR = "storage/role/editor"
STORAGE_VIEWER = "storage/role/viewer"
TABLE_EDITOR = "vektorius/role/editor"
TABLE_VIEWER = "vektorius/role/viewer"
ALLOWABLE_FOLDER_ROLES = {DISCOVER_VIEWER, DISCOVER_EDITOR}
ALLOWABLE_BLOB_ROLES = {STORAGE_VIEWER, STORAGE_EDITOR}
ALLOWABLE_TABLE_ROLES = {TABLE_VIEWER, TABLE_EDITOR}


class TargetBase(str):
    # Derived classes must define these class vars
    entityType = None  # Public
    _prefix = None
    _compiled_re = None
    _error_msg = None

    def __new__(cls, value: object) -> "TargetBase":
        # Allow the prefix, but strip it
        if value.startswith(cls._prefix):
            value = value[len(cls._prefix) :]

        if not cls._compiled_re.fullmatch(value):
            raise ValueError(cls._error_msg.format(value))

        return super().__new__(cls, f"{value}")

    def __eq__(self, other: object) -> bool:
        # Strip the prefix for other if present
        if other.startswith(self._prefix):
            other = other[len(self._prefix) :]

        return super().__eq__(other)

    def __ge__(self, other: object) -> bool:
        # Strip the prefix for other if present
        if other.startswith(self._prefix):
            other = other[len(self._prefix) :]

        return super().__ge__(other)


class UserEmail(TargetBase):
    """The given string is interpreted as a user email"""

    entityType = "user-email"
    _prefix = "email:"
    _error_msg = "This is not a valid email: {}"

    # Identical to the check made by the service.
    # following the advice of w3:
    # https://www.w3.org/TR/2016/REC-html51-20161101/sec-forms.html#email-state-typeemail
    # also see:
    # https://html.spec.whatwg.org/multipage/input.html#valid-e-mail-address
    _compiled_re = re.compile(
        r"[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@"
        r"[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?"
        r"(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*"
    )

    # For documentation purposes
    def __init__(self, email):
        """
        Parameters
        ----------
        email: str
            The user email to use.
        """
        pass


class Organization(TargetBase):
    """The given string is interpreted as an organization"""

    entityType = "organization"
    _prefix = "org:"
    _error_msg = "This is not a valid organization: {}"

    # Let's keep org's simple for now; all lowercase
    _compiled_re = re.compile(r"[a-z][a-z0-9_-]+")

    # For documentation purposes
    def __init__(self, org):
        """
        Parameters
        ----------
        org: str
            The name of the organization to use. You can only share with your own
            organization (visit https://iam.descarteslabs.com to see your
            organization's name).  An organization name is always all lowercase and
            only contains ascii characters, the `-` character and/or the `_`
            character.
        """
        pass


class AssetType(str, Enum):
    """The types of assets that discover supports."""

    BLOB = "blob"
    FOLDER = "folder"
    NAMESPACE = "namespace"
    SYM_LINK = "sym_link"
    STS_MODEL = "sts_model"
    TABLE = "vector"


class AssetListFilter(UserDict):
    """AssetListFilter represents a filter that will be applied to list_assets
    it can either be a string in the form of a urlencoded query string
    or it can be a dictionary.

    Examples:
    { type: "blob", name: "Some Name*" }
    { type: ["blob", "folder"], name: "Some Name*" }
    """

    _allowed_fields = ["type", "name"]

    def __setitem__(self, key, value):
        """Validate fields and values for Asset Filters"""
        if key not in self._allowed_fields:
            raise KeyError(
                f"The filter field {key} does not exist. Allowed fields are: {','.join(self._allowed_fields)}"
            )

        if key == "type":
            # validate type is a list of allowed AssetTypes
            allowedTypes = [e.value for e in AssetType]
            values = value if isinstance(value, list) else [value]
            invalid = set(values) - set(allowedTypes)

            if len(invalid) != 0:
                raise ValueError(
                    (
                        f"Unsupported filter type(s) found {','.join(invalid)}. "
                        "Type must be one or more of: {', '.join(allowedTypes)}"
                    )
                )

        super().__setitem__(key, value)


# ShareTargets can either be emails in string format,
# or users can explicitly indicate intended type with UserEmail and Organization helpers
ShareTarget = Union[str, UserEmail, Organization]


def _convert_target(target: ShareTarget):
    # Convert the given target, which can be a str, a UserEmail or an Organization
    # into a UserEmail or Organization
    if isinstance(target, (UserEmail, Organization)):
        return target

    if target.startswith(Organization._prefix):
        return Organization(target)
    else:
        return UserEmail(target)


def _organization_namespace_asset_name(organization: Organization) -> str:
    return f"asset/namespace/{organization}"


@dataclass
class AccessGrant:
    """
    The AccessGrant class contains metadata about access grants such as what asset the access grant is for,
    who to give access to, and what kind of access should be granted.

    Attributes
    ----------
    asset_name
        The asset the access grant is for
    target_id
        The user or organization to give access to
    access
        The role the share target should have
    """

    asset_name: str
    target_id: str
    access: str

    @classmethod
    def _from_proto(cls, response) -> "AccessGrant":
        """
        Maps protobuf object to the correct AccessGrant type.

        Parameters
        ----------
        response : Protobuf Object
            Protobuf response.

        Returns
        -------
        AccessGrant
            AccessGrant with fields from the protobuf response.
        """

        return cls(
            asset_name=response.access_grant.asset_name,
            target_id=response.access_grant.entity.id,
            access=response.access_grant.access,
        )


@dataclass
class SymLink:
    """
    SymLinks are used to represent pointers or shortcuts to other assets.
    This class contains metadata that is useful for interacting with the underlying asset.

    Attributes
    ----------
    target_asset_name
        Underlying asset that the symlink is pointing to.
    target_asset_display_name
        The display name of the underlying asset.
    """

    target_asset_name: str
    target_asset_display_name: str


@dataclass
class Asset:
    """
    The Asset class contains metadata about assets such as folders and blobs.

    Attributes
    ----------
    asset_name
        The name of the asset
    display_name
        The display name of the asset
    is_shared
        Whether access to the asset has been granted to other users
    sym_link (optional)
        If asset is a symlink, this field will be non-empty and will contain metadata about the
        underlying asset that the link points to. If asset is not a symlink, this will be None.
    description (optional)
        The description of the asset
    parent_asset_name (optional)
        The name of the parent asset
    """

    asset_name: str
    display_name: str
    is_shared: bool = False
    sym_link: Optional[SymLink] = None
    description: Optional[str] = None
    parent_asset_name: Optional[str] = None

    @classmethod
    def _from_proto_asset(cls, asset: discover_pb2.Asset) -> "Asset":
        symlink = None

        if "sym_link" in asset.name:
            symlink = SymLink(
                target_asset_name=asset.sym_link.target_name,
                target_asset_display_name=asset.sym_link.target_display_name,
            )

        return cls(
            asset_name=asset.name,
            display_name=asset.display_name,
            is_shared=asset.shared,
            sym_link=symlink,
            description=asset.description,
            parent_asset_name=asset.parent_name,
        )

    @classmethod
    def _from_proto(cls, response) -> "Asset":
        """
        Maps protobuf object to the correct Asset type.

        Parameters
        ----------
        response : Protobuf Object
            Protobuf Response

        Returns
        -------
        Asset
            Asset with fields from the protobuf response
        """

        return cls._from_proto_asset(response.asset)

    def _type(self) -> AssetType:
        """
        Determines the type of the Asset.

        Returns
        -------
        AssetType
            The type of the asset
        """
        asset_type = self.asset_name.split("/")[1]
        return AssetType(asset_type)


class _DiscoverRequestBuilder(ABC):
    def __init__(self, discover: "Discover", asset_name: str):
        self.discover = discover
        self.asset_name = self._resolve_name(asset_name)

    def _type(self) -> str:
        return type(self).__name__.lower()

    @abstractmethod
    def _resolve_name(self, asset_name: str) -> str:
        raise NotImplementedError()

    def share(self, with_: ShareTarget, as_: str) -> AccessGrant:
        """
        Adds access grant for an asset by specifying who to share with and as what role.

        Parameters
        ----------
        with_ : ShareTarget
            `UserEmail` or `Organization` to give access to.
            If just a plain string is passed, it is assumed to be an email address.
            Acceptable ShareTarget formats: "foo@bar.com", UserEmail("foo@bar.com") or Organization("foobar")
        as_ : str
            Type of access the share target should be granted.

        Returns
        -------
        access_grant : AccessGrant
            Access grant that was added.

        Examples
        --------
        >>> discover.blob("asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data.json").share(
        ...     with_="colleague@company.com",
        ...     as_="editor" # or "viewer"
        ... ) # doctest: +SKIP
        AccessGrant(
            asset_name='asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data.json',
            target_id='colleague@company.com',
            access='storage/role/editor'
        )

        >>> discover.blob("asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data.json").share(
        ...     with_=Organization("myorg"),
        ...     as_="editor" # or "viewer"
        ... ) # doctest: +SKIP
        AccessGrant(
            asset_name='asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data.json',
            target_id='myorg',
            access='storage/role/editor'
        )

        >>> discover.folder("asset/folder/ec1fa4ec361494e0e3f80feed065fc2f").share(
        ...     with_="colleague@company.com",
        ...     as_="viewer" # or "editor"
        ... ) # doctest: +SKIP
        AccessGrant(
            asset_name='asset/folder/ec1fa4ec361494e0e3f80feed065fc2f',
            target_id='colleague@company.com',
            access='discover/role/viewer'
        )
        """

        if (
            (self._type() == AssetType.FOLDER and as_.lower() in ALLOWABLE_FOLDER_ROLES)
            or (self._type() == AssetType.BLOB and as_.lower() in ALLOWABLE_BLOB_ROLES)
            or (
                self._type() == AssetType.TABLE and as_.lower() in ALLOWABLE_TABLE_ROLES
            )
        ):
            return self.discover.add_access_grant(self.asset_name, with_, as_)
        else:
            raise ValueError(
                f"The role '{as_}' is invalid. Roles must be of type viewer or editor, "
                "and you can only assign access grants to blobs, tables, and folders."
            )

    def revoke(self, from_: ShareTarget, as_: str):
        """
        Removes access grant for an asset by specifying who to revoke from and what role is being removed.
        If the role does not exist, revoke does nothing.

        Parameters
        ----------
        from_ : ShareTarget
            `UserEmail` or `Organization` to remove access from.
            If just a plain string is passed, it is assumed to be an email address.
            Acceptable ShareTarget formats: "foo@bar.com", UserEmail("foo@bar.com") or Organization("foobar")
        as_ : str
            Type of access that should be revoked from the share target.

        Examples
        --------
        >>> discover.blob("asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data.json").revoke(
        ...     from_=UserEmail("colleague@company.com"),
        ...     as_="viewer" # or "editor"
        ... ) # doctest: +SKIP

        >>> discover.folder("asset/folder/ec1fa4ec361494e0e3f80feed065fc2f").revoke(
        ...     from_="colleague@company.com",
        ...     as_="editor" # or "viewer"
        ... ) # doctest: +SKIP

        >>> discover.folder("asset/folder/ec1fa4ec361494e0e3f80feed065fc2f").revoke(
        ...     from_=Organization("myorg"),
        ...     as_="editor" # or "viewer"
        ... ) # doctest: +SKIP
        """

        # This warning is here for the security of our users. In the case that they are
        # revoking access, if they accidentally revoke with the wrong kind of access for
        # their asset (but a valid fully-resolved role), we will warn them that their
        # revoke was likely not successful.
        WARNING = (
            " The revoke you are trying to perform has likely not taken place."
            f" The asset you are trying to revoke access on is a '{self._type()}' and you used a role '{as_}',"
            " which is inappropriate for that type of asset."
        )

        def warning_on_one_line(
            message, category, filename, lineno, file=None, line=None
        ):
            return "%s:%s: %s:%s\n" % (filename, lineno, category.__name__, message)

        warnings.formatwarning = warning_on_one_line

        if (
            (self._type() == AssetType.FOLDER and as_ not in ALLOWABLE_FOLDER_ROLES)
            or (self._type() == AssetType.BLOB and as_ not in ALLOWABLE_BLOB_ROLES)
            or (self._type() == AssetType.TABLE and as_ not in ALLOWABLE_TABLE_ROLES)
        ):
            warnings.warn(message=WARNING)

        self.discover.remove_access_grant(self.asset_name, from_, as_)

    def replace_shares(
        self, user: ShareTarget, from_role: str, to_role: str
    ) -> AccessGrant:
        """
        Replaces access grant for an asset by specifying the share target,
        what role the target has and what role the target should be given.

        Parameters
        ----------
        user : ShareTarget
            `UserEmail` or `Organization` to replace access for.
            If just a plain string is passed, it is assumed to be an email address.
            Acceptable ShareTarget formats: "foo@bar.com", UserEmail("foo@bar.com") or Organization("foobar")
        from_role : str
            Type of access the share target currently has.
        to_role : str
            Type of access that should replace the current role.

        Returns
        -------
        access_grant : AccessGrant
            Access grant that the previous access grant was replaced with.

        Examples
        --------
        >>> discover.blob("asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data.json").replace_shares(
        ...     user=UserEmail("colleague@company.com"),
        ...     from_role="viewer",
        ...     to_role="editor"
        ... ) # doctest: +SKIP
        AccessGrant(
            asset_name='asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data.json',
            target_id='colleague@company.com',
            access='storage/role/editor'
        )

        >>> discover.folder("asset/folder/ec1fa4ec361494e0e3f80feed065fc2f").replace_shares(
        ...     user="colleague@company.com",
        ...     from_role="viewer",
        ...     to_role="editor"
        ... ) # doctest: +SKIP
        AccessGrant(
            asset_name='asset/folder/ec1fa4ec361494e0e3f80feed065fc2f',
            target_id='colleague@company.com',
            access='discover/role/editor'
        )
        """

        if (
            (
                self._type() == AssetType.FOLDER
                and to_role.lower() in ALLOWABLE_FOLDER_ROLES
            )
            or (
                self._type() == AssetType.BLOB
                and to_role.lower() in ALLOWABLE_BLOB_ROLES
            )
            or (
                self._type() == AssetType.TABLE
                and to_role.lower() in ALLOWABLE_TABLE_ROLES
            )
        ):
            return self.discover.replace_access_grant(
                self.asset_name, user, from_role, to_role
            )
        else:
            raise ValueError(
                f"The role '{to_role}' is invalid. Roles must be of type viewer or editor, "
                "and you can only assign access grants to blobs, tables and folders."
            )

    def list_shares(self) -> List[AccessGrant]:
        """
        Lists access grants for an asset.

        Returns
        -------
        access_grants : List[AccessGrant]
            List of access grants for the asset.

        Examples
        --------
        >>> discover.blob(
        ...     "asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.geojson"
        ... ).list_shares() # doctest: +SKIP
        [
            AccessGrant(
                asset_name='asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.geojson',
                target_id='user1@company.com',
                access='storage/role/owner'
            ),
            AccessGrant(
                asset_name='asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.geojson',
                target_id='user2@company.com',
                access='storage/role/viewer'
            ),
            AccessGrant(
                asset_name='asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.geojson',
                target_id='user3@company.com',
                access='storage/role/viewer'
            )
        ]

        >>> discover.folder(
        ...     "asset/folder/ec1fa4ec361494e0e3f80feed065fc2f"
        ... ).list_shares() # doctest: +SKIP
        [
            AccessGrant(
                asset_name='asset/folder/ec1fa4ec361494e0e3f80feed065fc2f',
                target_id='user1@company.com',
                access='discover/role/owner'
            ),
            AccessGrant(
                asset_name='asset/folder/ec1fa4ec361494e0e3f80feed065fc2f',
                target_id='user2@company.com',
                access='discover/role/viewer'
            ),
            AccessGrant(
                asset_name='asset/folder/ec1fa4ec361494e0e3f80feed065fc2f',
                target_id='user3@company.com',
                access='discover/role/viewer'
            )
        ]
        """

        return self.discover.list_access_grants(self.asset_name)

    def get(self) -> Asset:
        """
        Gets an asset.

        Returns
        -------
        asset : Asset
            Asset that has display name and description.

        Examples
        --------
        >>> discover.blob("asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/australia.geojson").get() # doctest: +SKIP
        Asset(
            asset_name='asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/australia.geojson',
            display_name='australia.geojson',
            is_shared=True,
            sym_link=None,
            description='',
            parent_asset_name='asset/namespace/4ec36180feed065fc2f494e0e3fec1fa'
        )

        >>> discover.folder("asset/folder/ec1fa4ec361494e0e3f80feed065fc2f").get() # doctest: +SKIP
        Asset(
            asset_name='asset/folder/ec1fa4ec361494e0e3f80feed065fc2f',
            display_name='Remote Sensing Dataset',
            is_shared=False,
            sym_link=None,
            description='awesome satellite imagery',
            parent_asset_name='asset/namespace/4ec36180feed065fc2f494e0e3fec1fa'
        )
        """

        return self.discover.get_asset(self.asset_name)

    def update(
        self, display_name: Optional[str] = None, description: Optional[str] = None
    ) -> Asset:
        """
        Updates display name and description for an asset.

        Parameters
        ----------
        display_name : str (optional)
            New display name for asset.
        description : str (optional)
            New description for asset.

        Examples
        --------
        >>> discover.blob("asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/australia.geojson").update(
        ...     display_name="australia.geojson",
        ...     description=""
        ... ) # doctest: +SKIP
        Asset(
            asset_name='asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/australia.geojson',
            display_name='australia.geojson',
            is_shared=False,
            sym_link=None,
            description='',
            parent_asset_name='asset/namespace/4ec36180feed065fc2f494e0e3fec1fa'
        )

        >>> discover.folder("asset/folder/ec1fa4ec361494e0e3f80feed065fc2f").update(
        ...     display_name="Remote Sensing",
        ...     description="NASA Earth Observation"
        ... ) # doctest: +SKIP
        Asset(
            asset_name='asset/folder/ec1fa4ec361494e0e3f80feed065fc2f',
            display_name='Remote Sensing',
            is_shared=False,
            sym_link=None,
            description='NASA Earth Observation',
            parent_asset_name='asset/namespace/4ec36180feed065fc2f494e0e3fec1fa'
        )
        """

        return self.discover.update_asset(self.asset_name, display_name, description)

    def move(self, to: Optional["Folder"] = None) -> Asset:
        """
        Move an asset into a different folder, or into the root of your workspace if no `to` is provided.

        Parameters
        ----------
        to : Folder (optional)
            The new parent. If this isn't provided, the asset will be moved into the root of your workspace.

        Examples
        --------
        Calling .move() with no arguments moves the specified asset to the root of your workspace.

        >>> discover.blob("asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json").move() # doctest: +SKIP
        Asset(
            asset_name='asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json',
            display_name='My Data File',
            is_shared=True,
            sym_link=None,
            description='some geospatial data',
            parent_asset_name='asset/namespace/4ec36180feed065fc2f494e0e3fec1fa'
        )

        >>> discover.folder("asset/folder/c361494e0e3f8ec1fa4e0feed065fc2f").move(
        ...     to="asset/folder/ec1fa4ec361494e0e3f80feed065fc2f"
        ... ) # doctest: +SKIP
        Asset(
            asset_name='asset/folder/c361494e0e3f8ec1fa4e0feed065fc2f',
            display_name='NASA Earth Data',
            is_shared=True,
            sym_link=None,
            description='NASA DC, GPS satellites, SMAP, JASON, METEOSAT, ALOS',
            parent_asset_name='asset/folder/ec1fa4ec361494e0e3f80feed065fc2f'
        )
        """

        new_parent = None

        if to is not None:
            new_parent = to.asset_name

        return self.discover.move_asset(self.asset_name, new_parent)


def _bad_asset_shape_error(asset_name: str, asset_type: str) -> ValueError:
    """
    Error for malformed asset name.

    Parameters
    ----------
    asset_name : str
        Path of the asset being resolved.
    asset_type : str
        Type of the asset being resolved.

    Returns
    -------
    ValueError
        Error that explains what a good path looks like for the given asset.
    """

    if asset_type == AssetType.BLOB:
        error_english = (
            "A well-formed path should include a user ID, as well as a colon and a tilda separator, eg. "
            f"asset/blob/b1a1d1c20e655402691582723261a02caa693834{NAME_SEPARATOR}my_blob"
        )
    elif asset_type == "table":
        error_english = (
            "A well-formed path should include a user ID, as well as a colon and a tilda separator, eg. "
            f"asset/vector/b1a1d1c20e655402691582723261a02caa693834{NAME_SEPARATOR}my_table"
        )
    elif asset_type == AssetType.FOLDER:
        error_english = (
            "A well-formed folder name must include a folder ID, eg. "
            "asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de"
        )
    else:
        raise ValueError(f"Invalid asset_type: {asset_type}")

    return ValueError(
        f"The specified asset name ({asset_name}) had an unexpected shape. {error_english}"
    )


def _is_valid_uuid(uuid: str, num_chars: int) -> bool:
    """
    Checks to make sure the given UUID is valid.

    Parameters
    ----------
    uuid : str
        The hexadecimal representation of a UUID.
    num_chars : int
        The expected number of characters in the UUID hexadecimal string.

    Returns
    -------
    bool
        Returns True if the uuid is `num_chars` characters long and contains all valid hex characters, otherwise False.
    """

    # Don't use double brackets or sphinx will fail when viewing the source code
    left_curly_bracket = "{"
    right_curly_bracket = "}"

    NAMESPACE_PATTERN = re.compile(
        rf"[0-9a-f]{left_curly_bracket}{num_chars}{right_curly_bracket}"
    )
    return len(uuid) == num_chars and NAMESPACE_PATTERN.match(uuid)


def _role_to_storage_role(role: str) -> str:
    """
    Converts shortcut role to the corresponding Storage role.

    Parameters
    ----------
    role : str
        Role given by user.

    Returns
    -------
    storage_role : str
        Fully-resolved Storage role if given "viewer" or "editor".
    """

    if role.lower() == "viewer":
        storage_role = STORAGE_VIEWER
    elif role.lower() == "editor":
        storage_role = STORAGE_EDITOR
    else:
        storage_role = role
    return storage_role


def _role_to_table_role(role: str) -> str:
    """
    Converts shortcut role to the corresponding Table role.

    Parameters
    ----------
    role : str
        Role given by user.

    Returns
    -------
    storage_role : str
        Fully-resolved Table role if given "viewer" or "editor".
    """

    if role.lower() == "viewer":
        table_role = TABLE_VIEWER
    elif role.lower() == "editor":
        table_role = TABLE_EDITOR
    else:
        table_role = role
    return table_role


def _role_to_discover_role(_as: str) -> str:
    """
    Converts shortcut role to the corresponding Discover role.

    Parameters
    ----------
    role : str
        Role given by user.

    Returns
    -------
    discover_role : str
        Fully-resolved Discover role if given "viewer" or "editor".
    """

    if _as.lower() == "viewer":
        discover_role = DISCOVER_VIEWER
    elif _as.lower() == "editor":
        discover_role = DISCOVER_EDITOR
    else:
        discover_role = _as
    return discover_role


class Blob(_DiscoverRequestBuilder):
    """A Blob is a binary asset from storage."""

    def _resolve_name(self, asset_name: str):
        """
        Resolves the asset_name to the correct format for blobs.

        Parameters
        ----------
        asset_name : str
            Path of the asset being resolved.
        """

        type_ = self._type()
        parts = asset_name.split(NAME_SEPARATOR)
        namespace = None
        blob_name = None

        # Case 1: User only provides file name (asset name will be length 1)
        # foo.txt, myfolder/foo.txt
        if len(parts) == 1:
            blob_name = parts[0].strip("~/")
            namespace = self.discover._discover_client.auth.namespace

        # Case 2: User provides a user_sha and file name (asset name will be length 2)
        # 3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt, blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt
        elif len(parts) == 2:
            first_part = parts[0]
            # if there are more than three items in parts (asset / blob / namespace)
            if first_part.count(PATH_SEPARATOR) > 2:
                raise _bad_asset_shape_error(asset_name, type_)

            # if there are only two items and the first item is "asset", raise (not specific enough)
            # asset/ed8a928b209284e63295aeaa69383bf0f29837e8:~/foo.txt, asset/blob:~/foo.txt
            elif (
                first_part.count(PATH_SEPARATOR) == 1
                and first_part.split(PATH_SEPARATOR, 1)[0] == "asset"
            ):
                raise _bad_asset_shape_error(asset_name, type_)
            # otherwise take the rightmost item in the first part (we will check it later)
            else:
                *_, namespace = first_part.rsplit(PATH_SEPARATOR, 1)
            blob_name = parts[1]

        # Case 3: More than one :~/
        # 12345:~/myfolder:~/myfile
        else:
            raise _bad_asset_shape_error(asset_name, type_)

        if not _is_valid_uuid(namespace, 40):
            raise _bad_asset_shape_error(asset_name, type_)

        return f"asset/{type_}/{namespace}{NAME_SEPARATOR}{blob_name}"

    def share(self, with_: ShareTarget, as_: str) -> AccessGrant:
        """
        Adds access grant for an asset by specifying who to share with and as what role.

        Parameters
        ----------
        with_ : ShareTarget
            `UserEmail` or `Organization` to give access to.
            If just a plain string is passed, it is assumed to be an email address.
            Acceptable ShareTarget formats: "foo@bar.com", UserEmail("foo@bar.com") or Organization("foobar")
        as_ : str
            Type of access the share target should be granted.

        Returns
        -------
        access_grant : AccessGrant
            Access grant that was added.

        Examples
        --------
        >>> discover.blob("asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json").share(
        ...     with_="colleague@company.com",
        ...     as_="viewer" # or "editor"
        ... ) # doctest: +SKIP
        AccessGrant(
            asset_name="asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json",
            target_id='colleague@company.com',
            access='storage/role/viewer'
        )

        >>> discover.blob("asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json").share(
        ...     with_=UserEmail("colleague@company.com"),
        ...     as_="viewer" # or "editor"
        ... ) # doctest: +SKIP
        AccessGrant(
            asset_name="asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json",
            target_id='colleague@company.com',
            access='storage/role/viewer'
        )

        >>> discover.blob("asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json").share(
        ...     with_=Organization("myorg"),
        ...     as_="viewer" # or "editor"
        ... ) # doctest: +SKIP
        AccessGrant(
            asset_name="asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json",
            target_id='myorg',
            access='storage/role/viewer'
        )
        """

        return super().share(with_=with_, as_=_role_to_storage_role(as_))

    def revoke(self, from_: ShareTarget, as_: str):
        """
        Removes access grant for an asset by specifying who to revoke from and what role is being removed.
        If the role does not exist, revoke does nothing.

        Parameters
        ----------
        from_ : ShareTarget
            `UserEmail` or `Organization` to revoke access from.
            If just a plain string is passed, it is assumed to be an email address.
            Acceptable ShareTarget formats: "foo@bar.com", UserEmail("foo@bar.com") or Organization("foobar")
        as_ : str
            Type of access that should be revoked from the share target.

        Examples
        --------
        >>> discover.blob("asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json").revoke(
        ...     from_="colleague@company.com",
        ...     as_="viewer" # or "editor"
        ... ) # doctest: +SKIP

        >>> discover.blob("asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json").revoke(
        ...     from_=UserEmail("colleague@company.com"),
        ...     as_="viewer" # or "editor"
        ... ) # doctest: +SKIP

        >>> discover.blob("asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json").revoke(
        ...     from_=Organization("colleague@company.com"),
        ...     as_="viewer" # or "editor"
        ... ) # doctest: +SKIP
        """

        super().revoke(from_=from_, as_=_role_to_storage_role(as_))

    def replace_shares(
        self, user: ShareTarget, from_role: str, to_role: str
    ) -> AccessGrant:
        """
        Replaces access grant for an asset by specifying the share target,
        what role the target has and what role the target should be given.

        Parameters
        ----------
        user : ShareTarget
            `UserEmail` or `Organization` to replace access for.
            If just a plain string is passed, it is assumed to be an email address.
            Acceptable ShareTarget formats: "foo@bar.com", UserEmail("foo@bar.com") or Organization("foobar")
        from_role : str
            Type of access the share target currently has.
        to_role : str
            Type of access that should replace the current role.

        Returns
        -------
        access_grant : AccessGrant
            Access grant that the previous access grant was replaced with.

        Examples
        --------
        >>> discover.blob("asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json").replace_shares(
        ...     user="colleague@company.com",
        ...     from_role="viewer",
        ...     to_role="editor"
        ... ) # doctest: +SKIP
        AccessGrant(
            asset_name='asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json',
            target_id='colleague@company.com',
            access='storage/role/editor'
        )

        >>> discover.blob("asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json").replace_shares(
        ...     user=UserEmail("colleague@company.com"),
        ...     from_role="viewer",
        ...     to_role="editor"
        ... ) # doctest: +SKIP
        AccessGrant(
            asset_name='asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json',
            target_id='colleague@company.com',
            access='storage/role/editor'
        )

        >>> discover.blob("asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json").replace_shares(
        ...     user=Organization("myorg"),
        ...     from_role="viewer",
        ...     to_role="editor"
        ... ) # doctest: +SKIP
        AccessGrant(
            asset_name='asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json',
            target_id='myorg',
            access='storage/role/editor'
        )
        """

        return super().replace_shares(
            user=user,
            from_role=_role_to_storage_role(from_role),
            to_role=_role_to_storage_role(to_role),
        )


class Folder(_DiscoverRequestBuilder):
    """A Folder is a container for other assets such as Blobs and Folders."""

    def _resolve_name(self, asset_name):
        """
        Resolves the asset_name to the correct format for folders.

        Parameters
        ----------
        asset_name : str
            Path of the asset being resolved.
        """

        type_ = self._type()
        parts = asset_name.split(PATH_SEPARATOR)

        # popping the uuid off and if not valid, raise
        uuid = parts.pop()
        if not _is_valid_uuid(uuid, 32):
            raise _bad_asset_shape_error(asset_name, type_)

        # need to check if the user included more than just the folder name
        if len(parts) != 0:
            # what's left should be "asset/folder" or just "folder", either way the next pop should be "folder"
            # and there should be at maximum, one item left in parts (the remaining part being "asset")
            should_be_folder = parts.pop()
            if should_be_folder != type_ or len(parts) > 1:
                raise _bad_asset_shape_error(asset_name, type_)

        return PATH_SEPARATOR.join(("asset", type_, uuid))

    def list(self) -> List[Asset]:
        """List the child assets of the Folder."""

        return self.discover.list_assets(self.asset_name)

    def share(self, with_: ShareTarget, as_: str) -> AccessGrant:
        """
        Adds access grant for an asset by specifying who to share with and as what role.

        Parameters
        ----------
        with_ : ShareTarget
            `UserEmail` or `Organization` to give access to.
            If just a plain string is passed, it is assumed to be an email address.
            Acceptable ShareTarget formats: "foo@bar.com", UserEmail("foo@bar.com") or Organization("foobar")
        as_ : str
            Type of access the share target should be granted.

        Returns
        -------
        access_grant : AccessGrant
            Access grant that was added.

        Examples
        --------
        >>> discover.folder("asset/folder/c361494e0e3f8ec1fa4e0feed065fc2f").share(
        ...     with_="colleague@company.com",
        ...     as_="viewer" # or "editor"
        ... ) # doctest: +SKIP
        AccessGrant(
            asset_name='asset/folder/c361494e0e3f8ec1fa4e0feed065fc2f',
            target_id='colleague@company.com',
            access='discover/role/viewer'
        )

        >>> discover.folder("asset/folder/c361494e0e3f8ec1fa4e0feed065fc2f").share(
        ...     with_=UserEmail("colleague@company.com"),
        ...     as_="viewer" # or "editor"
        ... ) # doctest: +SKIP
        AccessGrant(
            asset_name='asset/folder/c361494e0e3f8ec1fa4e0feed065fc2f',
            target_id='colleague@company.com',
            access='discover/role/viewer'
        )

        >>> discover.folder("asset/folder/c361494e0e3f8ec1fa4e0feed065fc2f").share(
        ...     with_=Organization("myorg"),
        ...     as_="viewer" # or "editor"
        ... ) # doctest: +SKIP
        AccessGrant(
            asset_name='asset/folder/c361494e0e3f8ec1fa4e0feed065fc2f',
            target_id='myorg',
            access='discover/role/viewer'
        )
        """

        return super().share(with_=with_, as_=_role_to_discover_role(as_))

    def revoke(self, from_: ShareTarget, as_: str):
        """
        Removes access grant for an asset by specifying who to revoke from and what role is being removed.
        If the role does not exist, revoke does nothing.

        Parameters
        ----------
        from_ : ShareTarget
            `UserEmail` or `Organization` to revoke access from.
            If just a plain string is passed, it is assumed to be an email address.
            Acceptable ShareTarget formats: "foo@bar.com", UserEmail("foo@bar.com") or Organization("foobar")
        as_ : str
            Type of access that should be revoked from the share target.

        Examples
        --------
        >>> discover.folder("asset/folder/c361494e0e3f8ec1fa4e0feed065fc2f").revoke(
        ...     from_="colleague@company.com",
        ...     as_="viewer" # or "editor"
        ... ) # doctest: +SKIP

        >>> discover.folder("asset/folder/c361494e0e3f8ec1fa4e0feed065fc2f").revoke(
        ...     from_=UserEmail("colleague@company.com"),
        ...     as_="viewer" # or "editor"
        ... ) # doctest: +SKIP

        >>> discover.folder("asset/folder/c361494e0e3f8ec1fa4e0feed065fc2f").revoke(
        ...     from_=Organization("myorg"),
        ...     as_="viewer" # or "editor"
        ... ) # doctest: +SKIP
        """

        super().revoke(from_=from_, as_=_role_to_discover_role(as_))

    def replace_shares(
        self, user: ShareTarget, from_role: str, to_role: str
    ) -> AccessGrant:
        """
        Replaces access grant for an asset by specifying the share target,
        what role the target has and what role the target should be given.

        Parameters
        ----------
        user : ShareTarget
            `UserEmail` or `Organization` to replace access for.
            If just a plain string is passed, it is assumed to be an email address.
            Acceptable ShareTarget formats: "foo@bar.com", UserEmail("foo@bar.com") or Organization("foobar")
        from_role : str
            Type of access the share target currently has.
        to_role : str
            Type of access that should replace the current role.

        Returns
        -------
        access_grant : AccessGrant
            Access grant that the previous access grant was replaced with.

        Examples
        --------
        >>> discover.folder("asset/folder/c361494e0e3f8ec1fa4e0feed065fc2f").replace_shares(
        ...     user="colleague@company.com",
        ...     from_role="viewer",
        ...     to_role="editor"
        ... ) # doctest: +SKIP
        AccessGrant(
            asset_name='asset/folder/c361494e0e3f8ec1fa4e0feed065fc2f',
            target_id='colleague@company.com',
            access='discover/role/editor'
        )

        >>> discover.folder("asset/folder/c361494e0e3f8ec1fa4e0feed065fc2f").replace_shares(
        ...     user=UserEmail("colleague@company.com"),
        ...     from_role="viewer",
        ...     to_role="editor"
        ... ) # doctest: +SKIP
        AccessGrant(
            asset_name='asset/folder/c361494e0e3f8ec1fa4e0feed065fc2f',
            target_id='colleague@company.com',
            access='discover/role/editor'
        )

        >>> discover.folder("asset/folder/c361494e0e3f8ec1fa4e0feed065fc2f").replace_shares(
        ...     user=Organization("myorg"),
        ...     from_role="viewer",
        ...     to_role="editor"
        ... ) # doctest: +SKIP
        AccessGrant(
            asset_name='asset/folder/c361494e0e3f8ec1fa4e0feed065fc2f',
            target_id='myorg',
            access='discover/role/editor'
        )
        """

        return super().replace_shares(
            user=user,
            from_role=_role_to_discover_role(from_role),
            to_role=_role_to_discover_role(to_role),
        )


class Table(_DiscoverRequestBuilder):
    """A Table is a pointer to a table."""

    def _type(self) -> str:
        return AssetType.TABLE

    def _resolve_name(self, asset_name: str):
        """
        Resolves the asset_name to the correct format for tables.

        Parameters
        ----------
        asset_name : str
            Path of the asset being resolved.
        """

        type_ = self._type()
        parts = asset_name.split(NAME_SEPARATOR)
        namespace = None
        table_name = None

        # Case 1: User only provides file name (asset name will be length 1)
        # foo.txt, myfolder/foo.txt
        if len(parts) == 1:
            table_name = parts[0].strip("~/")
            namespace = self.discover._discover_client.auth.namespace

        # Case 2: User provides a user_sha and file name (asset name will be length 2)
        # 3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt, blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt
        elif len(parts) == 2:
            first_part = parts[0]
            # if there are more than three items in parts (asset / blob / namespace)
            if first_part.count(PATH_SEPARATOR) > 2:
                raise _bad_asset_shape_error(asset_name, type_)

            # if there are only two items and the first item is "asset", raise (not specific enough)
            # asset/ed8a928b209284e63295aeaa69383bf0f29837e8:~/foo.txt, asset/blob:~/foo.txt
            elif (
                first_part.count(PATH_SEPARATOR) == 1
                and first_part.split(PATH_SEPARATOR, 1)[0] == "asset"
            ):
                raise _bad_asset_shape_error(asset_name, type_)
            # otherwise take the rightmost item in the first part (we will check it later)
            else:
                *_, namespace = first_part.rsplit(PATH_SEPARATOR, 1)
            table_name = parts[1]

        # Case 3: More than one :~/
        # 12345:~/myfolder:~/myfile
        else:
            raise _bad_asset_shape_error(asset_name, type_)

        if not _is_valid_uuid(namespace, 40):
            raise _bad_asset_shape_error(asset_name, type_)

        return f"asset/{type_}/{namespace}{NAME_SEPARATOR}{table_name}"

    def share(self, with_: ShareTarget, as_: str) -> AccessGrant:
        """
        Adds access grant for an asset by specifying who to share with and as what role.

        Parameters
        ----------
        with_ : ShareTarget
            `UserEmail` or `Organization` to give access to.
            If just a plain string is passed, it is assumed to be an email address.
            Acceptable ShareTarget formats: "foo@bar.com", UserEmail("foo@bar.com") or Organization("foobar")
        as_ : str
            Type of access the share target should be granted.

        Returns
        -------
        access_grant : AccessGrant
            Access grant that was added.

        Examples
        --------
        >>> discover.table("table").share(
        ...     with_="colleague@company.com",
        ...     as_="viewer" # or "editor"
        ... ) # doctest: +SKIP
        AccessGrant(
            asset_name="table",
            target_id='colleague@company.com',
            access='viewer'
        )

        >>> discover.table("table").share(
        ...     with_=UserEmail("colleague@company.com"),
        ...     as_="viewer" # or "editor"
        ... ) # doctest: +SKIP
        AccessGrant(
            asset_name="table",
            target_id='colleague@company.com',
            access='viewer'
        )

        >>> discover.table("table").share(
        ...     with_=Organization("myorg"),
        ...     as_="viewer" # or "editor"
        ... ) # doctest: +SKIP
        AccessGrant(
            asset_name="table",
            target_id='myorg',
            access='viewer'
        )
        """

        return super().share(with_=with_, as_=_role_to_table_role(as_))

    def revoke(self, from_: ShareTarget, as_: str):
        """
        Removes access grant for an asset by specifying who to revoke from and what role is being removed.
        If the role does not exist, revoke does nothing.

        Parameters
        ----------
        from_ : ShareTarget
            `UserEmail` or `Organization` to revoke access from.
            If just a plain string is passed, it is assumed to be an email address.
            Acceptable ShareTarget formats: "foo@bar.com", UserEmail("foo@bar.com") or Organization("foobar")
        as_ : str
            Type of access that should be revoked from the share target.

        Examples
        --------
        >>> discover.table("table").revoke(
        ...     from_="colleague@company.com",
        ...     as_="viewer" # or "editor"
        ... ) # doctest: +SKIP

        >>> discover.table("table").revoke(
        ...     from_=UserEmail("colleague@company.com"),
        ...     as_="viewer" # or "editor"
        ... ) # doctest: +SKIP

        >>> discover.table("table").revoke(
        ...     from_=Organization("myorg"),
        ...     as_="viewer" # or "editor"
        ... ) # doctest: +SKIP
        """

        super().revoke(from_=from_, as_=_role_to_table_role(as_))

    def replace_shares(
        self, user: ShareTarget, from_role: str, to_role: str
    ) -> AccessGrant:
        """
        Replaces access grant for an asset by specifying the share target,
        what role the target has and what role the target should be given.

        Parameters
        ----------
        user : ShareTarget
            `UserEmail` or `Organization` to replace access for.
            If just a plain string is passed, it is assumed to be an email address.
            Acceptable ShareTarget formats: "foo@bar.com", UserEmail("foo@bar.com") or Organization("foobar")
        from_role : str
            Type of access the share target currently has.
        to_role : str
            Type of access that should replace the current role.

        Returns
        -------
        access_grant : AccessGrant
            Access grant that the previous access grant was replaced with.

        Examples
        --------
        >>> discover.table("table").replace_shares(
        ...     user="colleague@company.com",
        ...     from_role="viewer",
        ...     to_role="editor"
        ... ) # doctest: +SKIP
        AccessGrant(
            asset_name="table",
            target_id='colleague@company.com',
            access='editor'
        )

        >>> discover.table("table").replace_shares(
        ...     user=UserEmail("colleague@company.com"),
        ...     from_role="viewer",
        ...     to_role="editor"
        ... ) # doctest: +SKIP
        AccessGrant(
            asset_name="table",
            target_id='colleague@company.com',
            access='editor'
        )

        >>> discover.table("table").replace_shares(
        ...     user=Organization("myorg"),
        ...     from_role="viewer",
        ...     to_role="editor"
        ... ) # doctest: +SKIP
        AccessGrant(
            asset_name="table",
            target_id='myorg',
            access='editor'
        )
        """

        return super().replace_shares(
            user=user,
            from_role=_role_to_table_role(from_role),
            to_role=_role_to_table_role(to_role),
        )


class Discover:
    """Discover is a client for interacting with assets such as Blobs and Folders."""

    def __init__(
        self,
        host: str = None,
        discover_client: DiscoverGrpcClient = None,
        auth=None,
        port=None,
    ):
        if discover_client is None:
            if host is None:
                host = get_settings().discover_host
            if port is None:
                port = int(get_settings().discover_port)

            discover_client = DiscoverGrpcClient(host, auth)
        self._discover_client = discover_client

    def blob(self, asset_name: str) -> Blob:
        """
        Constructs a `Blob` request builder.
        This is a helper that simplifies construction of Discover requests that involve storage assets (ie. "blobs").

        Parameters
        ----------
        asset_name : str
            Asset ID of the blob.

        Returns
        -------
        blob : Blob
            A Discover request builder for requests involving storage assets.
        """

        return Blob(self, asset_name)

    def folder(self, asset_name: str) -> Folder:
        """
        Constructs a `Folder` request builder.
        This is a helper that simplifies construction of Discover requests that involve Discover assets (ie. "folders").

        Parameters
        ----------
        asset_name : str
            Asset ID of the folder.

        Returns
        -------
        folder : Folder
            A Discover request builder for requests involving Discover assets.
        """

        return Folder(self, asset_name)

    def table(self, asset_name: str) -> Table:
        """
        Constructs a `Table` request builder.
        This is a helper that simplifies construction of Discover requests that involve Table assets.

        Parameters
        ----------
        asset_name : str
            Asset ID of the table.

        Returns
        -------
        table : Table
            A Discover request builder for requests involving Table assets.
        """

        return Table(self, asset_name)

    def add_access_grant(
        self, asset_name: str, group_or_user: ShareTarget, role: str
    ) -> AccessGrant:
        """
        Adds access grant for specified asset, group/user and role.

        Parameters
        ----------
        asset_name : str
            Path of the file or folder to give access to.
        group_or_user : ShareTarget
            `UserEmail` or `Organization` to grant access for.
            If just a plain string is passed, it is assumed to be an email address.
            Acceptable ShareTarget formats: "foo@bar.com", UserEmail("foo@bar.com") or Organization("foobar")
        role : str
            Type of access the share target should be granted.

        Returns
        -------
        access_grant : AccessGrant
            Access grant that was added.

        Examples
        --------
        >>> discover.add_access_grant(
        ...     asset_name="asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json"
        ...     group_or_user="colleague@company.com",
        ...     role="viewer" # or "editor"
        ... ) # doctest: +SKIP
        AccessGrant(
            asset_name='asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json',
            target_id='colleague@company.com',
            access='storage/role/viewer'
        )

        >>> discover.add_access_grant(
        ...     asset_name="asset/folder/ec1fa4ec361494e0e3f80feed065fc2f"
        ...     group_or_user="colleague@company.com",
        ...     role="editor" # or "viewer"
        ... ) # doctest: +SKIP
        AccessGrant(
            asset_name='asset/folder/c361494e0e3f8ec1fa4e0feed065fc2f',
            target_id='colleague@company.com',
            access='discover/role/editor'
        )
        """

        group_or_user = _convert_target(group_or_user)

        add_request = discover_pb2.CreateAccessGrantRequest(
            access_grant=discover_pb2.AccessGrant(
                asset_name=asset_name,
                entity=discover_pb2.Entity(
                    type=group_or_user.entityType,
                    id=group_or_user,
                ),
                access=role,
            )
        )

        response = self._discover_client.CreateAccessGrant(add_request)
        access_grant = AccessGrant._from_proto(response)

        return access_grant

    def remove_access_grant(
        self, asset_name: str, group_or_user: ShareTarget, role: str
    ):
        """
        Removes access grant for specified asset, group/user and role.

        Parameters
        ----------
        asset_name : str
            Path of the file or folder to remove access from.
        group_or_user : ShareTarget
            `UserEmail` or `Organization` to revoke access from.
            If just a plain string is passed, it is assumed to be an email address.
            Acceptable ShareTarget formats: "foo@bar.com", UserEmail("foo@bar.com") or Organization("foobar")
        role : str
            Type of access that should be revoked from the share target.

        Examples
        --------
        >>> discover.remove_access_grant(
        ...     asset_name="asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json",
        ...     group_or_user="colleague@company.com",
        ...     role="editor" # or "viewer"
        ... ) # doctest: +SKIP

        >>> discover.remove_access_grant(
        ...     asset_name="asset/folder/ec1fa4ec361494e0e3f80feed065fc2f",
        ...     group_or_user="colleague@company.com",
        ...     role="editor" # or "viewer"
        ... ) # doctest: +SKIP
        """

        group_or_user = _convert_target(group_or_user)

        remove_request = discover_pb2.DeleteAccessGrantRequest(
            access_grant=discover_pb2.AccessGrant(
                asset_name=asset_name,
                entity=discover_pb2.Entity(
                    type=group_or_user.entityType,
                    id=group_or_user,
                ),
                access=role,
            )
        )
        self._discover_client.DeleteAccessGrant(remove_request)

    def replace_access_grant(
        self,
        asset_name: str,
        group_or_user: ShareTarget,
        role_from: str,
        role_to: str,
    ) -> AccessGrant:
        """
        Replaces access grant for specified asset, group/user and role.

        Parameters
        ----------
        asset_name : str
            Path of the file or folder to replace access to.
        group_or_user : ShareTarget
            `UserEmail` or `Organization` to replace access for.
            If just a plain string is passed, it is assumed to be an email address.
            Acceptable ShareTarget formats: "foo@bar.com", UserEmail("foo@bar.com") or Organization("foobar")
        role_from : str
            Type of access the share target currently has.
        role_to : str
            Type of access that should replace the current role.

        Returns
        -------
        access_grant : AccessGrant
            Access grant that the previous access grant was replaced with.

        Examples
        --------
        >>> discover.replace_access_grant(
        ...     asset_name="asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json",
        ...     group_or_user="colleague@company.com",
        ...     role_from="viewer",
        ...     role_to="editor"
        ... ) # doctest: +SKIP
        AccessGrant(
            asset_name='asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json',
            target_id='colleague@company.com',
            access='storage/role/editor'
        )

        >>> discover.replace_access_grant(
        ...     asset_name="asset/folder/ec1fa4ec361494e0e3f80feed065fc2f",
        ...     group_or_user="colleague@company.com",
        ...     role_from="editor",
        ...     role_to="viewer"
        ... ) # doctest: +SKIP
        AccessGrant(
            asset_name='asset/folder/ec1fa4ec361494e0e3f80feed065fc2f',
            target_id='colleague@company.com',
            access='discover/role/viewer'
        )
        """

        group_or_user = _convert_target(group_or_user)

        replace_request = discover_pb2.ReplaceAccessGrantRequest(
            delete_access_grant=discover_pb2.AccessGrant(
                asset_name=asset_name,
                entity=discover_pb2.Entity(
                    type=group_or_user.entityType, id=group_or_user
                ),
                access=role_from,
            ),
            create_access_grant=discover_pb2.AccessGrant(
                asset_name=asset_name,
                entity=discover_pb2.Entity(
                    type=group_or_user.entityType, id=group_or_user
                ),
                access=role_to,
            ),
        )

        response = self._discover_client.ReplaceAccessGrant(replace_request)
        access_grant = AccessGrant._from_proto(response)

        return access_grant

    def list_access_grants(self, asset_name: str) -> List[AccessGrant]:
        """
        Lists access grants for specified asset.

        Parameters
        ----------
        asset_name : str
            Path of the file or folder to list access grants for.

        Returns
        -------
        access_grants : List[AccessGrant]
            List of access grants.

        Examples
        --------
        >>> discover.list_access_grants(
        ...     asset_name="asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json"
        ... ) # doctest: +SKIP
        [
            AccessGrant(
                asset_name='asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json',
                target_id='user1@company.com',
                access='storage/role/owner'
            ),
            AccessGrant(
                asset_name='asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json',
                target_id='user2@company.com',
                access='storage/role/viewer'
            ),
            AccessGrant(
                asset_name='asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json',
                target_id='user3@company.com',
                access='storage/role/editor'
            )
        ]

        >>> discover.list_access_grants(
        ...     asset_name="asset/folder/ec1fa4ec361494e0e3f80feed065fc2f"
        ... ) # doctest: +SKIP
        [
            AccessGrant(
                asset_name='asset/folder/ec1fa4ec361494e0e3f80feed065fc2f',
                target_id='user1@company.com',
                access='discover/role/owner'
            ),
            AccessGrant(
                asset_name='asset/folder/ec1fa4ec361494e0e3f80feed065fc2f',
                target_id='user2@company.com',
                access='discover/role/viewer'
            ),
            AccessGrant(
                asset_name='asset/folder/ec1fa4ec361494e0e3f80feed065fc2f',
                target_id='user3@company.com',
                access='discover/role/editor'
            )
        ]
        """

        access_grants = []  # type: List[AccessGrant]
        next_page = None

        # TODO: instead of blindly making multiple requests, we should only do this 1) if we have a successful
        # request, and 2) if we have a "next page"
        while True:
            list_request = discover_pb2.ListAccessGrantsRequest(
                asset_name=asset_name, page_token=next_page
            )

            response = self._discover_client.ListAccessGrants(list_request)
            for grant in response.access_grants:
                access_grants.append(
                    AccessGrant(grant.asset_name, grant.entity.id, grant.access)
                )

            next_page = response.next_page
            if not next_page:
                break

        return access_grants

    def create_folder(
        self,
        display_name: str,
        description: Optional[str] = None,
        parent_asset_name: Optional[str] = None,
    ) -> Asset:
        """
        Creates a folder asset.

        Parameters
        ----------
        display_name : str
            Display name for the folder.
        description : Optional[str]
            Optional description of the folder.
        parent_asset_name : Optional[str]
            Optionally create a folder inside another folder.
            If left blank, folder creation defaults to the caller's namespace.

        Returns
        -------
        asset : Asset
            The created folder asset.

        Example
        -------
        >>> discover.create_folder(
        ...     display_name="Geospatial Data",
        ...     description="This folder contains several remote sensing datasets", # optional
        ...     parent_asset_name="asset/folder/86c100d9ffa3c95f5c9236aa9a59d1dc" # optional
        ... ) # doctest: +SKIP
        Asset(
            asset_name='asset/folder/d95f5c9286c100d9ffa336aa9a59d1dc',
            display_name='Geospatial Data',
            is_shared=False,
            sym_link=None,
            description='This folder contains several remote sensing datasets',
            parent_asset_name='asset/folder/86c100d9ffa3c95f5c9236aa9a59d1dc'
        )
        """

        if display_name is None or display_name == "":
            raise ValueError("display_name cannot be missing or empty")

        create_request = discover_pb2.CreateAssetRequest(
            asset=discover_pb2.Asset(
                display_name=display_name,
                description=description,
                parent_name=parent_asset_name,
                folder=discover_pb2.Folder(),
            )
        )

        response = self._discover_client.CreateAsset(create_request)
        asset = Asset._from_proto(response)

        return asset

    def delete_asset(
        self,
        asset_name: str,
    ) -> Asset:
        """
        Deletes an asset.

        Parameters
        ----------
        asset_name : str
            The name of the asset

        Examples
        --------
        >>> discover.delete_asset(
        ...     asset_name="asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json"
        ... ) # doctest: +SKIP

        >>> discover.delete_asset(
        ...     asset_name="asset/folder/dd9ffa336aa9a59d1d95f5c9286c100c"
        ... ) # doctest: +SKIP
        """

        if asset_name is None or asset_name == "":
            raise ValueError("asset_name cannot be missing or empty")

        delete_request = discover_pb2.DeleteAssetRequest(asset_name=asset_name)

        self._discover_client.DeleteAsset(delete_request)

    def get_asset(self, asset_name: str) -> Asset:
        """
        Gets an asset.

        Parameters
        ----------
        asset_name : str
            Path of the file or folder to fetch.

        Returns
        -------
        asset : Asset
            Asset that only contains the asset name, display name and description.

        Examples
        --------
        >>> discover.get_asset(
        ...     asset_name="asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json"
        ... ) # doctest: +SKIP
        Asset(
            asset_name='asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json',
            display_name='data_file.json',
            is_shared=True,
            sym_link=None,
            description='',
            parent_asset_name='asset/namespace/f276fc038b3f49b4eed8c89338cac25a47a73397'
        )

        >>> discover.get_asset(
        ...     asset_name="asset/folder/dd9ffa336aa9a59d1d95f5c9286c100c"
        ... ) # doctest: +SKIP
        Asset(
            asset_name='asset/folder/dd9ffa336aa9a59d1d95f5c9286c100c',
            display_name='My Folder',
            is_shared=False,
            sym_link=None,
            description='',
            parent_asset_name='asset/namespace/f276fc038b3f49b4eed8c89338cac25a47a73397'
        )
        """

        asset = Asset._from_proto(
            self._discover_client.GetAsset(
                discover_pb2.GetAssetRequest(asset_name=asset_name)
            )
        )

        return asset

    def update_asset(
        self,
        asset_name,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Asset:
        """
        Updates display name and/or description for an asset.

        Parameters
        ----------
        asset_name : str
            Path of the file or folder to update.
        display_name : str (optional)
            New display name for the asset.
        description : str (optional)
            New description for the asset.

        Returns
        -------
        asset : Asset
            Asset that contains updated asset information (asset_name, display_name, description).

        Examples
        --------
        >>> discover.update_asset(
        ...     asset_name="asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json",
        ...     display_name="A Great JSON", # optional
        ...     description="This is the greatest json ever." # optional
        ... ) # doctest: +SKIP
        Asset(
            asset_name='asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json',
            display_name='A Great JSON',
            is_shared=False,
            sym_link=None,
            description='This is the greatest json ever.',
            parent_asset_name='asset/namespace/f276fc038b3f49b4eed8c89338cac25a47a73397'
        )

        >>> discover.update_asset(
        ...     asset_name="asset/folder/dd9ffa336aa9a59d1d95f5c9286c100c",
        ...     display_name="A Great Folder", # optional
        ...     description="This is the greatest folder ever." # optional
        ... ) # doctest: +SKIP
        Asset(
            asset_name='asset/folder/dd9ffa336aa9a59d1d95f5c9286c100c',
            display_name='A Great Folder',
            is_shared=False,
            sym_link=None,
            description='This is the greatest folder ever.',
            parent_asset_name='asset/namespace/f276fc038b3f49b4eed8c89338cac25a47a73397'
        )
        """

        asset = self.get_asset(asset_name)

        # if display name is not set, set it to the current value
        if display_name is None:
            display_name = asset.display_name

        # do not allow the user to set the display name to an empty string
        if display_name == "":
            raise ValueError("display_name cannot be an empty string")

        # if description is not set, set it to the current value
        # this field can be set to an empty string if the user wants to clear the current value
        if description is None:
            description = asset.description

        update_request = discover_pb2.UpdateAssetRequest(
            asset=discover_pb2.Asset(
                name=asset_name,
                display_name=display_name,
                description=description,
            )
        )
        response = self._discover_client.UpdateAsset(update_request)

        return Asset._from_proto(response)

    def move_asset(
        self, asset_name: str, new_parent_asset_name: Optional[str]
    ) -> Asset:
        """
        Move an asset into a different folder, or into the root of your workspace.

        Parameters
        ----------
        asset_name : str
            Asset name of the file or folder to move.
        new_parent_asset_name : str (optional)
            Asset name of the new parent asset. If this None, the asset will be moved into the root
            of your workspace.

        Returns
        -------
        asset : Asset
            Asset that was moved.

        Examples
        --------
        >>> discover.move_asset("asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json") # doctest: +SKIP
        Asset(
            asset_name='asset/blob/ec1fa4ec361494e0e3f80feed065fc2f:~/data_file.json',
            display_name='data_file.json',
            is_shared=True,
            sym_link=None,
            description='moving this file to my root',
            parent_asset_name='asset/namespace/f276fc038b3f49b4eed8c89338cac25a47a73397'
        )

        >>> discover.move_asset(
        ...     asset_name="asset/folder/ec1fa4ec361494e0e3f80feed065fc2f",
        ...     new_parent_asset_name="asset/folder/dd9ffa336aa9a59d1d95f5c9286c100c"
        ... ) # doctest: +SKIP
        Asset(
            asset_name='asset/folder/ec1fa4ec361494e0e3f80feed065fc2f',
            display_name='My Folder',
            is_shared=True,
            sym_link=None,
            description='moving this folder into another folder',
            parent_asset_name='asset/folder/dd9ffa336aa9a59d1d95f5c9286c100c'
        )
        """

        move_request = discover_pb2.MoveAssetRequest(
            asset_name=asset_name,
            destination_parent_name=new_parent_asset_name,
        )

        self._discover_client.MoveAsset(move_request)

        return self.get_asset(asset_name)

    def _page_list(self, asset_name: str, filters: str) -> Generator[Asset, None, None]:
        """
        Page through list results as a generator.

        Parameters
        ----------
        asset_name : str
            Asset name of the folder to list from.
        filters : str
            Filters to apply to the asset search query.
        """

        next_page = None
        while True:
            list_request = discover_pb2.ListAssetsRequest(
                parent_name=asset_name,
                page_token=next_page,
                filter=filters,
            )

            resp = self._discover_client.ListAssets(list_request)

            if not resp.assets:
                return

            yield from (Asset._from_proto_asset(elem) for elem in resp.assets)
            next_page = resp.next_page

    def list_assets(
        self,
        asset_name: Optional[Union[str, Organization]] = None,
        filters: Optional[Union[str, AssetListFilter]] = None,
    ) -> List[Asset]:
        """
        List child assets of the provided asset.

        Parameters
        ----------
        asset_name : Optional[Union[str, Organization]]
            Asset name of the folder or namespace to list from.

        filters : Optional[Union[str, AssetListFilter]]
            Field names and associated values to find assets matching the specified terms.

            ::

                Filters can be an encoded URL query string or an AssetListFilter object.

                Filters support the following fields:
                    type: If supplied, can be one or more supported AssetTypes.

                    name: If supplied, should be a string to find assets with the same display name.

                Wildcards supported by the name field:
                    * - Matches zero or more of any character between it and the next character in the filter.
                        Ex. name=Test* will find assets that start with Test which could include Test 1 and Test 2.

                        Ex. name=Te*.jpg will find assets like Test1.jpg, Test Image.jpg, and Testing 123.jpg

                    ? - Matches any one character.
                        Ex. name=Test?.jpg will find assets like Test1.jpg but not Test10.jpg

                Raw filter string examples:
                    type=blob&name=Test* : This filter will find Assets with a type of blob starting with the name Test.

                    type=blob&type=vector : This filter will find Assets with a type of blob or vector.

                    type=blob,vector : This syntax is an alternative to the above which is supported for types.

        Returns
        -------
        List[Asset]
            List of child assets of the given asset.

        Examples
        --------
        Calling .list_assets() with no arguments returns a list of all assets in the root of your workspace.

        >>> discover.list_assets() # doctest: +SKIP
        [
            Asset(
                asset_name='asset/folder/7278f8fa89cb28507d1b6543243a6f91',
                display_name='A Great Folder',
                is_shared=False,
                sym_link=None,
                description="There is important data in here",
                parent_asset_name='asset/namespace/eed8c8b3f49b49338cf276fc038ac25a47a73397'
            ),
            Asset(
                asset_name='asset/sym_link/b58507d1b7278f8fa89c6543143a6f91',
                display_name="Data Folder",
                is_shared=True,
                sym_link=SymLink(
                    target_asset_name='asset/folder/2ab1e07c2ee5b0f3cd58e97ec9fcc3f6'
                ),
                description='',
                parent_asset_name='asset/namespace/eed8c8b3f49b49338cf276fc038ac25a47a73397'
            ),
            Asset(
                asset_name='asset/blob/8f8fa89c6543143a6b58507d1b727f91:~/australia.geojson',
                display_name='australia.geojson',
                is_shared=True,
                sym_link=None,
                description='',
                parent_asset_name='asset/namespace/eed8c8b3f49b49338cf276fc038ac25a47a73397'
            )
        ]

        >>> discover.list_assets(asset_name="asset/folder/7578f8fa89cb58507d1b6543143a6f91") # doctest: +SKIP
        [
            Asset(
                asset_name='asset/folder/b585077278f8fa89cd1b6543143a6f91',
                display_name='A Great Folder',
                is_shared=False,
                sym_link=None,
                description="There is important data in here",
                parent_asset_name='asset/folder/7578f8fa89cb58507d1b6543143a6f91'
            ),
            Asset(
                asset_name='asset/folder/b585077278f8fa89cd1b6543143a6f91',
                display_name='Another Great Folder',
                is_shared=False,
                sym_link=None,
                description="There is important data in here too",
                parent_asset_name='asset/folder/7578f8fa89cb58507d1b6543143a6f91'
            )
        ]

        >>> discover.list_assets(Organization("descarteslabs")) # doctest: +SKIP
        [
            Asset(
                asset_name='asset/folder/b585077278f8fa89cd1b6543143a6f91',
                display_name='A Great Folder',
                is_shared=False,
                sym_link=None,
                description="There is important data in here",
                parent_asset_name='asset/namespace/org:descarteslabs'
            ),
            Asset(
                asset_name='asset/folder/b585077278f8fa89cd1b6543143a6f91',
                display_name='Another Great Folder',
                is_shared=False,
                sym_link=None,
                description="There is important data in here too",
                parent_asset_name='asset/namespace/org:descarteslabs'
            )
        ]

        Calling .list_assets() with a set of filters to find assets that are of type blob.

        >>> discover.list_assets(filters="type=blob,vector&name=australia*") # doctest: +SKIP
        [
            Asset(
                asset_name='asset/blob/8f8fa89c6543143a6b58507d1b727f91:~/australia.geojson',
                display_name='australia.geojson',
                is_shared=True,
                sym_link=None,
                description='',
                parent_asset_name='asset/namespace/eed8c8b3f49b49338cf276fc038ac25a47a73397'
            ),
            Asset(
                asset_name='asset/blob/3a68f8fa89c654314b58507d1b727f91:~/australia.txt',
                display_name='australia.txt',
                is_shared=True,
                sym_link=None,
                description='',
                parent_asset_name='asset/namespace/eed8c8b3f49b49338cf276fc038ac25a47a73397'
            )
        ]

        >>> discover.list_assets(filters={ "type": "blob" }) # doctest: +SKIP
        [
            Asset(
                asset_name='asset/blob/8f8fa89c6543143a6b58507d1b727f91:~/australia.geojson',
                display_name='australia.geojson',
                is_shared=True,
                sym_link=None,
                description='',
                parent_asset_name='asset/namespace/eed8c8b3f49b49338cf276fc038ac25a47a73397'
            )
        ]

        Calling .list_assets() with a set of filters using wildcards to find assets that contain
        "tra" in their display name.

        >>> discover.list_assets(filters={ "name": "*tra*" }) # doctest: +SKIP
        [
            Asset(
                asset_name='asset/blob/8f8fa89c6543143a6b58507d1b727f91:~/australia.geojson',
                display_name='australia.geojson',
                is_shared=True,
                sym_link=None,
                description='',
                parent_asset_name='asset/namespace/eed8c8b3f49b49338cf276fc038ac25a47a73397'
            )
        ],
        [
            Asset(
                asset_name='asset/blob/89c6548f3143a6b58507d8fa1b727f91:~/parks-and-trails.geojson',
                display_name='parks-and-trails.geojson',
                is_shared=False,
                sym_link=None,
                description='',
                parent_asset_name='asset/namespace/eed8c8b3f49b49338cf276fc038ac25a47a73397'
            )
        ]

        """

        if asset_name is None:
            asset_name = ""

        if filters is None:
            filters = ""

        if isinstance(asset_name, Organization):
            asset_name = f"{Organization._prefix}{asset_name}"

        if asset_name.startswith(Organization._prefix):
            asset_name = _organization_namespace_asset_name(asset_name)

        if not isinstance(filters, str):
            # some character don't need to be encoded since this isn't a full url
            filters = urllib.parse.urlencode(
                AssetListFilter(filters),
                doseq=True,
                safe="/[]+'*?, ",
                quote_via=urllib.parse.quote,
            )

        return list(self._page_list(asset_name, filters))

    def list_org_users(self, search=None):
        """
        List the users in your organization.

        If you're part of an organization, this will list all the users in your
        organization. If you provide a search string, only those users matching
        the search string in their email or name using a case-insensitive match,
        will be returned.

        Parameters
        ----------
        search : str, optional
            A search string that will be matched using a case-insensitive match against
            each user's email and name, and only the matched entries will be returned.

        Returns
        -------
        list(dict)
            A list containing a dictionary for each user with two entries:

            - email: the email of the user;
            - name: the name of the user.

            The list will be empty if you're not part of an organization or if no
            matches are found.

        Raises
        ------
        AuthError
            If you don't have proper authorization.
        """

        return _IamClient.get_default_client().list_org_users(search)


class _IamClient(Service):
    _instance = None

    def __init__(self, url=None, auth=None, retries=None):
        if auth is None:
            auth = Auth.get_default_auth()

        if url is None:
            url = get_settings().iam_url

        super().__init__(url, auth=auth, retries=retries)

    @property
    def session(self):
        # IAM for now only authenticates through cookies, so we bolt that on
        session = super().session
        cookie_value = "JWT=%s" % self.token

        if session.headers.get("Cookie") != cookie_value:
            session.headers["Cookie"] = cookie_value

        return session

    @classmethod
    def get_default_client(cls):
        """Retrieve the default IAM client."""

        if cls._instance is None:
            cls._instance = _IamClient()

        return cls._instance

    @classmethod
    def set_default_client(cls, client):
        """Change the default IAM client to the given IAM client."""

        if not isinstance(client, cls):
            raise TypeError(f"You must use a {cls.__name__} instance")

        cls._instance = client

    def list_org_users(self, search=None):
        url = "/org/self"

        if search:
            url += f"?q={search}"

        response = self.session.get(url)
        return response.json()
