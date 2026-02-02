from typing import Dict, Optional, Set, TYPE_CHECKING

if TYPE_CHECKING:
    from .API.JsonPayloadTypes import LabelDimensionObjectStructure

# Module constants for dimension keys
DIMENSION_KEY_ROLE: str = 'role'
DIMENSION_KEY_APP: str = 'app'
DIMENSION_KEY_ENV: str = 'env'
DIMENSION_KEY_LOC: str = 'loc'

# Set of built-in dimension keys
BUILTIN_DIMENSION_KEYS: Set[str] = {DIMENSION_KEY_ROLE, DIMENSION_KEY_APP, DIMENSION_KEY_ENV, DIMENSION_KEY_LOC}


class LabelDimension:
    """
    Represents a label dimension (type) in the Illumio PCE.
    
    Label dimensions define the categories of labels (e.g., role, app, env, loc).
    Built-in dimensions are the standard four dimensions, while custom dimensions
    can be created by the user.
    """

    __slots__ = ['key', 'display_name', 'href', 'created_at', 'created_by', 'updated_at', 'updated_by', 'raw_json']

    def __init__(self, key: str, display_name: str, href: Optional[str] = None,
                 created_at: Optional[str] = None, created_by: Optional[str] = None,
                 updated_at: Optional[str] = None, updated_by: Optional[str] = None,
                 raw_json: Optional['LabelDimensionObjectStructure'] = None) -> None:
        self.key: str = key
        self.display_name: str = display_name
        self.href: Optional[str] = href
        self.created_at: Optional[str] = created_at
        self.created_by: Optional[str] = created_by
        self.updated_at: Optional[str] = updated_at
        self.updated_by: Optional[str] = updated_by
        self.raw_json: Optional['LabelDimensionObjectStructure'] = raw_json

    @staticmethod
    def from_json(json_data: 'LabelDimensionObjectStructure') -> 'LabelDimension':
        """
        Factory method to create a LabelDimension from JSON data returned by the API.
        
        :param json_data: The JSON structure from the API
        :return: A new LabelDimension instance
        """
        created_by: Optional[str] = None
        if json_data.get('created_by') is not None:
            created_by = json_data['created_by'].get('href')
        
        updated_by: Optional[str] = None
        if json_data.get('updated_by') is not None:
            updated_by = json_data['updated_by'].get('href')

        return LabelDimension(
            key=json_data['key'],
            display_name=json_data['display_name'],
            href=json_data.get('href'),
            created_at=json_data.get('created_at'),
            created_by=created_by,
            updated_at=json_data.get('updated_at'),
            updated_by=updated_by,
            raw_json=json_data
        )

    @staticmethod
    def create_builtin_dimensions() -> Dict[str, 'LabelDimension']:
        """
        Create the four built-in label dimensions with default display names.
        
        :return: A dictionary mapping dimension keys to LabelDimension objects
        """
        return {
            DIMENSION_KEY_ROLE: LabelDimension(key=DIMENSION_KEY_ROLE, display_name='Role'),
            DIMENSION_KEY_APP: LabelDimension(key=DIMENSION_KEY_APP, display_name='Application'),
            DIMENSION_KEY_ENV: LabelDimension(key=DIMENSION_KEY_ENV, display_name='Environment'),
            DIMENSION_KEY_LOC: LabelDimension(key=DIMENSION_KEY_LOC, display_name='Location'),
        }

    def is_role(self) -> bool:
        """Check if this dimension is the Role dimension."""
        return self.key == DIMENSION_KEY_ROLE

    def is_application(self) -> bool:
        """Check if this dimension is the Application dimension."""
        return self.key == DIMENSION_KEY_APP

    def is_environment(self) -> bool:
        """Check if this dimension is the Environment dimension."""
        return self.key == DIMENSION_KEY_ENV

    def is_location(self) -> bool:
        """Check if this dimension is the Location dimension."""
        return self.key == DIMENSION_KEY_LOC

    def is_builtin(self) -> bool:
        """Check if this dimension is one of the four built-in dimensions."""
        return self.key in BUILTIN_DIMENSION_KEYS

    def is_custom(self) -> bool:
        """Check if this dimension is a custom (non-built-in) dimension."""
        return self.key not in BUILTIN_DIMENSION_KEYS

    def __repr__(self) -> str:
        return f"LabelDimension(key='{self.key}', display_name='{self.display_name}')"

    def __str__(self) -> str:
        return f"{self.display_name} ({self.key})"
