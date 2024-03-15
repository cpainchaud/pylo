from typing import Dict, Optional, List


class Command:
    def __init__(self, name: str, main_func, parser_func, load_specific_objects_only: Optional[List[str]] = None,
                 skip_pce_config_loading: bool = False,
                 native_parsers_as_class: Optional = None,
                 credentials_manager_mode: bool = False):
        self.name: str = name
        self.main = main_func
        self.fill_parser = parser_func
        self.load_specific_objects_only: Optional[List[str]] = load_specific_objects_only
        self.skip_pce_config_loading = skip_pce_config_loading
        self.native_parsers = native_parsers_as_class
        self.credentials_manager_mode = credentials_manager_mode
        available_commands[name] = self


available_commands: Dict[str, Command] = {}

from .ruleset_export import command_object
from .workload_used_in_rule_finder import command_object
from .workload_update import command_object
from .ven_duplicate_remover import command_object
from .workload_export import command_object
from .iplist_import_from_file import command_object
from .update_pce_objects_cache import command_object
from .ven_upgrader import command_object
from .workload_import import command_object
from .ven_idle_to_visibility import command_object
from .workload_reset_names_to_null import command_object
from .credential_manager import command_object
from .iplist_analyzer import command_object
from .ven_compatibility_report_export import command_object
