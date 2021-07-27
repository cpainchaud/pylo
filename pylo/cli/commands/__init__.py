from typing import Dict


class Command:
    def __init__(self, name: str, main_func, parser_func):
        self.name: str = name
        self.main = main_func
        self.fill_parser = parser_func
        available_commands[name] = self


available_commands: Dict[str, Command] = {}

from .ruleset_export import command_object