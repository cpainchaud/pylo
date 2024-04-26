from typing import Union
from .Exception import PyloEx
from .LabelStore import label_type_role, label_type_env, label_type_loc, label_type_app, LabelStore


class LabelCommon:

    __slots__ = ['owner', 'name', 'href', 'type']

    def __init__(self, name: str, href: str, label_type: str, owner: LabelStore):
        self.owner: LabelStore = owner
        self.name: str = name
        self.href: str = href
        self.type = label_type

    def is_label(self) -> bool:
        raise PyloEx("not implemented")

    def is_group(self) -> bool:
        raise PyloEx("not implemented")

    def type_to_short_string(self) -> str:
        return self.type

    def type_is_location(self) -> bool:
        return self.type == 'loc'

    def type_is_environment(self) -> bool:
        return self.type == 'env'

    def type_is_application(self) -> bool:
        return self.type == 'app'

    def type_is_role(self) -> bool:
        return self.type == 'role'

    def type_string(self) -> str:
        return self.type

    def api_set_name(self, new_name: str):
        find_collision = self.owner.find_label_by_name_and_type(new_name, self.type)
        if find_collision is not self:
            raise PyloEx("A Label/LabelGroup with name '{}' already exists".format(new_name))

        if self.is_group():
            self.owner.owner.connector.objects_labelgroup_update(self.href, data={'name': new_name})
        else:
            self.owner.owner.connector.objects_label_update(self.href, data={'value': new_name})

        self.name = new_name
