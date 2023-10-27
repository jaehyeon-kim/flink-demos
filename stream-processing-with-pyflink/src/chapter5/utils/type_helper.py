from abc import ABC, abstractstaticmethod
from typing import List, Dict

from pyflink.common.typeinfo import Types, TypeInformation


class TypeMapping(ABC):
    @abstractstaticmethod
    def type_mapping():
        pass


def set_type_info(type_mapping: Dict[str, TypeInformation], selects: List[str] = []):
    names, types = [], []
    for key in type_mapping.keys():
        if not selects or key in selects:
            names.append(key)
            types.append(type_mapping[key])
    return Types.ROW_NAMED(field_names=names, field_types=types)
