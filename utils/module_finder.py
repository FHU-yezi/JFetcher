from importlib import import_module
from os import listdir
from typing import List


def get_all_modules(base_path: str) -> List[str]:
    return [
        x.split(".")[0]
        for x in listdir(base_path)
        if x.endswith(".py") and not x.startswith("_")
    ]


def get_all_fetchers(base_path: str) -> List:
    modules: List[str] = get_all_modules(base_path)
    result: List = []
    for module_name in modules:
        module_obj = import_module(f"{base_path.split('/')[1]}.{module_name}")

        for name, obj in module_obj.__dict__.items():
            if name.endswith("Fetcher") and name != "Fetcher":
                result.append(obj)

    return result
