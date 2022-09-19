from importlib import import_module
from os import listdir
from typing import List


def get_all_modules(base_path: str) -> List[str]:
    return [
        x.split(".")[0]
        for x in listdir(base_path)
        if x.endswith(".py")
    ]


def run_all_modules(base_path: str) -> None:
    modules: List[str] = get_all_modules(base_path)
    for module in modules:
        import_module(f"{base_path.split('/')[1]}.{module}")
