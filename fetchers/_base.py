from abc import ABC, abstractmethod
from typing import Any, Generator

from const import FETCH_RESULT
from saver import Saver


class Fetcher(ABC):
    @abstractmethod
    def should_fetch(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def iter_data(self) -> Generator[Any, None, None]:
        raise NotImplementedError

    @abstractmethod
    def process_data(self, data: Any) -> Any:
        raise NotImplementedError

    @abstractmethod
    def should_save(self, data: Any) -> bool:
        raise NotImplementedError

    @abstractmethod
    def save_data(self, data: Any, saver: Saver) -> None:
        raise NotImplementedError

    @abstractmethod
    def is_success(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def run(self, saver: Saver) -> FETCH_RESULT:
        if not self.should_fetch():
            return FETCH_RESULT.SKIPPED

        for data in self.iter_data():
            processed_data: Any = self.process_data(data)
            if not self.should_save(data):
                continue
            self.save_data(processed_data, saver)

        return FETCH_RESULT.SUCCESSED if self.is_success() else FETCH_RESULT.FAILED
