from abc import ABC, abstractmethod
from time import time
from typing import Any, Dict, Generator, Tuple

from const import FETCH_RESULT
from saver import Saver
from utils.time_helper import cron_str_to_kwargs


class Fetcher(ABC):
    @abstractmethod
    def __init__(self) -> None:
        self.task_name = ""
        self.fetch_time_cron = ""
        self.collection_name = ""
        self.bulk_size = 0
        raise NotImplementedError

    @property
    def fetch_time_cron_kwargs(self) -> Dict[str, str]:
        return cron_str_to_kwargs(self.fetch_time_cron)

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
    def run(self) -> Tuple[FETCH_RESULT, int, int]:
        start_time = time()

        if not self.should_fetch():
            return (FETCH_RESULT.SKIPPED, 0, 0)

        saver = Saver(self.collection_name, self.bulk_size)

        for data in self.iter_data():
            processed_data: Any = self.process_data(data)
            if not self.should_save(data):
                continue
            self.save_data(processed_data, saver)

        fetch_result = (
            FETCH_RESULT.SUCCESSED if self.is_success() else FETCH_RESULT.FAILED
        )
        cost_time = round(time() - start_time)

        return (fetch_result, cost_time, saver.data_count)
