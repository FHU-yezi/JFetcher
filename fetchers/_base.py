from abc import ABC, abstractmethod
from time import time
from typing import Dict, Generator, Tuple

from constants import FetchResult
from saver import Saver
from utils.log import run_logger
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
    def should_fetch(self, saver: Saver) -> bool:
        raise NotImplementedError

    @abstractmethod
    def iter_data(self) -> Generator[Dict, None, None]:
        raise NotImplementedError

    @abstractmethod
    def process_data(self, data: Dict) -> Dict:
        raise NotImplementedError

    @abstractmethod
    def should_save(self, data: Dict, saver: Saver) -> bool:
        raise NotImplementedError

    @abstractmethod
    def save_data(self, data: Dict, saver: Saver) -> None:
        raise NotImplementedError

    @abstractmethod
    def is_success(self, saver: Saver) -> bool:
        raise NotImplementedError

    def run(self) -> Tuple[FetchResult, int, int]:
        start_time = time()

        saver = Saver(self.collection_name, self.bulk_size)
        run_logger.debug(f"已为任务 {self.task_name} 创建存储对象")

        if not self.should_fetch(saver):
            run_logger.debug(f"已跳过任务 {self.task_name}")
            return (FetchResult.SKIPPED, 0, 0)

        for original_data in self.iter_data():
            processed_data: Dict = self.process_data(original_data)
            if not self.should_save(processed_data, saver):
                continue
            self.save_data(processed_data, saver)
        saver.final_save()

        fetch_result = (
            FetchResult.SUCCESSED if self.is_success(saver) else FetchResult.FAILED
        )
        cost_time = round(time() - start_time)

        return (fetch_result, cost_time, saver.data_count)
