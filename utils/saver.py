from queue import Queue
from typing import Dict, List

from utils.db import get_collection


class Saver():
    def __init__(self, db_name: str, data_bulk_size: int) -> None:
        self._db = get_collection(db_name)
        self._queue: Queue = Queue()
        self._bulk_size: int = data_bulk_size
        self._data_count: int = 0

    def _should_save(self) -> bool:
        return self._queue.qsize() >= self._bulk_size

    def get_data_count(self) -> int:
        result: int = self._data_count
        self._data_count = 0  # 重置已采集数据量
        return result

    def _save_to_db(self) -> None:
        data_to_save: List[Dict] = []
        # 取出队列中的所有数据
        while not self._queue.empty():
            data_to_save.append(self._queue.get())

        self._db.insert_many(data_to_save)

    def add_data(self, data: Dict) -> None:
        self._queue.put(data)
        self._data_count += 1

        # 如果达到批大小，将数据保存到数据库
        if self._should_save():
            self._save_to_db()

    def final_save(self) -> None:
        if self._queue.qsize() != 0:
            self._save_to_db()
