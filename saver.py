from typing import Any, Dict, List, Sequence

from utils.db import get_collection


class Saver:
    def __init__(self, collection_name: str, bulk_size: int) -> None:
        if bulk_size <= 0:
            raise ValueError

        self._collection = get_collection(collection_name)
        self._bulk_size = bulk_size
        self._can_add = True
        self._data_count = 0
        self._queue: List[Dict] = []

    @property
    def data_count(self) -> int:
        return self._data_count

    def _should_save_to_db(self) -> bool:
        return len(self._queue) >= self._bulk_size

    def _save_to_db(self) -> None:
        self._collection.insert_many(self._queue)
        self._queue.clear()

    def is_in_db(self, query: Dict[str, Any]) -> bool:
        return self._collection.count_documents(query) != 0

    def final_save(self) -> None:
        self._save_to_db()
        self._can_add = False

    def add_one(self, data: Dict) -> None:
        if not self._can_add:
            raise ValueError

        self._queue.append(data)
        self._data_count += 1

        if self._should_save_to_db():
            self._save_to_db()

    def add_many(self, data: Sequence[Dict]) -> None:
        if not self._can_add:
            raise ValueError

        self._queue.extend(data)
        self._data_count += len(data)

        if self._should_save_to_db():
            self._save_to_db()
