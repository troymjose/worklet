from typing import Union, Any
from abc import ABC, abstractmethod


class Tracker(ABC):
    @abstractmethod
    def add(self, key: str, value: any) -> None:
        raise NotImplementedError()

    @abstractmethod
    def remove(self, key: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def pop(self, key: str) -> Union[Any, None]:
        raise NotImplementedError()

    @abstractmethod
    def exists(self, key) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def get(self, key) -> Union[Any, None]:
        raise NotImplementedError()

    @abstractmethod
    def keys(self) -> set[str]:
        raise NotImplementedError()

    @abstractmethod
    def values(self) -> list[any]:
        raise NotImplementedError()
