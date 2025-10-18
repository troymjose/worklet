import inspect
from typing import Callable


class FuncUtils:
    """ Utility class for function introspection and manipulation. """

    @staticmethod
    def get_canonical_func(*, func: Callable) -> Callable:
        """ Resolves the unwrapped function."""
        return inspect.unwrap(func)

    @staticmethod
    def get_canonical_func_name(*, func: Callable) -> str:
        """ Resolves the unwrapped function name. """
        return FuncUtils.get_canonical_func(func=func).__name__

    @staticmethod
    def get_canonical_func_path(*, func: Callable) -> str:
        """
        Resolves the fully qualified and unwrapped function path as `module.function`.

        This ensures that decorated functions, and differences in import paths,
        still produce consistent identifiers.
        """
        original_func = FuncUtils.get_canonical_func(func=func)
        module = inspect.getmodule(original_func)

        if module is None:
            raise ValueError(f"Cannot resolve module for function {func}")

        return f"{module.__name__}.{original_func.__name__}"


func_utils = FuncUtils()
