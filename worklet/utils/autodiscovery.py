import sys
import logging
import importlib
from pathlib import Path
from contextlib import contextmanager
from typing import Optional, Generator

logger = logging.getLogger(__name__)


@contextmanager
def temp_sys_path(path: str):
    if path not in sys.path:
        sys.path.insert(0, path)
        added = True
    else:
        added = False
    try:
        yield
    finally:
        if added:
            sys.path.remove(path)


class AutoDiscovery:
    def __init__(self,
                 folder_name: str,
                 search_from: Optional[Path] = None,
                 ignore_dirs: Optional[list[str]] = None, ):
        self.folder_name = folder_name
        self.search_from = search_from or Path.cwd()
        self.ignore_dirs = set(ignore_dirs or {".venv", "venv", "__pycache__", ".git", ".idea", ".mypy_cache"})

    def _should_ignore_path(self, path: Path) -> bool:
        return any(part in self.ignore_dirs or part.startswith(".") for part in path.parts)

    @staticmethod
    def _find_package_root(path: Path) -> Path:
        while path != path.parent:
            if not (path / "__init__.py").exists():
                return path
            path = path.parent
        return path

    @staticmethod
    def _convert_to_module_name(file_path: Path, package_root: Path) -> str:
        relative = file_path.relative_to(package_root)
        return ".".join(relative.with_suffix("").parts)

    def _target_python_files(self) -> Generator[tuple[str, Path], None, None]:
        """
        Lazily yield module_name and package_root for each .py file under folders named `folder_name`.
        """
        for path in self.search_from.rglob("*"):
            if self._should_ignore_path(path):
                continue

            if path.is_dir() and path.name == self.folder_name:
                package_root = self._find_package_root(path)
                for py_file in path.rglob("*.py"):
                    if py_file.name.startswith("__") or self._should_ignore_path(py_file):
                        continue
                    module_name = self._convert_to_module_name(py_file, package_root)
                    yield module_name, package_root

    def discover(self) -> None:
        """
        Lazily import discovered Python modules inside folders named `folder_name`.
        """
        found = False
        for module_name, package_root in self._target_python_files():
            found = True
            try:
                with temp_sys_path(str(package_root)):
                    importlib.import_module(module_name)
                logger.info(f"✅ Imported module: {module_name}")
            except Exception as e:
                logger.error(f"❌ Failed to import {module_name}: {e}", exc_info=True)

        if not found:
            logger.warning(f"No modules found in folders named '{self.folder_name}' for autodiscovery.")
