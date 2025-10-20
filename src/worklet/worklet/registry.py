import logging
from typing import TypeVar, Callable, Any, Dict, Iterator
from worklet.worker import runtime
from worklet.portal.models import PortalModel
from worklet.worklet.models import WorkletModel

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


class Singleton(type):
    """ Singleton Metaclass for creating singleton classes """
    # Store instances of all classes which uses this class as metaclass
    _instances = {}

    # Override __call__ method to return same instance of class
    def __call__(cls, *args, **kwargs):
        """ This method is called for every class instance creation """
        # If class is not in instances dictionary, create new instance and store it
        if cls not in cls._instances:
            # Create new instance of class and store it in class _instances dictionary class variable
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        # Return stored instance
        return cls._instances[cls]


class WorkletRegistry(metaclass=Singleton):
    """ Registry for worklets, allowing registration and retrieval of worklets by name. """

    def __init__(self):
        """ Initialize the worklet registry with an empty dictionary. """
        self._portal: PortalModel | None = None
        self._worklets: Dict[str, WorkletModel] = {}

    def set_portal(self, portal: PortalModel):
        """ Set the portal for the registry if not already set. """
        if self._portal is None:
            self._portal = portal

    @property
    def portal(self) -> PortalModel | None:
        """ Get the portal associated with the registry. """
        return self._portal

    @property
    def worklets(self) -> Dict[str, WorkletModel]:
        """ Get the dictionary of registered worklets. """
        return self._worklets

    def register(self, worklet: WorkletModel) -> None:
        """ Register a worklet with the given name, portal, function, and configuration. """
        if worklet.portal != runtime.PORTAL:
            return
        # Validate the provided function
        if not callable(worklet.func):
            raise TypeError(f"Provided func for worklet '{worklet.name}' is not callable.")
        # # Unique name for the worklet based on its module and function name
        # worklet_unique_name: str = f"{func.__module__}.{func.__name__}"
        # Check if the worklet is already registered
        if worklet.id in self._worklets:
            raise ValueError(f"Worklet '{worklet.id}' already registered.")
        # Register the worklet in the internal dictionary
        self._worklets[worklet.id] = worklet
        # Log the registration of the worklet
        logger.info(f"Registered task {worklet.id}")
        # If portal is not set, set it to the portal of the first registered worklet
        if self._portal is None:
            self._portal = worklet.portal

    def get(self, *, id: str) -> WorkletModel:
        """ Retrieve a worklet by its unique name. Raises KeyError if not found. """
        # Get the worklet from the internal dictionary
        result: WorkletModel = self._worklets.get(id, None)
        # If the worklet is not found, raise a KeyError
        if not result:
            raise KeyError(f"Worklet '{id}' not found in registry.")
        # Return the worklet
        return result

    def __iter__(self) -> Iterator[tuple[str, WorkletModel]]:
        """ Return an iterator over the registered worklets. """
        return iter(self._worklets.items())

    def __len__(self) -> int:
        """ Return the number of registered worklets. """
        return len(self._worklets)
