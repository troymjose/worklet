from typing import TypeVar, Callable, Any

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
