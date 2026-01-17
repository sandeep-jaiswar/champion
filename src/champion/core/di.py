"""Dependency injection container for service locator pattern.

Enables loose coupling by allowing services to be registered and resolved
at runtime. This makes testing easier and deployment configurations flexible.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class RegistrationError(Exception):
    """Raised when service registration fails."""

    pass


class ResolutionError(Exception):
    """Raised when service cannot be resolved."""

    pass


class ServiceDescriptor(Generic[T]):
    """Describes how to create a service instance."""

    def __init__(
        self,
        service_type: type[T],
        factory: Callable[..., T],
        lifetime: str = "transient",
    ):
        self.service_type = service_type
        self.factory = factory
        self.lifetime = lifetime  # transient, singleton, scoped
        self._instance: T | None = None

    def create_instance(self, container: Container) -> T:
        """Create service instance respecting lifetime."""
        if self.lifetime == "singleton" and self._instance is not None:
            return self._instance

        instance = self.factory(container)

        if self.lifetime == "singleton":
            self._instance = instance

        return instance


class Container:
    """IoC container for dependency injection.

    Usage:
        container = Container()
        container.register(DataSource, lambda c: FileDataSource())
        container.register(Validator, lambda c: JSONSchemaValidator())

        # Later:
        source = container.resolve(DataSource)
        validator = container.resolve(Validator)
    """

    def __init__(self):
        self._services: dict[type, ServiceDescriptor[Any]] = {}
        self._singletons: dict[type, Any] = {}

    def register(
        self,
        service_type: type[T],
        factory: Callable[[Container], T] | T,
        lifetime: str = "transient",
    ) -> None:
        """Register a service in the container.

        Args:
            service_type: Service interface/type
            factory: Factory function or instance
            lifetime: 'transient' (new each time), 'singleton', or 'scoped'
        """
        if not callable(factory):
            # If instance provided, wrap it in factory
            instance = factory

            def factory_fn(_: Any) -> T:
                return instance  # type: ignore

            factory_fn_typed: Any = factory_fn  # Wrapper function for instance
        else:
            factory_fn_typed = factory  # type: ignore[assignment]

        descriptor = ServiceDescriptor(service_type, factory_fn_typed, lifetime)
        self._services[service_type] = descriptor

    def resolve(self, service_type: type[T]) -> T:
        """Resolve service instance.

        Args:
            service_type: Service type to resolve

        Returns:
            Service instance

        Raises:
            ResolutionError: If service not registered
        """
        if service_type not in self._services:
            raise ResolutionError(
                f"Service {service_type.__name__} not registered. "
                f"Available services: {list(self._services.keys())}"
            )

        descriptor = self._services[service_type]
        return descriptor.create_instance(self)

    def is_registered(self, service_type: type) -> bool:
        """Check if service is registered."""
        return service_type in self._services


class ServiceLocator:
    """Global service locator (static accessor for container).

    Use sparingly - prefer constructor injection when possible.
    Useful for:
        - Legacy code migration
        - Plugin systems
        - Testing helpers
    """

    _container: Container | None = None

    @classmethod
    def set_container(cls, container: Container) -> None:
        """Set the global container."""
        cls._container = container

    @classmethod
    def get_container(cls) -> Container:
        """Get the global container."""
        if cls._container is None:
            cls._container = Container()
        return cls._container

    @classmethod
    def resolve(cls, service_type: type[T]) -> T:
        """Resolve service from global container."""
        return cls.get_container().resolve(service_type)


def get_container() -> Container:
    """Get the global DI container."""
    return ServiceLocator.get_container()
