"""Owned callback registrations."""

from abc import ABC, abstractmethod
from collections.abc import Callable


class SubscriptionABC(ABC):
    """Release capability returned by a callback-registration authority."""

    @abstractmethod
    def release(self) -> bool:
        """Release the registration, returning whether it was still active."""


class CallbackSubscription(SubscriptionABC):
    """Subscription backed by its registration authority's release operation."""

    def __init__(self, release: Callable[[], bool]) -> None:
        self._release = release

    def release(self) -> bool:
        return self._release()


__all__ = ["CallbackSubscription", "SubscriptionABC"]
