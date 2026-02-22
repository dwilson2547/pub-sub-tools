"""
Clearer factory — returns the right :class:`BaseClearer` for the requested
pub-sub system.
"""

from __future__ import annotations

from typing import Dict, Type

from .base import BaseClearer, ClearerStats, ThreadSafeCounter  # noqa: F401

# Lazy mapping: system name → (module path, class name)
# Modules are imported only when actually requested so that missing optional
# dependencies do not break unrelated clearers.
_REGISTRY: Dict[str, tuple] = {
    "kafka": ("clearers.kafka", "KafkaClearer"),
    "pulsar": ("clearers.pulsar", "PulsarClearer"),
    "streamnative": ("clearers.pulsar", "PulsarClearer"),
    "iggy": ("clearers.iggy", "IggyClearer"),
    "googlepubsub": ("clearers.googlepubsub", "GooglePubSubClearer"),
    "eventhubs": ("clearers.eventhubs", "EventHubsClearer"),
}

SUPPORTED_SYSTEMS = list(_REGISTRY.keys())


def get_clearer(system: str, config: dict) -> BaseClearer:
    """Instantiate and return the clearer for *system*.

    Args:
        system: One of ``kafka``, ``pulsar``, ``streamnative``, ``iggy``,
                ``googlepubsub``, ``eventhubs`` (case-insensitive).
        config: System-specific configuration dictionary.

    Returns:
        An uninitialised :class:`BaseClearer` subclass instance.

    Raises:
        ValueError: If *system* is not recognised.
    """
    key = system.lower().replace("-", "").replace("_", "")
    if key not in _REGISTRY:
        raise ValueError(
            f"Unknown pub-sub system '{system}'. "
            f"Supported: {', '.join(SUPPORTED_SYSTEMS)}"
        )

    module_path, class_name = _REGISTRY[key]
    import importlib

    module = importlib.import_module(module_path)
    cls: Type[BaseClearer] = getattr(module, class_name)
    return cls(config)
