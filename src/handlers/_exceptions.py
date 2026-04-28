"""Typed handler exceptions consumed by the receiver's TTL-DLX failure path."""


class MissingDependencyError(Exception):
    """Raised when a handler cannot resolve a referenced entity in Salesforce.

    The receiver's `_handle_failure` reads `identifier_label` to build the
    `x-error` header (`f"missing-{identifier_label}"`) and emits the value
    under `x-missing-<label>` so cross-team DLQ dashboards can pivot on
    *what* was missing without parsing the original payload.
    """

    def __init__(self, identifier_label: str, identifier_value: str) -> None:
        self.identifier_label = identifier_label
        self.identifier_value = identifier_value
        super().__init__(f"missing-{identifier_label}: {identifier_value}")
