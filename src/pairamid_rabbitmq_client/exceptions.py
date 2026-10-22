"""Public errors for contract validation and confirmed publication."""

from dataclasses import dataclass


class PublishError(RuntimeError):
    """A publication could not be confirmed as routed."""


class UnroutableMessageError(PublishError):
    """The broker returned a mandatory publication without a destination."""


class PublishNackedError(PublishError):
    """The broker negatively acknowledged the publication."""


class PublishConnectionError(PublishError):
    """The connection failed; delivery may be ambiguous and retries may duplicate."""


@dataclass(frozen=True)
class ValidationIssue:
    path: str
    message: str


class ContractValidationError(ValueError):
    """A message violates a versioned contract."""

    def __init__(self, errors: list[ValidationIssue]) -> None:
        self.errors = tuple(errors)
        super().__init__("; ".join(f"{e.path}: {e.message}" for e in errors))
