"""The sidecar's typed errors, each with its HTTP status and JSON body.

Bodies are adjacently tagged like the spec's enums: `{"type": ...,
"data": {...}}`, or `{"type": ...}` for a variant without data.
"""

import json
from typing import Any, ClassVar


class SidecarError(Exception):
    """Every error the HTTP contract names."""

    status: ClassVar[int]
    kind: ClassVar[str]

    def data(self) -> dict[str, Any] | None:
        return None

    def body(self) -> bytes:
        payload: dict[str, Any] = {"type": self.kind}
        data = self.data()
        if data is not None:
            payload["data"] = data
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()


class InvalidRequest(SidecarError):
    status = 400
    kind = "invalid_request"

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason

    def data(self) -> dict[str, Any]:
        return {"reason": self.reason}


class NotFound(SidecarError):
    status = 404
    kind = "not_found"


class MethodNotAllowed(SidecarError):
    status = 405
    kind = "method_not_allowed"


class PayloadTooLarge(SidecarError):
    status = 413
    kind = "payload_too_large"

    def __init__(self, limit_bytes: int) -> None:
        super().__init__(f"request body above {limit_bytes} bytes")
        self.limit_bytes = limit_bytes

    def data(self) -> dict[str, Any]:
        return {"limit_bytes": self.limit_bytes}


class TooFewSamples(SidecarError):
    """A topic fit over fewer documents than its parameters need."""

    status = 422
    kind = "too_few_samples"

    def __init__(self, needed: int, got: int) -> None:
        super().__init__(f"topic fit needs {needed} documents, got {got}")
        self.needed = needed
        self.got = got

    def data(self) -> dict[str, Any]:
        return {"needed": self.needed, "got": self.got}


class TooFewPoints(SidecarError):
    """A layout over no more points than UMAP's neighbours."""

    status = 422
    kind = "too_few_points"

    def __init__(self, needed: int, got: int) -> None:
        super().__init__(f"layout needs {needed} points, got {got}")
        self.needed = needed
        self.got = got

    def data(self) -> dict[str, Any]:
        return {"needed": self.needed, "got": self.got}


class NonFiniteLayout(SidecarError):
    status = 422
    kind = "non_finite_layout"


class Internal(SidecarError):
    status = 500
    kind = "internal"

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason

    def data(self) -> dict[str, Any]:
        return {"reason": self.reason}
