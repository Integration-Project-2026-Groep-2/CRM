"""Compatibility shim for unit tests.

Some unit tests import `handle_registration` / `handle_company_created` from a
top-level `receiver` module. The actual implementation lives in `src.receiver`.
"""

from src.receiver import handle_company_created, handle_registration  # noqa: F401
