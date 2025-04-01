from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Callable, cast

from advanced_alchemy.extensions.litestar._utils import get_aa_scope_state, set_aa_scope_state
from advanced_alchemy.extensions.litestar.plugins.init.config import SQLAlchemyAsyncConfig
from advanced_alchemy.utils.text import slugify
from litestar import Request
from sqlalchemy.ext.asyncio import AsyncSession

if TYPE_CHECKING:
    from litestar.datastructures.state import State
    from litestar.types import Scope

ALEMBIC_TEMPLATE_PATH = Path(__file__).parent / "alembic_templates"


@dataclass
class SQLAlchemyMultiTenantAsyncConfig(SQLAlchemyAsyncConfig):
    """Async SQLAlchemy Configuration for multi-tenant setup."""

    core_schema_name: str = field(default="core")
    """The core schema name."""
    core_revision_name: str = field(default="core")
    """The core schema name."""
    tenant_schema_prefix: str = field(default="tenant")
    """Prefix for tenant schema names."""
    tenant_revision_name: str = field(default="tenant")
    """Prefix for tenant schema names."""
    tenant_schema_separator: str = field(default="_")

    def provide_session(self, state: State, scope: Scope) -> AsyncSession:
        """Create a session instance.

        Args:
            state: The ``Litestar.state`` instance.
            scope: The current connection's scope.

        Returns:
            A session instance.
        """
        session = cast("AsyncSession | None", get_aa_scope_state(scope, self.session_scope_key))
        request = Request(scope)  # type: ignore
        engine = self.get_engine()
        schema_translation_map = None

        organisation_slug = request.path_params.get("organisation_slug", None)

        if organisation_slug:
            schema_name = (
                f"{self.tenant_schema_prefix}{slugify(organisation_slug, separator=self.tenant_schema_separator)}"
            )
            schema_translation_map = {None: f"{schema_name}"}

        if session is None:
            session_maker = cast("Callable[[], AsyncSession]", state[self.session_maker_app_state_key])
            if schema_translation_map:
                session = session_maker(bind=engine.execution_options(schema_translate_map=schema_translation_map))  # type: ignore
            else:
                session = session_maker()
            set_aa_scope_state(scope, self.session_scope_key, session)
        return session

    def __post_init__(self) -> None:
        super().__post_init__()
        self.alembic_config.template_path = ALEMBIC_TEMPLATE_PATH.as_posix()
