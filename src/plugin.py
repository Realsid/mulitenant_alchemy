from __future__ import annotations
from typing import Sequence, TYPE_CHECKING

from advanced_alchemy.extensions.litestar.plugins import SQLAlchemyInitPlugin

if TYPE_CHECKING:
    from .config import SQLAlchemyMultiTenantAsyncConfig

from click import Group


class SQLAlchemyInitMultiTenantPlugin(SQLAlchemyInitPlugin):
    """A plugin that provides SQLAlchemy integration with support for multitenancy."""

    def __init__(
        self,
        config: SQLAlchemyMultiTenantAsyncConfig | Sequence[SQLAlchemyMultiTenantAsyncConfig],
    ) -> None:
        """Initialize ``SQLAlchemyPlugin``.

        Args:
            config: configure DB connection and hook handlers and dependencies.
        """
        self._config = config

    @property
    def config(
        self,
    ) -> Sequence[SQLAlchemyMultiTenantAsyncConfig]:
        return self._config if isinstance(self._config, Sequence) else [self._config]

    def on_cli_init(self, cli: Group) -> None:
        from .plugin_commands import database_group

        cli.add_command(database_group)
