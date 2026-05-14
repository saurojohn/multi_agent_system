"""Database migration management for schema changes."""

import logging
import threading
import time
import hashlib
from typing import Dict, List, Optional, Any, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger('migration')


class MigrationDirection(Enum):
    """Migration direction."""
    UP = "up"      # Apply migration
    DOWN = "down"  # Rollback migration


class MigrationStatus(Enum):
    """Migration status."""
    PENDING = "pending"
    APPLIED = "applied"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


@dataclass
class MigrationStep:
    """A single migration step."""
    step_id: str
    sql: str
    timeout: int = 30
    dry_run: bool = False


@dataclass
class Migration:
    """A database migration."""
    migration_id: str
    version: str
    name: str
    steps: List[MigrationStep]
    applied_at: Optional[float] = None
    status: MigrationStatus = MigrationStatus.PENDING
    checksum: str = ""
    metadata: Dict = field(default_factory=dict)


@dataclass
class MigrationResult:
    """Result of a migration operation."""
    success: bool
    migration_id: str
    applied_steps: int
    failed_step: Optional[str] = None
    error: Optional[str] = None
    duration_ms: float = 0


class MigrationHistory:
    """Tracks migration history."""

    def __init__(self, storage: Dict = None):
        self._storage = storage or {}
        self._lock = threading.RLock()

    def record(self, migration: Migration):
        """Record a migration."""
        with self._lock:
            self._storage[migration.migration_id] = {
                'version': migration.version,
                'name': migration.name,
                'applied_at': migration.applied_at,
                'status': migration.status.value,
                'checksum': migration.checksum
            }

    def get(self, migration_id: str) -> Optional[Dict]:
        """Get migration record."""
        with self._lock:
            return self._storage.get(migration_id)

    def get_all(self) -> List[Dict]:
        """Get all migration records."""
        with self._lock:
            return list(self._storage.values())

    def get_by_version(self, version: str) -> Optional[Dict]:
        """Get migration by version."""
        with self._lock:
            for record in self._storage.values():
                if record['version'] == version:
                    return record
        return None

    def is_applied(self, migration_id: str) -> bool:
        """Check if migration was applied."""
        record = self.get(migration_id)
        return record is not None and record['status'] == MigrationStatus.APPLIED.value


class MigrationManager:
    """
    Manages database migrations.
    """

    def __init__(self):
        self._migrations: Dict[str, Migration] = {}
        self._history: MigrationHistory = MigrationHistory()
        self._lock = threading.RLock()
        self._pre_migration_hooks: List[Callable] = []
        self._post_migration_hooks: List[Callable] = []
        self._executor: Callable = None  # SQL executor function

    def set_executor(self, executor: Callable[[str], Tuple[bool, str]]):
        """Set the SQL executor function.
        Executor should take SQL string and return (success, error).
        """
        self._executor = executor

    def register_migration(self, version: str, name: str,
                           steps: List[MigrationStep]) -> Migration:
        """Register a new migration."""
        migration_id = f"{version}_{name}".replace(' ', '_').lower()

        # Calculate checksum
        sql_content = ' '.join(s.sql for s in steps)
        checksum = hashlib.md5(sql_content.encode()).hexdigest()

        migration = Migration(
            migration_id=migration_id,
            version=version,
            name=name,
            steps=steps,
            checksum=checksum
        )

        with self._lock:
            self._migrations[migration_id] = migration

        logger.info(f"Registered migration: {migration_id}")
        return migration

    def add_pre_hook(self, hook: Callable[[Migration], None]):
        """Add a pre-migration hook."""
        self._pre_migration_hooks.append(hook)

    def add_post_hook(self, hook: Callable[[Migration, MigrationResult], None]):
        """Add a post-migration hook."""
        self._post_migration_hooks.append(hook)

    def migrate(self, target_version: str = None,
                direction: MigrationDirection = MigrationDirection.UP) -> List[MigrationResult]:
        """Run migrations."""
        if not self._executor:
            raise ValueError("SQL executor not set")

        results = []

        with self._lock:
            # Get migrations to run
            if direction == MigrationDirection.UP:
                migrations = self._get_pending_migrations(target_version)
            else:
                migrations = self._get_rollback_migrations(target_version)

        for migration in migrations:
            result = self._execute_migration(migration, direction)
            results.append(result)

            if result.success:
                migration.status = MigrationStatus.APPLIED
                migration.applied_at = time.time()
                self._history.record(migration)
            else:
                logger.error(f"Migration failed: {migration.migration_id}")
                break

        return results

    def _execute_migration(self, migration: Migration,
                          direction: MigrationDirection) -> MigrationResult:
        """Execute a single migration."""
        start_time = time.time()

        # Pre-migration hooks
        for hook in self._pre_migration_hooks:
            try:
                hook(migration)
            except Exception as e:
                logger.error(f"Pre-migration hook failed: {e}")

        # Execute steps
        applied_steps = 0

        if direction == MigrationDirection.DOWN:
            migration.steps = list(reversed(migration.steps))

        for step in migration.steps:
            success, error = self._executor(step.sql)

            if not success:
                return MigrationResult(
                    success=False,
                    migration_id=migration.migration_id,
                    applied_steps=applied_steps,
                    failed_step=step.step_id,
                    error=error,
                    duration_ms=(time.time() - start_time) * 1000
                )

            applied_steps += 1

        duration_ms = (time.time() - start_time) * 1000

        result = MigrationResult(
            success=True,
            migration_id=migration.migration_id,
            applied_steps=applied_steps,
            duration_ms=duration_ms
        )

        # Post-migration hooks
        for hook in self._post_migration_hooks:
            try:
                hook(migration, result)
            except Exception as e:
                logger.error(f"Post-migration hook failed: {e}")

        return result

    def _get_pending_migrations(self, target_version: str = None) -> List[Migration]:
        """Get migrations that need to be applied."""
        pending = []

        for migration in sorted(self._migrations.values(),
                               key=lambda m: m.version):
            if target_version and migration.version > target_version:
                break

            if not self._history.is_applied(migration.migration_id):
                pending.append(migration)

        return pending

    def _get_rollback_migrations(self, target_version: str = None) -> List[Migration]:
        """Get migrations to rollback."""
        to_rollback = []

        for migration in sorted(self._migrations.values(),
                               key=lambda m: m.version,
                               reverse=True):
            if target_version and migration.version <= target_version:
                break

            if self._history.is_applied(migration.migration_id):
                to_rollback.append(migration)

        return to_rollback

    def get_status(self) -> Dict:
        """Get migration status."""
        with self._lock:
            applied = sum(1 for m in self._migrations.values()
                         if self._history.is_applied(m.migration_id))

            return {
                'total': len(self._migrations),
                'applied': applied,
                'pending': len(self._migrations) - applied,
                'migrations': [
                    {
                        'id': m.migration_id,
                        'version': m.version,
                        'name': m.name,
                        'status': 'applied' if self._history.is_applied(m.migration_id) else 'pending'
                    }
                    for m in sorted(self._migrations.values(), key=lambda x: x.version)
                ]
            }


class MigrationBuilder:
    """Builder for creating migrations."""

    def __init__(self, version: str, name: str):
        self.version = version
        self.name = name
        self._steps: List[MigrationStep] = []

    def add_step(self, sql: str, step_id: str = None, **config) -> 'MigrationBuilder':
        """Add a migration step."""
        step = MigrationStep(
            step_id=step_id or f"step_{len(self._steps) + 1}",
            sql=sql,
            **{k: v for k, v in config.items()}
        )
        self._steps.append(step)
        return self

    def create_table(self, table: str, columns: Dict[str, str]):
        """Helper to create a table."""
        cols_sql = ', '.join(f"{name} {dtype}" for name, dtype in columns.items())
        sql = f"CREATE TABLE {table} ({cols_sql})"
        return self.add_step(sql, f"create_{table}")

    def add_column(self, table: str, column: str, dtype: str):
        """Helper to add a column."""
        sql = f"ALTER TABLE {table} ADD COLUMN {column} {dtype}"
        return self.add_step(sql, f"add_{column}_to_{table}")

    def drop_column(self, table: str, column: str):
        """Helper to drop a column."""
        sql = f"ALTER TABLE {table} DROP COLUMN {column}"
        return self.add_step(sql, f"drop_{column}_from_{table}")

    def add_index(self, table: str, columns: List[str], index_name: str = None):
        """Helper to add an index."""
        idx_name = index_name or f"idx_{table}_{'_'.join(columns)}"
        cols = ', '.join(columns)
        sql = f"CREATE INDEX {idx_name} ON {table} ({cols})"
        return self.add_step(sql, f"add_index_{idx_name}")

    def build(self, manager: MigrationManager) -> Migration:
        """Build and register the migration."""
        return manager.register_migration(self.version, self.name, self._steps)


# Global migration manager
_migration_manager = MigrationManager()


def get_migration_manager() -> MigrationManager:
    return _migration_manager


def create_migration(version: str, name: str) -> MigrationBuilder:
    """Create a new migration."""
    return MigrationBuilder(version, name)