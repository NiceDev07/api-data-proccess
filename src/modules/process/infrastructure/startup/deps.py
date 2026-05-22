"""
Dependencias compartidas del módulo process.

Objetos sin estado (stateless) que se crean una sola vez en el arranque
de la aplicación y se reutilizan en cada request. Se almacenan en
app.state.process_deps y se acceden a través de get_shared_deps().

Los objetos que dependen de la sesión de base de datos (repositorios,
servicios) se siguen creando por request en routes/process.py.
"""
from dataclasses import dataclass

from modules._common.infrastructure.cache.redis import RedisCache
from modules.process.app.files.factory import ReaderFileFactory
from modules.process.app.services.numeration import NumerationService
from modules.process.app.services.cost import CostService
from modules.process.infrastructure.validators.level_validator import LevelValidator
from modules.process.infrastructure.storage.local import LocalStorage
from modules.process.infrastructure.exclusions.customer_exclusion_source import CustomerExclusionSource
from modules.process.infrastructure.audio import FfprobeAudioDurationProvider


@dataclass(frozen=True)
class ProcessSharedDeps:
    cache: RedisCache
    file_reader_factory: ReaderFileFactory
    level_validator: LevelValidator
    storage: LocalStorage
    duration_provider: FfprobeAudioDurationProvider
    exclusion_source: CustomerExclusionSource


def build_process_shared_deps(
    redis_client,
    max_records_level1: int = 10,
    max_records_elevated: int = 700_000,
) -> ProcessSharedDeps:
    """
    Construye el contenedor de dependencias compartidas.
    Llamar una sola vez desde lifespan.
    """
    file_reader_factory = ReaderFileFactory()
    return ProcessSharedDeps(
        cache=RedisCache(redis_client),
        file_reader_factory=file_reader_factory,
        level_validator=LevelValidator(
            max_records=max_records_level1,
            max_records_elevated=max_records_elevated,
        ),
        storage=LocalStorage(),
        duration_provider=FfprobeAudioDurationProvider(),
        exclusion_source=CustomerExclusionSource(file_reader_factory),
    )
