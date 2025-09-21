import polars as pl
from modules.data_processing.domain.interfaces.forbidden_words_repository import IForbiddenWordsRepository
from modules.data_processing.domain.interfaces.cache_interface import ICache
from modules.data_processing.domain.utils.forbidden_words_adapter import ForbiddenWordsCacheAdapter
from modules.data_processing.domain.utils.clean_content import clean_content
from modules.data_processing.domain.validators.forbidden_validator import ForbiddenWordsValidator

class ForbiddenWordsService:
    CACHE_KEY = "forbidden_words:v1"
    TTL_SECONDS = 3600 * 24

    def __init__(
        self,
        repo: IForbiddenWordsRepository,
        cache: ICache
    ):
        self.repo = repo
        self.cache = cache

    async def _load_forbidden_words(self) -> dict[str, list[str]]:
        """Carga y adapta los datos crudos desde el repositorio."""
        raw_words = await self.repo.get_word_not_allowed()
        adapter = ForbiddenWordsCacheAdapter(raw_words)
        return adapter.to_dict_by_user()

    async def get_forbidden_words_for_user(self, user_id: int) -> list[str]:
        cache_data = await self.cache.get(self.CACHE_KEY)
        if cache_data is None:
            cache_data = await self._load_forbidden_words()
            await self.cache.set(self.CACHE_KEY, cache_data, ttl=self.TTL_SECONDS)

        return cache_data.get(str(user_id), []) + cache_data.get("global", [])

    async def validate_message(self, message: str, user_id: int) -> bool:
        cleaned_message = clean_content(message)
        words = await self.get_forbidden_words_for_user(user_id)
        # print(f"Forbidden words for user {user_id}: {words}")
        validator = ForbiddenWordsValidator(words)
        return validator.validate(cleaned_message)

    async def ensure_message_is_valid(self, message: str, user_id: int):
        """Lanza excepción si el mensaje no es válido."""
        if not await self.validate_message(message, user_id):
            raise ValueError(f"El Contenido base contiene palabras no autorizadas. {message}")
        
    async def ensure_dataframe_values_are_valid(self, df: pl.DataFrame, columns: list[str], user_id: int):
        """
        Valida que ninguna palabra prohibida esté presente en las columnas especificadas.
        Usa comparación directa y única (sin regex), altamente eficiente.
        """
        forbidden_words = set(word.lower() for word in await self.get_forbidden_words_for_user(user_id) if word.strip())
        if not forbidden_words:
            return

        for col in columns:
            try:
                # Obtener valores únicos en lowercase
                unique_values = (
                    df.select(pl.col(col).cast(pl.Utf8).str.strip_chars().str.to_lowercase())
                    .unique()
                    .get_column(col)
                    .to_list()
                )

                # Buscar intersección con palabras prohibidas
                intersecting = forbidden_words.intersection(unique_values)
                if intersecting:
                    raise ValueError(
                        f"La columna '{col}' contiene palabras no autorizadas"
                    )

            except Exception as e:
                raise RuntimeError(f"Error validando columna '{col}': {str(e)}") from e

