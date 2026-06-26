from abc import ABC, abstractmethod


class IForbiddenWordsRepository(ABC):
    @abstractmethod
    async def get_active_terms(self) -> dict[str, set[int]]:
        """
        Retorna {termino: authorized_user_ids} para los términos con activo=1.
        - set vacío   → palabra bloqueada globalmente para todos los usuarios.
        - set con ids → bloqueada para todos EXCEPTO esos user_ids.
        """
        ...
