from collections import defaultdict
from modules.data_processing.domain.utils.clean_content import clean_content

class ForbiddenWordsCacheAdapter:
    def __init__(self, raw_rows: list[tuple[str, str | None]]):
        """
        raw_rows: Lista de tuplas (termino, id_autorizados)
        donde id_autorizados es un string de ids separados por coma o None.
        """
        self.raw_rows = raw_rows

    def to_dict_by_user(self) -> dict[str, list[str]]:
        """
        Retorna un diccionario con las palabras prohibidas agrupadas por usuario:
        {
            "123": ["nequi", "david plata"],
            "456": ["bitcoin"],
            "global": ["terrorismo", "bomba"]
        }
        """
        user_word_map = defaultdict(list)

        for word, user_ids in self.raw_rows:
            word_clean = word.strip().lower()
            if not user_ids or user_ids.strip() == "":
                user_word_map["global"].append(word_clean)
            else:
                for uid in user_ids.split(","):
                    uid = uid.strip()
                    if uid:
                        user_word_map[uid].append(word_clean)

        return user_word_map
