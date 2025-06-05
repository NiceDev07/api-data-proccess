class TariffRepository:
    """
    Interface for Tariff Repository.
    This interface defines the methods that a Tariff Repository should implement.
    """

    def get_tariff(self, tariff_id: str) -> dict:
        pass
        