class TariffRepositoryInterface:
    """
    Interface for Tariff Repository.
    This interface defines the methods that a Tariff Repository should implement.
    """

    def get_tariff(self, country_id: int, tariff_id: int, service: str) -> dict:
        pass
        