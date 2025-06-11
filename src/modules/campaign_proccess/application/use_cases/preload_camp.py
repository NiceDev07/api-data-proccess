

class PreloadCampaignsUseCase:
    _register = {
        # "sms": SMSCampaignProcessorUseCase,
        # "email": EmailCampaignProcessorUseCase,
        # "call_blasting": CallBlastingCampaignProcessorUseCase
    }

    def __init__(self):
        pass

    def execute(self):

        # Logic to preload campaigns goes here
        return {"campaign_id": 200}