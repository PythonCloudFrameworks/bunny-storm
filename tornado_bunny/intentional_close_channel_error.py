class IntentionalCloseChannelError(Exception):
    """
    Error used to signal to a consumer that it should not reopen a closed channel
    """
    pass
