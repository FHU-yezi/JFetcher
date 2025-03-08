class JFetcherError(Exception):
    pass


class DataExistsError(JFetcherError):
    pass


class MissingCredentialError(JFetcherError):
    pass


class BinarySearchMaxTriesReachedError(JFetcherError):
    pass
