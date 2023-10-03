""" Module containing all downloader specific exceptions """


class UnexpectedReturnCode(Exception):
    """ HTTP Server returned an unexpected return code """


class RetriableFailureReturnCode(UnexpectedReturnCode):
    """ HTTP Server returned a return code >= 500 """
