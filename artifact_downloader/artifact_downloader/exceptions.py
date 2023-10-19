""" Module containing all downloader specific exceptions """


class UnexpectedReturnCode(Exception):
    """ HTTP Server returned an unexpected return code """


class RetriableFailureReturnCode(UnexpectedReturnCode):
    """ HTTP Server returned a return code >= 500 """


class UnexpectedContainerMessage(Exception):
    """ Used for container handlers """


class UnknownMDFParserArtifact(Exception):
    """ An unknown message from MDFParser has been received """


class UnknownSQSMessage(Exception):
    """ An unknown SQS message was received """
