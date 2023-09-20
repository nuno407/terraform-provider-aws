""" Module containing all downloader specific exceptions """


class UnexpectedReturnCode(Exception):
    """ HTTP Server returned an unexpected return code """


class RetriableFailureReturnCode(UnexpectedReturnCode):
    """ HTTP Server returned a return code >= 500 """


class UnexpectedContainerMessage(Exception):
    """ Used for container handlers """


class UknownMDFParserArtifact(Exception):
    """ An uknown message from MDFParser has been recieved """


class UknownSQSMessage(Exception):
    """ An uknown SQS message was recieved """
