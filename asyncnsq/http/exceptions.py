from collections import defaultdict


class NsqHttpException(Exception):
    pass


class TransportError(NsqHttpException):
    @property
    def status_code(self):
        return self.args[0]

    @property
    def error(self):
        ''' A string error message.
        '''
        return self.args[1]

    @property
    def info(self):
        ''' Dict of returned error info from NSQ, where available.
        '''
        return self.args[2]

    def __str__(self):
        return 'TransportError(%s, %r)' % (self.status_code, self.error)


class HttpConnectionError(TransportError):
    ''' A generic HTTP error.
    '''
    def __str__(self):
        return 'HttpConnectionError(%s) caused by: %s(%s)' % (
            self.error, self.info.__class__.__name__, self.info)


class NotFoundError(TransportError):
    ''' Exception representing a 404 status code.
    '''


class ConflictError(TransportError):
    ''' Exception representing a 409 status code.
    '''


class RequestError(TransportError):
    ''' Exception representing a 400 status code.
    '''


# more generic mappings from status_code to python exceptions
HTTP_EXCEPTIONS = defaultdict(lambda: NsqHttpException, {
    400: RequestError,
    404: NotFoundError,
    409: ConflictError
})
