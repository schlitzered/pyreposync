class OSRepoSyncException(Exception):
    pass


class OSRepoSyncDownLoadError(OSRepoSyncException):
    pass


class OSRepoSyncHashError(OSRepoSyncException):
    pass
