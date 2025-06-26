import logging
import os
import shutil

from pyreposync.downloader import Downloader
from pyreposync.exceptions import OSRepoSyncException


class SyncGeneric:
    def __init__(
        self,
        base_url,
        destination,
        reponame,
        date,
        allow_missing_packages,
        basic_auth_user=None,
        basic_auth_pass=None,
        proxy=None,
        client_cert=None,
        client_key=None,
        ca_cert=None,
    ):
        self._allow_missing_packages = allow_missing_packages
        self._base_url = base_url
        self._date = date
        self._destination = destination
        self._reponame = reponame
        self.downloader = Downloader(
            basic_auth_user=basic_auth_user,
            basic_auth_pass=basic_auth_pass,
            proxy=proxy,
            client_cert=client_cert,
            client_key=client_key,
            ca_cert=ca_cert,
        )
        self.log = logging.getLogger("application")

    @property
    def allow_missing_packages(self):
        return self._allow_missing_packages

    @property
    def base_url(self):
        return self._base_url

    @property
    def date(self):
        return self._date

    @property
    def destination(self):
        return self._destination

    @property
    def reponame(self):
        return self._reponame

    def migrate(self):
        pass

    def revalidate(self):
        pass

    def _snap(self):
        raise NotImplementedError("this method must be implemented by subclasses")

    def snap(self):
        self.log.info("creating snapshot")
        self._snap()
        current = f"{self.destination}/snap/{self.reponame}/{self.date}"
        latest = f"{self.destination}/snap/{self.reponame}/latest"
        timestamp = f"{self.destination}/snap/{self.reponame}/{self.date}/timestamp"
        self.log.info("setting latest to current release")
        try:
            os.unlink(latest)
        except FileNotFoundError:
            pass
        os.symlink(current, latest)
        with open(timestamp, "w") as _timestamp:
            _timestamp.write(f"{self.date}\n")
        self.log.info("done creating snapshot")

    def snap_cleanup(self):
        referenced_timestamps = self.snap_list_get_referenced_timestamps()
        for snap in self.snap_list_timestamp_snapshots():
            if snap not in referenced_timestamps:
                snap = f"{self.destination}/snap/{self.reponame}/{snap}"
                shutil.rmtree(snap)

    def snap_list_get_referenced_timestamps(self):
        result = dict()
        base = f"{self.destination}/snap/{self.reponame}/"
        for candidate in self.snap_list_named_snapshots():
            candidate = f"named/{candidate}"
            timestamp = self.snap_list_named_snapshot_target(f"{base}/{candidate}")
            if timestamp not in result:
                result[timestamp] = [candidate]
            else:
                result[timestamp].append(candidate)
        timestamp = self.snap_list_named_snapshot_target(f"{base}/latest")
        if timestamp not in result:
            result[timestamp] = ["latest"]
        else:
            result[timestamp].append("latest")
        return result

    def snap_list_named_snapshots(self):
        try:
            return os.listdir(f"{self.destination}/snap/{self.reponame}/named")
        except FileNotFoundError:
            return []

    @staticmethod
    def snap_list_named_snapshot_target(path):
        try:
            return os.readlink(path).split("/")[-1]
        except FileNotFoundError:
            return None

    def snap_list_timestamp_snapshots(self):
        try:
            result = os.listdir(f"{self.destination}/snap/{self.reponame}/")
            try:
                result.remove("latest")
            except ValueError:
                pass
            try:
                result.remove("named")
            except ValueError:
                pass
            return result
        except FileNotFoundError:
            return []

    def snap_name(self, timestamp, snapname):
        self.log.info("creating named snapshot")
        try:
            int(timestamp)
            if not len(timestamp) == 14:
                raise ValueError
        except ValueError:
            self.log.error(
                f"{timestamp} is not a valid timestamp, checking if its a named snapshot"
            )
            source = f"{self.destination}/snap/{self.reponame}/{timestamp}"
            _timestamp = self.snap_list_named_snapshot_target(source)
            if _timestamp:
                self.log.info(f"setting timestamp to {_timestamp}")
                timestamp = _timestamp
            else:
                raise OSRepoSyncException(f"{snapname} is not a valid named snapshot")
        source = f"{self.destination}/snap/{self.reponame}/{timestamp}"
        target = f"{self.destination}/snap/{self.reponame}/named/{snapname}"
        target_dir = f"{self.destination}/snap/{self.reponame}/named/"
        if os.path.isdir(source):
            self.log.debug(f"source directory exists: {source}")
        else:
            self.log.debug(f"source directory missing: {source}")
            raise OSRepoSyncException(f"Source directory missing: {source}")
        try:
            os.makedirs(os.path.dirname(target_dir))
        except OSError:
            pass
        try:
            os.unlink(target)
        except OSError:
            pass
        os.symlink(source, target)
        self.log.info("done creating named snapshot")

    def snap_unname(self, snapname):
        self.log.info("removing named snapshot")
        target = f"{self.destination}/snap/{self.reponame}/named/{snapname}"
        try:
            os.unlink(target)
        except FileNotFoundError:
            pass
        self.log.info("done removing named snapshot")

    def sync(self):
        pass
