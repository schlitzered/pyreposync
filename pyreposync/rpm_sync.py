from shutil import copyfile

import bz2
import gzip
import configparser
import logging
import os
import shutil

import xml.etree.ElementTree

from pyreposync.downloader import Downloader
from pyreposync.exceptions import OSRepoSyncException, OSRepoSyncHashError


class RPMSync:
    def __init__(
        self,
        base_url,
        destination,
        reponame,
        date,
        treeinfo,
        syncdir=None,
        proxy=None,
        client_cert=None,
        client_key=None,
        ca_cert=None,
    ):
        self._base_url = base_url
        self._date = date
        self._destination = destination
        self._reponame = reponame
        self._syncdir = syncdir
        self._treeinfo = treeinfo
        self.downloader = Downloader(
            proxy=proxy, client_cert=client_cert, client_key=client_key, ca_cert=ca_cert
        )
        self.log = logging.getLogger("application")

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

    @property
    def syncdir(self):
        if not self._syncdir:
            return self.reponame
        else:
            return self._syncdir

    @property
    def treeinfo(self):
        return self._treeinfo

    def packages(self):
        primary = None
        for location, hash_algo, hash_sum in self.repomod_files():
            destination = "{0}/sync/{1}/{2}".format(
                self.destination, self.reponame, location
            )
            if "primary.xml" in destination.lower():
                primary = destination
        if not primary:
            self.log.fatal("no primary.xml found in repomd.xml")
            raise OSRepoSyncException("no primary.xml found in repomd.xml")

        if primary.endswith(".gz"):
            with gzip.open(primary, "rb") as source:
                root = xml.etree.ElementTree.parse(source).getroot()
        elif primary.endswith("bz2"):
            with bz2.open(primary, "rb") as source:
                root = xml.etree.ElementTree.parse(source).getroot()
        else:
            with open(primary, "rb") as source:
                root = xml.etree.ElementTree.parse(source).getroot()
        packages = root.findall("{http://linux.duke.edu/metadata/common}package")
        for package in packages:
            checksum = package.find("{http://linux.duke.edu/metadata/common}checksum")
            hash_algo = checksum.get("type")
            hash_sum = checksum.text
            location = package.find("{http://linux.duke.edu/metadata/common}location")
            yield location.get("href"), hash_algo, hash_sum

    def sync_packages(self):
        for location, hash_algo, hash_sum in self.packages():
            url = "{0}{1}".format(self.base_url, location)
            destination = "{0}/sync/{1}/{2}".format(
                self.destination, self.syncdir, location
            )
            self.downloader.get(url, destination, hash_sum, hash_algo, replace=False)

    def treeinfo_files(self):
        treeinfo_file = "{0}/sync/{1}/{2}".format(
            self.destination, self.reponame, self.treeinfo
        )
        treeinfo = configparser.ConfigParser()
        treeinfo.optionxform = str
        try:
            treeinfo.read_file(open(treeinfo_file))
        except FileNotFoundError:
            return
        try:
            for file in treeinfo.options("checksums"):
                if file == "repodata/repomd.xml":
                    continue
                hash_algo, hash_sum = treeinfo.get("checksums", file).split(":", 1)
                yield file, hash_algo, hash_sum
        except configparser.NoSectionError:
            files = set()
            for section in treeinfo.sections():
                if section.startswith("images-") or section.startswith("stage2"):
                    for option in treeinfo.options(section):
                        files.add(treeinfo.get(section, option))
            for file in files:
                yield file, None, None

    def sync_treeinfo(self):
        url = "{0}{1}".format(self.base_url, self.treeinfo)
        destination = "{0}/sync/{1}/{2}".format(
            self.destination, self.reponame, self.treeinfo
        )
        try:
            self.downloader.get(url, destination, replace=True)
        except OSRepoSyncException:
            return
        for file, hash_algo, hash_sum in self.treeinfo_files():
            if file == "repodata/repomd.xml":
                continue
            url = "{0}{1}".format(self.base_url, file)
            destination = "{0}/sync/{1}/{2}".format(
                self.destination, self.reponame, file
            )
            self.downloader.get(url, destination, hash_sum, hash_algo, replace=True)

    def repomod_files(self):
        destination = "{0}/sync/{1}/repodata/repomd.xml".format(
            self.destination, self.reponame
        )
        repomd = xml.etree.ElementTree.parse(destination).getroot()
        datas = repomd.findall("{http://linux.duke.edu/metadata/repo}data")
        for data in datas:
            checksum = data.find("{http://linux.duke.edu/metadata/repo}checksum")
            hash_algo = checksum.get("type")
            hash_sum = checksum.text
            location = data.find("{http://linux.duke.edu/metadata/repo}location")
            yield location.get("href"), hash_algo, hash_sum

    def revalidate(self):
        try:
            for location, hash_algo, hash_sum in self.packages():
                destination = "{0}/sync/{1}/{2}".format(
                    self.destination, self.syncdir, location
                )
                try:
                    self.log.info("validating: {0}".format(destination))
                    self.downloader.check_hash(
                        destination=destination, checksum=hash_sum, hash_type=hash_algo
                    )
                except OSRepoSyncHashError:
                    self.log.error("hash mismatch for: {0}".format(destination))
        except FileNotFoundError:
            self.log.error("no repodata found")

    def revalidate2(self):
        packages = dict()
        try:
            for location, hash_algo, hash_sum in self.packages():
                destination = "{0}/sync/{1}/{2}".format(
                    self.destination, self.syncdir, location
                )
                packages[destination] = {"hash_algo": hash_algo, "hash_sum": hash_sum}
        except FileNotFoundError:
            self.log.error("no repodata found")
        return packages

    def sync_repomod(self):
        url = "{0}repodata/repomd.xml".format(self.base_url)
        destination = "{0}/sync/{1}/repodata/repomd.xml".format(
            self.destination, self.reponame
        )
        try:
            shutil.rmtree(
                "{0}/sync/{1}/repodata/".format(self.destination, self.reponame)
            )
        except FileNotFoundError:
            pass
        self.downloader.get(url, destination, replace=True)
        for location, hash_algo, hash_sum in self.repomod_files():
            url = "{0}{1}".format(self.base_url, location)
            destination = "{0}/sync/{1}/{2}".format(
                self.destination, self.reponame, location
            )
            self.downloader.get(url, destination, hash_sum, hash_algo, replace=True)
        self.sync_packages()
        self.sync_treeinfo()

    def snap(self):
        self.log.info("creating snapshot")
        self.snap_repodata()
        self.snap_treeinfo()
        self.snap_packages()
        current = "{0}/snap/{1}/{2}".format(self.destination, self.reponame, self.date)
        latest = "{0}/snap/{1}/latest".format(self.destination, self.reponame)
        timestamp = "{0}/snap/{1}/{2}/timestamp".format(
            self.destination, self.reponame, self.date
        )
        self.log.info("setting latest to current release")
        try:
            os.unlink(latest)
        except FileNotFoundError:
            pass
        os.symlink(current, latest)
        with open(timestamp, "w") as _timestamp:
            _timestamp.write("{0}\n".format(self.date))
        self.log.info("done creating snapshot")

    def snap_cleanup(self):
        referenced_timestamps = self.snap_list_get_referenced_timestamps()
        for snap in self.snap_list_timestamp_snapshots():
            if snap not in referenced_timestamps:
                snap = "{0}/snap/{1}/{2}".format(self.destination, self.reponame, snap)
                shutil.rmtree(snap)

    def snap_list_get_referenced_timestamps(self):
        result = dict()
        base = "{0}/snap/{1}/".format(self.destination, self.reponame)
        for candidate in self.snap_list_named_snapshots():
            candidate = "named/{0}".format(candidate)
            timestamp = self.snap_list_named_snapshot_target(
                "{0}/{1}".format(base, candidate)
            )
            if timestamp not in result:
                result[timestamp] = [candidate]
            else:
                result[timestamp].append(candidate)
        timestamp = self.snap_list_named_snapshot_target("{0}/latest".format(base))
        if timestamp not in result:
            result[timestamp] = ["latest"]
        else:
            result[timestamp].append("latest")
        return result

    def snap_list_named_snapshots(self):
        try:
            return os.listdir(
                "{0}/snap/{1}/named".format(self.destination, self.reponame)
            )
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
            result = os.listdir("{0}/snap/{1}/".format(self.destination, self.reponame))
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
                "{0} is not a valid timestamp, checking if its a named snapshot".format(
                    timestamp
                )
            )
            source = "{0}/snap/{1}/{2}".format(
                self.destination, self.reponame, timestamp
            )
            _timestamp = self.snap_list_named_snapshot_target(source)
            if _timestamp:
                self.log.info("setting timestamp to {0}".format(_timestamp))
                timestamp = _timestamp
            else:
                raise OSRepoSyncException("{0} is not a valid named snapshot")
        source = "{0}/snap/{1}/{2}".format(self.destination, self.reponame, timestamp)
        target = "{0}/snap/{1}/named/{2}".format(
            self.destination, self.reponame, snapname
        )
        target_dir = "{0}/snap/{1}/named/".format(self.destination, self.reponame)
        if os.path.isdir(source):
            self.log.debug("source directory exists: {0}".format(source))
        else:
            self.log.debug("source directory missing: {0}".format(source))
            raise OSRepoSyncException("Source directory missing: {0}".format(source))
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
        target = "{0}/snap/{1}/named/{2}".format(
            self.destination, self.reponame, snapname
        )
        try:
            os.unlink(target)
        except FileNotFoundError:
            pass
        self.log.info("done removing named snapshot")

    def snap_repodata(self):
        self.log.info("copy repodata")
        repomd_dst = "{0}/snap/{1}/{2}/repodata/repomd.xml".format(
            self.destination, self.reponame, self.date
        )
        repomd_src = "{0}/sync/{1}/repodata/repomd.xml".format(
            self.destination, self.reponame
        )
        try:
            os.makedirs(os.path.dirname(repomd_dst))
        except OSError:
            pass
        copyfile(repomd_src, repomd_dst)
        for location, hash_algo, hash_sum in self.repomod_files():
            dst = "{0}/snap/{1}/{2}/{3}".format(
                self.destination, self.reponame, self.date, location
            )
            src = "{0}/sync/{1}/{2}".format(self.destination, self.reponame, location)
            try:
                os.makedirs(os.path.dirname(dst))
            except OSError:
                pass
            copyfile(src, dst)
        self.log.info("done copy repodata")

    def snap_treeinfo(self):
        self.log.info("copy treeinfo")
        try:
            dst = "{0}/snap/{1}/{2}/{3}".format(
                self.destination, self.reponame, self.date, self.treeinfo
            )
            src = "{0}/sync/{1}/{2}".format(
                self.destination, self.reponame, self.treeinfo
            )
            copyfile(src, dst)
        except (OSError, FileNotFoundError) as err:
            self.log.error("could not copy {0}: {1}".format(self.treeinfo, err))
        for location, hash_algo, hash_sum in self.treeinfo_files():
            dst = "{0}/snap/{1}/{2}/{3}".format(
                self.destination, self.reponame, self.date, location
            )
            src = "{0}/sync/{1}/{2}".format(self.destination, self.reponame, location)
            try:
                os.makedirs(os.path.dirname(dst))
            except OSError:
                pass
            copyfile(src, dst)
        self.log.info("done copy treeinfo")

    def snap_packages(self):
        self.log.info("copy packages")
        for location, hash_algo, hash_sum in self.packages():
            dst = "{0}/snap/{1}/{2}/{3}".format(
                self.destination, self.reponame, self.date, location
            )
            src = "{0}/sync/{1}/{2}".format(self.destination, self.syncdir, location)
            try:
                os.makedirs(os.path.dirname(dst))
            except OSError:
                pass
            try:
                os.symlink(src, dst)
            except FileExistsError as err:
                self.log.error("could not copy {0}: {1}".format(location, err))
        self.log.info("done copy packages")

    def sync(self):
        self.log.info("starting thread")
        self.sync_repomod()
        self.log.info("shutdown thread complete")
