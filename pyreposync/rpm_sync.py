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
        proxy=None,
        client_cert=None,
        client_key=None,
        ca_cert=None,
    ):
        self._base_url = base_url
        self._date = date
        self._destination = destination
        self._reponame = reponame
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
    def treeinfo(self):
        return self._treeinfo

    def packages(self, base_path=None):
        if not base_path:
            base_path = f"{self.destination}/sync/{self.reponame}"
        primary = None
        for location, hash_algo, hash_sum in self.repomod_files(base_path=base_path):
            destination = f"{base_path}/{location}"
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

    def migrate(self):
        for location, hash_algo, hash_sum in self.packages():
            destination_old = f"{self.destination}/sync/{self.reponame}/{location}"
            destination_new = f"{self.destination}/sync/{self.reponame}/{location}.{hash_algo}.{hash_sum}"
            try:
                os.remove(destination_new)
                os.rename(destination_old, destination_new)
                os.symlink(destination_new, destination_old)
            except FileNotFoundError:
                self.log.error(f"could not migrate {location}: {destination_old} not found")
                continue
            except OSError as err:
                self.log.error(f"could not migrate {location}: {err}")
                continue
            self.log.info(f"migrated {location} to {destination_new}")

        for snap in self.snap_list_timestamp_snapshots():
            self.log.info(f"migrating {snap}")
            base_path = f"{self.destination}/snap/{self.reponame}/{snap}"
            for location, hash_algo, hash_sum in self.packages(base_path=base_path):
                dst = f"{self.destination}/snap/{self.reponame}/{self.date}/{location}"
                src = f"{self.destination}/sync/{self.reponame}/{location}.{hash_algo}.{hash_sum}"
                try:
                    os.unlink(dst)
                    os.symlink(src, dst)
                except OSError:
                    pass


    def sync_packages(self):
        for location, hash_algo, hash_sum in self.packages():
            url = f"{self.base_url}{location}"
            destination = f"{self.destination}/sync/{self.reponame}/{location}.{hash_algo}.{hash_sum}"
            self.downloader.get(url, destination, hash_sum, hash_algo, replace=False)

    def treeinfo_files(self):
        treeinfo_file = f"{self.destination}/sync/{self.reponame}/{self.treeinfo}"
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
        url = f"{self.base_url}{self.treeinfo}"
        destination = f"{self.destination}/sync/{self.reponame}/{self.treeinfo}"
        try:
            self.downloader.get(url, destination, replace=True)
        except OSRepoSyncException:
            return
        for file, hash_algo, hash_sum in self.treeinfo_files():
            if file == "repodata/repomd.xml":
                continue
            url = f"{self.base_url}{file}"
            destination = f"{self.destination}/sync/{self.reponame}/{file}"
            self.downloader.get(url, destination, hash_sum, hash_algo, replace=True)

    def repomod_files(self, base_path):
        base_path = f"{base_path}/repodata/repomd.xml"
        repomd = xml.etree.ElementTree.parse(base_path).getroot()
        datas = repomd.findall("{http://linux.duke.edu/metadata/repo}data")
        for data in datas:
            checksum = data.find("{http://linux.duke.edu/metadata/repo}checksum")
            hash_algo = checksum.get("type")
            hash_sum = checksum.text
            location = data.find("{http://linux.duke.edu/metadata/repo}location")
            yield location.get("href"), hash_algo, hash_sum

    def revalidate(self):
        packages = dict()
        try:
            for location, hash_algo, hash_sum in self.packages():
                destination = f"{self.destination}/sync/{self.reponame}/{location}.{hash_algo}.{hash_sum}"
                packages[destination] = {"hash_algo": hash_algo, "hash_sum": hash_sum}
        except FileNotFoundError:
            self.log.error("no repodata found")
        return packages

    def sync_repomod(self):
        url = f"{self.base_url}repodata/repomd.xml"
        destination = f"{self.destination}/sync/{self.reponame}/repodata/repomd.xml"
        try:
            shutil.rmtree(f"{self.destination}/sync/{self.reponame}/repodata/")
        except FileNotFoundError:
            pass
        self.downloader.get(url, destination, replace=True)
        for location, hash_algo, hash_sum in self.repomod_files():
            url = f"{self.base_url}{location}"
            destination = f"{self.destination}/sync/{self.reponame}/{location}"
            self.downloader.get(url, destination, hash_sum, hash_algo, replace=True)
        self.sync_packages()
        self.sync_treeinfo()

    def snap(self):
        self.log.info("creating snapshot")
        self.snap_repodata()
        self.snap_treeinfo()
        self.snap_packages()
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
            _timestamp.write(f"{self.destination}\n")
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

    def snap_repodata(self):
        self.log.info("copy repodata")
        repomd_dst = f"{self.destination}/snap/{self.reponame}/{self.destination}/repodata/repomd.xml"
        repomd_src = f"{self.destination}/sync/{self.reponame}/repodata/repomd.xml"
        try:
            os.makedirs(os.path.dirname(repomd_dst))
        except OSError:
            pass
        copyfile(repomd_src, repomd_dst)
        for location, hash_algo, hash_sum in self.repomod_files():
            dst = f"{self.destination}/snap/{self.reponame}/{self.destination}/{location}"
            src = f"{self.destination}/sync/{self.reponame}/{location}"
            try:
                os.makedirs(os.path.dirname(dst))
            except OSError:
                pass
            copyfile(src, dst)
        self.log.info("done copy repodata")

    def snap_treeinfo(self):
        self.log.info("copy treeinfo")
        try:
            dst = f"{self.destination}/snap/{self.reponame}/{self.destination}/{self.treeinfo}"
            src = f"{self.destination}/sync/{self.reponame}/{self.treeinfo}"
            copyfile(src, dst)
        except (OSError, FileNotFoundError) as err:
            self.log.error(f"could not copy {self.treeinfo}: {err}")
        for location, hash_algo, hash_sum in self.treeinfo_files():
            dst = f"{self.destination}/snap/{self.reponame}/{self.destination}/{location}"
            src = f"{self.destination}/sync/{self.reponame}/{location}"
            try:
                os.makedirs(os.path.dirname(dst))
            except OSError:
                pass
            copyfile(src, dst)
        self.log.info("done copy treeinfo")

    def snap_packages(self):
        self.log.info("copy packages")
        for location, hash_algo, hash_sum in self.packages():
            dst = f"{self.destination}/snap/{self.reponame}/{self.date}/{location}"
            src = f"{self.destination}/sync/{self.reponame}/{location}.{hash_algo}.{hash_sum}"
            try:
                os.makedirs(os.path.dirname(dst))
            except OSError:
                pass
            try:
                os.symlink(src, dst)
            except FileExistsError as err:
                self.log.error(f"could not copy {location}: {err}")
        self.log.info("done copy packages")

    def sync(self):
        self.log.info("starting thread")
        self.sync_repomod()
        self.log.info("shutdown thread complete")
