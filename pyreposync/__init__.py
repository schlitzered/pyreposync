import argparse
import collections
import configparser
import datetime
import logging
import sys
import threading
import time

from pyreposync.downloader import Downloader
from pyreposync.exceptions import OSRepoSyncException, OSRepoSyncHashError
from pyreposync.rpm_sync import RPMSync


def main():
    parser = argparse.ArgumentParser(description="OS Repo Sync Tool")

    parser.add_argument(
        "--cfg",
        dest="cfg",
        action="store",
        default="/etc/pyreposync/reposync.ini",
        help="Full path to configuration",
    )

    parser.add_argument(
        "--repo",
        dest="repo",
        action="store",
        default=None,
        help="""
                        execute command on this repository, if not set, 
                        command applies to all configured repositories.
                        
                        This command is mutually exclusive with --tags
                        """,
    )

    parser.add_argument(
        "--tags",
        dest="tags",
        action="store",
        default=None,
        help="""
                        Comma separated list of repo tags the command applies to.
                        putting a '!' in front of a tag negates it.
                        At least one not negated tag has to be match.
                        
                        This command is mutually exclusive with --repo
                        """,
    )

    subparsers = parser.add_subparsers(
        help="commands",
        dest="method",
    )
    subparsers.required = True

    migrate_parser = subparsers.add_parser(
        "migrate",
        help="migrate to new sync format",
    )
    migrate_parser.set_defaults(
        method="migrate",
    )

    snap_cleanup_parser = subparsers.add_parser(
        "snap_cleanup",
        help="remove all unnamed snapshots and unreferenced rpms.",
    )
    snap_cleanup_parser.set_defaults(
        method="snap_cleanup",
    )

    snap_list_parser = subparsers.add_parser(
        "snap_list",
        help="list snapshots",
    )
    snap_list_parser.set_defaults(
        method="snap_list",
    )

    snap_name_parser = subparsers.add_parser(
        "snap_name",
        help="give timed snapshot a name",
    )
    snap_name_parser.set_defaults(
        method="snap_name",
    )
    snap_name_parser.add_argument(
        "--timestamp",
        dest="timestamp",
        action="store",
        required=True,
        default=None,
        help="source timestampm might also be a named snapshot or latest",
    )
    snap_name_parser.add_argument(
        "--name",
        dest="snapname",
        action="store",
        required=True,
        default=None,
        help="name to be created",
    )

    snap_unname_parser = subparsers.add_parser(
        "snap_unname",
        help="remove name from timed snapshot",
    )
    snap_unname_parser.set_defaults(
        method="snap_unname",
    )
    snap_unname_parser.add_argument(
        "--name",
        dest="snapname",
        action="store",
        required=True,
        help="name to be removed",
    )

    snap_parser = subparsers.add_parser(
        "snap",
        help="create new snapshots",
    )
    snap_parser.set_defaults(
        method="snap",
    )

    sync_parser = subparsers.add_parser(
        "sync",
        help="sync all repos",
    )
    sync_parser.set_defaults(
        method="sync",
    )

    validate_parser = subparsers.add_parser(
        "validate",
        help="re validate package downloads",
    )
    validate_parser.set_defaults(
        method="validate",
    )

    parsed_args = parser.parse_args()
    try:
        snapname = parsed_args.snapname
    except AttributeError:
        snapname = None
    try:
        timestamp = parsed_args.timestamp
    except AttributeError:
        timestamp = None

    osreposync = PyRepoSync(
        cfg=parsed_args.cfg,
        method=parsed_args.method,
        snapname=snapname,
        repo=parsed_args.repo,
        tags=parsed_args.tags,
        timestamp=timestamp,
    )
    osreposync.work()


class PyRepoSync:
    def __init__(self, cfg, snapname, method, repo, tags, timestamp):
        self._config_file = cfg
        self._config = configparser.ConfigParser()
        self._config_dict = None
        self._method = method
        self._snapname = snapname
        self._repo = repo
        self._tags = None
        self._timestamp = timestamp
        self.tags = tags
        self.log = logging.getLogger("application")
        self.config.read_file(open(self._config_file))
        self._config_dict = self._cfg_to_dict(self.config)
        self._logging()
        if self._tags and self._repo:
            self.log.fatal("both tags & repo have been specified, choose one")

    @property
    def method(self):
        return self._method

    @property
    def snapname(self):
        return self._snapname

    @property
    def repo(self):
        return self._repo

    @property
    def tags(self):
        return self._tags

    @tags.setter
    def tags(self, tags):
        if tags:
            self._tags = tags.split(",")

    @property
    def timestamp(self):
        return self._timestamp

    def _logging(self):
        logfmt = logging.Formatter(
            "%(asctime)sUTC - %(levelname)s - %(threadName)s - %(message)s"
        )
        logfmt.converter = time.gmtime
        aap_level = self.config.get("main", "loglevel")
        handler = logging.StreamHandler()

        handler.setFormatter(logfmt)
        self.log.addHandler(handler)
        self.log.setLevel(aap_level)
        self.log.debug("logger is up")

    @staticmethod
    def _cfg_to_dict(config):
        result = {}
        for section in config.sections():
            result[section] = {}
            for option in config.options(section):
                try:
                    result[section][option] = config.getint(section, option)
                    continue
                except ValueError:
                    pass
                try:
                    result[section][option] = config.getfloat(section, option)
                    continue
                except ValueError:
                    pass
                try:
                    result[section][option] = config.getboolean(section, option)
                    continue
                except ValueError:
                    pass
                try:
                    result[section][option] = config.get(section, option)
                    continue
                except ValueError:
                    pass
        return result

    @property
    def config(self):
        return self._config

    @property
    def config_dict(self):
        return self._config_dict

    def get_jobs(self, date, section):
        versions = self.config.get(section, "versions", fallback=None)
        jobs = set()
        if versions:
            for version in versions.split(","):
                job = RPMSync(
                    base_url=self.config.get(section, "baseurl").replace(
                        ":VERSION:", version
                    ),
                    destination=self.config.get("main", "destination"),
                    reponame=section[:-4].replace(":VERSION:", version),
                    date=date,
                    treeinfo=self.config.get(section, "treeinfo", fallback=".treeinfo"),
                    proxy=self.config.get("main", "proxy", fallback=None),
                    client_cert=self.config.get(
                        section, "sslclientcert", fallback=None
                    ),
                    client_key=self.config.get(section, "sslclientkey", fallback=None),
                    ca_cert=self.config.get(section, "sslcacert", fallback=None),
                )
                jobs.add(job)
        else:
            job = RPMSync(
                base_url=self.config.get(section, "baseurl"),
                destination=self.config.get("main", "destination"),
                reponame=section[:-4],
                date=date,
                treeinfo=self.config.get(section, "treeinfo", fallback=".treeinfo"),
                proxy=self.config.get("main", "proxy", fallback=None),
                client_cert=self.config.get(section, "sslclientcert", fallback=None),
                client_key=self.config.get(section, "sslclientkey", fallback=None),
                ca_cert=self.config.get(section, "sslcacert", fallback=None),
            )
            jobs.add(job)
        return jobs

    def get_sections(self):
        sections = set()
        for section in self.config:
            if section.endswith(":rpm"):
                if self.repo and section != self.repo:
                    continue
                if self._tags:
                    if not self.validate_tags(section):
                        continue
                sections.add(section)
        return sections

    def work(self):
        self.log.info("starting up")
        date = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
        queue = collections.deque()
        for section in self.get_sections():
            queue.append(self.get_jobs(date=date, section=section))
        workers = set()
        if self.method == "sync":
            num_worker = self.config.getint("main", "downloaders", fallback=1)
        else:
            num_worker = 1
        for _ in range(num_worker):
            workers.add(
                RepoSyncThread(
                    queue=queue,
                    action=self.method,
                    snapname=self.snapname,
                    timestamp=self.timestamp,
                )
            )

        for worker in workers:
            worker.start()
        return_code = 0
        for worker in workers:
            worker.join()
            if worker.status != 0:
                return_code = 1
        sys.exit(return_code)

    def validate_tags(self, section):
        try:
            section_tags = self.config.get(section, "tags").split(",")
        except Exception as err:
            return False
        for tag in self.tags:
            if tag.startswith("!"):
                if tag[1:] in section_tags:
                    return False
            else:
                if tag not in section_tags:
                    return False
        self.log.info(f"section {section} has matching tags")
        return True


class RepoSyncThread(threading.Thread):
    def __init__(self, queue, action, snapname, timestamp):
        super().__init__()
        self._action = action
        self._snapname = snapname
        self._queue = queue
        self._status = 0
        self._timestamp = timestamp
        self.daemon = True
        self.log = logging.getLogger("application")

    @property
    def action(self):
        return self._action

    @property
    def snapname(self):
        return self._snapname

    @property
    def queue(self):
        return self._queue

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        self._status = value

    @property
    def timestamp(self):
        return self._timestamp

    def do_migrate(self, jobs):
        for job in jobs:
            try:
                self.log.info(f"{self.action} start repo {job.reponame}")
                self.name = job.reponame
                job.migrate()
                self.log.info(f"{self.action} done repo {job.reponame}")
            except OSRepoSyncException:
                self.log.fatal(f"could not {self.action} repo {job.reponame}")
                self.status = 1

    def do_sync(self, jobs):
        for job in jobs:
            try:
                self.log.info(f"{self.action} start repo {job.reponame}")
                self.name = job.reponame
                job.sync()
                self.log.info(f"{self.action} done repo {job.reponame}")
            except OSRepoSyncException:
                self.log.fatal(f"could not {self.action} repo {job.reponame}")
                self.status = 1

    def do_snap(self, jobs):
        for job in jobs:
            try:
                self.log.info(f"{self.action} start repo {job.reponame}")
                self.name = job.reponame
                job.snap()
                self.log.info(f"{self.action} done repo {job.reponame}")
            except OSRepoSyncException:
                self.log.fatal(f"could not {self.action} repo {job.reponame}")
                self.status = 1

    def do_snap_cleanup(self, jobs):
        for job in jobs:
            try:
                self.log.info(f"{self.action} start repo {job.reponame}")
                self.name = job.reponame
                job.snap_cleanup()
                self.log.info(f"{self.action} done repo {job.reponame}")
            except OSRepoSyncException:
                self.log.fatal(f"could not {self.action} repo {job.reponame}")
                self.status = 1

    def do_snap_list(self, jobs):
        for job in jobs:
            try:
                self.name = job.reponame
                referenced_timestamps = job.snap_list_get_referenced_timestamps()
                self.log.info(f"Repository: {job.reponame}")
                self.log.info("The following timestamp snapshots exist:")
                for timestamp in job.snap_list_timestamp_snapshots():
                    self.log.info(
                        f"{timestamp} -> {referenced_timestamps.get(timestamp, [])}"
                    )
                self.log.info("The following named snapshots exist:")
                base = f"{job.destination}/snap/{job.reponame}/"
                for named in job.snap_list_named_snapshots():
                    timestamp = job.snap_list_named_snapshot_target(
                        f"{base}/named/{named}"
                    )
                    self.log.info(f"named/{named} -> {timestamp}")
                latest = f"{base}/latest"
                self.log.info(
                    f"latest -> {job.snap_list_named_snapshot_target(latest)}"
                )

            except OSRepoSyncException:
                self.status = 1

    def do_snap_name(self, jobs):
        for job in jobs:
            try:
                self.log.info(f"{self.action} start repo {job.reponame}")
                self.name = job.reponame
                job.snap_name(self.timestamp, self.snapname)
                self.log.info(f"{self.action} done repo {job.reponame}")
            except OSRepoSyncException:
                self.log.fatal(f"could not {self.action} repo {job.reponame}")
                self.status = 1

    def do_snap_unname(self, jobs):
        for job in jobs:
            try:
                self.log.info(f"{self.action} start repo {job.reponame}")
                self.name = job.reponame
                job.snap_unname(self.snapname)
                self.log.info(f"{self.action} done repo {job.reponame}")
            except OSRepoSyncException:
                self.log.fatal(f"could not {self.action} repo {job.reponame}")
                self.status = 1

    def do_validate(self, jobs):
        _downloader = Downloader()
        packages = dict()
        for job in jobs:
            try:
                self.log.info(f"{self.action} start repo {job.reponame}")
                packages.update(job.revalidate())
            except OSRepoSyncException:
                self.log.fatal(f"could not {self.action} repo {job.reponame}")
                self.status = 1
        for destination, hash_info in packages.items():
            try:
                self.log.info(f"validating: {destination}")
                _downloader.check_hash(
                    destination=destination,
                    checksum=hash_info["hash_sum"],
                    hash_type=hash_info["hash_algo"],
                )
            except OSRepoSyncHashError:
                self.log.error(f"hash mismatch for: {destination}")
            except FileNotFoundError:
                self.log.error(f"file not found: {destination}")

    def run(self):
        while True:
            try:
                jobs = self.queue.pop()
                if self.action == "migrate":
                    self.do_migrate(jobs)
                elif self.action == "sync":
                    self.do_sync(jobs)
                elif self.action == "snap_cleanup":
                    self.do_snap_cleanup(jobs)
                elif self.action == "snap_list":
                    self.do_snap_list(jobs)
                elif self.action == "snap_name":
                    self.do_snap_name(jobs)
                elif self.action == "snap_unname":
                    self.do_snap_unname(jobs)
                elif self.action == "snap":
                    self.do_snap(jobs)
                elif self.action == "validate":
                    self.do_validate(jobs)
            except IndexError:
                break


if __name__ == "__main__":
    main()
