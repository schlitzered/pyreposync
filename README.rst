PyRepoSync
==========

PyRepoSync is an OS repository synchronization tool designed to mirror and snapshot RPM and Deb822 (Debian/Ubuntu) repositories using symbolic links. It is particularly useful when repositories do not support `rsync` or when local snapshots are needed for patch management.

Installing
----------

To install PyRepoSync, you can use pip:

.. code-block:: bash

    pip install pyreposync


Configuration
-------------

By default, PyRepoSync reads configuration from `/etc/pyreposync/reposync.ini`. You can override this using the `--cfg` option.

Global Configuration
~~~~~~~~~~~~~~~~~~~~

Configuration options in the ``[main]`` section:

* ``destination``: (Required) Path where synced repositories and snapshots will be stored.
* ``downloaders``: Number of parallel downloader threads (default: 1).
* ``loglevel``: Logging level (e.g., ``DEBUG``, ``INFO``, ``WARNING``, ``ERROR``).
* ``proxy``: Global proxy server URL (e.g., ``http://proxy.example.com:3128``). Used as a fallback for both RPM and Deb822 repositories.
* ``timeout_connect``: Default connect timeout in seconds (default: 30).
* ``timeout_read``: Default read timeout in seconds (default: 300).

Example ``[main]`` section:

.. code-block:: ini

    [main]
    destination = /var/spool/pyreposync
    downloaders = 10
    loglevel = INFO
    proxy = http://proxy.example.com:3128
    timeout_connect = 30
    timeout_read = 300

RPM Repository Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

RPM sections use the format ``[<reponame>:rpm]``:

.. code-block:: ini

    [RockyLinux/9/BaseOS/x86_64:rpm]
    baseurl = https://mirror.23m.com/rocky/9/BaseOS/x86_64/os/
    tags = EXTERNAL,OS:RockyLinux9,BASE,X86_64
    allow_missing_packages = false
    # Optional authentication
    basic_auth_user = myuser
    basic_auth_pass = mypass
    # Optional SSL client certificate authentication (e.g. for RHEL)
    sslclientcert = /etc/pki/entitlement/12345.pem
    sslclientkey = /etc/pki/entitlement/12345-key.pem
    sslcacert = /etc/rhsm/ca/redhat-uep.pem
    # Optional repo-specific proxy configuration
    proxy = http://proxy.example.com:3128
    # Optional specific timeouts
    timeout_connect = 15
    timeout_read = 120

Deb822 (Debian/Ubuntu) Repository Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Debian/Ubuntu sections use the format ``[<reponame>:deb822]``:

.. code-block:: ini

    [ubuntu/ubuntu:deb822]
    baseurl = https://archive.ubuntu.com/ubuntu/
    suites = noble noble-updates noble-security
    components = main restricted universe multiverse
    binary_archs = amd64
    # Optional repo-specific proxy configuration
    proxy = http://proxy.example.com:3128
    # Optional specific timeouts
    timeout_connect = 20
    timeout_read = 180

Usage
-----

Run the synchronization:

.. code-block:: bash

    pyreposync --cfg reposync.ini sync

Validate downloaded package checksums:

.. code-block:: bash

    pyreposync --cfg reposync.ini validate

Other commands include:
* ``snap``: Create snapshot symlinks.
* ``snap_cleanup``: Clean up unnamed snapshots and unreferenced files.
* ``snap_list``: List current snapshots.
* ``snap_name`` / ``snap_unname``: Manage named snapshots.

License
-------

Unless stated otherwise, PyRepoSync uses the MIT license. Check the ``LICENSE.txt`` file.