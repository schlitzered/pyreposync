from setuptools import setup

setup(
    name='pyreposync',
    version='0.0.12',
    description='PyReposync repository sync & snapshot tool',
    long_description="""
PyReposync allows to sync and snaptshot arbitrary RPM repositories, without using rsync

Copyright (c) 2024, Stephan Schultchen.

License: MIT (see LICENSE for details)
    """,
    packages=['pyreposync'],
    scripts=[
        'contrib/pyreposync',
    ],
    url='https://github.com/schlitzered/pyreposync',
    license='MIT',
    author='schlitzer',
    author_email='stephan.schultchen@gmail.com',
    test_suite='test',
    platforms='posix',
    classifiers=[
            'License :: OSI Approved :: MIT License',
            'Programming Language :: Python :: 3'
    ],
    setup_requires=[
        'requests'
    ],
    install_requires=[
        'requests'
    ],
    keywords=[
        'rpm', 'sync'
    ]
)
