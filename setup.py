"""

    Setup

"""
from setuptools import find_packages, setup

setup(
    packages=find_packages(exclude=["tests"]),
    install_requires=[
        "APScheduler==3.7.0",
        "click==7.1.2",
        "dill==0.3.3",
        "redis==3.5.3",
    ],
    extras_require={
        "diskcache": ["diskcache==5.2.1"],
    },
    entry_points={"console_scripts": ["alsek = alsek.cli.cli:alsek"]},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3 :: Only",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Topic :: System :: Distributed Computing",
    ],
)
