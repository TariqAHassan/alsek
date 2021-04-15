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
)
