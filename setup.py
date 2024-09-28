"""

    Setup

"""

from setuptools import find_packages, setup


def _get_requirements() -> list[str]:
    with open("requirements.txt", "r") as f:
        reqs = [i.strip().strip("\n") for i in f.readlines()]
    return reqs


setup(
    packages=find_packages(exclude=["tests", "examples"]),
    install_requires=_get_requirements(),
    extras_require={
        "diskcache": ["diskcache==5.2.1"],
    },
    entry_points={"console_scripts": ["alsek = alsek.cli.cli:main"]},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3 :: Only",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Topic :: System :: Distributed Computing",
    ],
)
