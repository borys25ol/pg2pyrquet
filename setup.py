"""
Setup the package.
"""

from setuptools import find_packages, setup

version = "0.1.0"

description = "Postgres Exports to Apache Parquet with Python"

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

packages = [
    package for package in find_packages(where=".", exclude=("test*",))
]

install_requires = (
    [
        "adbc_driver_postgresql==1.1.0",
        "psycopg==3.2.1",
        "psycopg-binary==3.2.1",
        "pyarrow==7.0.0",
        "pydantic-settings==2.4.0",
        "pydantic==2.8.2",
        "python-dotenv==1.0.1",
        "typer==0.12.4",
    ],
)


setup(
    name="pg2pyrquet",
    version=version,
    author="Borys Oliinyk",
    author_email="oleynik.boris@gmail.com",
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/borys25ol/pg2pyrquet",
    license="MIT",
    entry_points="""
        [console_scripts]
        pg2pyrquet=pg2pyrquet.__main__:cli
    """,
    packages=packages,
    package_data={"pg2pyrquet": ["py.typed"]},
    include_package_data=True,
    install_requires=install_requires,
    python_requires=">=3.10",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)
