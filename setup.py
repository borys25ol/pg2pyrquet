import setuptools

version = "0.1.0"

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pg2pyrquet",
    version=version,
    author="Borys Oliinyk",
    author_email="oleynik.boris@gmail.com",
    description="Postgres Exports to Apache Parquet with Python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/borys25ol/pg2pyrquet",
    license="MIT",
    packages=["pg2pyrquet"],
    python_requires=">=3.8",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    package_data={"pg2pyrquet": ["py.typed"]},
)