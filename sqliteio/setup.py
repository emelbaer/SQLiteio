from setuptools import setup, find_packages

setup(
    name="sqliteio",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "SQLAlchemy",
        "asyncio",
    ],
    entry_points={
        "console_scripts": [
            "sqliteio-server = sqliteio.examples.server:main",
            "sqliteio-client = sqliteio.examples.client:main",
        ],
    },
)
