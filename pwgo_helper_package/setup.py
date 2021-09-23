"""setuptools script"""
from setuptools import setup, find_packages

setup_args = {
    "name": "pwgo_helper",
    "version": "0.18.1",
    "packages": find_packages(),
    "install_requires": [
        "python-dotenv",
        "click",
        "pymysql",
        "asyncmy",
        "mysql-replication",
        "path",
        "requests",
        "beautifulsoup4",
        "py-linq",
        "aiobotocore",
        "fs",
        "pillow",
        "pyexiv2",
        "slack_sdk",
        "pid",
        "pyicloud"
    ],
    "extras_require": {
        "dev": [
            "pylint",
            "pytest",
            "pytest-asyncio",
            "pytest-integration",
            "pytest-cov",
            "pytest-mock",
            "imagehash",
            "pandas",
            "rope"
        ]
    },
    "entry_points": { "console_scripts": ["pwgo-helper = pwgo_helper.pwgo_helper:pwgo_helper"] }
}

setup(**setup_args)
