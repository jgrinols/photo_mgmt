"""setuptools script"""
from setuptools import setup, find_packages

setup_args = {
    "name": "pwgo_helper",
    "version": "0.19.4",
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
        "pyicloud @ git+https://github.com/jgrinols/pyicloud.git@master"
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
            "rope",
            "sphinx",
            "sphinx-click",
            "sphinx-markdown-builder",
            "myst-parser"
        ]
    },
    "entry_points": { "console_scripts": ["pwgo-helper = pwgo_helper.pwgo_helper:pwgo_helper"] }
}

setup(**setup_args)
