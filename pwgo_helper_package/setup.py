"""setuptools script"""
import os

from setuptools import setup, find_packages

mod_path = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(mod_path, "requirements.txt")) as f:
    required = f.readlines()

with open(os.path.join(mod_path, "dev_requirements.txt")) as f:
    dev_extras = f.readlines()

setup(
    name='pwgo_helper',
    version='0.9.11',
    packages=find_packages(),
    install_requires=required,
    extras_require={
        'dev': dev_extras
    },
    entry_points={
        "console_scripts": [
            "pwgo-sync = pwgo_helper.pwgo_sync.sync:entry",
            "pwgo-helper = pwgo_helper.pwgo_helper:pwgo_helper_entry"
        ]
    }
)
