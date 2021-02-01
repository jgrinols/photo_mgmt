from setuptools import setup, find_packages

with open("icloudpd_requirements.txt") as f:
    required = f.readlines()

with open("requirements.txt") as f:
    required.extend(f.readlines())

setup(
    name='com.grinols.photo-utils',
    version='0.1.0',
    packages=find_packages(),
    setup_requires=["wheel"],
    install_requires=required,
    entry_points={
        "console_scripts": [
            "pwgo-virtualfs = pwgo_virtualfs.virtualfs:entry",
            "icloudpd = icloudpd.base:main"
        ]
    }
)