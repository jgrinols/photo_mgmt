from setuptools import setup, find_packages

with open("icloudpd_requirements.txt") as f:
    required = f.readlines()

with open("requirements.txt") as f:
    required.extend(f.readlines())

setup(
    name='photolibutils',
    version='0.3.2',
    packages=find_packages(),
    setup_requires=["wheel"],
    install_requires=required,
    include_package_data=True,
    entry_points={
        "console_scripts": [
            "pwgo-virtualfs = photolibutils.pwgo_virtualfs.virtualfs:entry",
            "icloudpd = photolibutils.icloudpd.base:main",
            "pwgo-sync = photolibutils.pwgo_sync.sync:entry"
        ]
    }
)