from setuptools import setup, find_packages

with open("icloudpd_requirements.txt") as f:
    required = f.readlines()

with open("requirements.txt") as f:
    required.extend(f.readlines())

with open("dev_requirements.txt") as f:
    dev_extras = f.readlines()

setup(
    name='photolibutils',
    version='0.6.0',
    packages=find_packages(),
    setup_requires=['wheel'],
    install_requires=required,
    extras_require={
        'dev': dev_extras
    },
    include_package_data=True,
    entry_points={
        "console_scripts": [
            "pwgo-virtualfs = photolibutils.pwgo_virtualfs.virtualfs:entry",
            "icloudpd = photolibutils.icloudpd.base:main",
            "pwgo-sync = photolibutils.pwgo_sync.sync:entry",
            "pwgo-metadata-agent = photolibutils.pwgo_metadata_agent.metadata_agent:entry"
        ]
    }
)
