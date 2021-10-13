"""Handles username/password authentication and two-step authentication"""
import sys, json, re, random, asyncio

import pyicloud
from asyncmy import connect
from asyncmy.replication import BinLogStream
from asyncmy.replication.row_events import WriteRowsEvent

from ..config import Configuration as ProgramConfig
from .config import Configuration as ICDLConfig

async def wait_for_mfa_code():
    """setup a stream reader to wait for the mfa code sent
    to messaging db"""
    prg_cfg = ProgramConfig.get()
    icdl_cfg = ICDLConfig.get()
    logger = prg_cfg.get_logger(__name__)
    stream = BinLogStream(
        connection = await connect(**prg_cfg.db_config),
        ctl_connection = await connect(**prg_cfg.db_config),
        server_id = random.randint(100, 999999999),
        only_tables = f"{icdl_cfg.auth_msg_db}.{icdl_cfg.auth_msg_tbl}",
        only_events = [WriteRowsEvent],
        blocking = True,
        resume_stream = True
    )

    async for event in stream:
        for row in event.rows:
            logger.info("message")
            escaped_msg = row["values"]["message"].encode('unicode-escape')
            msg_obj = json.loads(escaped_msg)
            logger.info("checking for verification code")
            match = re.search(r'.*:\s*(\d{6})[^\d]'
                , msg_obj["Message"]["Body"]
                , re.IGNORECASE | re.MULTILINE)
            if match:
                code = match.group(1)
                logger.debug("found verification code")
                stream.close()

                return code

            logger.warning("unable to find verification code in message. waiting...")

async def authenticate(client_id=None):
    """Authenticate with iCloud username and password"""
    prg_cfg = ProgramConfig.get()
    icdl_cfg = ICDLConfig.get()
    logger = prg_cfg.get_logger(__name__)
    logger.debug("Authenticating...")
    icloud = pyicloud.PyiCloudService(
        icdl_cfg.username, icdl_cfg.password,
        cookie_directory=icdl_cfg.cookie_directory,
        client_id=client_id)

    if icloud.requires_2sa:
        logger.info("Two-step/two-factor authentication is required!")
        await request_mfa_code(icloud)
    return icloud

async def request_mfa_code(icloud):
    """Request two-step authentication. Prompts for SMS or device"""
    prg_cfg = ProgramConfig.get()
    icdl_cfg = ICDLConfig.get()
    logger = prg_cfg.get_logger(__name__)
    devices = icloud.trusted_devices
    devices_count = len(devices)
    device_index = next((i for i, d in enumerate(devices) if d["deviceType"] == "SMS" and d["phoneNumber"]
                        .endswith(icdl_cfg.auth_phone_digits)), None)

    if not device_index or device_index == devices_count:
        # We're using the 2FA code that was automatically sent to the user's device,
        # so can just use an empty dict()
        device = dict()
    else:
        device = devices[device_index]
        if not icloud.send_verification_code(device):
            logger.error("Failed to send two-factor authentication code")
            sys.exit(1)

    try:
        code = await asyncio.wait_for(wait_for_mfa_code(), icdl_cfg.mfa_timeout)
    except asyncio.TimeoutError as err:
        raise TimeoutError("Failed to receive two-factor authentication code within timeout period") from err
    if not icloud.validate_verification_code(device, code):
        raise RuntimeError("Failed to verify two-factor authentication code")
