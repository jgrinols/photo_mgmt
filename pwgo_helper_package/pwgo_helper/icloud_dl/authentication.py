"""Handles username/password authentication and two-step authentication"""
import sys, json, re, random, time

import pyicloud_ipd
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent

from ..config import Configuration as ProgramConfig
from .config import Configuration as ICDLConfig

def wait_for_mfa_code():
    """setup a stream reader to wait for the mfa code sent
    to messaging db"""
    prg_cfg = ProgramConfig.get()
    icdl_cfg = ICDLConfig.get()
    logger = prg_cfg.get_logger(__name__)
    code = None
    stream = BinLogStreamReader(
        connection_settings = prg_cfg.db_config,
        server_id = random.randint(100, 999999999),
        only_schemas = icdl_cfg.auth_msg_db,
        only_tables = icdl_cfg.auth_msg_tbl,
        only_events = [WriteRowsEvent],
        blocking = False,
        resume_stream = True
    )

    start_time = time.time()
    while True:
        for event in stream:
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
                else:
                    logger.warning("unable to find verification code in message. waiting...")
        time.sleep(.1)
        if time.time() - start_time > icdl_cfg.mfa_timeout:
            raise TimeoutError("did not receive mfa token during specified timeout period.")


def authenticate(client_id=None):
    """Authenticate with iCloud username and password"""
    prg_cfg = ProgramConfig.get()
    icdl_cfg = ICDLConfig.get()
    logger = prg_cfg.get_logger(__name__)
    logger.debug("Authenticating...")
    icloud = pyicloud_ipd.PyiCloudService(
        icdl_cfg.username, icdl_cfg.password,
        cookie_directory=icdl_cfg.cookie_directory,
        client_id=client_id)

    if icloud.requires_2sa:
        logger.info("Two-step/two-factor authentication is required!")
        request_mfa_code(icloud)
    return icloud


def request_mfa_code(icloud):
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

    code = wait_for_mfa_code()
    if not icloud.validate_verification_code(device, code):
        raise RuntimeError("Failed to verify two-factor authentication code")
