import logging

from framework import config
from framework.ssh_connection_manager import SSHConnectionManager
from noobaa_sa.defaults import HEALTH
from noobaa_sa import constants
import noobaa_sa.exceptions as e

log = logging.getLogger(__name__)


# Function for getting the noobaa health status
def get_noobaa_health_status(
        config_root=config.ENV_DATA["config_root"],
        **kwargs,
        ):
    """
    Noobaa Health Status

    Args:
        config_root (str): Path to config root

        Supported update options via kwargs:
        https_port (int): Get connection info
        all_account_details (str): Get all account health info
        all_bucket_details (str): Get all bucket health info
        deployment_type (str): Set deployment type for health check

    Example usage:
        status = get_noobaa_health_status(
            config_root=config_root,
            https_port=6443,
            all_account_details=--all_account_details
            all_bucket_details=--all_bucket_details
        )

    Returns:
        string: String of health response

    """
    log.info("Getting current Noobaa Health status")
    conn = SSHConnectionManager().connection
    cmd_options_data = kwargs
    base_cmd = f"sudo {HEALTH}"
    cmd_options = ""
    if "https_port" in cmd_options_data:
        assert cmd_options_data.get('https_port'), f"Default values are not supported for health check operation"
        cmd_options = cmd_options + f"--https_port {cmd_options_data.get('https_port')} "
    if "deployment_type" in cmd_options_data:
        cmd_options = cmd_options + f"--deployment_type {cmd_options_data.get('deployment_type')} "
    if "all_account_details" in cmd_options_data:
        cmd_options = cmd_options + f"--all_account_details {cmd_options_data.get('all_account_details')} "
    if "all_bucket_details" in cmd_options_data:
        cmd_options = cmd_options + f"--all_bucket_details {cmd_options_data.get('all_bucket_details')} "
    cmd = f"{base_cmd} {cmd_options} --config_root {config_root} {constants.UNWANTED_LOG}"
    retcode, stdout, _ = conn.exec_cmd(cmd)
    if retcode != 0:
        raise e.HealthStatusFailed(
            f"Faied to get health status of Noobaa with error {stdout}"
        )
    log.info(stdout)
    return stdout
