import logging

from framework import config
from framework.ssh_connection_manager import SSHConnectionManager
from noobaa_sa.defaults import HEALTH
import noobaa_sa.exceptions as e

log = logging.getLogger(__name__)

conf_root = config.ENV_DATA["config_root"]
health = HEALTH
base_cmd = f"sudo /usr/local/noobaa-core/bin/node {health}"
unwanted_log = "2>/dev/null"


# Function for gtting the noobaa health status
def get_noobaa_health_status(config_root=None, **kwargs):
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
            https_port=https_port,
            all_account_details=all_account_details
            all_bucket_details=all_bucket_details
        )

    Returns:
        string: String of health response
    """
    log.info("Getting current Noobaa Health status")
    conn = SSHConnectionManager().connection
    if config_root is None:
        config_root = conf_root
    update_data = kwargs
    update_cmd = ""
    if "https_port" in update_data:
        if update_data.get('https_port') is None:
            update_cmd = update_cmd + "--https_port "
        else:
            update_cmd = update_cmd + f"--https_port {update_data.get('https_port')} "
    if "deployment_type" in update_data:
        update_cmd = update_cmd + f"--deployment_type {update_data.get('deployment_type')} "
    if "all_account_details" in update_data:
        if update_data.get('all_account_details') is None:
            update_cmd = update_cmd + "--all_account_details "
        else:
            update_cmd = update_cmd + f"--all_account_details {update_data.get('all_account_details')} "
    if "all_bucket_details" in update_data:
        if update_data.get('all_bucket_details') is None:
            update_cmd = update_cmd + "--all_bucket_details "
        else:
            update_cmd = update_cmd + f"--all_bucket_details {update_data.get('all_bucket_details')} "
    cmd = f"{base_cmd} {update_cmd} --config_root {config_root} {unwanted_log}"
    retcode, stdout, _ = conn.exec_cmd(cmd)
    if retcode != 0:
        raise e.HealthStatusFailed(
            f"Faied to get health status of Noobaa with error {stdout}"
        )
    log.info(stdout)
    return stdout
