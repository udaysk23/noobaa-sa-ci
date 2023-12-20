import logging
import os
import tempfile

from common_ci_utils.templating import Templating
from framework import config
from framework.connection import SSHConnection

log = logging.getLogger(__name__)


def test_account_operations(account_manager, unique_resource_name, random_hex):
    conn = SSHConnection().connection
    prefix = "account"
    account_name = unique_resource_name(prefix=prefix)
    account_email = config.ENV_DATA["email"]
    aws_access_key = random_hex()
    aws_secret_key = random_hex()

    # get home directory on noobaa-sa host
    cmd = "echo $HOME"
    _, hd, _ = conn.exec_cmd(cmd)
    bucket_path = os.path.join(hd, f"fs_{account_name}")

    # create bucket path
    cmd = f"sudo mkdir {bucket_path}"
    conn.exec_cmd(cmd)

    # form the account json file
    templating = Templating(base_path=config.ENV_DATA["template_dir"])
    account_template = "account.json"
    account_data = {
        "account_name": account_name,
        "account_email": account_email,
        "access_key": aws_access_key,
        "secret_key": aws_secret_key,
        "bucket_path": bucket_path,
    }
    account_data_full = templating.render_template(account_template, account_data)
    log.info(f"account content: {account_data_full}")

    # write to file
    with tempfile.NamedTemporaryFile(
        mode="w+", prefix="account_", delete=False
    ) as account_file:
        account_file.write(account_data_full)

    # upload to noobaa-sa host
    conn.upload_file(account_file.name, account_file.name)

    # account operations
    config_root = config.ENV_DATA["config_root"]
    account_manager.create(config_root, account_file.name)
    account_manager.list()
    account_manager.delete(config_root, account_file.name)
    account_manager.list()
