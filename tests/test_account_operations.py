import logging
import random

from common_ci_utils.random_utils import generate_unique_resource_name

from framework import config
from framework.customizations.marks import tier1
from noobaa_sa import constants
from utility.utils import flatten_dict, generate_random_key

log = logging.getLogger(__name__)


@tier1
def test_account_operations(account_manager):
    # account operations
    account_name = generate_unique_resource_name(prefix="account")
    access_key = generate_random_key(constants.EXPECTED_ACCESS_KEY_LEN)
    secret_key = generate_random_key(constants.EXPECTED_SECRET_KEY_LEN)
    config_root = config.ENV_DATA["config_root"]
    account_manager.create(account_name, access_key, secret_key)
    account_manager.list(config_root)
    account_manager.delete(account_name, config_root)
    account_manager.list()


@tier1
def test_account_update_and_status_operations(account_manager):
    """
    Test the update account operation:
    1. Create an account
    2. Update multiple fields of the account via one update operation
    3. Verify that the new values are updated correctly
    4. Use the update operation to regenerate the account's access key and secret
    5. Verify that new valid access key and secret key were generated

    """
    # 1. Create an account
    account_name, _, _ = account_manager.create()

    # 2. Update every field of the account which can be updated
    update_params = {
        # "new_name": generate_unique_resource_name(prefix="account"),
        "uid": random.randint(1000, 2000),
        "gid": random.randint(1000, 2000),
        "access_key": generate_random_key(constants.EXPECTED_ACCESS_KEY_LEN),
        "secret_key": generate_random_key(constants.EXPECTED_SECRET_KEY_LEN),
        "allow_bucket_creation": False,
        "new_buckets_path": "/tmp",  # Has to be an accessible dir path
    }
    account_manager.update(account_name, update_params)

    # 3. Verify that the new values are updated correctly
    updated_acc_params = flatten_dict(account_manager.status(account_name))
    for key, expected_val in update_params.items():
        assert updated_acc_params[key] == expected_val, (
            f"Expected value for {key} is {expected_val} post account update, "
            f"but found {updated_acc_params[key]} instead"
        )

    # 4. Use the update operation to regenerate the account's access key and secret
    pre_regen_access_key = updated_acc_params["access_key"]
    pre_regen_secrey_key = updated_acc_params["secret_key"]
    account_manager.update(account_name, {"regenerate": ""})

    # 5. Verify that new valid access key and secret key were generated
    updated_acc_params = flatten_dict(account_manager.status(account_name))
    post_regen_access_key = updated_acc_params["access_key"]
    post_regen_secret_key = updated_acc_params["secret_key"]

    assert (
        pre_regen_access_key != post_regen_access_key
        and pre_regen_secrey_key != post_regen_secret_key
    ), "Both the account's access key and secret key should have been changed"

    assert (
        len(post_regen_access_key) == constants.EXPECTED_ACCESS_KEY_LEN
        and post_regen_access_key.isalnum(),
        "The regenerated access key should be alphanumeric and of length "
        f"{constants.EXPECTED_ACCESS_KEY_LEN}, but found {post_regen_access_key}",
    )

    assert (
        len(post_regen_secret_key) == constants.EXPECTED_SECRET_KEY_LEN
        and post_regen_secret_key.isalnum(),
        "The regenerated secret key should be alphanumeric and of length "
        f"{constants.EXPECTED_SECRET_KEY_LEN}, but found {post_regen_secret_key}",
    )
