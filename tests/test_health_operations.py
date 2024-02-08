import json
import logging
import pytest

from utils.utils import get_noobaa_health_status
import noobaa_sa.exceptions as e

log = logging.getLogger(__name__)


class Test_health_operations:
    def setup_prereqs(self, unique_resource_name, random_hex, account_manager, bucket_manager):
        """
            Create account and bucket for performing health operation
        """
        account_name = unique_resource_name(prefix="account")
        access_key = random_hex()
        secret_key = random_hex()
        account_manager.create(account_name, access_key, secret_key)
        account_manager.list()
        bucket_name = unique_resource_name(prefix="bucket")
        bucket_manager.create(account_name, bucket_name)

    # Noobaa port health operation
    @pytest.mark.parametrize(
        argnames="flag",
        argvalues=[
            pytest.param(
                    {
                        "https_port": 6443
                    },
            ),
            pytest.param(
                    {
                        "https_port": None
                    },
            )
        ],
        ids=[
             "custom_value",
             "default_value",
        ],
    )
    def test_noobaa_port_flag(self, unique_resource_name, random_hex, account_manager, bucket_manager, flag):
        # Perform health operation on noobaa
        log.info("Performing Port operations on node health CLI")
        self.setup_prereqs(unique_resource_name, random_hex, account_manager, bucket_manager)
        get_info = json.loads(get_noobaa_health_status(**flag))
        if "error" in get_info.keys():
            raise e.HealthStatusFailed(
                    f"Health check failed for get port with error {get_info['error']['error_code']}"
                )
        log.info(get_info)

    # Noobaa account health operation
    @pytest.mark.parametrize(
        argnames="flag",
        argvalues=[
            pytest.param(
                    {
                        "all_account_details": True
                    },
            ),
            pytest.param(
                    {
                        "all_account_details": False
                    },
            ),
            pytest.param(
                    {
                        "all_account_details": None
                    },
            ),
        ],
        ids=[
             "true_value",
             "false_value",
             "default_value",
        ],
    )
    def test_noobaa_account_flag(self, unique_resource_name, random_hex, account_manager, bucket_manager, flag):
        # Perform health operation on noobaa
        log.info("Performing account operations on node health CLI")
        self.setup_prereqs(unique_resource_name, random_hex, account_manager, bucket_manager)
        get_info = json.loads(get_noobaa_health_status(**flag))
        if "error" in get_info.keys():
            raise e.HealthStatusFailed(
                    f"Health check failed for get all account with error {get_info['error']['error_code']}"
                )
        if flag.get('all_account_details') is True and len(get_info['checks']['valid_accounts']) == 0:
            raise e.HealthStatusFailed(
                    f"Health command failed to get all account info with flag --all_account_details {flag.get('all_account_details')}"
                )
        if (flag.get('all_account_details') is False or flag.get('all_account_details') is None) and len(get_info['checks']['valid_accounts']) != 0:
            raise e.HealthStatusFailed(
                    f"Health command failed to get all account info with flag --all_account_details {flag.get('all_account_details')}"
                )
        log.info(get_info)

    # Noobaa bucket health operation
    @pytest.mark.parametrize(
        argnames="flag",
        argvalues=[
            pytest.param(
                    {
                        "all_bucket_details": True
                    },
            ),
            pytest.param(
                    {
                        "all_bucket_details": False
                    },
            ),
            pytest.param(
                    {
                        "all_bucket_details": None
                    },
            ),
        ],
        ids=[
             "true_value",
             "false_value",
             "default_value",
        ],
    )
    def test_noobaa_bucket_flag(self, unique_resource_name, random_hex, account_manager, bucket_manager, flag):
        # Perform health operation on noobaa
        log.info("Performing bucket operations on node health CLI")
        self.setup_prereqs(unique_resource_name, random_hex, account_manager, bucket_manager)
        get_info = json.loads(get_noobaa_health_status(**flag))
        if "error" in get_info.keys():
            raise e.HealthStatusFailed(
                    f"Health check failed for get all account with error {get_info['error']['error_code']}"
                )
        if flag.get('all_bucket_details') is True and len(get_info['checks']['valid_buckets']) == 0:
            raise e.HealthStatusFailed(
                    f"Health command failed to get all account info with flag --all_bucket_details {flag.get('all_bucket_details')}"
                )
        if (flag.get('all_bucket_details') is False or flag.get('all_bucket_details') is None) and len(get_info['checks']['valid_buckets']) != 0:
            raise e.HealthStatusFailed(
                    f"Health command failed to get all account info with flag --all_account_details {flag.get('all_bucket_details')}"
                )
        log.info(get_info)
