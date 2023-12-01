"""
Module to connect to remote host
"""

import logging

from common_ci_utils.connection import Connection
from framework import config
from paramiko.auth_handler import AuthenticationException, SSHException

log = logging.getLogger(__name__)


class SSHConnection:
    """
    A class that connects to remote host
    """

    def __init__(self):
        """
        Get connection to host

        Raises:
            authException: In-case of authentication failed
            sshException: In-case of ssh connection failed

        """
        self.host = config.ENV_DATA["noobaa_sa_host"]
        self.user = config.ENV_DATA["user"]
        self.password = config.ENV_DATA.get("password")
        self.private_key = config.ENV_DATA.get("private_key")
        try:
            if self.private_key:
                self.conn = Connection(
                    host=self.host,
                    user=self.user,
                    private_key=self.private_key,
                )
            else:
                self.conn = Connection(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                )
        except AuthenticationException as authException:
            log.error(f"Authentication failed: {authException}")
            raise authException
        except SSHException as sshException:
            log.error(f"SSH connection failed: {sshException}")
            raise sshException

    def get_connection(self):
        """
        Get connection to host

        Returns:
            paramiko.client: Paramiko SSH client connection to host

        """
        return self.conn

    def close_connection(self):
        """
        Closes SSH connection
        """
        self.conn.close()


ssh_conn = SSHConnection()
conn = ssh_conn.get_connection()


def pytest_sessionfinish(session, exitstatus):
    # Close the SSH connection at the end of the pytest session
    ssh_conn.close_connection()
