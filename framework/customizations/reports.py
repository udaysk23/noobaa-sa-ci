import os
import logging
import smtplib
import pytest
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from bs4 import BeautifulSoup

from framework import config


log = logging.getLogger(__name__)


def pytest_sessionstart(session):
    """
    Prepare results dict
    """
    session.results = dict()


@pytest.mark.hookwrapper
def pytest_runtest_makereport(item, call):
    """
    Add extra column( Log File) and link the log file location
    """
    pytest_html = item.config.pluginmanager.getplugin("html")
    outcome = yield
    report = outcome.get_result()
    report.description = str(item.function.__doc__)
    extra = getattr(report, "extra", [])

    if report.when == "call":
        log_file = ""
        for handler in logging.getLogger().handlers:
            if isinstance(handler, logging.FileHandler):
                log_file = handler.baseFilename
                break
        extra.append(pytest_html.extras.url(log_file, name="Log File"))
        report.extra = extra
        item.session.results[item] = report
    if report.skipped:
        item.session.results[item] = report
    if report.when in ("setup", "teardown") and report.failed:
        item.session.results[item] = report


def pytest_sessionfinish(session, exitstatus):
    """
    save session's report files and send email report
    """
    # send_email_reports(session)
    if config.RUN["cli_params"].get("email"):
        send_email_reports(session)


def send_email_reports(session):
    """
    Email results of test run

    """
    mailids = config.RUN["cli_params"].get("email")
    recipients = []
    [recipients.append(mailid) for mailid in mailids.split(",")]
    sender = "ocs-ci@redhat.com"
    msg = MIMEMultipart("alternative")
    msg["Subject"] = "noobaa standalone results"
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)

    msg = create_results_html(session)
    with open("/home/oviner/ClusterPath/test7.html", "w") as file:
        # Write the data to the file
        file.write(msg)
    try:
        s = smtplib.SMTP(config.REPORTING["email"]["smtp_server"])
        s.sendmail(sender, recipients, msg)
        s.quit()
        log.info(f"Results have been emailed to {recipients}")
    except Exception as e:
        log.exception(f"Sending email with results failed! {e}")


def create_results_html(session):
    """
    Add squad analysis to the html test results used in email reporting

    Args:
        session (obj): Pytest session object

    """
    failed = []
    skipped = []
    passed = []
    # sort out passed, failed and skipped test cases
    for result in session.results.values():
        elapsed_time = f"{int(result.stop - result.start)} sec"
        if result.passed:
            passed.append((result.nodeid, elapsed_time))
        elif result.failed:
            failed.append((result.nodeid, elapsed_time))
        elif result.skipped:
            skipped.append((result.nodeid, elapsed_time))
        current_dir = Path(__file__).parent.parent.parent
        html_template = os.path.join(
            current_dir, "templates", "html_reports", "html_template.html"
        )
    with open(html_template) as fd:
        html_data = fd.read()
    soup = BeautifulSoup(html_data, "html.parser")

    # Helper function to insert rows into the appropriate table
    def add_test_data_to_table(test_data, table_id):
        tbody = soup.find(id=table_id)
        for test_name, test_time in test_data:
            row = soup.new_tag("tr")

            # Test name cell
            test_cell = soup.new_tag("td")
            test_cell.string = test_name
            row.append(test_cell)

            # Time cell
            time_cell = soup.new_tag("td")
            time_cell.string = test_time
            row.append(time_cell)

            # Add the row to the table
            tbody.append(row)

    # Insert test data into the respective tables
    add_test_data_to_table(failed, "failed_tests")
    add_test_data_to_table(passed, "passed_tests")
    add_test_data_to_table(skipped, "skipped_tests")

    # Return the generated HTML as a string
    return soup.prettify()
