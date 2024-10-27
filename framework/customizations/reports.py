import os
import logging
import smtplib
import textwrap
import pytest
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from bs4 import BeautifulSoup

from framework import config
from utility.utils import get_noobaa_sa_rpm_name


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
    rpm_name = get_noobaa_sa_rpm_name()
    msg["Subject"] = f"noobaa standalone results {rpm_name}"
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)

    soup = create_results_html(session)
    part1 = MIMEText(soup, "html")
    msg.attach(part1)
    # with open("/home/oviner/ClusterPath/test7.html", "w") as file:
    #     # Write the data to the file
    #     file.write(msg.as_string())
    try:
        s = smtplib.SMTP(config.REPORTING["email"]["smtp_server"])
        s.sendmail(sender, recipients, msg.as_string())
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
    failed_tests = []
    passed_tests = []
    skipped_tests = []
    # sort out passed, failed and skipped test cases
    for result in session.results.values():
        elapsed_time = f"{int(result.stop - result.start)} sec"
        if result.passed:
            passed_tests.append((result.nodeid, elapsed_time, result.longreprtext))
        elif result.failed:
            failed_tests.append((result.nodeid, elapsed_time, result.longreprtext))
        elif result.skipped:
            skipped_tests.append((result.nodeid, elapsed_time, result.longreprtext))
        current_dir = Path(__file__).parent.parent.parent
        html_template = os.path.join(
            current_dir, "templates", "html_reports", "html_template.html"
        )
    total_tests = len(failed_tests) + len(passed_tests) + len(skipped_tests)
    if total_tests == 0:
        return
    passed_percentage = f"{float((len(passed_tests) / total_tests) * 100):.2f}%"
    failed_percentage = f"{float((len(failed_tests) / total_tests) * 100):.2f}%"
    skipped_percentage = f"{float((len(skipped_tests) / total_tests) * 100):.2f}%"

    with open(html_template) as fd:
        html_data = fd.read()
    soup = BeautifulSoup(html_data, "html.parser")

    # Insert versions into the versions table
    def add_version_data_to_table(versions, table_id):
        tbody = soup.find(id=table_id)
        for component, version in versions.items():
            row = soup.new_tag("tr")

            # Component name cell
            component_cell = soup.new_tag("td")
            component_cell.string = component
            row.append(component_cell)

            # Version cell
            version_cell = soup.new_tag("td")
            version_cell.string = version
            row.append(version_cell)

            # Add the row to the table
            tbody.append(row)

    rpm_name = get_noobaa_sa_rpm_name()
    add_version_data_to_table({"rpm_name": rpm_name}, "versions_table")

    # Helper function to insert rows into the appropriate table
    def add_test_data_to_table(test_data, table_id):
        tbody = soup.find(id=table_id)
        for test_name, test_time, comments in test_data:
            row = soup.new_tag("tr")

            # Test name cell
            test_cell = soup.new_tag("td")
            test_cell.string = test_name
            row.append(test_cell)

            # Time cell
            time_cell = soup.new_tag("td")
            time_cell.string = test_time
            row.append(time_cell)

            # Comments cell (insert line breaks after every 10 characters)
            comments_cell = soup.new_tag("td")
            wrapped_comments = "<br>".join(textwrap.wrap(comments, 150))
            comments_cell.append(BeautifulSoup(wrapped_comments, "html.parser"))
            row.append(comments_cell)

            # Add the row to the table
            tbody.append(row)

    # Insert test data into the respective tables
    add_test_data_to_table(failed_tests, "failed_tests")
    add_test_data_to_table(passed_tests, "passed_tests")
    add_test_data_to_table(skipped_tests, "skipped_tests")
    website_link = config.RUN.get("jenkins_build_url")
    link_section = soup.find("a")
    link_section["href"] = website_link
    link_section.string = f"Job Link: {website_link}"

    # Step 8: Insert the statistics and website link into the HTML
    stats_section = soup.find("ul")
    stats_section.find_all("li")[0].string = f"Passed: {passed_percentage}"
    stats_section.find_all("li")[1].string = f"Failed: {failed_percentage}"
    stats_section.find_all("li")[2].string = f"Skipped: {skipped_percentage}"

    # Return the generated HTML
    return soup
