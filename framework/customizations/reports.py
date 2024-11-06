import os
import logging
import smtplib
import pytest

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
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
    Add analysis to the html test results used in email reporting.

    Args:
        session (obj): Pytest session object

    """
    failed_tests = []
    passed_tests = []
    skipped_tests = []

    # Sort out passed, failed, and skipped test cases
    for result in session.results.values():
        elapsed_time = f"{int(result.stop - result.start)} sec"
        test_info = {
            "name": result.nodeid,
            "time": elapsed_time,
            "comments": result.longreprtext,
        }

        if result.passed:
            passed_tests.append(test_info)
        elif result.failed:
            failed_tests.append(test_info)
        elif result.skipped:
            skipped_tests.append(test_info)

    # Calculate statistics
    total_tests = len(failed_tests) + len(passed_tests) + len(skipped_tests)
    if total_tests == 0:
        return

    statistics = {
        "Passed": f"{(len(passed_tests) / total_tests) * 100:.2f}",
        "Failed": f"{(len(failed_tests) / total_tests) * 100:.2f}",
        "Skipped": f"{(len(skipped_tests) / total_tests) * 100:.2f}",
    }

    # Versions data
    rpm_name = get_noobaa_sa_rpm_name()  # Assuming this function gets version info
    versions = {"rpm_name": rpm_name}

    # jenkins job link
    website_link = config.RUN.get("jenkins_build_url")

    # Set up Jinja2 environment and load template
    current_dir = Path(__file__).parent.parent.parent
    template_dir = os.path.join(current_dir, "templates", "html_reports")
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template("report_template.j2")

    # Render the template with context data
    html_output = template.render(
        statistics=statistics,
        website_link=website_link,
        versions=versions,
        failed_tests=failed_tests,
        passed_tests=passed_tests,
        skipped_tests=skipped_tests,
    )

    return html_output
