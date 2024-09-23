import os
import pytest
import logging
import smtplib
from py.xml import html
from framework import GlobalVariables as GV
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from bs4 import BeautifulSoup

from framework import config


log = logging.getLogger(__name__)


@pytest.mark.optionalhook
def pytest_html_results_table_header(cells):
    """
    Add Description header to the table
    """
    cells.insert(2, html.th("Description"))


@pytest.mark.optionalhook
def pytest_html_results_table_row(report, cells):
    """
    Add content to the column Description
    """
    try:
        cells.insert(2, html.td(report.description))
    except AttributeError:
        cells.insert(2, html.td("--- no description ---"))
    # if logs_url is defined, replace local path Log File links to the logs_url
    if config.RUN.get("logs_url"):
        for tag in cells[4][0]:
            if (
                hasattr(tag, "xmlname")
                and tag.xmlname == "a"
                and hasattr(tag.attr, "href")
            ):
                tag.attr.href = tag.attr.href.replace(
                    os.path.expanduser(config.RUN.get("log_dir")),
                    config.RUN.get("logs_url"),
                )


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


def pytest_sessionstart(session):
    """
    Prepare results dict
    """
    session.results = dict()


def pytest_sessionfinish(session, exitstatus):
    """
    save session's report files and send email report
    """
    import csv

    email_reports(session)

    # creating report of test cases with total time in ascending order
    data = GV.TIMEREPORT_DICT
    sorted_data = dict(
        sorted(data.items(), key=lambda item: item[1].get("total"), reverse=True)
    )
    try:
        time_report_file = os.path.join(
            config.RUN.get("log_dir"), "session_test_time_report_file.csv"
        )
        with open(time_report_file, "a") as fil:
            c = csv.writer(fil)
            c.writerow(["testName", "setup", "call", "teardown", "total"])
            for test, values in sorted_data.items():
                row = [
                    test,
                    values.get("setup", "NA"),
                    values.get("call", "NA"),
                    values.get("teardown", "NA"),
                    values.get("total", "NA"),
                ]
                c.writerow(row)
        log.info(f"Test Time report saved to '{time_report_file}'")
    except Exception as e:
        log.warning(
            f"Failed to save Test Time report to logs directory with exception. {e}"
        )

    # for i in range(ocsci_config.nclusters):
    #     ocsci_config.switch_ctx(i)
    #     if not (
    #         ocsci_config.RUN["cli_params"].get("--help")
    #         or ocsci_config.RUN["cli_params"].get("collectonly")
    #     ):
    #         config_file = os.path.expanduser(
    #             os.path.join(
    #                 ocsci_config.RUN["log_dir"],
    #                 f"run-{ocsci_config.RUN['run_id']}-cl{i}-config-end.yaml",
    #             )
    #         )
    #         dump_config_to_file(config_file)
    #         log.info(f"Dump of the consolidated config is located here: {config_file}")


def pytest_report_teststatus(report, config):
    """
    This function checks the status of the test at which stage it is at an calculates
    the time take by each stage to complete it.
    There are three stages:
    setup : when the test case is setup
    call : when the test case is run
    teardown: when the teardown of the test case happens.
    """
    GV.TIMEREPORT_DICT[report.nodeid] = GV.TIMEREPORT_DICT.get(report.nodeid, {})

    if report.when == "setup":
        setup_duration = round(report.duration, 2)
        log.info(
            f"duration reported by {report.nodeid} immediately after test execution: {setup_duration}"
        )
        GV.TIMEREPORT_DICT[report.nodeid]["setup"] = setup_duration
        GV.TIMEREPORT_DICT[report.nodeid]["total"] = setup_duration

    if "total" not in GV.TIMEREPORT_DICT[report.nodeid]:
        GV.TIMEREPORT_DICT[report.nodeid]["total"] = 0

    if report.when == "call":
        call_duration = round(report.duration, 2)
        log.info(
            f"duration reported by {report.nodeid} immediately after test execution: {call_duration}"
        )
        GV.TIMEREPORT_DICT[report.nodeid]["call"] = call_duration
        GV.TIMEREPORT_DICT[report.nodeid]["total"] = round(
            GV.TIMEREPORT_DICT[report.nodeid]["total"] + call_duration, 2
        )

    if report.when == "teardown":
        teardown_duration = round(report.duration, 2)
        log.info(
            f"duration reported by {report.nodeid} immediately after test execution: {teardown_duration}"
        )
        GV.TIMEREPORT_DICT[report.nodeid]["teardown"] = teardown_duration
        GV.TIMEREPORT_DICT[report.nodeid]["total"] = round(
            GV.TIMEREPORT_DICT[report.nodeid]["total"] + teardown_duration, 2
        )


# def dump_config_to_file(file_path):
#     """
#     Dump the config to the yaml file with censored secret values.
#
#     Args:
#         file_path (str): Path to file where to write the configuration.
#
#     """
#     config_copy = deepcopy(config.to_dict())
#     censor_values(config_copy)
#     filter_unrepresentable_values(config_copy)
#     with open(file_path, "w+") as fs:
#         yaml.safe_dump(config_copy, fs)


def email_reports(session):
    """
    Email results of test run

    """
    # calculate percentage pass
    # reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    # passed = len(reporter.stats.get("passed", []))
    # failed = len(reporter.stats.get("failed", []))
    # error = len(reporter.stats.get("error", []))
    # total = passed + failed + error
    # percentage_passed = (passed / total) * 100

    mailids = config.ENV_DATA.get("email")
    recipients = []
    [recipients.append(mailid) for mailid in mailids.split(",")]
    sender = "ocs-ci@redhat.com"
    msg = MIMEMultipart("alternative")
    aborted_message = ""
    if config.RUN.get("aborted"):
        aborted_message = "[JOB ABORTED] "
    msg["Subject"] = (
        f"{aborted_message}"
        f"ocs-ci results for "
        f"("
        f"RUN ID: ) "
        # f"Passed: {percentage_passed:.0f}%"
    )
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)

    html = config.ENV_DATA["html_path"]
    with open(os.path.expanduser(html)) as fd:
        html_data = fd.read()
    soup = BeautifulSoup(html_data, "html.parser")

    parse_html_for_email(soup)
    move_summary_to_top(soup)
    add_time_report_to_email(session, soup)
    part1 = MIMEText(soup, "html")
    msg.attach(part1)
    try:
        s = smtplib.SMTP(config.REPORTING["email"]["smtp_server"])
        s.sendmail(sender, recipients, msg.as_string())
        s.quit()
        log.info(f"Results have been emailed to {recipients}")
    except Exception:
        log.exception("Sending email with results failed!")


def parse_html_for_email(soup):
    """
    Parses the html and filters out the unnecessary data/tags/attributes
    for email reporting

    Args:
        soup (obj): BeautifulSoup object

    """
    attributes_to_decompose = ["extra"]
    if not config.RUN.get("logs_url"):
        attributes_to_decompose.append("col-links")
    decompose_html_attributes(soup, attributes_to_decompose)
    soup.find(id="not-found-message").decompose()

    if not config.RUN.get("logs_url"):
        for tr in soup.find_all("tr"):
            for th in tr.find_all("th"):
                if "Links" in th.text:
                    th.decompose()

    for p in soup.find_all("p"):
        if "(Un)check the boxes to filter the results." in p.text:
            p.decompose()
        if "pytest-html" in p.text:
            data = p.text.split("by")[0]
            p.string = data

    for ip in soup.find_all("input"):
        if not ip.has_attr("disabled"):
            ip["disabled"] = "true"

    for td in soup.find_all("td"):
        if "pytest" in td.text or "html" in td.text:
            data = td.text.replace("&apos", "")
            td.string = data
    main_header = soup.find("h1")
    main_header.string.replace_with("OCS-CI RESULTS")


def move_summary_to_top(soup):
    """
    Move summary to the top of the eamil report

    """
    summary = []
    summary.append(soup.find("h2", string="Summary"))
    for tag in summary[0].next_siblings:
        if tag.name == "h2":
            break
        else:
            summary.append(tag)
    for tag in summary:
        tag.extract()
    main_header = soup.find("h1")
    # because we are inserting the tags just after the header one by one, we
    # have to insert them in reverse order
    summary.reverse()
    for tag in summary:
        main_header.insert_after(tag)


def add_time_report_to_email(session, soup):
    """
    Takes the time report dictionary and converts it into HTML table
    """
    data = GV.TIMEREPORT_DICT
    sorted_data = dict(
        sorted(data.items(), key=lambda item: item[1].get("total", 0), reverse=True)
    )
    from jinja2 import FileSystemLoader, Environment

    file_loader = FileSystemLoader("templates/html_reports")
    env = Environment(loader=file_loader)
    table_html_template = env.get_template("test_time_table.html.j2")
    data = list(sorted_data.items())
    table_html = table_html_template.render(sorted_data=data[:5])
    summary_tag = soup.find("h2", string="Summary")
    time_div = soup.new_tag("div")
    table = BeautifulSoup(table_html, "html.parser")
    time_div.append(table)
    summary_tag.insert_after(time_div)


def decompose_html_attributes(soup, attributes):
    """
    Decomposes the given html attributes

    Args:
        soup (obj): BeautifulSoup object
        attributes (list): attributes to decompose

    Returns: None

    """
    for attribute in attributes:
        tg = soup.find_all(attrs={"class": attribute})
        for each in tg:
            each.decompose()
