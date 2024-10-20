import os
import time
import logging
import smtplib
import threading
import pytest
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from bs4 import BeautifulSoup

from framework import config


log = logging.getLogger(__name__)

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

def send_email_reports(session):
    """
    Email results of test run

    """
    time.sleep(20)
    # mailids = config.ENV_DATA.get("email")
    mailids = "ov"
    recipients = []
    [recipients.append(mailid) for mailid in mailids.split(",")]
    sender = "ocs-ci@redhat.com"
    msg = MIMEMultipart("alternative")
    msg["Subject"] = "noobaa standalone results"
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)

    html = config.ENV_DATA["html_path"]
    with open(os.path.expanduser(html)) as fd:
        html_data = fd.read()
    soup = BeautifulSoup(html_data, "html.parser")
    parse_html_for_email(soup)
    add_squad_analysis_to_email(session, soup)
    part1 = MIMEText(soup, "html")
    msg.attach(part1)
    with open("/home/oviner/ClusterPath/test7.html", "w") as file:
        # Write the data to the file
        file.write(msg.as_string())
    try:
        s = smtplib.SMTP(config.REPORTING["email"]["smtp_server"])
        s.sendmail(sender, recipients, msg.as_string())
        s.quit()
        log.info(f"Results have been emailed to {recipients}")
    except Exception:
        log.exception("Sending email with results failed!")


def pytest_sessionfinish(session, exitstatus):
    """
    save session's report files and send email report
    """
    # send_email_reports(session)
    # if config.ENV_DATA.get("email"):
    #     send_email_reports(session)
    a=1
    thread = threading.Thread(target=send_email_reports, args=(session,))
    thread.start()
    a=1

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


def pytest_sessionstart(session):
    """
    Prepare results dict
    """
    session.results = dict()

def add_squad_analysis_to_email(session, soup):
    """
    Add squad analysis to the html test results used in email reporting

    Args:
        session (obj): Pytest session object
        soup (obj): BeautifulSoup object of HTML Report data

    """
    failed = {}
    skipped = {}
    # sort out failed and skipped test cases to failed and skipped dicts
    for result in session.results.values():
        if result.failed or result.skipped:
            squad_marks = [
                key[:-6].capitalize() for key in result.keywords if "_squad" in key
            ]
            if squad_marks:
                for squad in squad_marks:
                    if result.failed:
                        if squad not in failed:
                            failed[squad] = []
                        failed[squad].append(result.nodeid)

                    if result.skipped:
                        if squad not in skipped:
                            skipped[squad] = []
                        try:
                            skipped_message = result.longrepr[2][8:]
                        except TypeError:
                            skipped_message = "--unknown--"
                        skipped[squad].append((result.nodeid, skipped_message))

            else:
                # unassigned
                if result.failed:
                    if "UNASSIGNED" not in failed:
                        failed["UNASSIGNED"] = []
                    failed["UNASSIGNED"].append(result.nodeid)
                if result.skipped:
                    if "UNASSIGNED" not in skipped:
                        skipped["UNASSIGNED"] = []
                    try:
                        skipped_message = result.longrepr[2][8:]
                    except TypeError:
                        skipped_message = "--unknown--"
                    skipped["UNASSIGNED"].append((result.nodeid, skipped_message))

    # no failed or skipped tests - exit the function
    if not failed and not skipped:
        return

    # add CSS for the Squad Analysis report
    style = soup.find("style")
    style = soup.new_tag("style")
    soup.head.append(style)
    # use colors for squad names from squad names
    style.string = "\n".join(
        [
            f"h4.squad-{color.lower()} {{\n    color: {color.lower()};\n}}"
            for color in SQUADS
        ]
    )
    # few additional styles
    style.string += """
    .squad-analysis {
        color: black;
        font-family: monospace;
        background-color: #eee;
        padding: 5px;
        margin-top: 10px;
    }
    .squad-analysis h2 {
        margin: 0px;
    }
    .squad-analysis h3 {
        margin: 0px;
        margin-top: 10px;
    }
    .squad-analysis h4 {
        margin: 0px;
    }
    .squad-analysis ul {
        margin: 0px;
    }
    .squad-analysis ul li em {
        margin-left: 1em;
    }
    .squad-unassigned {
        background-color: #FFBA88;
    }
    h4.squad-yellow {
        color: black;
        background-color: yellow;
        display: inline;
    }
    """
    # prepare place for the Squad Analysis in the email
    squad_analysis_div = soup.new_tag("div")
    squad_analysis_div["class"] = "squad-analysis"
    main_header = soup.find("h1")
    main_header.insert_after(squad_analysis_div)
    failed_h2_tag = soup.new_tag("h2")
    failed_h2_tag.string = "Squad Analysis - please analyze:"
    squad_analysis_div.append(failed_h2_tag)
    if failed:
        # print failed testcases peer squad
        failed_div_tag = soup.new_tag("div")
        squad_analysis_div.append(failed_div_tag)
        failed_h3_tag = soup.new_tag("h3")
        failed_h3_tag.string = "Failures:"
        failed_div_tag.append(failed_h3_tag)
        for squad in failed:
            failed_h4_tag = soup.new_tag("h4")
            failed_h4_tag.string = f"{squad} squad"
            failed_h4_tag["class"] = f"squad-{squad.lower()}"
            failed_div_tag.append(failed_h4_tag)
            failed_ul_tag = soup.new_tag("ul")
            failed_ul_tag["class"] = f"squad-{squad.lower()}"
            failed_div_tag.append(failed_ul_tag)
            for test in failed[squad]:
                failed_li_tag = soup.new_tag("li")
                failed_li_tag.string = test
                failed_ul_tag.append(failed_li_tag)
    if skipped:
        # print skipped testcases with reason peer squad
        skips_div_tag = soup.new_tag("div")
        squad_analysis_div.append(skips_div_tag)
        skips_h3_tag = soup.new_tag("h3")
        skips_h3_tag.string = "Skips:"
        skips_div_tag.append(skips_h3_tag)
        if config.RUN.get("display_skipped_msg_in_email"):
            skip_reason_h4_tag = soup.new_tag("h4")
            skip_reason_h4_tag.string = config.RUN.get("display_skipped_msg_in_email")
            skips_div_tag.append(skip_reason_h4_tag)
        for squad in skipped:
            skips_h4_tag = soup.new_tag("h4")
            skips_h4_tag.string = f"{squad} squad"
            skips_h4_tag["class"] = f"squad-{squad.lower()}"
            skips_div_tag.append(skips_h4_tag)
            skips_ul_tag = soup.new_tag("ul")
            skips_ul_tag["class"] = f"squad-{squad.lower()}"
            skips_div_tag.append(skips_ul_tag)
            for test in skipped[squad]:
                skips_li_tag = soup.new_tag("li")
                skips_test_span_tag = soup.new_tag("span")
                skips_test_span_tag.string = test[0]
                skips_li_tag.append(skips_test_span_tag)
                skips_li_tag.append(soup.new_tag("br"))
                skips_reason_em_tag = soup.new_tag("em")
                skips_reason_em_tag.string = f"Reason: {test[1]}"
                skips_li_tag.append(skips_reason_em_tag)
                skips_ul_tag.append(skips_li_tag)


SQUADS = {
    "Aqua": ["/lvmo/"],
    "Brown": ["/z_cluster/", "/nfs_feature/"],
    "Green": ["/encryption/", "/pv_services/", "/storageclass/"],
    "Blue": ["/monitoring/"],
    "Red": ["/mcg/", "/rgw/"],
    "Purple": ["/ecosystem/"],
    "Magenta": ["/workloads/", "/flowtest/", "/lifecycle/", "/kcs/", "/system_test/"],
    "Grey": ["/performance/"],
    "Orange": ["/scale/"],
    "Black": ["/ui/"],
    "Yellow": ["/managed-service/"],
    "Turquoise": ["/disaster-recovery/"],
}