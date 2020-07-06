import os

from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from dotenv import load_dotenv


load_dotenv(verbose=True, dotenv_path='secrets.env')
DRIVER_PATH = '/Users/reysantos7/Documents/IBMRepositories/preprocessor/fetchers/geckodriver'


class EngineerReportsFetcher:
    URL = os.getenv("ENGINEER_REPORTS_ENDPOINT")

    def __init__(self):
        # options = Options()
        # options.add_argument('-headless')
        # self.driver = webdriver.Firefox(executable_path=DRIVER_PATH, options=options)
        self.driver = webdriver.Firefox(executable_path=DRIVER_PATH)
        self.user = os.getenv("ENGINEER_REPORTS_USER")
        self.password = os.getenv("ENGINEER_REPORTS_PASSWORD")

    def process(self):
        self.driver.get(self.URL)

        self.driver.find_element_by_id('desktop').send_keys(self.user)
        pass_input = self.driver.find_element_by_name('password')
        pass_input.send_keys(self.password)
        pass_input.send_keys(Keys.ENTER)


# e = EngineerReportsFetcher()
# e.process()