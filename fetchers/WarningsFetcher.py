import os
import time

from selenium import webdriver
from dotenv import load_dotenv

from utils.VerseUtils import VerseUtils


load_dotenv(verbose=True, dotenv_path='secrets.env')
DRIVER_PATH = '/Users/reysantos7/Documents/IBMRepositories/preprocessor/fetchers/chromedriver'


class WarningsFetcher:
    URL = os.getenv("WARNINGS_REPORTS_ENDPOINT")
    WAIT_PAGE = 20
    DOWNLOAD_PATH = f"{os.getcwd()}/data/short_run"

    def __init__(self):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.headless = True
        prefs = {"download.default_directory": self.DOWNLOAD_PATH}
        chrome_options.add_experimental_option("prefs", prefs)

        self.user = os.getenv("ENGINEER_REPORTS_USER")
        self.password = os.getenv("ENGINEER_REPORTS_PASSWORD")
        self.driver = webdriver.Chrome(
            executable_path=DRIVER_PATH, chrome_options=chrome_options)

    def process(self):
        # Get the current len in the download folder to compare at the end
        prev_len = len([name for name in os.listdir(self.DOWNLOAD_PATH)
                        if os.path.isfile(os.path.join(self.DOWNLOAD_PATH, name))])

        self.driver.get(self.URL)

        # Login
        VerseUtils.login(self.driver)

        VerseUtils.handle_verification(self.driver)

        # Execute the script to download the file
        self.driver.execute_script('descargar_libro()')

        # Wait until the file is downloaded, wait at most 10 tries of 2 seconds
        for _ in range(10):
            time.sleep(2)
            new_len = len([name for name in os.listdir(self.DOWNLOAD_PATH)
                           if os.path.isfile(os.path.join(self.DOWNLOAD_PATH, name))])

            if new_len > prev_len:
                print(f"File was downloaded in {self.DOWNLOAD_PATH}")
                return

        print("The file couldn't be downloaded")
