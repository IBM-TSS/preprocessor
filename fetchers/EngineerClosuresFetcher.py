import os
import time

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium import webdriver
from dotenv import load_dotenv

from fetchers.VerseMailFetcher import VerseMailFetcher


load_dotenv(verbose=True, dotenv_path='secrets.env')
DRIVER_PATH = '/Users/reysantos7/Documents/IBMRepositories/preprocessor/fetchers/chromedriver'


class EngineerClosuresFetcher:
    URL = os.getenv("ENGINEER_REPORTS_ENDPOINT")
    WAIT_PAGE = 20
    DOWNLOAD_PATH = f"{os.getcwd()}/data/closures"

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

        # Avoid signin without a password
        WebDriverWait(self.driver, self.WAIT_PAGE).until(
            EC.presence_of_element_located(
                (By.XPATH, '//*[@id="alternate-signin-link"]')
            )
        ).click()

        # Login
        WebDriverWait(self.driver, self.WAIT_PAGE).until(
            EC.presence_of_element_located(
                (By.ID, 'desktop')
            )
        ).send_keys(self.user)
        pass_input = self.driver.find_element_by_name('password')
        pass_input.send_keys(self.password)
        pass_input.send_keys(Keys.ENTER)

        VerseMailFetcher.handle_verification(self.driver)

        # Wait until the file is downloaded, wait at most 10 tries of 2 seconds
        for _ in range(10):
            time.sleep(2)
            new_len = len([name for name in os.listdir(self.DOWNLOAD_PATH)
                           if os.path.isfile(os.path.join(self.DOWNLOAD_PATH, name))])

            if new_len > prev_len:
                print(f"File was downloaded in {self.DOWNLOAD_PATH}")
                return

        print("The file couldn't be downloaded")
