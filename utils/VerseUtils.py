import os

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium import webdriver
from dotenv import load_dotenv


load_dotenv(verbose=True, dotenv_path='secrets.env')
DRIVER_PATH = '/Users/reysantos7/Documents/IBMRepositories/preprocessor/fetchers/chromedriver'


class VerseUtils:
    URL = os.getenv("VERSE_MAIL_ENDPOINT")

    @staticmethod
    def handle_verification(driver, wait_page=15):
        # Verify if the page is asking for authentication
        try:
            WebDriverWait(driver, wait_page).until(
                EC.presence_of_element_located(
                    (By.XPATH, '/html/body/div[1]/div[2]/div/div[3]/div/a')
                )
            )
        except TimeoutException:
            print("Authentication no needed or not found")
            return

        # Ask for verification type
        option = 0
        while option != 1 and option != 2:
            option = int(
                input("Type 1 for phone verification or 2 for email verification: "))

        # Phone option
        if option == 1:
            WebDriverWait(driver, wait_page).until(
                EC.presence_of_element_located(
                    (By.XPATH, '/html/body/div[1]/div[2]/div/div[3]/div/a')
                )
            ).click()
        # Email
        else:
            WebDriverWait(driver, wait_page).until(
                EC.presence_of_element_located(
                    (By.XPATH, '/html/body/div[1]/div[2]/div/div[4]/div/a')
                )
            ).click()

        # Wait for code
        code = None
        while not code:
            code = input("Type the code received by phone/email: ")

        code_input = WebDriverWait(driver, wait_page).until(
            EC.presence_of_element_located(
                (By.ID, 'otp')
            )
        )
        code_input.send_keys(code)
        code_input.send_keys(Keys.ENTER)

        # Remember me
        try:
            WebDriverWait(driver, wait_page).until(
                EC.presence_of_element_located(
                    (By.ID, 'continue')
                )
            ).click()
        except TimeoutException:
            print("No Rember me asked by the page.")
            return

    @staticmethod
    def login(driver, wait_page=15):
        user = os.getenv("ENGINEER_REPORTS_USER")
        password = os.getenv("ENGINEER_REPORTS_PASSWORD")

        WebDriverWait(driver, wait_page).until(
            EC.presence_of_element_located(
                (By.ID, 'desktop')
            )
        ).send_keys(user)
        pass_input = driver.find_element_by_name('password')
        pass_input.send_keys(password)
        pass_input.send_keys(Keys.ENTER)

    @staticmethod
    def avoid_signin_without_password(driver, wait_page=15):
        WebDriverWait(driver, wait_page).until(
            EC.presence_of_element_located(
                (By.XPATH, '//*[@id="alternate-signin-link"]')
            )
        ).click()

    def process(self):
        self.driver.get(self.URL)

        # Type the email
        pass_input = WebDriverWait(self.driver, self.wait_page).until(
            EC.presence_of_element_located(
                (By.ID, 'username')
            )
        )
        pass_input.send_keys(self.user)
        pass_input.send_keys(Keys.ENTER)

        # Login
        WebDriverWait(self.driver, self.wait_page).until(
            EC.presence_of_element_located(
                (By.ID, 'desktop')
            )
        ).send_keys(self.user)
        pass_input = self.driver.find_element_by_name('password')
        pass_input.send_keys(self.password)
        pass_input.send_keys(Keys.ENTER)
