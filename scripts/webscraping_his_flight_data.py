import undetected_chromedriver as uc
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import time
import os

class PDCrawler:
    def __init__(self):
        options = uc.ChromeOptions()
        options.add_argument("--start-minimized")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        self.driver = uc.Chrome(options=options, use_subprocess=False)
        self.driver.set_window_size(800, 600)

    def goto(self, url):
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        except Exception as e:
            print(f"Error navigating to {url}: {e}")

    def interact_select(self, ID, desired_value):
        try:
            WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, ID)))
            select_element = self.driver.find_element(By.ID, ID)
            select = Select(select_element)
            select.select_by_value(str(desired_value))
        except Exception as e:
            print(f"Error selecting {desired_value} for {ID}: {e}")

    def interact_checkbox(self, ID):
        try:
            checkbox = WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.ID, ID)))
            if not checkbox.is_selected():
                checkbox.click()
        except Exception as e:
            print(f"Error interacting with checkbox {ID}: {e}")

    def interact_btn(self, ID, download_dir):
        try:
            button = WebDriverWait(self.driver, 15).until(EC.element_to_be_clickable((By.ID, ID)))
            self.driver.execute_script("arguments[0].scrollIntoView();", button)
            time.sleep(1)
            button.click()
            print("Download initiated")

            before_download = set(os.listdir(download_dir))
            while True:
                time.sleep(5)
                after_download = set(os.listdir(download_dir))
                new_files = after_download - before_download
                if new_files:
                    downloaded_file = list(new_files)[0]
                    print(f"Download successful: {downloaded_file}")
                    break
        except Exception as e:
            print(f"Error clicking button {ID}: {e}")
    
    def close(self):
        try:
            if self.driver:
                self.driver.quit()
                self.driver = None  # Prevent double closing
            os.system("taskkill /F /IM chromedriver.exe /IM chrome.exe /T 2>nul")  # Ensure all processes are killed
        except Exception as e:
            print(f"Error closing WebDriver: {e}")

if __name__ == "__main__":
    crawler = PDCrawler()
    base_url = "https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FGJ&QO_fu146_anzr="
    crawler.goto(base_url)

    year_month = [
        [2024,5],
        [2024,6],
        [2024,7],
        [2024,8],
        [2024,9],
        [2024,10],
        [2024,11],
        [2024,12]
    ]
    
    checkboxes = [
        "FL_DATE", "OP_UNIQUE_CARRIER", "OP_CARRIER", "TAIL_NUM", "ORIGIN",
        "ORIGIN_CITY_NAME", "DEST", "DEST_CITY_NAME", "DEP_TIME", "DEP_DELAY",
        "TAXI_OUT", "TAXI_IN", "ARR_TIME", "ARR_DELAY", "CANCELLED",
        "CANCELLATION_CODE", "CARRIER_DELAY", "WEATHER_DELAY",
        "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY"
    ]

    download_directory = "C:/Users/youss/Downloads"  # Change to actual download path

    for year, month in year_month:
        crawler.goto(base_url)
        
        crawler.interact_select("cboYear", year)
        crawler.interact_select("cboPeriod", month)

        for checkbox in checkboxes:
            crawler.interact_checkbox(checkbox)

        crawler.interact_btn("btnDownload", download_directory)
    
    crawler.close()
