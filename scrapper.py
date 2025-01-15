import sys
import sqlite3
import subprocess
import pkg_resources
import threading
import tkinter as tk
from tkinter import scrolledtext, messagebox
import webbrowser
import os
import time
import logging
import csv
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import (
    NoSuchElementException, TimeoutException, StaleElementReferenceException,
    ElementClickInterceptedException, WebDriverException, ElementNotInteractableException
)
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd

# -------------------------------------------------------------
# Глобальная переменная для хранения ссылки на файл-блокировку (Windows)
# -------------------------------------------------------------
lockfile = None

def check_single_instance():
    """
    Проверяет единственный экземпляр приложения:
    - На Windows — через файл-блокировку (app.lock).
    - На других ОС — через локальный сокет.
    Если программа уже запущена, выводит предупреждение и завершает работу.
    """
    global lockfile
    if os.name == 'nt':  # Если ОС Windows
        import msvcrt
        try:
            # Определяем путь рядом с исполняемым файлом (или скриптом)
            exe_dir = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(__file__)
            lock_path = os.path.join(exe_dir, "app.lock")

            lockfile = open(lock_path, "w")
            msvcrt.locking(lockfile.fileno(), msvcrt.LK_NBLCK, 1)
        except IOError:
            messagebox.showwarning("Warning", "Program is already running.")
            sys.exit(0)
    else:
        # Для остальных ОС используем проверку через сокет
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.bind(("127.0.0.1", 65432))
        except socket.error:
            messagebox.showwarning("Warning", "Program is already running.")
            sys.exit(0)
        # Возвращаем сокет, чтобы он не освободился
        return s

def sort_column(tv, col, reverse):
    l = [(tv.set(k, col), k) for k in tv.get_children('')]
    try:
        l.sort(key=lambda t: float(t[0]) if t[0] != "" else float('-inf'), reverse=reverse)
    except ValueError:
        l.sort(reverse=reverse)
    for index, (val, k) in enumerate(l):
        tv.move(k, '', index)
    tv.heading(col, command=lambda: sort_column(tv, col, not reverse))


# -------------------------------
# Step 1: Automatic Dependency Installation
# -------------------------------
required = {
    'selenium',
    'webdriver-manager',
    'beautifulsoup4',
    'pandas'
}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed
if missing:
    try:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', *missing],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError as e:
        print(f"Error installing packages: {e}")
        sys.exit(1)

# -------------------------------
# Step 2: GUI Setup with Tkinter
# -------------------------------
class ScraperGUI:
    def __init__(self, master):
        self.master = master
        master.title("Web Scraper")
        master.geometry("800x600")
        master.resizable(False, False)
        
        # Create Start Button
        self.start_button = tk.Button(master, text="Start Scraping", command=self.start_scraping, width=20, height=2, bg="green", fg="white")
        self.start_button.pack(pady=10)
        
        # Create View Results Button
        self.view_button = tk.Button(master, text="View Results", command=self.view_results, width=20, height=2, bg="blue", fg="white")
        self.view_button.pack(pady=10)
        
        # Create Export to Excel Button
        self.export_button = tk.Button(master, text="Export to Excel", command=self.export_to_excel, width=20, height=2, bg="orange", fg="white")
        self.export_button.pack(pady=10)

        
        # Create Scrolled Text for Logs
        self.log_area = scrolledtext.ScrolledText(master, wrap=tk.WORD, width=95, height=25, state='disabled')
        self.log_area.pack(padx=10, pady=10)
        
        # Initialize Logging
        self.setup_logging()
        
    def setup_logging(self):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        
        # Create custom handler for Tkinter
        self.gui_handler = GUIHandler(self.log_area)
        self.gui_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(self.gui_handler)
        
        # Also log to file
        file_handler = logging.FileHandler("scraper.log")
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(file_handler)
        
    def start_scraping(self):
        self.start_button.config(state='disabled')
        self.logger.info("Starting the scraping process...")
        threading.Thread(target=self.run_scraper, daemon=True).start()
        
    def run_scraper(self):
        try:
            main()
            self.logger.info("Scraping process completed successfully.")
            messagebox.showinfo("Success", "Scraping process completed successfully.")
        except Exception as e:
            self.logger.error(f"Scraping process terminated with an error: {e}")
            messagebox.showerror("Error", f"Scraping process terminated with an error:\n{e}")
        finally:
            self.start_button.config(state='normal')
    
    def view_results(self):
        # Получение данных из базы данных
        try:
            conn = sqlite3.connect('materials.db')
            df = pd.read_sql_query("SELECT * FROM materials_combined", conn)
            conn.close()
        except Exception as e:
            messagebox.showerror("Error", f"Error retrieving data: {e}")
            return

        top = tk.Toplevel(self.master)
        top.title("Search Data")
        from tkinter import ttk
        import re

        # Фрейм для полей поиска и фильтрации
        search_frame = tk.Frame(top)
        search_frame.pack(fill='x', padx=5, pady=5)

        search_label = tk.Label(search_frame, text="Search:")
        search_label.pack(side='left')

        search_entry = tk.Entry(search_frame)
        search_entry.pack(side='left', fill='x', expand=True, padx=5)
        search_entry.bind("<KeyRelease>", lambda event: on_search())  # Быстрый поиск в реальном времени

        col_label = tk.Label(search_frame, text=" in ")
        col_label.pack(side='left')

        # Выпадающий список для выбора столбца
        col_combo = ttk.Combobox(search_frame, values=list(df.columns))
        col_combo.pack(side='left')

        val_label = tk.Label(search_frame, text=" equals ")
        val_label.pack(side='left')

        # Выпадающий список для выбора значения
        val_combo = ttk.Combobox(search_frame, values=[])
        val_combo.pack(side='left')

        search_button = tk.Button(search_frame, text="Search")
        search_button.pack(side='left')

        tree = ttk.Treeview(top)
        tree.pack(fill='both', expand=True)

        tree["columns"] = list(df.columns)
        tree["show"] = "headings"
        for column in tree["columns"]:
            tree.heading(column, text=column, command=lambda _col=column: sort_column(tree, _col, False))

        def update_treeview(search_text="", column=None):
            for item in tree.get_children():
                tree.delete(item)
            try:
                pattern = re.compile(search_text, re.IGNORECASE)
            except re.error:
                pattern = None
            if search_text and column and pattern:
                filtered_df = df[df[column].astype(str).apply(lambda x: bool(pattern.search(x)))]
            elif search_text and pattern:
                filtered_df = df[df.apply(lambda row: any(pattern.search(str(val)) for val in row.values), axis=1)]
            else:
                filtered_df = df
            for _, row in filtered_df.iterrows():
                tree.insert("", "end", values=list(row))

        def update_treeview_data(dataframe):
            for item in tree.get_children():
                tree.delete(item)
            for _, row in dataframe.iterrows():
                tree.insert("", "end", values=list(row))

        # Первоначальное заполнение без фильтрации
        update_treeview()

        def on_search():
            text = search_entry.get()
            col = col_combo.get() if col_combo.get() in df.columns else None
            val = val_combo.get() if val_combo.get() else None
            # Если выбран столбец и значение, фильтровать по точному совпадению
            if col and val:
                filtered_df = df[df[col].astype(str) == str(val)]
                update_treeview_data(filtered_df)
            else:
                update_treeview(text, col)

        search_button.config(command=on_search)

        def update_val_combo(event):
            selected_col = col_combo.get()
            if selected_col in df.columns:
                unique_vals = df[selected_col].dropna().unique().tolist()
                val_combo['values'] = unique_vals

        col_combo.bind("<<ComboboxSelected>>", update_val_combo)





            
    def export_to_excel(self):
        try:
            conn = sqlite3.connect('materials.db')
            df = pd.read_sql_query("SELECT * FROM materials_combined", conn)
            conn.close()
            df.to_excel('materials_combined.xlsx', index=False, engine='openpyxl')
            messagebox.showinfo("Export", "Data exported to materials_combined.xlsx successfully.")
            self.logger.info("Data exported to Excel.")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to export data to Excel: {e}")
            self.logger.error(f"Failed to export data to Excel: {e}")



class GUIHandler(logging.Handler):
    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget
        
    def emit(self, record):
        msg = self.format(record)
        def append():
            self.text_widget.config(state='normal')
            self.text_widget.insert(tk.END, msg + '\n')
            self.text_widget.see(tk.END)
            self.text_widget.config(state='disabled')
        self.text_widget.after(0, append)

# -------------------------------
# Step 3: Logging Configuration
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
    ]
)
for handler in logging.root.handlers[:]:
    if isinstance(handler, logging.StreamHandler):
        logging.root.removeHandler(handler)

# -------------------------------
# Helper Functions
# -------------------------------
def init_screenshot_folder():
    today = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    folder_name = f"screenshots_{today}"
    os.makedirs(folder_name, exist_ok=True)
    return folder_name

def setup_driver(screenshot_folder):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_prefs = {"profile.default_content_setting_values": {"images": 2}}
    chrome_options.add_experimental_option("prefs", chrome_prefs)
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-dev-shm-usage")
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        logging.info("Chrome WebDriver initialized successfully.")
        return driver
    except Exception as e:
        logging.error(f"Failed to initialize Chrome WebDriver: {e}")
        raise

def save_screenshot(driver, screenshot_folder, filename):
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_filename = filename.replace("/", "_").replace("\\", "_").replace(":", "_") \
                               .replace("*", "_").replace("?", "_").replace('"', '_') \
                               .replace("<", "_").replace(">", "_").replace("|", "_")
        filepath = os.path.join(screenshot_folder, f"{timestamp}_{safe_filename}")
        driver.save_screenshot(filepath)
        logging.info(f"Screenshot saved: {filepath}")
    except Exception as e:
        logging.error(f"Failed to save screenshot '{filename}': {e}")

# -------------------------------
# Функции для OSH Cut
# -------------------------------
def navigate_to_sheet_page(driver, screenshot_folder):
    try:
        SHEET_URL = "https://app.oshcut.com/catalog/sheet"
        driver.get(SHEET_URL)
        logging.info(f"Loaded page: {SHEET_URL}")
        wait = WebDriverWait(driver, 60)
        wait.until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'filterBoxHeader') and contains(text(), 'Material')]")))
        logging.info("Categories loaded.")
        time.sleep(2)
    except TimeoutException:
        logging.exception("Timeout while loading the 'Sheet' page.")
        if driver:
            save_screenshot(driver, screenshot_folder, 'navigate_to_sheet_timeout.png')
        raise
    except Exception as e:
        logging.exception(f"Error loading 'Sheet' page: {e}")
        if driver:
            save_screenshot(driver, screenshot_folder, 'navigate_to_sheet_error.png')
        raise

def extract_categories(driver):
    categories = []
    try:
        wait = WebDriverWait(driver, 60)
        categories_xpath = "//div[contains(@class, 'supertype') and contains(@class, 'clickable')]"
        category_elements = wait.until(EC.presence_of_all_elements_located((By.XPATH, categories_xpath)))
        logging.info(f"Found {len(category_elements)} categories with 'supertype clickable' class.")
        for elem in category_elements:
            try:
                cat_name_elem = elem.find_element(By.XPATH, ".//b[@class='header']")
                cat_name = cat_name_elem.text.strip()
                if cat_name:
                    categories.append(cat_name)
            except NoSuchElementException:
                logging.warning("Failed to extract category name from element.")
                continue
        if not categories:
            logging.warning("No categories found in the 'Material' filter.")
    except TimeoutException:
        logging.exception("Timeout while loading categories from the 'Material' filter.")
    except Exception as e:
        logging.exception(f"Error extracting categories: {e}")
    return categories

def extract_material_elements(driver, screenshot_folder):
    material_elements = []
    try:
        wait = WebDriverWait(driver, 60)
        SCROLL_PAUSE_TIME = 1.0
        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            logging.info("Scrolling down to load more materials...")
            time.sleep(SCROLL_PAUSE_TIME)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
        materials_xpath = "//div[contains(@class, 'materialType')]"
        wait.until(EC.presence_of_all_elements_located((By.XPATH, materials_xpath)))
        material_elements = driver.find_elements(By.XPATH, materials_xpath)
        logging.info(f"Found {len(material_elements)} materials in the current category.")
        if not material_elements:
            save_screenshot(driver, screenshot_folder, 'no_materials_found.png')
            logging.info("Screenshot saved: no_materials_found.png")
        else:
            time.sleep(2)
    except TimeoutException:
        logging.exception("Timeout while loading materials.")
        save_screenshot(driver, screenshot_folder, 'timeout_materials.png')
        logging.info("Screenshot saved: timeout_materials.png")
    except WebDriverException as e:
        logging.exception(f"Error extracting materials: {e}")
        if "session deleted because of page crash" in str(e):
            logging.error("Browser crashed. Attempting to restart the driver.")
            raise
        if driver:
            save_screenshot(driver, screenshot_folder, 'extract_materials_error.png')
            logging.info("Screenshot saved: extract_materials_error.png")
    except Exception as e:
        logging.exception(f"Error extracting materials: {e}")
        if driver:
            save_screenshot(driver, screenshot_folder, 'extract_materials_error.png')
            logging.info("Screenshot saved: extract_materials_error.png")
    return material_elements

def safe_click(driver, element, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'center'});", element)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", element)
            return
        except ElementClickInterceptedException:
            logging.warning("ElementClickInterceptedException: retrying click.")
            try:
                ActionChains(driver).move_to_element(element).click().perform()
                return
            except ElementClickInterceptedException:
                logging.warning("Repeated ElementClickInterceptedException. Waiting before retrying.")
                time.sleep(1)
        except StaleElementReferenceException:
            logging.exception("StaleElementReferenceException when clicking element. Retrying.")
            retries += 1
            time.sleep(1)
        except ElementNotInteractableException:
            logging.exception("ElementNotInteractableException when clicking element. Retrying.")
            retries += 1
            time.sleep(1)
    logging.error("Failed to click the element after multiple attempts.")
    raise ElementClickInterceptedException("Failed to click the element after multiple attempts.")

def safe_click_with_retries(driver, element, screenshot_folder, csvfile, max_retries=5, delay=2):
    retries = 0
    while retries < max_retries:
        try:
            safe_click(driver, element)
            return
        except (ElementClickInterceptedException, ElementNotInteractableException, StaleElementReferenceException):
            logging.warning(f"Attempt {retries+1} failed. Retrying in {delay} seconds...")
            time.sleep(delay)
            retries += 1
    logging.error(f"Failed to click the element after {max_retries} attempts.")
    raise ElementClickInterceptedException("Failed to click the element after multiple attempts.")

def reset_filters_if_applied(driver, screenshot_folder, csvfile):
    try:
        wait = WebDriverWait(driver, 10)
        active_filters = driver.find_elements(By.XPATH, "//span[@class='pill']")
        if active_filters:
            reset_filters(driver, screenshot_folder, csvfile)
        else:
            logging.info("No active filters. Reset not required.")
    except Exception as e:
        logging.exception(f"Error checking filters: {e}")

def reset_filters(driver, screenshot_folder, csvfile):
    try:
        wait = WebDriverWait(driver, 30)
        reset_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//span[contains(@class, 'filterReset')]")))
        safe_click_with_retries(driver, reset_button, screenshot_folder, csvfile)
        logging.info("Filters reset.")
        wait.until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'filterBoxHeader') and contains(text(), 'Material')]")))
        time.sleep(2)
    except TimeoutException:
        logging.exception("Timeout while resetting filters.")
        save_screenshot(driver, screenshot_folder, 'timeout_reset_filters.png')
        logging.info("Screenshot saved: timeout_reset_filters.png")
    except WebDriverException as e:
        logging.exception(f"Error resetting filters: {e}")
        if "invalid session id" in str(e):
            logging.error("Session invalid. Browser closed.")
            raise
        if driver:
            save_screenshot(driver, screenshot_folder, 'reset_filters_error.png')
            logging.info("Screenshot saved: reset_filters_error.png")
    except Exception as e:
        logging.exception(f"Error resetting filters: {e}")
        if driver:
            save_screenshot(driver, screenshot_folder, 'reset_filters_error.png')
            logging.info("Screenshot saved: reset_filters_error.png")

def close_modal(driver, screenshot_folder):
    try:
        wait = WebDriverWait(driver, 10)
        close_buttons = driver.find_elements(By.XPATH, "//button[contains(text(), 'Close') or contains(text(), 'Закрыть') or contains(@class, 'close')]")
        back_buttons = driver.find_elements(By.XPATH, "//button[contains(@class, 'btnTertiary') and contains(text(), 'Back to Catalog')]")
        if close_buttons:
            for close_button in close_buttons:
                try:
                    safe_click_with_retries(driver, close_button, screenshot_folder, None)
                    logging.info("Modal window closed via 'Close' button.")
                    time.sleep(2)
                except Exception as e:
                    logging.exception(f"Error closing modal via 'Close' button: {e}")
        if back_buttons:
            for back_button in back_buttons:
                try:
                    safe_click_with_retries(driver, back_button, screenshot_folder, None)
                    logging.info("Clicked 'Back to Catalog' to return to the material list.")
                    time.sleep(2)
                except Exception as e:
                    logging.exception(f"Error clicking 'Back to Catalog' button: {e}")
        if not (close_buttons or back_buttons):
            logging.warning("Modal window not found to close.")
    except Exception as e:
        logging.exception(f"Error closing modal windows: {e}")
        save_screenshot(driver, screenshot_folder, f"close_modal_error_{int(time.time())}.png")

def ensure_modal_closed(driver, screenshot_folder):
    try:
        modal_close_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Close') or contains(@class, 'close') or contains(text(), 'Back to Catalog')]")
        safe_click_with_retries(driver, modal_close_button, screenshot_folder, None)
        logging.info("Modal window closed.")
    except NoSuchElementException:
        logging.info("Modal window already closed.")

def return_to_material_list(driver, screenshot_folder):
    try:
        material_list_xpath = "//div[contains(@class, 'materialType')]"
        WebDriverWait(driver, 30).until(
            EC.presence_of_all_elements_located((By.XPATH, material_list_xpath))
        )
        logging.info("Successfully returned to the material list.")
    except TimeoutException:
        logging.error("Failed to return to the material list.")
        save_screenshot(driver, screenshot_folder, 'return_to_material_list_error.png')
        raise

def go_to_next_material(driver, current_category_name, processed_materials, screenshot_folder, csvfile):
    try:
        ensure_modal_closed(driver, screenshot_folder)
        return_to_material_list(driver, screenshot_folder)
        reset_filters_if_applied(driver, screenshot_folder, csvfile)
        click_category(driver, current_category_name, screenshot_folder, csvfile)
        material_elements = extract_material_elements(driver, screenshot_folder)
        for material_element in material_elements:
            mat_name = material_element.find_element(By.XPATH, ".//header").text.strip()
            if mat_name not in processed_materials:
                return mat_name
        logging.info("All materials in the category have been processed.")
        return None
    except Exception as e:
        logging.exception(f"Error navigating to the next material: {e}")
        raise

def click_category(driver, cat_name, screenshot_folder, csvfile):
    wait = WebDriverWait(driver, 60)
    category_xpath = f"//div[contains(@class, 'supertype') and contains(@class, 'clickable')]//b[@class='header' and normalize-space(text())='{cat_name}']/ancestor::div[contains(@class, 'supertype') and contains(@class, 'clickable')]"
    try:
        cat_elem = wait.until(EC.element_to_be_clickable((By.XPATH, category_xpath)))
        logging.info(f"Category '{cat_name}' found and clickable.")
        safe_click_with_retries(driver, cat_elem, screenshot_folder, csvfile)
        logging.info(f"Category '{cat_name}' selected.")
        time.sleep(2)
    except TimeoutException:
        logging.exception(f"Timeout while searching for category '{cat_name}'.")
        save_screenshot(driver, screenshot_folder, f"click_category_timeout_{cat_name}_{int(time.time())}.png")
        raise
    except Exception as e:
        logging.exception(f"Error clicking on category '{cat_name}': {e}")
        save_screenshot(driver, screenshot_folder, f"click_category_error_{cat_name}_{int(time.time())}.png")
        raise

def click_material_name(driver, mat_name, screenshot_folder, csvfile):
    try:
        mat_element_xpath = f"//div[contains(@class, 'materialType')]//header[contains(text(), '{mat_name}')]/ancestor::div[contains(@class, 'materialType')]"
        mat_element = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.XPATH, mat_element_xpath))
        )
        header_element = mat_element.find_element(By.XPATH, ".//header")
        safe_click_with_retries(driver, header_element, screenshot_folder, csvfile)
        logging.info(f"Material name '{mat_name}' expanded.")
        time.sleep(5)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        wait = WebDriverWait(driver, 30)
        wait.until(EC.visibility_of_element_located((By.XPATH, f"{mat_element_xpath}//button[contains(text(), 'More info')]")))
        logging.info(f"'More info...' buttons for material '{mat_name}' are now visible.")
    except TimeoutException:
        logging.error("Timeout while expanding the thickness list.")
        save_screenshot(driver, screenshot_folder, f"click_material_name_timeout_{mat_name}_{int(time.time())}.png")
        raise
    except NoSuchElementException:
        logging.error("Material name not found for clicking.")
        save_screenshot(driver, screenshot_folder, f"click_material_name_missing_{mat_name}_{int(time.time())}.png")
        raise
    except Exception as e:
        logging.exception(f"Error clicking on material name '{mat_name}': {e}")
        save_screenshot(driver, screenshot_folder, f"click_material_name_error_{mat_name}_{int(time.time())}.png")
        raise

def click_more_info(driver, mat_name, btn_idx, more_info_button, screenshot_folder, csvfile):
    try:
        safe_click_with_retries(driver, more_info_button, screenshot_folder, csvfile)
        logging.info(f"Clicked 'More info...' button {btn_idx} for material '{mat_name}'.")
        wait = WebDriverWait(driver, 60)
        wait.until(EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'materialDescription')]")))
        logging.info("Modal window 'More info...' appeared.")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)
    except TimeoutException:
        logging.error(f"Timeout while opening modal window 'More info...' for material '{mat_name}'.")
        save_screenshot(driver, screenshot_folder, f"more_info_modal_timeout_{mat_name}_{btn_idx}_{int(time.time())}.png")
        raise
    except Exception as e:
        logging.exception(f"Error clicking 'More info...' button for material '{mat_name}': {e}")
        save_screenshot(driver, screenshot_folder, f"more_info_click_error_{mat_name}_{btn_idx}_{int(time.time())}.png")
        raise

def extract_material_details(driver, category):
    details = {
        'Thickness Name': '',
        'K-factor': '',
        'Minimum Flange Support': '',
        'Bend Deduction': '',
        'Maximum Bend Length': ''
    }
    try:
        wait = WebDriverWait(driver, 60)
        thickness_xpath = "//div[@class='materialActionBar']//div[@class='subHeader']"
        thickness_element = wait.until(EC.presence_of_element_located((By.XPATH, thickness_xpath)))
        thickness_text = thickness_element.text.strip()
        if '"' in thickness_text:
            thickness_name = thickness_text.split('"')[0] + '"'
        else:
            thickness_name = thickness_text
        details['Thickness Name'] = thickness_name
        logging.info(f"Extracted thickness name: {details['Thickness Name']} for category '{category}'")
        k_factor_xpath = """
        //table[contains(@class, 'metalProperties')]//tr[
            td[normalize-space(text())='K-factor'] 
            or td[normalize-space(text())='K-фактор']
        ]/td[2]
        """
        try:
            k_factor_element = driver.find_element(By.XPATH, k_factor_xpath)
            details['K-factor'] = k_factor_element.text.strip()
            logging.info(f"Extracted K-factor: {details['K-factor']} for category '{category}'")
        except NoSuchElementException:
            logging.warning(f"K-factor not found for category '{category}'.")
            details['K-factor'] = 'Not Found'
        tables = {
            'Minimum Flange Support': "//table[contains(@class, 'MaterialBendTable') and (.//td[contains(text(), 'Flange') or contains(text(), 'Фланца')])]",
            'Bend Deduction': "//table[contains(@class, 'MaterialBendTable') and (.//td[contains(text(), 'Bend Deduction') or contains(text(), 'Уменьшение изгиба')])]",
            'Maximum Bend Length': "//table[contains(@class, 'MaterialBendTable') and (.//td[contains(text(), 'Maximum Bend Length') or contains(text(), 'Максимальная длина изгиба')])]"
        }
        for key, xpath in tables.items():
            try:
                table = driver.find_element(By.XPATH, xpath)
                details[key] = extract_table_data(table)
                logging.info(f"Extracted data for {key} in category '{category}'.")
            except NoSuchElementException:
                logging.warning(f"Table {key} not found for category '{category}'.")
                details[key] = 'Not Found'
        all_table_titles = driver.find_elements(By.XPATH, "//table[contains(@class, 'MaterialBendTable')]//td[@class='tableTitle']")
        for table_title in all_table_titles:
            logging.info(f"Found table title: {table_title.text.strip()} for category '{category}'")
    except TimeoutException:
        logging.exception(f"Timeout while extracting material details for category '{category}'.")
    except Exception as e:
        logging.exception(f"Error extracting material details for category '{category}': {e}")
    return details

def extract_table_data(table_element):
    data = []
    try:
        rows = table_element.find_elements(By.XPATH, ".//tr")
        for row in rows:
            cols = row.find_elements(By.XPATH, ".//td")
            cols_text = [col.text.strip() for col in cols]
            if len(cols_text) >= 2:
                data.append(" | ".join(cols_text))
            else:
                data.append(" | ".join(cols_text))
    except Exception as e:
        logging.error(f"Error extracting data from table: {e}")
    return "; ".join(data)

def parse_and_collect_all_categories(driver, screenshot_folder):
    processed_materials = set()
    categories = extract_categories(driver)
    all_data = []
    if not categories:
        logging.warning("No categories found.")
        return all_data

    logging.info(f"Extracted categories: {categories}")
    for cat_idx, cat_name in enumerate(categories, start=1):
        logging.info(f"Processing category {cat_idx}/{len(categories)}: {cat_name}")
        try:
            click_category(driver, cat_name, screenshot_folder, None)
            while True:
                mat_name = go_to_next_material(driver, cat_name, processed_materials, screenshot_folder, None)
                if not mat_name:
                    break
                logging.info(f"Processing material: {mat_name}")
                try:
                    click_material_name(driver, mat_name, screenshot_folder, None)
                    more_info_buttons = driver.find_elements(By.XPATH, f"//header[contains(text(), '{mat_name}')]/ancestor::div[contains(@class, 'materialType')]//button[contains(text(), 'More info')]")
                    for btn_idx, more_info_button in enumerate(more_info_buttons):
                        try:
                            click_more_info(driver, mat_name, btn_idx, more_info_button, screenshot_folder, None)
                            details = extract_material_details(driver, cat_name)
                            all_data.append({
                                'Category': cat_name,
                                'Material Name': mat_name,
                                'Thickness Name': details.get('Thickness Name', ''),
                                'K-factor': details.get('K-factor', ''),
                                'Minimum Flange Support': details.get('Minimum Flange Support', ''),
                                'Bend Deduction': details.get('Bend Deduction', ''),
                                'Maximum Bend Length': details.get('Maximum Bend Length', '')
                            })
                            logging.info(f"Data collected for material '{mat_name}'.")
                            close_modal(driver, screenshot_folder)
                        except Exception as e:
                            logging.exception(f"Error processing 'More info...' button {btn_idx + 1} for material '{mat_name}': {e}")
                            close_modal(driver, screenshot_folder)
                            continue
                    processed_materials.add(mat_name)
                    logging.info(f"Completed processing material '{mat_name}'.")
                except Exception as e:
                    logging.exception(f"Error processing material '{mat_name}': {e}")
                    save_screenshot(driver, screenshot_folder, f"error_material_{mat_name}.png")
                    continue
            logging.info(f"Completed processing category '{cat_name}'.")
            reset_filters_if_applied(driver, screenshot_folder, None)
        except Exception as e:
            logging.exception(f"Error processing category '{cat_name}': {e}")
            reset_filters_if_applied(driver, screenshot_folder, None)
            continue
    return all_data


def parse_oshcut():
    screenshot_folder = init_screenshot_folder()
    logging.info(f"Screenshot folder created: {screenshot_folder}")
    driver = None
    df_oshcut = pd.DataFrame()
    try:
        driver = setup_driver(screenshot_folder)
        navigate_to_sheet_page(driver, screenshot_folder)
        data = parse_and_collect_all_categories(driver, screenshot_folder)
        df_oshcut = pd.DataFrame(data)
        df_oshcut['Source'] = 'OSH Cut'
    except Exception as e:
        logging.error(f"Error during OSH Cut parsing: {e}")
        df_oshcut = pd.DataFrame()
    finally:
        if driver:
            try:
                save_screenshot(driver, screenshot_folder, 'final_screenshot.png')
            except Exception as e:
                logging.error(f"Failed to save final screenshot: {e}")
            driver.quit()
            logging.info("WebDriver closed.")
    return df_oshcut


# -------------------------------
# Новый функционал: Парсинг с сайта SendCutSend
# -------------------------------
def scroll_to_bottom(driver, pause_time=2):
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause_time)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
    logging.info("Reached bottom of the page.")

def analyze_debug_page():
    try:
        with open('debug_page.html', 'r', encoding='utf-8') as f:
            html_content = f.read()
        soup = BeautifulSoup(html_content, 'html.parser')
        error_messages = [
            "Access Denied", "403 Forbidden", "404 Not Found", "Please complete the CAPTCHA",
            "Our systems have detected unusual traffic", "Error", "This page isn't working",
            "We're sorry, but something went wrong", "You have been blocked", "Доступ запрещен",
            "Страница не найдена", "Ошибка", "Вы были заблокированы", "Cloudflare"
        ]
        page_text = soup.get_text(separator=' ').strip()
        found_error = False
        for error_msg in error_messages:
            if error_msg.lower() in page_text.lower():
                logging.error(f"Error message found on page: '{error_msg}'")
                found_error = True
                break
        if not found_error:
            logging.info("No explicit error messages found on the page.")
    except Exception as e:
        logging.error(f"Error analyzing debug_page.html: {e}")

def get_subcategory_links(driver):
    try:
        menu = driver.find_element(By.ID, "menu-1-711fca")
        categories = menu.find_elements(By.CSS_SELECTOR, 'li.menu-item-has-children')
        logging.info(f"Found {len(categories)} material categories.")
        subcategory_links = []
        for category in categories:
            try:
                category_link = category.find_element(By.CSS_SELECTOR, 'a')
                category_name = category_link.text.strip().title()
                logging.info(f"Processing category: {category_name}")
                action = ActionChains(driver)
                action.move_to_element(category_link).perform()
                time.sleep(1)
                submenu = category.find_element(By.CSS_SELECTOR, 'ul.sub-menu')
                sublinks = submenu.find_elements(By.CSS_SELECTOR, 'li a')
                logging.info(f"Found {len(sublinks)} materials in category '{category_name}'")
                for sublink in sublinks:
                    href = sublink.get_attribute('href')
                    material_name = sublink.get_attribute('textContent').strip().title()
                    logging.info(f"Found material: {material_name} - {href}")
                    if href:
                        subcategory_links.append((category_name, material_name, href))
            except Exception as e:
                logging.error(f"Error processing category: {e}")
                continue
        logging.info(f"Total materials found across all categories: {len(subcategory_links)}")
        return subcategory_links
    except Exception as e:
        logging.error(f"Error retrieving categories and subcategories: {e}")
        driver.save_screenshot('metals_menu_error.png')
        logging.info("Screenshot saved as metals_menu_error.png.")
        analyze_debug_page()
        return []

def scrape_subcategory(driver, category_name, material_name, url):
    try:
        logging.info(f"Loading subcategory: {url}")
        driver.get(url)
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        logging.info("Scrolling to bottom of the page to load all content.")
        scroll_to_bottom(driver)
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(1)
    except Exception as e:
        logging.error(f"Error loading subcategory {url}: {e}")
        driver.save_screenshot('subcategory_generic_error.png')
        logging.info("Screenshot saved as subcategory_generic_error.png.")
        return []
    data_list = []
    logging.info(f"Parsing material: {material_name}")
    try:
        try:
            tabs_content = driver.find_element(By.CSS_SELECTOR, "div.e-n-tabs-content")
            logging.info("Found div with class 'e-n-tabs-content'.")
        except Exception as e:
            logging.error(f"Could not find div 'e-n-tabs-content' for material '{material_name}': {e}")
            driver.save_screenshot('tabs_content_error.png')
            logging.info("Screenshot saved as tabs_content_error.png.")
            return []
        try:
            tabs_heading = driver.find_element(By.CSS_SELECTOR, "div.e-n-tabs-heading")
            tab_buttons = tabs_heading.find_elements(By.CSS_SELECTOR, 'button.e-n-tab-title')
            thickness_mapping = {}
            for button in tab_buttons:
                thickness_text = button.find_element(By.CSS_SELECTOR, 'span.e-n-tab-title-text').text.strip()
                if '"' in thickness_text:
                    thickness = thickness_text.replace('"', '').strip()
                    aria_controls = button.get_attribute('aria-controls')
                    if aria_controls:
                        thickness_mapping[aria_controls] = thickness
            logging.info(f"Extracted thicknesses (inches only): {thickness_mapping}")
        except Exception as e:
            logging.error(f"Error extracting thicknesses for '{material_name}': {e}")
            driver.save_screenshot('thickness_extraction_error.png')
            logging.info("Screenshot saved as thickness_extraction_error.png.")
            thickness_mapping = {}
        if not thickness_mapping:
            logging.warning(f"No thickness found via tabs for '{material_name}'. Attempting table extraction.")
            try:
                tables = tabs_content.find_elements(By.TAG_NAME, 'table')
                logging.info(f"Found {len(tables)} spec tables for '{material_name}'.")
                for table in tables:
                    table_html = table.get_attribute('outerHTML')
                    soup = BeautifulSoup(table_html, 'html.parser')
                    parsed_table = soup.find('table')
                    if not parsed_table:
                        continue
                    rows = parsed_table.find_all('tr')
                    data = {
                        "Category": category_name,
                        "Material Name": material_name,
                        "Thickness": "",
                        "Effective bend radius @90°": "",
                        "K factor": "",
                        "Gauge": ""
                    }
                    thickness_is_inches = False
                    for row in rows:
                        cols = row.find_all(['th', 'td'])
                        if len(cols) != 2:
                            continue
                        key = cols[0].get_text(strip=True).lower()
                        value = cols[1].get_text(strip=True)
                        if 'advertised thickness' in key:
                            if '"' in value:
                                data["Thickness"] = value.replace('"', '').strip()
                                thickness_is_inches = True
                        elif 'effective bend radius' in key:
                            data["Effective bend radius @90°"] = value.replace('"', '').strip()
                        elif 'k factor' in key:
                            data["K factor"] = value.replace('"', '').strip()
                        elif 'gauge' in key:
                            data["Gauge"] = value.replace('"', '').strip()
                    if data["Thickness"] and thickness_is_inches:
                        logging.info(f"Extracted data for {material_name} (thickness {data['Thickness']}\"): {data}")
                        data_list.append(data)
                    else:
                        if data["Thickness"]:
                            logging.warning(f"Thickness not in inches for '{material_name}'. Skipping value {data['Thickness']}.")
                        else:
                            logging.warning(f"'Advertised Thickness' not found in table for '{material_name}'.")
            except Exception as e:
                logging.error(f"Error extracting thickness from tables for '{material_name}': {e}")
                return data_list
        else:
            try:
                content_divs = tabs_content.find_elements(By.CSS_SELECTOR, 'div[id^="e-n-tab-content-"]')
                logging.info(f"Found {len(content_divs)} content divs for '{material_name}'.")
                for content_div in content_divs:
                    content_id = content_div.get_attribute('id')
                    thickness = thickness_mapping.get(content_id, "Unknown")
                    if thickness == "Unknown":
                        logging.warning(f"Thickness undefined for content_id {content_id}. Skipping.")
                        continue
                    tables = content_div.find_elements(By.TAG_NAME, 'table')
                    logging.info(f"Found {len(tables)} spec tables for thickness {thickness}\".")
                    if not tables:
                        logging.warning(f"No spec tables found for '{material_name}' thickness '{thickness}'.")
                        continue
                    for table in tables:
                        table_html = table.get_attribute('outerHTML')
                        soup = BeautifulSoup(table_html, 'html.parser')
                        parsed_table = soup.find('table')
                        if not parsed_table:
                            continue
                        rows = parsed_table.find_all('tr')
                        data = {
                            "Category": category_name,
                            "Material Name": material_name,
                            "Thickness": thickness,
                            "Effective bend radius @90°": "",
                            "K factor": "",
                            "Gauge": ""
                        }
                        for row in rows:
                            cols = row.find_all(['th', 'td'])
                            if len(cols) != 2:
                                continue
                            key = cols[0].get_text(strip=True).lower()
                            value = cols[1].get_text(strip=True)
                            if 'effective bend radius' in key:
                                data["Effective bend radius @90°"] = value.replace('"', '').strip()
                            elif 'k factor' in key:
                                data["K factor"] = value.replace('"', '').strip()
                            elif 'gauge' in key:
                                data["Gauge"] = value.replace('"', '').strip()
                        logging.info(f"Extracted data for {material_name} (thickness {thickness}\"): {data}")
                        data_list.append(data)
            except Exception as e:
                logging.error(f"Error processing tables with thicknesses for '{material_name}': {e}")
    except Exception as e:
        logging.error(f"Error processing material '{material_name}': {e}")
        driver.save_screenshot('material_processing_error.png')
        logging.info("Screenshot saved as material_processing_error.png.")
    if not data_list:
        data = {
            "Category": category_name,
            "Material Name": material_name,
            "Thickness": "N/A",
            "Effective bend radius @90°": "",
            "K factor": "",
            "Gauge": ""
        }
        logging.info(f"Extracted data for {material_name} (thickness N/A): {data}")
        data_list.append(data)
    return data_list

def scrape_materials_page():
    url = "https://sendcutsend.com/materials/"
    logging.info(f"Navigating to main page: {url}")
    screenshot_folder = init_screenshot_folder()
    driver = setup_driver(screenshot_folder)
    driver.get(url)
    try:
        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.ID, "menu-1-711fca"))
        )
        logging.info("Main page loaded successfully.")
    except Exception as e:
        logging.error(f"Error loading main page: {e}")
        with open('debug_page.html', 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
        logging.info("Page HTML saved to debug_page.html for debugging.")
        driver.save_screenshot('main_page_error.png')
        logging.info("Screenshot saved as main_page_error.png.")
        analyze_debug_page()
        driver.quit()
        return []
    subcategory_links = get_subcategory_links(driver)
    if not subcategory_links:
        logging.error("No subcategories to process.")
        driver.quit()
        return []
    all_data = []
    for idx, (category_name, material_name, sub_url) in enumerate(subcategory_links):
        logging.info(f"Processing subcategory [{idx+1}/{len(subcategory_links)}]: {sub_url}")
        data = scrape_subcategory(driver, category_name, material_name, sub_url)
        all_data.extend(data)
        time.sleep(1)
    driver.quit()
    return all_data

# -------------------------------
# Основная функция main
# -------------------------------
def main():
    # Парсинг данных с OSH Cut
    df_oshcut = pd.DataFrame()
    try:
        df_oshcut = parse_oshcut()
        if df_oshcut.empty:
            logging.warning("No data parsed from OSH Cut.")
    except Exception as e:
        logging.error(f"Error parsing OSH Cut data: {e}")

    # Парсинг данных с SendCutSend
    df_sendcutsend = pd.DataFrame()
    try:
        sendcutsend_data = scrape_materials_page()
        if sendcutsend_data:
            df_sendcutsend = pd.DataFrame(sendcutsend_data)
            string_columns = ["Category", "Material Name", "Gauge"]
            for col in string_columns:
                if col in df_sendcutsend.columns:
                    df_sendcutsend[col] = df_sendcutsend[col].str.replace('"', '').str.strip()
            specific_columns = ["Effective bend radius @90°", "K factor"]
            for col in specific_columns:
                if col in df_sendcutsend.columns:
                    df_sendcutsend[col] = df_sendcutsend[col].astype(str).str.replace('"', '').str.strip()
            numeric_columns = ["Thickness", "Effective bend radius @90°", "K factor"]
            for col in numeric_columns:
                if col in df_sendcutsend.columns:
                    df_sendcutsend[col] = pd.to_numeric(df_sendcutsend[col], errors='coerce')
            for col in string_columns:
                if col in df_sendcutsend.columns:
                    df_sendcutsend[col] = df_sendcutsend[col].fillna('')
            df_sendcutsend['Source'] = 'SendCutSend'
        else:
            logging.error("No data from SendCutSend to process.")
    except Exception as e:
        logging.error(f"Error parsing SendCutSend data: {e}")

    # Объединение данных и сохранение в SQLite и экспорт в Excel
    if not df_oshcut.empty or not df_sendcutsend.empty:
        combined_frames = []
        if not df_oshcut.empty:
            combined_frames.append(df_oshcut)
        if not df_sendcutsend.empty:
            combined_frames.append(df_sendcutsend)
        df_combined = pd.concat(combined_frames, ignore_index=True)

        # Сравнение с предыдущими данными
        conn = sqlite3.connect('materials.db')
        try:
            old_df = pd.read_sql_query("SELECT * FROM materials_combined", conn)
        except Exception:
            old_df = pd.DataFrame()
        if not old_df.empty:
            # Найти новые строки, которых нет в старых данных
            new_rows = df_combined.merge(old_df.drop_duplicates(), how='left', indicator=True).loc[lambda x: x['_merge'] == 'left_only']
            if not new_rows.empty:
                logging.info(f"New changes found: {len(new_rows)} new rows")
                messagebox.showinfo("New Changes", f"{len(new_rows)} new rows found in the latest parsing.")
            else:
                logging.info("No new changes found.")
        else:
            logging.info("No previous data to compare for changes.")
        conn.close()

        # Сохранение объединенных данных в SQLite
        conn = sqlite3.connect('materials.db')
        df_combined.to_sql('materials_combined', conn, if_exists='replace', index=False)
        conn.close()
        logging.info("Combined data saved to SQLite database.")

        # Экспорт объединенных данных в Excel
        df_combined.to_excel('materials_combined.xlsx', index=False, engine='openpyxl')
        logging.info("Combined data exported to Excel.")
    else:
        logging.error("No data to combine.")




if __name__ == "__main__":
    root = tk.Tk()
    gui = ScraperGUI(root)
    root.mainloop()
