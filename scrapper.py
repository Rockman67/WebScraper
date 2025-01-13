import sys
import sqlite3
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
import socket

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
    """
    Вспомогательная функция для сортировки столбцов в Treeview (используется в view_results).
    """
    l = [(tv.set(k, col), k) for k in tv.get_children('')]
    try:
        l.sort(key=lambda t: float(t[0]) if t[0] != "" else float('-inf'), reverse=reverse)
    except ValueError:
        l.sort(reverse=reverse)
    for index, (val, k) in enumerate(l):
        tv.move(k, '', index)
    tv.heading(col, command=lambda: sort_column(tv, col, not reverse))

# -------------------------------------------------------------
# Класс GUI
# -------------------------------------------------------------
class ScraperGUI:
    def __init__(self, master):
        self.master = master
        master.title("Web Scraper")
        master.geometry("800x600")
        master.resizable(False, False)
        
        # Кнопка "Start Scraping"
        self.start_button = tk.Button(master, text="Start Scraping",
                                      command=self.start_scraping,
                                      width=20, height=2, bg="green", fg="white")
        self.start_button.pack(pady=10)
        
        # Кнопка "View Results"
        self.view_button = tk.Button(master, text="View Results",
                                     command=self.view_results,
                                     width=20, height=2, bg="blue", fg="white")
        self.view_button.pack(pady=10)
        
        # Кнопка "Export to Excel"
        self.export_button = tk.Button(master, text="Export to Excel",
                                       command=self.export_to_excel,
                                       width=20, height=2, bg="orange", fg="white")
        self.export_button.pack(pady=10)

        # Поле лога (ScrolledText)
        self.log_area = scrolledtext.ScrolledText(master, wrap=tk.WORD,
                                                  width=95, height=25, state='disabled')
        self.log_area.pack(padx=10, pady=10)
        
        # Инициализация логирования
        self.setup_logging()
        
    def setup_logging(self):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        
        # Обработчик логов, отправляющий сообщения в Tkinter
        self.gui_handler = GUIHandler(self.log_area)
        self.gui_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(self.gui_handler)
        
        # Лог в файл
        file_handler = logging.FileHandler("scraper.log")
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(file_handler)
        
    def start_scraping(self):
        # Отключаем кнопку, чтобы не нажимали повторно
        self.start_button.config(state='disabled')
        self.logger.info("Starting the scraping process...")
        # Запуск парсинга в отдельном потоке (не блокируем GUI)
        threading.Thread(target=self.run_scraper, daemon=True).start()
        
    def run_scraper(self):
        try:
            main()  # вызов основной функции парсинга (см. ниже)
            self.logger.info("Scraping process completed successfully.")
            messagebox.showinfo("Success", "Scraping process completed successfully.")
        except Exception as e:
            self.logger.error(f"Scraping process terminated with an error: {e}")
            messagebox.showerror("Error",
                                 f"Scraping process terminated with an error:\n{e}")
        finally:
            self.start_button.config(state='normal')
    
    def view_results(self):
        """
        Пробуем прочитать данные из БД (таблица materials_combined) и выводим короткое окно,
        либо можно сделать полноценный Toplevel c Treeview (как в старом коде).
        """
        try:
            conn = sqlite3.connect('materials.db')
            df = pd.read_sql_query("SELECT * FROM materials_combined", conn)
            conn.close()
        except Exception as e:
            messagebox.showerror("Error", f"Error retrieving data: {e}")
            return
        
        if df.empty:
            messagebox.showwarning("No Data", "No data found. Please run scraping first.")
            return
        
        # Вариант: просто показать количество строк
        messagebox.showinfo("Data Loaded",
                            f"Data has {len(df)} rows. You can now export to Excel or do other actions.")
        # Или вы можете сделать полноценную таблицу (как в предыдущей версии),
        # но чтобы не раздувать код здесь — оставляем упрощённый вариант.

    def export_to_excel(self):
        """
        Выгружаем таблицу 'materials_combined' в Excel-файл.
        """
        try:
            conn = sqlite3.connect('materials.db')
            df = pd.read_sql_query("SELECT * FROM materials_combined", conn)
            conn.close()
            if df.empty:
                messagebox.showwarning("No Data", "No data found. Please run scraping first.")
                return
            
            df.to_excel('materials_combined.xlsx', index=False, engine='openpyxl')
            messagebox.showinfo("Export",
                                "Data exported to materials_combined.xlsx successfully.")
            self.logger.info("Data exported to Excel.")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to export data to Excel: {e}")
            self.logger.error(f"Failed to export data to Excel: {e}")

class GUIHandler(logging.Handler):
    """
    Класс-логгер, который перенаправляет логи в Text-виджет Tkinter
    """
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

# -------------------------------------------------------------
# Настройка logging (убираем StreamHandler, чтобы не дублировать в консоль)
# -------------------------------------------------------------
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

# =====================================================================
# Общая настройка Selenium (драйвера)
# =====================================================================
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

        # Добавляем таймауты
        driver.set_page_load_timeout(180)    # Можно увеличить, если нужно
        driver.set_script_timeout(180)       # Аналогично, время на выполнение JS

        logging.info("Chrome WebDriver initialized successfully.")
        return driver
    except Exception as e:
        logging.error(f"Failed to initialize Chrome WebDriver: {e}")
        raise


# =====================================================================
# Вспомогательные функции для скриншотов, каталогов и т.д.
# =====================================================================
def init_screenshot_folder():
    """
    Создаёт папку для скриншотов с меткой текущей даты/времени.
    """
    today = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    folder_name = f"screenshots_{today}"
    os.makedirs(folder_name, exist_ok=True)
    return folder_name

def save_screenshot(driver, screenshot_folder, filename):
    """
    Сохраняет скриншот экрана (driver) в папку screenshot_folder, добавляя timestamp.
    """
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


# =====================================================================
# ------------------------------- OSH Cut Parsing -----------------------
# =====================================================================
def navigate_to_sheet_page(driver, screenshot_folder):
    SHEET_URL = "https://app.oshcut.com/catalog/sheet"
    MAX_RETRIES = 2  # Можно 3, если хотите больше попыток

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logging.info(f"Loading OSH Cut page (attempt {attempt}/{MAX_RETRIES}): {SHEET_URL}")
            driver.get(SHEET_URL)

            wait = WebDriverWait(driver, 60)
            wait.until(EC.presence_of_element_located(
                (By.XPATH, "//div[contains(@class, 'filterBoxHeader') and contains(text(), 'Material')]"))
            )
            logging.info("Categories loaded. Page is ready.")
            time.sleep(2)
            return  # Если здесь успешно - выходим из функции
        except TimeoutException:
            logging.exception("Timeout while waiting for 'Material' filter to appear.")
            save_screenshot(driver, screenshot_folder, f"navigate_to_sheet_timeout_attempt{attempt}.png")
            if attempt == MAX_RETRIES:
                raise
            else:
                logging.info(f"Retrying page load (attempt {attempt+1}/{MAX_RETRIES})...")
        except Exception as e:
            logging.exception(f"Error loading '{SHEET_URL}' (attempt {attempt}/{MAX_RETRIES}): {e}")
            save_screenshot(driver, screenshot_folder, f"navigate_to_sheet_error_attempt{attempt}.png")
            if attempt == MAX_RETRIES:
                raise
            else:
                logging.info(f"Retrying page load (attempt {attempt+1}/{MAX_RETRIES})...")


def extract_categories(driver):
    """
    Собирает список категорий (по XPATH 'supertype clickable').
    """
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
    """
    Собирает список материалов (div с классом 'materialType') на текущей категории.
    Прокручивает страницу вниз, чтобы загрузить все.
    """
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
    """
    Осторожно кликает по элементу, с несколькими ретраями.
    """
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
        except (StaleElementReferenceException, ElementNotInteractableException):
            logging.exception("Click error: StaleElement or NotInteractable. Retrying.")
            retries += 1
            time.sleep(1)
    logging.error("Failed to click the element after multiple attempts.")
    raise ElementClickInterceptedException("Failed to click the element after multiple attempts.")

def safe_click_with_retries(driver, element, screenshot_folder, csvfile, max_retries=5, delay=2):
    """
    Безопасный клик с несколькими ретраями и задержкой.
    """
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
    """
    Если применены какие-то фильтры (span.pill), сбрасываем их.
    """
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
    """
    Нажимаем кнопку "Clear all filters" (span.filterReset).
    """
    try:
        wait = WebDriverWait(driver, 30)
        reset_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//span[contains(@class, 'filterReset')]")))
        safe_click_with_retries(driver, reset_button, screenshot_folder, csvfile)
        logging.info("Filters reset.")
        wait.until(EC.presence_of_element_located((By.XPATH,
            "//div[contains(@class, 'filterBoxHeader') and contains(text(), 'Material')]")))
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
    """
    Закрывает активное модальное окно (кнопки 'Close', 'Back to Catalog').
    """
    try:
        wait = WebDriverWait(driver, 10)
        close_buttons = driver.find_elements(By.XPATH,
            "//button[contains(text(), 'Close') or contains(text(), 'Закрыть') or contains(@class, 'close')]")
        back_buttons = driver.find_elements(By.XPATH,
            "//button[contains(@class, 'btnTertiary') and contains(text(), 'Back to Catalog')]")
        found_any = False

        if close_buttons:
            for close_button in close_buttons:
                try:
                    safe_click_with_retries(driver, close_button, screenshot_folder, None)
                    logging.info("Modal window closed via 'Close' button.")
                    time.sleep(2)
                    found_any = True
                except Exception as e:
                    logging.exception(f"Error closing modal via 'Close' button: {e}")

        if back_buttons:
            for back_button in back_buttons:
                try:
                    safe_click_with_retries(driver, back_button, screenshot_folder, None)
                    logging.info("Clicked 'Back to Catalog' to return to the material list.")
                    time.sleep(2)
                    found_any = True
                except Exception as e:
                    logging.exception(f"Error clicking 'Back to Catalog' button: {e}")

        if not (close_buttons or back_buttons):
            logging.warning("Modal window not found to close.")
        return found_any
    except Exception as e:
        logging.exception(f"Error closing modal windows: {e}")
        save_screenshot(driver, screenshot_folder, f"close_modal_error_{int(time.time())}.png")

def ensure_modal_closed(driver, screenshot_folder):
    """
    Убедиться, что модалка закрыта (если кнопка Close есть).
    """
    try:
        modal_close_button = driver.find_element(By.XPATH,
            "//button[contains(text(), 'Close') or contains(@class, 'close') or contains(text(), 'Back to Catalog')]")
        safe_click_with_retries(driver, modal_close_button, screenshot_folder, None)
        logging.info("Modal window closed.")
    except NoSuchElementException:
        logging.info("Modal window already closed.")

def return_to_material_list(driver, screenshot_folder):
    """
    Убедиться, что мы вернулись к списку материалов.
    """
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

def click_category(driver, cat_name, screenshot_folder, csvfile):
    """
    Находим категорию по тексту cat_name и кликаем.
    """
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
    """
    Кликает по заголовку материала, чтобы раскрыть список толщин, и ждёт "More info..." кнопки.
    """
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
    """
    Кликает "More info..." и ждёт появления модального окна.
    """
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
    """
    Извлекает подробности о материале (thickness name, K-factor и т.д.)
    """
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
        # Маленькая логика по выделению "0.06" из "0.06\" (16 Ga.)", если нужно
        if '"' in thickness_text:
            thickness_name = thickness_text.split('"')[0] + '"'
        else:
            thickness_name = thickness_text
        details['Thickness Name'] = thickness_name
        logging.info(f"Extracted thickness name: {details['Thickness Name']} for category '{category}'")

        # K-factor (как пример)
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

        # Дополнительные таблицы
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

        # Для отладки: выводим все tableTitle
        all_table_titles = driver.find_elements(By.XPATH, "//table[contains(@class, 'MaterialBendTable')]//td[@class='tableTitle']")
        for table_title in all_table_titles:
            logging.info(f"Found table title: {table_title.text.strip()} for category '{category}'")

    except TimeoutException:
        logging.exception(f"Timeout while extracting material details for category '{category}'.")
    except Exception as e:
        logging.exception(f"Error extracting material details for category '{category}': {e}")
    return details

def extract_table_data(table_element):
    """
    Извлекает данные из <table> (строки, ячейки) и возвращает в виде одного текстового поля.
    """
    data = []
    try:
        rows = table_element.find_elements(By.XPATH, ".//tr")
        for row in rows:
            cols = row.find_elements(By.XPATH, ".//td")
            cols_text = [col.text.strip() for col in cols]
            data.append(" | ".join(cols_text))
    except Exception as e:
        logging.error(f"Error extracting data from table: {e}")
    return "; ".join(data)

def go_to_next_material(driver, current_category_name, processed_materials, screenshot_folder, csvfile):
    """
    Закрываем модальное окно, возвращаемся в список материалов, сбрасываем фильтры, кликаем по категории,
    и пропускаем уже обработанные материалы. Возвращаем имя следующего материала или None.
    """
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

def parse_and_collect_all_categories(driver, screenshot_folder):
    """
    Проходит по всем категориям, собирает данные (по 'More info') и возвращает список словарей.
    """
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
            # Сразу кликаем по категории
            click_category(driver, cat_name, screenshot_folder, None)
            while True:
                # Пытаемся найти имя следующего материала
                mat_name = go_to_next_material(driver, cat_name, processed_materials, screenshot_folder, None)
                if not mat_name:
                    break

                logging.info(f"Processing material: {mat_name}")
                try:
                    # Клик по заголовку материала, ждём "More info"
                    click_material_name(driver, mat_name, screenshot_folder, None)
                    more_info_buttons = driver.find_elements(By.XPATH,
                        f"//header[contains(text(), '{mat_name}')]/ancestor::div[contains(@class, 'materialType')]//button[contains(text(), 'More info')]"
                    )
                    for btn_idx, more_info_button in enumerate(more_info_buttons):
                        try:
                            # "More info..."
                            click_more_info(driver, mat_name, btn_idx, more_info_button, screenshot_folder, None)
                            details = extract_material_details(driver, cat_name)
                            # Сохраняем в общий список
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
    """
    Полноценная функция парсинга OSH Cut. Возвращает DataFrame.
    """
    screenshot_folder = init_screenshot_folder()
    logging.info(f"Screenshot folder created: {screenshot_folder}")
    driver = None
    df_oshcut = pd.DataFrame()
    try:
        driver = setup_driver(screenshot_folder)
        navigate_to_sheet_page(driver, screenshot_folder)
        data = parse_and_collect_all_categories(driver, screenshot_folder)
        df_oshcut = pd.DataFrame(data)
        if not df_oshcut.empty:
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

# =====================================================================
# -------------------------- SendCutSend Parsing -----------------------
# =====================================================================
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
    """
    Анализ HTML (debug_page.html) на предмет типичных ошибок (Captcha, 403 и т.д.).
    """
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
    """
    Находит категории и подкатегории на сайте SendCutSend (по селектору #menu-1-711fca).
    Возвращает список (category_name, material_name, href).
    """
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
    """
    Переходит на конкретный подуровень материала (material_name) по URL и парсит таблицы.
    Возвращает список словарей.
    """
    data_list = []
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
        return data_list

    logging.info(f"Parsing material: {material_name}")
    try:
        # Ищем div.e-n-tabs-content (если материал представлен вкладками).
        tabs_content = None
        try:
            tabs_content = driver.find_element(By.CSS_SELECTOR, "div.e-n-tabs-content")
            logging.info("Found div with class 'e-n-tabs-content'.")
        except Exception as e2:
            logging.error(f"Could not find 'e-n-tabs-content' for material '{material_name}': {e2}")
            driver.save_screenshot('tabs_content_error.png')
            logging.info("Screenshot saved as tabs_content_error.png.")

        # Попробуем извлечь толщины по кнопкам tab-title
        thickness_mapping = {}
        try:
            tabs_heading = driver.find_element(By.CSS_SELECTOR, "div.e-n-tabs-heading")
            tab_buttons = tabs_heading.find_elements(By.CSS_SELECTOR, 'button.e-n-tab-title')
            for button in tab_buttons:
                thickness_text = button.find_element(By.CSS_SELECTOR, 'span.e-n-tab-title-text').text.strip()
                if '"' in thickness_text:
                    thickness = thickness_text.replace('"', '').strip()
                    aria_controls = button.get_attribute('aria-controls')
                    if aria_controls:
                        thickness_mapping[aria_controls] = thickness
            logging.info(f"Extracted thicknesses (inches only): {thickness_mapping}")
        except Exception as e3:
            logging.error(f"Error extracting thicknesses for '{material_name}': {e3}")
            driver.save_screenshot('thickness_extraction_error.png')
            logging.info("Screenshot saved as thickness_extraction_error.png.")

        # Если не нашли вкладок/толщин, fallback: попробуем просто собрать таблицы
        if not thickness_mapping or not tabs_content:
            logging.warning(f"No thickness found via tabs for '{material_name}'. Attempting table extraction.")
            try:
                if tabs_content:
                    tables = tabs_content.find_elements(By.TAG_NAME, 'table')
                else:
                    tables = driver.find_elements(By.TAG_NAME, 'table')
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
                        data_list.append(data)
                    else:
                        logging.warning(f"Thickness not in inches or missing for '{material_name}': {data['Thickness']}")
            except Exception as e4:
                logging.error(f"Error extracting thickness from tables for '{material_name}': {e4}")
            if not data_list:
                # Создаём запись «без толщины»
                data = {
                    "Category": category_name,
                    "Material Name": material_name,
                    "Thickness": "N/A",
                    "Effective bend radius @90°": "",
                    "K factor": "",
                    "Gauge": ""
                }
                data_list.append(data)
        else:
            # Имеем словарь thickness_mapping = { 'e-n-tab-content-123': '0.06', ... }
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
                    data_list.append(data)

    except Exception as e:
        logging.error(f"Error processing material '{material_name}': {e}")
        driver.save_screenshot('material_processing_error.png')
        logging.info("Screenshot saved as material_processing_error.png.")
    # Если вообще ничего не нашли, добавим хоть пустую запись
    if not data_list:
        data = {
            "Category": category_name,
            "Material Name": material_name,
            "Thickness": "N/A",
            "Effective bend radius @90°": "",
            "K factor": "",
            "Gauge": ""
        }
        data_list.append(data)

    return data_list

def scrape_materials_page():
    """
    Полноценная функция парсинга второго сайта (SendCutSend).
    Возвращает list[dict].
    """
    url = "https://sendcutsend.com/materials/"
    logging.info(f"Navigating to main page: {url}")
    screenshot_folder = init_screenshot_folder()
    driver = setup_driver(screenshot_folder)
    all_data = []

    try:
        driver.get(url)
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

    # Собираем подкатегории
    subcategory_links = get_subcategory_links(driver)
    if not subcategory_links:
        logging.error("No subcategories to process.")
        driver.quit()
        return []

    # Обходим подкатегории
    for idx, (category_name, material_name, sub_url) in enumerate(subcategory_links):
        logging.info(f"Processing subcategory [{idx+1}/{len(subcategory_links)}]: {sub_url}")
        data = scrape_subcategory(driver, category_name, material_name, sub_url)
        all_data.extend(data)
        time.sleep(1)

    driver.quit()
    return all_data


# =====================================================================
# ----------------------------- main() ---------------------------------
# =====================================================================
def main():
    """
    Основная логика: 
      1) Парсим OSH Cut (parse_oshcut),
      2) Парсим SendCutSend (scrape_materials_page),
      3) Объединяем DataFrame, 
      4) Сохраняем в materials.db (таблица materials_combined),
      5) Смотрим, есть ли изменения (сравнение с предыдущей версией)
      6) Опционально экспортируем в Excel (по кнопке).
    """
    # 1. OSH Cut
    df_oshcut = pd.DataFrame()
    try:
        df_oshcut = parse_oshcut()
        if df_oshcut.empty:
            logging.warning("No data parsed from OSH Cut.")
    except Exception as e:
        logging.error(f"Error parsing OSH Cut data: {e}")

    # 2. SendCutSend
    df_sendcutsend = pd.DataFrame()
    try:
        sendcutsend_data = scrape_materials_page()
        if sendcutsend_data:
            df_sendcutsend = pd.DataFrame(sendcutsend_data)
            # Очистка данных от кавычек, пробелов, приведение к числовым типам
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

    # 3. Объединяем и сохраняем
    if not df_oshcut.empty or not df_sendcutsend.empty:
        combined_frames = []
        if not df_oshcut.empty:
            combined_frames.append(df_oshcut)
        if not df_sendcutsend.empty:
            combined_frames.append(df_sendcutsend)

        df_combined = pd.concat(combined_frames, ignore_index=True)

        # 4. Сравнение с предыдущими данными
        conn = sqlite3.connect('materials.db')
        try:
            old_df = pd.read_sql_query("SELECT * FROM materials_combined", conn)
        except Exception:
            old_df = pd.DataFrame()

        if not old_df.empty:
            new_rows = df_combined.merge(
                old_df.drop_duplicates(), how='left', indicator=True
            ).loc[lambda x: x['_merge'] == 'left_only']
            if not new_rows.empty:
                logging.info(f"New changes found: {len(new_rows)} new rows")
                messagebox.showinfo("New Changes",
                                    f"{len(new_rows)} new rows found in the latest parsing.")
            else:
                logging.info("No new changes found.")
        else:
            logging.info("No previous data to compare for changes.")

        conn.close()

        # 5. Сохранение объединённых данных в SQLite
        conn = sqlite3.connect('materials.db')
        df_combined.to_sql('materials_combined', conn, if_exists='replace', index=False)
        conn.close()
        logging.info("Combined data saved to SQLite database.")

        # 6. (По желанию) сразу экспорт в Excel — но у нас есть отдельная кнопка
        # df_combined.to_excel('materials_combined.xlsx', index=False, engine='openpyxl')
        # logging.info("Combined data exported to Excel.")
    else:
        logging.error("No data to combine.")

# =====================================================================
# ----------------------------- Точка входа ----------------------------
# =====================================================================
if __name__ == "__main__":
    check_single_instance()

    root = tk.Tk()
    root.withdraw()
    messagebox.showinfo("Launching", "Program is starting...")
    logging.info("Program has been launched.")
    root.deiconify()

    gui = ScraperGUI(root)
    root.mainloop()
