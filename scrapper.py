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
import socket

# -------------------------------
# Глобальная переменная для хранения ссылки на файл-блокировку (Windows)
# -------------------------------
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

# ------------------------------------------------------------------------------
# ВАЖНО! УДАЛЁН БЛОК "Automatic Dependency Installation" (pip install ...).
# Так вы избегаете повторных запусков / проблем внутри exe.
# ------------------------------------------------------------------------------

def sort_column(tv, col, reverse):
    l = [(tv.set(k, col), k) for k in tv.get_children('')]
    try:
        l.sort(key=lambda t: float(t[0]) if t[0] != "" else float('-inf'), reverse=reverse)
    except ValueError:
        l.sort(reverse=reverse)
    for index, (val, k) in enumerate(l):
        tv.move(k, '', index)
    tv.heading(col, command=lambda: sort_column(tv, col, not reverse))


class ScraperGUI:
    def __init__(self, master):
        self.master = master
        master.title("Web Scraper")
        master.geometry("800x600")
        master.resizable(False, False)
        
        # Create Start Button
        self.start_button = tk.Button(master, text="Start Scraping",
                                      command=self.start_scraping,
                                      width=20, height=2, bg="green", fg="white")
        self.start_button.pack(pady=10)
        
        # Create View Results Button
        self.view_button = tk.Button(master, text="View Results",
                                     command=self.view_results,
                                     width=20, height=2, bg="blue", fg="white")
        self.view_button.pack(pady=10)
        
        # Create Export to Excel Button
        self.export_button = tk.Button(master, text="Export to Excel",
                                       command=self.export_to_excel,
                                       width=20, height=2, bg="orange", fg="white")
        self.export_button.pack(pady=10)

        
        # Create Scrolled Text for Logs
        self.log_area = scrolledtext.ScrolledText(master, wrap=tk.WORD,
                                                  width=95, height=25, state='disabled')
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
        # Отключаем кнопку, чтобы пользователь не нажал снова
        self.start_button.config(state='disabled')
        self.logger.info("Starting the scraping process...")
        # Запускаем парсинг в отдельном потоке, чтобы не блокировать GUI
        threading.Thread(target=self.run_scraper, daemon=True).start()
        
    def run_scraper(self):
        try:
            main()
            self.logger.info("Scraping process completed successfully.")
            messagebox.showinfo("Success", "Scraping process completed successfully.")
        except Exception as e:
            self.logger.error(f"Scraping process terminated with an error: {e}")
            messagebox.showerror("Error",
                                 f"Scraping process terminated with an error:\n{e}")
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

        # Реализация быстрого поиска в реальном времени по событию KeyRelease
        search_entry.bind("<KeyRelease>", lambda event: on_search())

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
            tree.heading(column, text=column,
                        command=lambda _col=column: sort_column(tree, _col, False))

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
                filtered_df = df[df.apply(
                    lambda row: any(pattern.search(str(val)) for val in row.values),
                    axis=1
                )]
            else:
                filtered_df = df

            for _, row in filtered_df.iterrows():
                tree.insert("", "end", values=list(row))

        def update_treeview_data(dataframe):
            # Полностью очистить Treeview
            for item in tree.get_children():
                tree.delete(item)
            # Добавить данные
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
            messagebox.showinfo("Export",
                                "Data exported to materials_combined.xlsx successfully.")
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
# Logging Configuration
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

def navigate_to_sheet_page(driver, screenshot_folder):
    try:
        SHEET_URL = "https://app.oshcut.com/catalog/sheet"
        driver.get(SHEET_URL)
        logging.info(f"Loaded page: {SHEET_URL}")
        wait = WebDriverWait(driver, 60)
        wait.until(EC.presence_of_element_located((By.XPATH,
            "//div[contains(@class, 'filterBoxHeader') and contains(text(), 'Material')]")))
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
        category_elements = wait.until(
            EC.presence_of_all_elements_located((By.XPATH, categories_xpath)))
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
            # Удаляем лишние кавычки, чистим строки, приводим к числовым типам и т.д.
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

    # Объединение данных и сохранение в SQLite + экспорт в Excel
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

        # Сохранение объединённых данных в SQLite
        conn = sqlite3.connect('materials.db')
        df_combined.to_sql('materials_combined', conn, if_exists='replace', index=False)
        conn.close()
        logging.info("Combined data saved to SQLite database.")

        # Экспорт объединенных данных в Excel
        df_combined.to_excel('materials_combined.xlsx', index=False, engine='openpyxl')
        logging.info("Combined data exported to Excel.")
    else:
        logging.error("No data to combine.")


# -------------------------------
# Вызов check_single_instance и запуск GUI
# -------------------------------
if __name__ == "__main__":
    # 1) Сначала проверяем единственный экземпляр:
    check_single_instance()

    # 2) Инициализируем Tkinter
    root = tk.Tk()
    # 3) Временно скрываем основное окно, чтобы показать всплывающее
    root.withdraw()

    # 4) Показываем всплывающее окно
    messagebox.showinfo("Launching", "Program is starting...")
    logging.info("Program has been launched.")

    # 5) Отображаем главное окно
    root.deiconify()

    # 6) Запускаем GUI
    gui = ScraperGUI(root)
    root.mainloop()
