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

# Глобальная переменная для хранения ссылки на файл-блокировку (на Windows)
lockfile = None

def sort_column(tv, col, reverse):
    l = [(tv.set(k, col), k) for k in tv.get_children('')]
    try:
        l.sort(key=lambda t: float(t[0]) if t[0] != "" else float('-inf'), reverse=reverse)
    except ValueError:
        l.sort(reverse=reverse)
    for index, (val, k) in enumerate(l):
        tv.move(k, '', index)
    tv.heading(col, command=lambda: sort_column(tv, col, not reverse))

def check_single_instance():
    """
    Проверяет единственный экземпляр приложения:
    - На Windows — через файл-блокировку (msvcrt).
    - На других ОС — через локальный сокет.
    Если программа уже запущена, показывает предупреждение и завершает работу.
    """
    logging.info("Entering check_single_instance() ...")
    global lockfile
    if os.name == 'nt':  # Если ОС Windows
        import msvcrt
        try:
            lockfile = open("app.lock", "w")
            msvcrt.locking(lockfile.fileno(), msvcrt.LK_NBLCK, 1)
            logging.info("Successfully locked app.lock on Windows.")
        except IOError:
            logging.warning("Program is already running (lockfile in use).")
            messagebox.showwarning("Warning", "Program is already running.")
            sys.exit(0)
    else:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.bind(("127.0.0.1", 65432))
            logging.info("Successfully bound to port 65432 on Unix-like system.")
        except socket.error:
            logging.warning("Program is already running (socket in use).")
            messagebox.showwarning("Warning", "Program is already running.")
            sys.exit(0)
        # Чтобы не освобождать сокет, return s
        return s

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

class ScraperGUI:
    def __init__(self, master):
        self.master = master
        master.title("Web Scraper")
        master.geometry("800x600")
        master.resizable(False, False)
        
        # Кнопка Start Scraping
        self.start_button = tk.Button(master, text="Start Scraping",
                                      command=self.start_scraping,
                                      width=20, height=2, bg="green", fg="white")
        self.start_button.pack(pady=10)
        
        # Кнопка View Results
        self.view_button = tk.Button(master, text="View Results",
                                     command=self.view_results,
                                     width=20, height=2, bg="blue", fg="white")
        self.view_button.pack(pady=10)
        
        # Кнопка Export to Excel
        self.export_button = tk.Button(master, text="Export to Excel",
                                       command=self.export_to_excel,
                                       width=20, height=2, bg="orange", fg="white")
        self.export_button.pack(pady=10)

        # Поле лога
        self.log_area = scrolledtext.ScrolledText(master, wrap=tk.WORD,
                                                  width=95, height=25, state='disabled')
        self.log_area.pack(padx=10, pady=10)
        
        self.setup_logging()
        
    def setup_logging(self):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        
        self.gui_handler = GUIHandler(self.log_area)
        self.gui_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(self.gui_handler)
        
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

        search_frame = tk.Frame(top)
        search_frame.pack(fill='x', padx=5, pady=5)

        search_label = tk.Label(search_frame, text="Search:")
        search_label.pack(side='left')

        search_entry = tk.Entry(search_frame)
        search_entry.pack(side='left', fill='x', expand=True, padx=5)
        search_entry.bind("<KeyRelease>", lambda event: on_search())

        col_label = tk.Label(search_frame, text=" in ")
        col_label.pack(side='left')

        col_combo = ttk.Combobox(search_frame, values=list(df.columns))
        col_combo.pack(side='left')

        val_label = tk.Label(search_frame, text=" equals ")
        val_label.pack(side='left')

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
            import re
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
            for item in tree.get_children():
                tree.delete(item)
            for _, row in dataframe.iterrows():
                tree.insert("", "end", values=list(row))

        update_treeview()

        def on_search():
            text = search_entry.get()
            col = col_combo.get() if col_combo.get() in df.columns else None
            val = val_combo.get() if val_combo.get() else None
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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("scraper.log"),]
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
        safe_filename = filename.replace("/", "_").replace("\\", "_")\
                                .replace(":", "_").replace("*", "_")\
                                .replace("?", "_").replace('"', '_')\
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
        wait.until(EC.presence_of_element_located(
            (By.XPATH, "//div[contains(@class, 'filterBoxHeader') and contains(text(), 'Material')]")))
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

# Остальные функции (safe_click, reset_filters, close_modal, etc.) здесь опущены ради компактности —
# но в вашем коде они такие же, как и были.


def parse_oshcut():
    screenshot_folder = init_screenshot_folder()
    logging.info(f"Screenshot folder created: {screenshot_folder}")
    driver = None
    df_oshcut = pd.DataFrame()
    try:
        driver = setup_driver(screenshot_folder)
        navigate_to_sheet_page(driver, screenshot_folder)
        # Допустим, у вас есть parse_and_collect_all_categories(driver, screenshot_folder)
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

def scrape_materials_page():
    # Аналогичный код парсинга SendCutSend
    pass

def main():
    logging.info("Starting main() parsing logic...")
    # Примерно так же, как в вашем коде
    df_oshcut = pd.DataFrame()
    try:
        df_oshcut = parse_oshcut()
        if df_oshcut.empty:
            logging.warning("No data parsed from OSH Cut.")
    except Exception as e:
        logging.error(f"Error parsing OSH Cut data: {e}")

    df_sendcutsend = pd.DataFrame()
    try:
        # Здесь ваш код scrape_materials_page()
        sendcutsend_data = []
        if sendcutsend_data:
            df_sendcutsend = pd.DataFrame(sendcutsend_data)
            # Очистка и пр.
        else:
            logging.error("No data from SendCutSend to process.")
    except Exception as e:
        logging.error(f"Error parsing SendCutSend data: {e}")

    # Объединение, сравнение и запись в БД
    if not df_oshcut.empty or not df_sendcutsend.empty:
        logging.info("Combining data frames from OSH Cut and SendCutSend...")
        combined_frames = []
        if not df_oshcut.empty:
            combined_frames.append(df_oshcut)
        if not df_sendcutsend.empty:
            combined_frames.append(df_sendcutsend)
        df_combined = pd.concat(combined_frames, ignore_index=True)

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
                messagebox.showinfo("New Changes", f"{len(new_rows)} new rows found in the latest parsing.")
            else:
                logging.info("No new changes found.")
        else:
            logging.info("No previous data to compare for changes.")
        conn.close()

        # Записываем объединённый df
        conn = sqlite3.connect('materials.db')
        df_combined.to_sql('materials_combined', conn, if_exists='replace', index=False)
        conn.close()
        logging.info("Combined data saved to SQLite database.")

        df_combined.to_excel('materials_combined.xlsx', index=False, engine='openpyxl')
        logging.info("Combined data exported to Excel.")
    else:
        logging.error("No data to combine.")

if __name__ == "__main__":
    logging.info("=== Program entry point ===")
    check_single_instance()  # Проверка на единственный экземпляр
    logging.info("Single-instance check complete.")

    root = tk.Tk()
    root.withdraw()

    messagebox.showinfo("Launching", "Program is starting...")
    logging.info("Program has been launched.")

    root.deiconify()
    gui = ScraperGUI(root)
    root.mainloop()
