import os , shutil
import socket
import time 
from zipfile import ZipFile
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support import ui

TARGET_FILES = ['A_lvr_land_A.csv','B_lvr_land_A.csv','E_lvr_land_A.csv','F_lvr_land_A.csv','H_lvr_land_A.csv']
class Crawler():
    """
        透過selenium進行網路爬蟲，將下載檔案解壓並存至data資料夾供後續使用
    """
    
    
    def __init__(self):
        self.current_path = os.getcwd() + os.sep
        self.web_path = 'https://plvr.land.moi.gov.tw/DownloadOpenData'
        self.tmp_download = '/tmp/'

        self.target_download = self.tmp_download+ 'download' 
        if not os.path.exists(self.target_download):
            os.mkdir(self.target_download)

        self.data_file_path = self.current_path + 'data' 
        if not os.path.exists(self.data_file_path):
            os.mkdir(self.data_file_path)
        else:
            # 檢查file是否已存在
            for data in os.listdir(self.data_file_path):
                if data in TARGET_FILES:
                    print(data + ' already exist ... ')
                    TARGET_FILES.remove(data)
            if len(TARGET_FILES) == 0:
                exit('all data exists , exiting...')


    def get_crawler_service(self,):
        """透過chrome driver Remote連線至chrome driver container 後連線至目標網站
        Return:
            chrome driver連線服務物件
        """
        prefs = {
            'profile.default_content_settings_popups' : 0 ,
            'download.default_directory' : self.target_download
        }
        options = webdriver.ChromeOptions()
        options.add_experimental_option('prefs',prefs)

        ip = socket.gethostbyname('selenium-hub')
        ip_addr = 'http://' + ip + ':4444/wd/hub'
        web= webdriver.Remote(command_executor=ip_addr,desired_capabilities=DesiredCapabilities.CHROME,options=options)
        web.get(self.web_path)
        return web

    def crawler_execute(self,web):
        """操作selenium進行爬蟲
        Parameters:
            web - chrome driver連線服務物件
        """
        """
            1. 點選 非本期下載
            2. 選取 108年第2季
            3. 選取 CSV 格式
            4. 點選目標城市
            5. 點選下載按鈕
        """
        time.sleep(1)
        print('crawler start...')
        
        # 透過 element id 搜尋 非本期下載 並點選
        web.find_element(By.ID,'ui-id-2').click()

        # 等候取得element 
        wait = ui.WebDriverWait(web,3)
        wait.until(lambda web:web.find_element(By.ID,"historySeason_id"))

        # 透過 element id 搜尋 108年第2季 並選取
        Select(web.find_element(By.ID,"historySeason_id")).select_by_visible_text("108年第2季")
        web.find_element(By.ID,"fileFormatId").click()

        # 透過 element id 搜尋 CSV 格式 並選取
        Select(web.find_element(By.ID,"fileFormatId")).select_by_visible_text("CSV 格式")
        web.find_element(By.ID,"downloadTypeId2").click()

        # 透過 element xpath 搜尋 目標城市 並點選
        web.find_element(By.XPATH,"//input[@value='A_lvr_land_A']").click()
        web.find_element(By.XPATH,"//input[@value='B_lvr_land_A']").click()
        web.find_element(By.XPATH,"//input[@value='E_lvr_land_A']").click()
        web.find_element(By.XPATH,"//input[@value='F_lvr_land_A']").click()
        web.find_element(By.XPATH,"//input[@value='H_lvr_land_A']").click()

        # 透過 element id 搜尋 下載按鈕 並點選
        web.find_element(By.ID,"downloadBtnId").click()

    def wait_for_download(self,web):
        """監控檔案下載進度，完畢後斷開web driver服務
        Parameters:
            web - chrome driver連線服務物件
        """
        while True:
            if 'download.zip' in os.listdir(self.target_download):
                print ('download completed')
                web.quit()
                break
            else:
                print ('downloading...')
                time.sleep(1)

    def zip_file_get_target_file(self,):
        """解壓縮下載完畢之檔案，保留所需csv檔移至data資料夾，清除其餘非必要檔案
        """
        # 解壓縮檔案
        with ZipFile(self.target_download+ os.sep+'download.zip','r') as zip:
            zip.extractall(self.target_download)

        for data in os.listdir(self.target_download):
            if data in TARGET_FILES :
                shutil.move(self.target_download+os.sep+data,self.data_file_path+os.sep)

        shutil.rmtree(self.target_download,ignore_errors=True)

    def run(self):
        """ 爬蟲主要執行程式
        """
        web = self.get_crawler_service()

        self.crawler_execute(web)

        self.wait_for_download(web)

        self.zip_file_get_target_file()
        
if __name__ == '__main__':
    crawler = Crawler()
    crawler.run()