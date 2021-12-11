# spark_proj
## 透過selenium進行爬蟲取得內政部資料 以spark進行資料處理寫入json檔&amp;建置API提供資料集查詢

基於gettyimages/spark + selenium/hub + selenium/node-chrome 建置

環境建置流程:

./build_image.sh => 建置主要image name=myspark

./start_service.sh => 透過docker-compose運行服務

※ 服務運行時啟用Django內建server 可於網頁127.0.0.1:8000確認(若8000 port正在使用 可至enrtypoint.sh內修改python manage.py runserver < port > 設定port號)


---

## 爬蟲程式

目的 : 抓取 內政部不動產時價登錄網 中位於 臺北市 新北市 桃園市 臺中市 高雄市  的  不動產買賣  csv格式資料 ， 資料內容為108年第2季 

環境 : 
  * 透過 container selenium/hub & selenium/node-chrome-debug 實行 chrome driver 使用
  * 基於node-chrome-debug ,可以透過VNC Viewer 至 5900 port 查看目前web crwaler執行畫面
  
程式流程: 

使用selenium透過chrome_driver取得連線服務並連線至目標網站

按照爬蟲規則進行網頁自動化處理

下載完成進行解壓縮並留下data資料夾內數據集(該數據集為spark過濾&寫入json檔 以及 WEB API使用之數據)

程式執行 :  (in containrt spark_env) /crawler/run_main.py

---

## spark 過濾並寫入json檔

目的 : 透過 spark 合併 臺北市 新北市 桃園市 臺中市 高雄市 所有數據集，以過濾條件篩選資料 ， 並將結果隨機寫入json檔

流程:

1. 程式透過pyspark.SparkSession連線至spark，並讀取各個資料集取得所有數據
2. 透過withColumn新增一個欄位city儲存對應城市名稱
3. 透過withColumn新增一個欄位來儲存對應總樓層數之數字版本
4. 按照篩選條件過濾出符合 : 總樓層數大於等於13層 、 主要用途為<住家用> 建物類型為<住宅大樓>(模糊查詢) ，並以<交易年月日>降冪排列
5. 將過濾完的數據逐筆轉成json型態並且按照城市整理出字典檔
6. 透過隨機數值randint隨機將不同城市資料寫入兩檔案內。

程式執行 :  (in containrt spark_env) /spark/run_main.py

### *spark過濾&寫入json檔 以及 WEB API 使用之資料集為爬蟲程式結果 , 使用前須先執行爬蟲程式*

---

## WEB API

Web API環境:  使用Django建置web server

使用需先啟用Django內建server:

python manage.py runserver

WEB API使用方法:

使用者將以網頁 GET 方式進行查詢，可查詢參數有:
< city > , < floor > , < building_type > 對應之資訊為 <鄉鎮市區>,<總樓層數>,<建物型態>

參數型態為 : < string > , < int > , < string >

URL格式 : <ip:port/get/getData?param=?&param=?>   *目前設計上不允許不帶參數使用。*

範例 http://127.0.0.1:8000/data/getData?city=台北市&floor=13&building_type=其他

回傳為json格式 (response = [ {…} , {…} , {…} ])

*預設python manager runserver將會在本地端建立一server , port為8000 , 也可在runserver後加上指定port使用特定port號*

### *spark過濾&寫入json檔 以及 WEB API 使用之資料集為爬蟲程式結果 , 使用前須先執行爬蟲程式*
