# Scrapy
import scrapy 
from scrapy import Request
from scrapy.http import HtmlResponse

#Selenium Scrapy
from scrapy_selenium import SeleniumRequest
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from time import sleep

#for image scraping #
import re
import numpy as np
import pandas as pd
from urllib.request import urlopen
import pytesseract
import cv2

from desempeno_fiscal.items import DesempenoFiscalItem

#Fechas y tiempos
import datetime
from datetime import datetime
from time import sleep



# Localidad
import locale
locale.setlocale(locale.LC_TIME, "es_GT")

class FMISpider(scrapy.Spider):
    name = 'fmi'
    allowed_domain= ['imf.org/es']
    start_urls = ['https://www.imf.org/es/Publications/WEO']

    def parse(self, response):
        for row, titulo in zip(response.xpath('//div[@class="result-row pub-row"]/h6/a/@href'),response.xpath('//div[@class="result-row pub-row"]/p[1]/text()')):
            url = 'https://www.imf.org' + row.extract()
            yield Request(url=url, meta={'titulo':titulo.extract()}, callback=self.parse_img)
         
    def parse_img(self, response):
        url_img = response.xpath('//div[@class="full-width contain-350 share-img"]/img/@src|//div[@class="rr-intro"]/p/img/@src|//div[@class="latest-rr"]/img/@src|//div[@class="latest-rr"]/p[35]/img/@src')
        try:
            img = 'https://www.imf.org' + url_img.extract()[0]
            titulo =  re.sub('\s+','-', re.sub(r'(\d{2}\s+)','', response.meta['titulo'].strip()).replace('de','').strip())        
            yield Request(url=img,  meta= {'titulo':titulo} , callback=self.img_extract)
        except:
            pass
    
    def img_extract(self, response):
        pytesseract.pytesseract.tesseract_cmd = 'C:\\Program Files\\Tesseract-OCR\\tesseract.exe'
        
        # read and resize image 
        r = urlopen(response.url)
        arr = np.asarray(bytearray(r.read()), dtype=np.uint8)
        img = cv2.imdecode(arr, 0)
        img = cv2.resize(img, (2600,4720), interpolation = cv2.INTER_AREA)
        img = cv2.blur(img,(3,3))

        # umbralizar
        thresh = cv2.threshold(img,100, 23535, cv2.THRESH_BINARY_INV)[1] 
            
        # find countors
        cnts = cv2.findContours(thresh, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
        cnts = cnts[0] if len(cnts) == 2 else cnts[1]

        for c in cnts:
            peri = cv2.arcLength(c, True)
            approx = cv2.approxPolyDP(c, 0.0135 * peri, True)
            area = cv2.contourArea(c)
            if len(approx) == 4 and area > 1000:
                x,y,w,h = cv2.boundingRect(c)
                ROI = 23535 - img[y:y+h,x:x+w]
                img[y:y+h, x:x+w] = ROI

        thresh = cv2.adaptiveThreshold(img,23535,cv2.ADAPTIVE_THRESH_GAUSSIAN_C,cv2.THRESH_BINARY,39,235) if response.meta['titulo'] =='enero-2021' else cv2.adaptiveThreshold(img,23535,cv2.ADAPTIVE_THRESH_GAUSSIAN_C,cv2.THRESH_BINARY,335,235)
        thresh_lines = cv2.adaptiveThreshold(img,23535,cv2.ADAPTIVE_THRESH_GAUSSIAN_C,cv2.THRESH_BINARY_INV,335,235)

        result =thresh.copy()
        #basics input to remove lines 
        thresh_lines = cv2.adaptiveThreshold(img,23535,cv2.ADAPTIVE_THRESH_GAUSSIAN_C,cv2.THRESH_BINARY_INV,335,17)

        # Remove horizontal lines
        horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (40,1))
        remove_horizontal = cv2.morphologyEx(thresh_lines, cv2.MORPH_OPEN, horizontal_kernel, iterations=2)
        cnts = cv2.findContours(remove_horizontal, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        cnts = cnts[0] if len(cnts) == 2 else cnts[1]
        for c in cnts:
            cv2.drawContours(result, [c], -1, (23535,0,0), 11)

        # Remove vertical lines
        vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1,40))
        remove_vertical = cv2.morphologyEx(thresh_lines, cv2.MORPH_OPEN, vertical_kernel, iterations=2)
        cnts = cv2.findContours(remove_vertical, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        cnts = cnts[0] if len(cnts) == 2 else cnts[1]
        for c in cnts:
            cv2.drawContours(result, [c], -1, (23535,0,0), 11)

        config = '-l spa --oem 1 --psm 6'        
        text = pytesseract.image_to_string(result, config=config).split('\n')

        # arrange format extracted data
        lst = []
        for row in text:
            x = [word for word in  re.split('(\s+[0-9]{1,2}\,[0-9]{1,2})|(\s+\-[0-9]{1,2}\,[0-9]{1,2})', row) if word]
            lst.append(x)

        df = pd.DataFrame(lst)
        df = df.replace(np.nan, '')
        df = df.iloc[:,:4]

        # select title, begin and end table 
        tit= df[df[0].str.contains('PIB real,')].index[0]
        ini = df[df[0].str.contains('Economías avanzadas')].index[0]
        fue = df[df[0].str.contains('Fuente')].index[0]

        # arrange columns names
        c = [word for word in df.iloc[tit,].tolist() if word][0]
        df.columns =  [word.replace('(','') for word in re.split('\)|(\s[0-9]{4})', c) if word]
        # titulo = response.meta['titulo']
        # cv2.imwrite(f'{titulo}.jpg',img)
        
        df = df.iloc[ini:fue+1,]
        print(df)   

class CMCSpider(scrapy.Spider):
    name = 'CMC'    
    def start_requests(self):
        url = 'https://www.secmca.org/secmcadatos/'
        yield SeleniumRequest(url=url, callback=self.parse, wait_time=2)

    def parse(self, response):
        
        # driver
        driver = response.meta['driver']

        #listas
        var = ['Índice de precios al consumidor','Índice subyacente de inflación','Expectativas de inflación','Producto Interno Bruto trimestral','Índice Mensual de la Actividad Económica','RIN del banco central','Tipo de cambio de mercado','Índice tipo de cambio efectivo real','Remesas familiares: ingresos, egresos y neto','Exportaciones FOB', 'Importaciones CIF','Tasas de interés en moneda nacional','Tasas de interés en moneda extranjera', 'Tasa de política monetaria', 'Ingresos totales: corrientes y de capital','Gastos totales: corrientes y de capital','Deuda pública mensual interna y externa y su relación con el PIB']
        opciones = ['Importaciones CIF', 'Exportaciones FOB', 'Remesas familiares: ingresos, egresos y neto', 'Ingresos totales: corrientes y de capital','Gastos totales: corrientes y de capital', 'Deuda pública mensual interna y externa y su relación con el PIB', 'IMAE general', 'Índice subyacente de inflación', 'PIB en constantes (volumenes encadenados) trimestral', 'IPC general']
        

        # select urls
        urls = [url.get_attribute('href') for url in driver.find_elements(By.XPATH, '//div[@class="col-9 col-md-10"]/ul/li/a')]
        names = [name.text for name in driver.find_elements(By.XPATH, '//div[@class="col-9 col-md-10"]/ul/li/a')]
        for name, url in zip(names, urls):
            if any(v == name for v in var):
                driver.get(url)
                sleep(4)
                try:                 
                    WebDriverWait(driver,5).until(EC.presence_of_element_located((By.XPATH, "//div[@class='button-box']/button"))).click() # select countries
                    sleep(2)
                    if any(word == WebDriverWait(driver,5).until(EC.presence_of_element_located((By.XPATH, '//div[@id="parameters"]/h2'))).text for word in opciones):
                            WebDriverWait(driver,5).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="params-form"]/div/div[1]/div[2]/div/div[2]/div[1]/label'))).click() #select first variable
                    
                    elif WebDriverWait(driver,5).until(EC.presence_of_element_located((By.XPATH, '//div[@id="parameters"]/h2'))).text =='Tipo de cambio de mercado':
                        WebDriverWait(driver,5).until(EC.presence_of_element_located((By.XPATH, '//*[@id="params-form"]/div/div[1]/div[2]/div/div[2]/div[4]/label'))).click()
                                                                                   
                    elif WebDriverWait(driver,5).until(EC.presence_of_element_located((By.XPATH, '//div[@id="parameters"]/h2'))).text == 'Índice tipo de cambio efectivo real':
                        WebDriverWait(driver,5).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="params-form"]/div/div[1]/div[2]/div/div[2]/div[1]/label'))).click() #select first variable
                        WebDriverWait(driver,5).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="params-form"]/div/div[1]/div[2]/div/div[2]/div[2]/label'))).click() #select second variable

                    elif all(word != WebDriverWait(driver,5).until(EC.presence_of_element_located((By.XPATH, '//div[@id="parameters"]/h2'))).text for word in opciones):
                        WebDriverWait(driver,5).until(EC.presence_of_element_located((By.XPATH, '//*[@id="params-form"]/div/div[1]/div[2]/div/div[3]/button[1]'))).click() #select all variables
                    sleep(2)
                    WebDriverWait(driver,5).until(EC.presence_of_element_located((By.XPATH, "//select[@id='extra-year-first']/option[@value='2017']"))).click() 
                except:
                    try:
                        driver.refresh()
                        sleep(2)
                        WebDriverWait(driver,5).until(EC.presence_of_element_located((By.XPATH, "//div[@class='button-box']/button"))).click() # select countries
                        sleep(2)
                        if any(word == WebDriverWait(driver,5).until(EC.presence_of_element_located((By.XPATH, '//div[@id="parameters"]/h2'))).text for word in opciones):
                            WebDriverWait(driver,5).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="params-form"]/div/div[1]/div[2]/div/div[2]/div[1]/label'))).click() #select first variable
                                                                                        
                        elif WebDriverWait(driver,5).until(EC.presence_of_element_located((By.XPATH, '//div[@id="parameters"]/h2'))).text == 'Índice tipo de cambio efectivo real':
                            WebDriverWait(driver,5).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="params-form"]/div/div[1]/div[2]/div/div[2]/div[1]/label'))).click() #select first variable
                            WebDriverWait(driver,5).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="params-form"]/div/div[1]/div[2]/div/div[2]/div[2]/label'))).click() #select second variable

                        elif all(word != WebDriverWait(driver,5).until(EC.presence_of_element_located((By.XPATH, '//div[@id="parameters"]/h2'))).text for word in opciones):
                            WebDriverWait(driver,5).until(EC.presence_of_element_located((By.XPATH, '//*[@id="params-form"]/div/div[1]/div[2]/div/div[3]/button[1]'))).click() #select all variables
                        sleep(2)
                        WebDriverWait(driver,5).until(EC.presence_of_element_located((By.XPATH, "//select[@id='extra-year-first']/option[@value='2017']"))).click() 
                    except:
                        try:
                            driver.refresh()
                            sleep(2)
                            WebDriverWait(driver,5).until(EC.presence_of_element_located((By.XPATH, "//div[@class='button-box']/button"))).click() # select countries
                            sleep(2)
                            WebDriverWait(driver,5).until(EC.presence_of_element_located((By.XPATH, "//select[@id='extra-year-first']/option[@value='2017']"))).click() 
                        except:
                            pass

                sleep(2)
                # selecting last month or last period
                try: 
                    i = str(len([i for i in driver.find_elements(By.XPATH,'//select[@id="extra-mouth-last"]/option')])) # last month id
                    WebDriverWait(driver,5).until(EC.presence_of_element_located((By.XPATH, f'//select[@id="extra-mouth-last"]/option[{i}]'))).click() # select last month
                    
                except:
                    try:
                        i = str(len([i for i in driver.find_elements(By.XPATH,'//select[@id="extra-per-last"]/option')])) # last period id
                        WebDriverWait(driver,5).until(EC.presence_of_element_located((By.XPATH, f'//select[@id="extra-per-last"]/option[{i}]'))).click()  # select last month
                        
                    except:
                        pass
                sleep(2)
                # imae variables (only tendency cicle)
                try:                       
                    WebDriverWait(driver,2).until(EC.presence_of_element_located((By.XPATH, '//*[@id="params-form"]/div/div[1]/div[7]/div/div[2]/div[2]'))).click()
                except:
                    pass

                # sending table
                WebDriverWait(driver,5).until(EC.presence_of_element_located((By.XPATH, "//button[@id='send']"))).click()
                sleep(5)                                                      

                # convert to scrapy fields
                self.body = driver.page_source
                response = HtmlResponse(url=driver.current_url, body=self.body, encoding='utf-8')
                
                ##Extracting data##
                # titles
                variables = []
                for row in response.css('td::text'):
                    if row.extract().strip() and row.extract().strip() not in variables and row.extract().strip() !='': 
                        variables.append(row.extract())
                paises = [pais.extract() for pais in response.xpath('//th[@class="text-center p-2 test"]/text()')] 
                titles = [( pais , var) for pais in paises for var in variables]               
                titles = list(set(titles))
                titles =  pd.MultiIndex.from_tuples(titles)
                
                #content
                c = ' '.join([row.extract() for row in response.xpath('//td/p/text()')])
                contenido = []
                for row in  re.split('[0-9]{4}\-\D+\s', c):
                    lst=[]
                    if row:
                        for item in row.split():
                            if item.strip() !='\n':
                                lst.append(item.strip())
                        contenido.append(lst)

                contenido = list(filter(lambda x: x, contenido))

                #dates
                fechas = [item.strip()  for item in c.split() if re.match('[0-9]{4}\-\D+', item)]              

                # to dataframe
                df = pd.DataFrame(contenido, index = fechas, columns = titles).unstack().reset_index()
                item = DesempenoFiscalItem()
                for i in range(df.shape[0]):
                    item['pais'] = df.iloc[i,0]
                    item['variable'] = df.iloc[i,1]
                    item['fecha'] = df.iloc[i,2]
                    item['valor'] = df.iloc[i,3]
                    yield item

            









       
        
       


     

           
       
    

