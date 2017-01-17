import os
from selenium import webdriver
from selenium.webdriver.common.keys import Keys

chromedriver = "C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe"
os.environ["webdriver.chrome.driver"] = chromedriver

driver = webdriver.Firefox()
driver.get("https://passport.lianjia.com/cas/login?service=http%3A%2F%2Fbj.lianjia.com%2F");
driver("http://bj.lianjia.com/chengjiao")
# elem = driver.find_element_by_name("q")
# elem.send_keys("selenium")
# elem.send_keys(Keys.RETURN)
#driver.close()
#driver.quit()