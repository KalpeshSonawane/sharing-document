import gc
from datetime import date
from selenium import webdriver
import concurrent.futures
from browsermobproxy import Server
import time, urllib.request, hashlib
import json, requests, os, m3u8
from selenium.webdriver.common.by import By
from happybase import ConnectionPool
from w3lib.url import to_bytes
from urllib.parse import urlparse


headers = {'user-agent': 'Mozilla/5.0'}

# SAN storage path
FS_PATH = "/hdd_storage/Downloads/data/"

# image dir path
image_dir = os.path.join(FS_PATH,"images")
if not os.path.exists(image_dir):
    os.mkdir(image_dir)
    print("images folder created")
path_for_images = FS_PATH + "images/"

#video dir path
video_dir = os.path.join(FS_PATH,"videos")
if not os.path.exists(video_dir):
    os.mkdir(video_dir)
    print("videos folder created")
path_for_videos = FS_PATH + "videos/"


# HBase Happybase configs
HBASE_THRIFT_HOST = "192.168.1.82"
HBASE_THRIFT_PORT = 9090
HBASE_USE_FRAMED_COMPACT = True
HBASE_DROP_TABLE = False

namespace = "NCRB"
in_table_name = to_bytes("Crawler")
out_table_name = to_bytes("WebContent")

kwargs = {
    "host": HBASE_THRIFT_HOST,
    "port": HBASE_THRIFT_PORT,
    "table_prefix": namespace,
    "table_prefix_separator": ":",
}

if HBASE_USE_FRAMED_COMPACT:
    kwargs.update({
        "protocol": "compact",
        "transport": "framed"
    })

pool = ConnectionPool(2, **kwargs)
with pool.connection() as connection:
    tables = set(connection.tables())
    if in_table_name not in tables:
        print("Crawler table doesn't exist")
    if out_table_name not in tables:
        schema = {
            "info": {'max_versions': 1}
        }
        connection.create_table(out_table_name, schema)


# for driver and proxy Session --- Pushkraj
path_to_browsermobproxy = "driver/browsermob-proxy-2.1.4/bin/browsermob-proxy"
# path_to_browsermobproxy = "/home/pushkraj/browsermob/browsermob-proxy-2.1.4/bin/browsermob-proxy"
server = Server(path_to_browsermobproxy, options={'port': 8090})
server.start()
proxy = server.create_proxy(params={"trustAllServers": "true"})
options = webdriver.ChromeOptions()
options.add_argument("--ignore-certificate-errors")
#setting proxy for chrome
options.add_argument("--proxy-server={0}".format(proxy.proxy))
# driver = webdriver.Chrome(executable_path="/home/pushkraj/browsermob/chromedriver",
#                           options=options)
# driver = webdriver.Chrome(executable_path="driver/chromedriver",
#                           options=options)

def database_webcontent(key, src, url, path, content_type, success=0):
    today_date = date.today().strftime("%Y-%m-%d")
    hostname = urllib.parse.urlsplit(url).hostname
    abs_path = os.path.abspath(path)
    data_dic = {
        b'info:content_url': to_bytes(src),
        b'info:page_url': to_bytes(url),
        b'info:domain_name': to_bytes(hostname),
        b'info:date': to_bytes(str(today_date)),
        b'info:MIME_type': to_bytes(str(content_type)),
        b'info:duplicate': to_bytes("NA"),
        b'info:CA_status': to_bytes("NA")
    }
    if success:
        data_dic.update({
            b'info:download_status': to_bytes("1"),
            b'info:filepath': to_bytes(abs_path)
        })
    else:
        data_dic.update({
            b'info:download_status': to_bytes("0")
        })

    with pool.connection() as connection:
        table = connection.table(out_table_name)
        table.put(to_bytes(str(key)), data_dic)




#Pushkraj made changes
def src_download(src, path, content_type, extension):
    comb_url = str(url) + str(src)
    hash_url_name = hashlib.md5(comb_url.encode()).hexdigest()
    save_path = path + hash_url_name + extension
    domain = "https://" + urlparse(main_url).netloc
    try:
        if not os.path.exists(save_path):            
            r = requests.get(src, headers={'user-agent': 'Mozilla/5.0', 'referer': domain}, timeout=60)
            with open(save_path, 'wb') as f:
                f.write(r.content)
            print("downloading")
            database_webcontent(hash_url_name, src, url, save_path, content_type, success=1)
    except:
        database_webcontent(hash_url_name, src, url, save_path, content_type)


def m3u8_downloader(src_url, path, extension):
    comb_url = str(url) + str(src_url)
    hash_url_name = hashlib.md5(comb_url.encode()).hexdigest()
    save_path = path + hash_url_name + extension
    if not os.path.exists(save_path):
        boolean = ""
        m3data = requests.get(src_url, timeout=60)
        print(m3data.text)
        m3_master = m3u8.loads(m3data.text)
        playlist = m3_master.data['playlists']
        if m3_master.data['segments']:
            print("yes")
            boolean = "true"
        htt = src_url.rpartition('/')
        http = htt[0] + htt[1]
        # if needed further
        print(http)
        if not bool(boolean):
            playurl = playlist[0]['uri']
            playurl = http + playurl
            print(playurl)
            cntnt = requests.get(playurl, timeout=60)
            m3_master = m3u8.loads(cntnt.text)
        print("Downloading...")

        with open(save_path, "wb") as f:
            for segment in m3_master.data['segments']:
                furl = segment['uri']
                furl = http + furl
                print(furl)
                r = requests.get(furl, timeout=60)
                f.write(r.content)

        print("Download complete")
        database_webcontent(key=hash_url_name, src=src_url, url=url, path=save_path , content_type="video", success=1)

def video_scrapy_from_pagesource(link) :
    if link is not None:
        try:
            src = link.get_attribute('src')
            path = path_for_videos
            extension = ".mp4"
            src_download(src, path, "video", extension)
        except:
            pass

def video_from_src(link):
    if link is not None:
        try:
            src = link.get_attribute('src')
            srctype = link.get_attribute('type')
            path = path_for_videos
            extension = ".mp4"
            if 'video' in srctype:
                src_download(src, path, "video", extension)
        except:
            pass
        try:
            src = link.get_attribute('href')
            path = path_for_videos
            if 'mp4' in src:
                extension = ".mp4"
                src_download(src, path, extension)
        except:
            pass

def image_scrapy_from_pagesource(link):
    if link is not None:
        try:
            src = link.get_attribute('src')
            path = path_for_images
            extension = ".jpg"
            src_download(src, path, "image", extension)
        except:
            pass

def download_from_trafficlog(log):
    try:
        # URL is present inside the following keys
        type = log['response']['content']['mimeType']
        url = log['request']['url']
        if 'image' in type or '.jpg' in url or '.png' in url or '.gif' in url or '.jpeg' in url:
            path = path_for_images
            extension = ".jpg"
            try:
                src_download(url, path, "image", extension)
            except Exception as e:
                pass

            if '.png' in url:
                path = path_for_images
                extension = ".png"
                src_download(url, path, "image", extension)

            if '.gif' in url:
                path = path_for_images
                extension = ".gif"
                src_download(url, path, "image", extension)

            if '.jpeg' in url:
                path = path_for_images
                extension = ".jpeg"
                src_download(url, path, "image", extension)
        if 'webp' in type:
            path = path_for_images
            extension = ".webp"
            src_download(url, path, "image", extension)

        if 'media' in type or 'video' in type:
            path = path_for_videos
            extension = ".mp4"
            src_download(url, path, "video", extension)

        if '.m3u8' in url:
            path = path_for_videos
            extension = ".ts"
            m3u8_downloader(url, path, extension)

    except Exception as e:
        pass

def scrapper(url):
    if __name__ == "__main__":
        global main_url
        main_url = url
        proxy.new_har(url)
        driver = webdriver.Chrome(executable_path="driver/chromedriver",
                                  options=options)
        driver.set_page_load_timeout(30)
        flag = True
        count = 0
        while flag and count != 4:
            try:
                driver.get(url)
                flag = False
                count = 4
            except:
                count = count + 1
                flag = True
        count = 0
        x = 1

        try:
            last_height = driver.execute_script("return document.body.scrollHeight")
            while (x != 8):
                driver.execute_script("window.scrollBy(0,1000)")
                time.sleep(1)
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    x = x + 1
                else:
                    last_height = new_height
                    x = 1
        except:
            pass
        print('done scrolling')
        driver.implicitly_wait(10)
        # Sleeps for 10 seconds
        count = 0
        linksimg = driver.find_elements(By.TAG_NAME, 'img')
        linksvideo = driver.find_elements(By.TAG_NAME, 'video')
        linksrc = driver.find_elements(By.TAG_NAME, 'source')
        link_a = driver.find_elements(By.TAG_NAME, 'a')

        with concurrent.futures.ThreadPoolExecutor() as executor:
           executor.map(image_scrapy_from_pagesource, linksimg)
        # for links in linksimg:
        #     image_scrapy_from_pagesource(links)

        with concurrent.futures.ThreadPoolExecutor() as executor:
           executor.map(video_scrapy_from_pagesource, linksvideo)
        # for links in linksvideo:
        #     video_scrapy_from_pagesource(links)

        with concurrent.futures.ThreadPoolExecutor() as executor:
           executor.map(video_from_src, linksrc)
        # for links in linksrc:
        #     video_from_src(links)

        with concurrent.futures.ThreadPoolExecutor() as executor:
           executor.map(video_from_src, link_a)
        # for links in link_a:
        #     video_from_src(links)

        del linksimg, linksvideo, linksrc, link_a
        gc.collect()

        with open("network_log1.har", "w", encoding="utf-8") as f:
            f.write(json.dumps(proxy.har))

        # Read HAR File and parse it using JSON
        # to find the urls containing images.
        har_file_path = "network_log1.har"
        with open(har_file_path, "r", encoding="utf-8") as f:
            logs = json.loads(f.read())

        # Store the network logs from 'entries' key and
        # iterate them
        network_logs = logs['log']['entries']
        with concurrent.futures.ThreadPoolExecutor() as executor:
           executor.map(download_from_trafficlog, network_logs)
        # for logs in network_logs:
        #     download_from_trafficlog(logs)

        del(network_logs)
        gc.collect()

        # close driver
        print("quitting webdriver")
        driver.quit()

        return 1


# scrapper('')
# urlfile = open('./urls.txt', 'r')
# urls  = urlfile.readlines()
# for line in urls:
#     print(line)
#     scrapper(line)

# endpoint api

with pool.connection() as connection:
    table = connection.table(in_table_name)

count = 0
flag = True
while flag:
    response = requests.get("http://192.168.1.15:5000")
    msg_dict = json.loads(response.content.decode('utf-8'))
    key = msg_dict["key"]
    if key == "NA":
        flag = False
        continue
    url = msg_dict["url"]
    count += 1
    print("Passed for scrapping: ", url)
    ret = scrapper(url)
    if ret:
        table.put(to_bytes(key), {b'info:scrap_status': to_bytes(to_bytes(str(ret)))})
        print("updated scrap_states for key :", key, ret)
    print(count)

print("finished")
