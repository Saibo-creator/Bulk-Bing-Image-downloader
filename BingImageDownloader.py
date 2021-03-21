#!/usr/bin/env python3
import argparse
import hashlib
import imghdr
import os
import pickle
import posixpath
import re
import signal
import socket
import threading
import time
import urllib.parse
import urllib.request

from logging_setup import logger

from ImageLabelingPackage.ExifImageAgeLabeler import ExifImageAgeLabeler

# config
output_dir = './bing'  # default output dir
ADULT_FILTER_ON = True  # Do not disable adult filter by default
socket.setdefaulttimeout(2)

tried_urls = []
image_md5s = {}
in_progress = 0
adlt = ""
urlopenheader = {'User-Agent': 'Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0'}


def download_label_single_image(pool_sema: threading.Semaphore, img_sema: threading.Semaphore, url: str,
                                output_dir: str, limit: int, ageLabeler, dateOfBirth_str):
    global in_progress

    if url in tried_urls:
        print('SKIP: Already checked url, skipping')
        return
    pool_sema.acquire()
    in_progress += 1
    acquired_img_sema = False
    path = urllib.parse.urlsplit(url).path
    filename = posixpath.basename(path).split('?')[0]  # Strip GET parameters from filename
    name, ext = os.path.splitext(filename)
    name = name[:36].strip()
    filename = name + ext

    try:
        request = urllib.request.Request(url, None, urlopenheader)
        image = urllib.request.urlopen(request).read()
        if not imghdr.what(None, image):
            print('SKIP: Invalid image, not saving ' + filename)
            return

        md5_key = hashlib.md5(image).hexdigest()
        if md5_key in image_md5s:
            print('SKIP: Image is a duplicate of ' + image_md5s[md5_key] + ', not saving ' + filename)
            return

        i = 0
        while os.path.exists(os.path.join(output_dir, filename)):
            if hashlib.md5(open(os.path.join(output_dir, filename), 'rb').read()).hexdigest() == md5_key:
                print('SKIP: Already downloaded ' + filename + ', not saving')
                return
            i += 1
            filename = "%s-%d%s" % (name, i, ext)

        image_md5s[md5_key] = filename

        img_sema.acquire()
        acquired_img_sema = True
        if limit is not None and len(tried_urls) >= limit:
            return

        imagefile = open(os.path.join(output_dir, filename), 'wb')
        imagefile.write(image)
        imagefile.close()
        print(" OK : " + filename)
        tried_urls.append(url)

        age, age_labeler = ageLabeler.label_age(filename, dateOfBirth_str, image_dir=output_dir)
        src = os.path.join(output_dir, filename)
        imagename_with_age = os.path.splitext(filename)[0] + "|{}".format(age) + os.path.splitext(filename)[1]
        dst = os.path.join(output_dir, imagename_with_age)
        os.rename(src, dst)


    except Exception as e:
        print("FAIL: " + filename)
        print(e)
    finally:
        pool_sema.release()
        if acquired_img_sema:
            img_sema.release()
        in_progress -= 1


def fetch_images_for_person(pool_sema: threading.Semaphore, img_sema: threading.Semaphore, keyword: str,
                            output_dir: str, filters: str, limit: int, ageLabeler, dateOfBirth_str):
    current = 0
    last = ''
    while True:
        time.sleep(0.1)

        if in_progress > 10:
            continue

        request_url = 'https://www.bing.com/images/async?q=' + urllib.parse.quote_plus(keyword) + '&first=' + str(
            current) + '&count=35&adlt=' + adlt + '&qft=' + ('' if filters is None else filters)
        request = urllib.request.Request(request_url, None, headers=urlopenheader)
        response = urllib.request.urlopen(request)
        html = response.read().decode('utf8')
        links = re.findall('murl&quot;:&quot;(.*?)&quot;', html)
        try:
            if links[-1] == last:
                return
            for index, link in enumerate(links):
                if limit is not None and len(tried_urls) >= limit:
                    return
                t = threading.Thread(target=download_label_single_image, args=(pool_sema, img_sema, link, output_dir, limit, ageLabeler, dateOfBirth_str))
                t.start()
                current += 1
            last = links[-1]
        except IndexError:
            print('FAIL: No search results for "{0}"'.format(keyword))
            return


def backup_history(*args):
    download_history = open(os.path.join(output_dir, 'download_history.pickle'), 'wb')
    pickle.dump(tried_urls, download_history)
    copied_image_md5s = dict(
        image_md5s)  # We are working with the copy, because length of input variable for pickle must not be changed during dumping
    pickle.dump(copied_image_md5s, download_history)
    download_history.close()
    print('history_dumped')
    if args:
        exit(0)


def main(person_keyword, dateOfBirth_str, search_file, limit, threads=10, filters=None, output_dir="./bing"):
    global tried_urls, image_md5s, adlt
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    output_dir_origin = output_dir
    signal.signal(signal.SIGINT, backup_history)
    try:
        download_history = open(os.path.join(output_dir, 'download_history.pickle'), 'rb')
        tried_urls = pickle.load(download_history)
        image_md5s = pickle.load(download_history)
        download_history.close()
    except (OSError, IOError):
        tried_urls = []
    assert ADULT_FILTER_ON, "adult filter must be turned on"
    if ADULT_FILTER_ON:
        adlt = ''

    pool_sema = threading.BoundedSemaphore(threads)
    img_sema = threading.Semaphore()
    ageLabeler = ExifImageAgeLabeler()
    if person_keyword:
        fetch_images_for_person(pool_sema, img_sema, person_keyword, output_dir, filters, limit, ageLabeler,
                                dateOfBirth_str)
    elif search_file:
        try:
            inputFile = open(search_file)
        except (OSError, IOError):
            print("FAIL: Couldn't open file {}".format(search_file))
            exit(1)
        else:
            for keyword, dateOfBirth_str in inputFile.readlines():
                output_sub_dir = os.path.join(output_dir_origin, keyword.strip().replace(' ', '_'))
                if not os.path.exists(output_sub_dir):
                    os.makedirs(output_sub_dir)
                fetch_images_for_person(pool_sema, img_sema, person_keyword, output_dir, filters, limit,
                                        ageLabeler,
                                        dateOfBirth_str)
                backup_history()
                time.sleep(10)
            inputFile.close()


if __name__ == "__main__":
    # keyword = "Kyle Harrison Breitkopf actor"
    # dateOfBirth = "2005-07-13T00:00:00Z"
    keyword = "Hannah Schiller actor"
    dateOfBirth = "2006-02-17T00:00:00Z"
    main(person_keyword=keyword, dateOfBirth_str=dateOfBirth, search_file=None, limit=50,
         threads=10, output_dir="../images/"+keyword.replace(" ", "_"))
