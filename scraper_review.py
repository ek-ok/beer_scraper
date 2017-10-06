
"""
Scrape beer info and reviews from ratebeer.com

1). Loop through "beer_id" up from 1 to 319483

    http://www.ratebeer.com/beer/1/
    http://www.ratebeer.com/beer/2/
     .
     .
     .
    http://www.ratebeer.com/beer/319483/

2). Sometimes there are name changes so chgeck if "beer_id" is valid by calling "is_valid_beer_id"
3). Scrape basic beer info once by calling "get_beer_info"
4). Scrape basic beer reviews for "max_page" times which comes from "get_beer_info"
5). Every 10,000 beer, store data in mongodb to prevent memory overflow
6). Using ThreadPool create 10,000 thereads to pricess
"""

# coding: utf-8

import re
import urllib2
import bs4
import pandas as pd
from multiprocessing.pool import ThreadPool
import pymongo


root_url = 'http://www.ratebeer.com'


def get_soup(url):
    """
    Get BeautifulSoup's soup object from an url
    In case url open fails, try again
    """
    request = urllib2.Request(url)
    try:
        response = urllib2.urlopen(request)
    except urllib2.URLError:
        response = urllib2.urlopen(request)

    return bs4.BeautifulSoup(response)


def is_valid_beer_id(soup):
    """
    This part performs beer_id validation
    """
    if soup.get_text() != "this page requires a valid beer. didn't find the beer name.":
        return True
    else:
        False

def get_beer_info(soup):
    """
    Get basic beer info (top part) such as name, brewer_url, beerstyles..
    """
    beer_info = {}

    # name
    name = soup.find('h1', {'itemprop': 'itemreviewed'})
    if name:
        beer_info['name'] = name.get_text()
    else:
        beer_info['name'] = None

    # Brewer URL
    for url in soup.select('a[href^=http://www.ratebeer.com/brewers/]'):
        if url:
            beer_info['brewer_url'] = url['href'].split('http://www.ratebeer.com/brewers')[1]
        else:
            beer_info['brewer_url'] = None

    # img
    images = soup.select('div a[href^=http://res.cloudinary.com/ratebeer/image/upload/]')
    if images:
        beer_info['beer_img'] = images[0]['href']
        beer_info['beer_img_src'] = images[0].find('img')['src']
    else:
        beer_info['beer_img'] = None
        beer_info['beer_img_src'] = None

    # beerstyles
    beerstyle = soup.select('a[href^=/beerstyles/]')
    if beerstyle:
        beer_info['beerstyles'] = beerstyle[0]['href']
    else:
        beer_info['beerstyles'] = None

    bottle_type = soup.find('a', {'rel': 'modal:open'})

    # bottle_type
    if bottle_type:
        beer_info['bottle_type'] = bottle_type.get_text()
    else:
        beer_info['bottle_type'] = None

    # rating_fields
    ratings_tmp = soup.find('div', {'style': 'padding: 0px 10px; font-weight: bold; font-size: 12px;'})
    if ratings_tmp:
        rating_tmp = re.split(u'\xa0\xa0|:', ratings_tmp.get_text())
        ratings = {key.strip(): rating_tmp[idx + 1].strip('%') for idx, key in enumerate(rating_tmp) if idx % 2 == 0}

        for old_key in ratings.keys():
            new_key = re.sub(r' |\. ', '_', old_key.strip()).lower()
            ratings[new_key] = ratings.pop(old_key)

        beer_info['rating_fields'] = ratings
    else:
        beer_info['rating_fields'] = {}

    # max_page
    beer_info['max_page'] = 1
    beer_info['beer_url'] = None

    for page in soup.select('a.ballno'):

        # page num
        page_tmp = int(page.getText())
        if page_tmp > beer_info['max_page']:
            beer_info['max_page'] = page_tmp

        # beer url
        if beer_info['beer_url'] is None:
            m = re.match('(/beer/.+?/\d+/).+', page['href'])
            beer_info['beer_url'] = m.group(1)

    return beer_info


def get_reviews(soup):
    """
    Scrape beer review (bottom) part
    """
    user_ids = []
    review_dates = []
    in_ratings = []
    bold_scores = []
    detail_scores = []
    review_txts = []
    score_names = ['aroma', 'appearance', 'taste', 'palate', 'overall']
    reviews = []

    # Get all the fields separately
    for table in soup.findAll('table', attrs={'style': 'padding: 10px;'}):
        for small in table.findAll('small', {'style': 'color: #666666; font-size: 12px; font-weight: bold;'}):

            # Review date
            date_str = small.get_text()
            date_re = r'.+((:?JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s\d{1,2},\s20\d{2}).*'
            m = re.search(date_re, date_str)
            if m:
                review_dates.append(m.group(1))
            else:
                review_dates.append(None)

            # Count toward ratings
            in_rating = small.select('a[href^=/RatingsQA.htm]')
            if in_rating:
                in_ratings.append(False)
            else:
                in_ratings.append(True)

        for user_id in soup.select('a[href^=/user/]'):
            id_tmp = int(user_id['href'].split('/')[2])
            user_ids.append(id_tmp)

        for item in table.findAll('div', {'style': 'display:inline; padding: 0px 0px; font-size: 24px; font-weight: bold; color: #036;'}):
            bold_scores.append(item.get_text())

        # AROMA, APPEARANCE, TASTE, PALATE, OVERALL
        for item in table.findAll('big', {'style': 'color: #999;'}):
            detail_scores.append(item.getText().split(r'/')[0])

        for review in table.findAll('div', {'style': 'padding: 20px 10px 20px 0px; border-bottom: 1px solid #e0e0e0; line-height: 1.5;'}):
            review_txts.append(review.get_text().strip())

    # Combine fields by reviews
    detail_scores_chunk = chunks(detail_scores, 5)
    for idx, user_id in enumerate(user_ids):
        review_tmp = {}
        review_tmp['user_id'] = user_id
        review_tmp['bold_score'] = bold_scores[idx]
        review_tmp['review'] = review_txts[idx]
        review_tmp['review_date'] = review_dates[idx]
        review_tmp['in_rating'] = in_ratings[idx]

        scores_tmp = {score_names[i]:score for i, score in enumerate(detail_scores_chunk.next())}

        review_tmp.update(scores_tmp)
        reviews.append(review_tmp)

    return reviews


def scrape(beer_id):
    """
    Calls get_beer_info and get_reviews.
    If there are multiple pages of reviews, repeat get_reviews
    """

    data_tmp = {}
    data_tmp['beer_id'] = beer_id
    url_init = root_url + '/beer/%s/' % (beer_id)

    try:
        # get soup, in case url open fails try again
        soup = get_soup(url_init)

        # if valid url proceed
        if not is_valid_beer_id(soup):
            data_tmp['error'] = 'Invalid beer_id'
        else:
            data_tmp.update(get_beer_info(soup))
            data_tmp['reviews'] = get_reviews(soup)

            # Scrape again if there are more than one pages
            if data_tmp['max_page'] > 1:
                for page_num in xrange(1, data_tmp['max_page'] + 1):
                    url_page = root_url + data_tmp['beer_url'] + '1/%s/' % (page_num)
                    soup = get_soup(url_page)
                    review_tmp = get_reviews(soup)
                    data_tmp['reviews'].extend(review_tmp)

    except Exception, e:
        data_tmp['error'] = str(e)

    return data_tmp


def chunks(lst, chunk_size):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

if __name__ == '__main__':

    # max beer_id = 319483
    beer_ids = [i for i in xrange(1200, 319483 + 1, 1050)]
    print 'Number of beers: ', len(beer_ids)
    print 'Scraping beers..'

    # instantiate pymongo amd ThreadPool classes
    client = pymongo.MongoClient("192.168.0.31", 27017)
    db = client.beer
    pool = ThreadPool(16)

    # every 10000 beer, store data in mongodb
    # for chunk in chunks(beer_ids, 16):
    #     print chunk[0]
    #     results = (pool.map(scrape, chunk))
    #     db.beer_review.insert(results)

    # Without multi threads
    for beer_id in beer_ids:
        print beer_id
        result = scrape(beer_id)
        db.beer_review.insert(result)