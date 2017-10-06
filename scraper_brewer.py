import pymongo
import re
from multiprocessing.pool import ThreadPool
import urllib2
import bs4

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

    # return bs4.BeautifulSoup(response)
    return bs4.BeautifulSoup(response, "lxml")


def get_brewer_info(brewer_url):
    brewer_info = {}

    brewer_info['brewer_url'] = brewer_url
    brewer_info['brewer_id'] = int(brewer_url.split('/')[2])

    url = root_url + '/brewers/' + brewer_url
    soup = get_soup(url)

    # name
    name = soup.find('title')
    if name:
        brewer_info['brewer_name'] = name.get_text().split(',')[0].strip()

    # des
    des = soup.find('small').find('font', {'color': '#999999'})
    if des:
        brewer_info['des'] = des.get_text()
    else:
        brewer_info['des'] = None

    # get address
    address_items = ['streetAddress', 'addressLocality', 'addressRegion', 'addressCountry', 'postalCode', 'telephone']

    for item in address_items:
        itemprop_tag = soup.find('span', {'itemprop': item})
        if itemprop_tag:
            brewer_info[item] = itemprop_tag.get_text().strip()
        else:
            brewer_info[item] = None

    # get img
    src_large = soup.select('a[href^=http://res.cloudinary.com/ratebeer/image/upload/]')
    if len(src_large) > 0:
        brewer_info['src_large'] = src_large[0]['href']
    else:
        brewer_info['src_large'] = None

    src_small = soup.find('img', {'class': 'curvy'})
    if src_small:
        brewer_info['src_small'] = src_small['src']
    else:
        brewer_info['src_small'] = None

    # brewer_type
    brewer_type = soup.find(text=re.compile(r'Type:\s.+'))
    if brewer_type:
        brewer_info['brewer_type'] = brewer_type.split('Type: ')[1].strip()
    else:
        brewer_info['brewer_type'] = None

    # get wbsite
    for a in soup.findAll('a', {'target': '_blank'}):
        if 'facebook' not in a['href']:
            if 'twitter' not in a['href']:
                if r'.' in a.get_text():
                    brewer_info['website'] = a['href']

    return brewer_info


def chunks(lst, chunk_size):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


if __name__ == '__main__':
    client = pymongo.MongoClient("192.168.0.31", 27017)
    db = client.beer
    brewer_urls = db.beer_review.distinct('brewer_url')

    len(brewer_urls)

    # pool = ThreadPool(256)
    # for chunk in chunks(brewer_urls, 1024):
    #     print chunk[0]
    #
    #     results = (pool.map(get_brewer_info, chunk))
    #     db.brewer.insert(results)

    for idx, brewer_url in enumerate(brewer_urls):
        print
        idx
        results = get_brewer_info(brewer_url)
        db.brewer.insert(results)
