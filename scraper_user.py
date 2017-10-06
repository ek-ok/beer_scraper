import re
from multiprocessing.pool import ThreadPool
import urllib2
import bs4
import pymongo

root_url = r'http://www.ratebeer.com'

def get_soup(url):
    """
    Get BeautifulSoup's soup object from an url
    In case url open fails, try again
    """
    request = urllib2.Request(url)

    try:
        response = urllib2.urlopen(request)
    except URLError:
        response = urllib2.urlopen(request)

    return bs4.BeautifulSoup(response)

def is_bad_user_id(soup):
    if 'user not found' in soup.find('body').get_text():
        return True
    else:
        False

def get_user_data(user_id):
    user_data = {}
    user_data['user_id'] = user_id

    soup = get_soup('http://www.ratebeer.com/user/%s/' % (user_id))

    # check user id
    if is_bad_user_id(soup):
        user_data['error'] = 'User not found'
        return user_data

    # date
    user_data['joined_date'] = None
    user_data['last_login'] = None
    dates = soup.find_all('span', {'class': 'GrayItalic'})
    if len(dates) > 0:
        user_data['joined_date'] = dates[0].get_text()
        user_data['last_login'] = dates[1].get_text()

    # favorite
    favorite = soup.select('a[href^=/beerstyles/]')
    if len(favorite) > 0:
        user_data['favorite_style'] = favorite[0]['href']
    else:
        user_data['favorite_style'] = None

    # user name
    table = soup.find('table', {'border':0, 'cellpadding':0, 'cellspacing':0, 'width': '100%'})
    user_name = table.find('span',{'class': 'userIsDrinking'})
    if user_name:
        user_data['user_name'] = user_name.get_text()

    # location
    user_data['location'] = None
    spans = table.find_all('span')
    if len(spans) > 0:
        loc = str(spans[1]).split('<br/>')
        if len(loc) > 0:
            loc = loc[0][8:].strip()
            if len(loc) > 0:
                user_data['location'] = loc

    return user_data


def chunks(lst, chunk_size):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


if __name__ == '__main__':
    client = pymongo.MongoClient("192.168.0.31", 27017)
    db = client.beer

    users = db.beer_review.aggregate([{'$unwind':"$reviews"},{'$group':{'_id':"$reviews.user_id"}}])
    users = [u['_id'] for u in users['result']]
    users.sort()

    max_idx = users.index(26981)
    users = users[max_idx:]

    print 'Length', len(users)

    pool = ThreadPool(8)

    for chunk in chunks(users, 32):
        print chunk[0]

        results = (pool.map(get_user_data, chunk))
        db.user.insert(results)