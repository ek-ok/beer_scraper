from scipy import mean


def moreFreqwords(cl, others):
    ret = {}
    ln = float(len(cl))
    for word in cl.keys():
        pct = cl[word] / ln
        otherPcts = []
        for other in others:
            if word not in other:
                otherPcts.append(0.)
            else:
                otherPcts.append(other[word] / float(len(other)))
        if max(pct, max(otherPcts)) == pct:
            ret[word] = pct - mean(otherPcts)
    return ret


taste = {'balanced': 3, 'favorite': 3, 'delicious': 7, 'good': 1, 'wonderful': 5, 'aroma': 1}
smell = {'aroma': 3, 'smell': 3, 'nose': 3, 'wonderful': 3}
look = {'head': 3, 'light': 3, 'wonderful': 3, 'good': 3}

print
moreFreqwords(taste, [smell, look])
