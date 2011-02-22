
from redis import Redis
from json import dumps, loads
from uuid import uuid4

siteList = ["http://www.cnn.com/2010/POLITICS/08/13/democrats.social.security/index.html",
            "http://www.huffingtonpost.com/2010/08/13/federal-reserve-pursuing_n_681540.html",
            "http://techcrunch.com/2010/08/13/gantto-takes-on-microsoft-project-with-web-based-project-management-application/",
            ]

redis = Redis("localhost")
siteList.extend(siteList)
siteList.extend(siteList)
siteList.extend(siteList)    
for site in siteList:
    sid = str(uuid4())
    redis.push('readability:jobs', dumps({
                'id':sid,
                'url':site}))


    redis.subscribe(sid)
    gen = redis.listen() ; next(gen)
    result = None
    try:
        result = next(gen)['data']
        print "ok"
    finally:
        redis.unsubscribe() ; next(gen)




