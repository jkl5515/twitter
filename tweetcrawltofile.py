# -*- coding:utf-8 -*-
import os,sys,codecs,pprint, pickle, argparse
import datetime, time
from pymongo import MongoClient
import twitter

if __name__ == '__main__':
#  print twitter.__version__
  print 'crawl text from twitter.com'
  # argparse
  argparser = argparse.ArgumentParser(description='crawl text of tweet from twitter.com')
  #argparser.add_argument('-ckey', dest='ckey', required=True, help='consumer_key of twitter app.')
  #argparser.add_argument('-csecret', dest='csecret', required=True, help='consumer_secret of twitter app.')
  #argparser.add_argument('-akey', dest='akey', required=True, help='access_token_key of twitter app.')
  #argparser.add_argument('-asecret', dest='asecret', required=True, help='access_token_secret of twitter app.')
  argparser.add_argument('-kfp', dest='fpkeys', required=True, help='file path contained  keys and secret.')
  argparser.add_argument('-bidx', dest='bidx', required=True, help='beginning index of tweet list.')
  argparser.add_argument('-eidx', dest='eidx', required=True, help='ending index of tweet list. set "end" for the last')
  argparser.add_argument('-dberror',dest='dberror', action='store_true', help='save error meassge to database. (have set in this code )')
  args = argparser.parse_args()
  bidx = int()
  eidx = int()
  bidx = int(args.bidx)
  if args.eidx == 'end':
    eidx = None
  else:
    eidx = int(args.eidx)

  # load keys and secrets
  ckey = ''
  csecret = ''
  akey = ''
  asecret = ''
  if os.path.exists(args.fpkeys):
    print args.fpkeys
  else:
    print 'cannot find %s' % args.fpkeys
    sys.exit()
  with codecs.open(args.fpkeys, encoding='utf-8') as fkeys:
    keys = fkeys.read().strip().split('\n')
  if len(keys) == 4:
    ckey, csecret, akey, asecret = keys
  else:
    print 'number of keys is %d, need to be 4' % len(keys)
    sys.exit()
  
  
  # setting dir and filepath
  corpusroot = '.'
  fpids = os.sep.join([corpusroot, 'tweetidlist.txt'])
  if os.path.exists(fpids):
    print fpids
  else:
    print 'cannot find %s' % fpids
    sys.exit()

  # load tweet id list
  idlist = list()
  with codecs.open(fpids, encoding='utf-8') as fids:
    for line in fids:
      idlist.append(line.strip())
  print 'number of tweets: %d' % len(idlist)

  
  # crawl text from twitter.com, save to 'dbstcjp' mongodb, 'tweets' collection
  fres = codecs.open('res.txt','wb+', encoding='utf-8')
  twapi = twitter.Api(consumer_key=ckey, consumer_secret=csecret, access_token_key=akey, access_token_secret=asecret)
  stime = datetime.datetime.now()
  for item in idlist[bidx : eidx]:
    ok = False
    while(not ok):
      try:
        tweet = twapi.GetStatus(item)
        fres.write('%s\t%s\n' % (item, tweet.text))
        ok = True 
      except twitter.TwitterError, e:
        print e, '#', item
        if not ( isinstance(e, twitter.TwitterError) or isinstance(e[0][0], dict) ):
          continue
        elif e[0][0].has_key('code'):
          if e[0][0]['code'] == 32 or e[0][0]['code']== 215:
            sys.exit()
          elif e[0][0]['code'] == 88:
            time.sleep(900) 
            continue
        ok = True

      time.sleep(5)
  fres.close()
  print 'finished:', datetime.datetime.now() - stime

