
import json
import gzip
import pandas as pd

import botometer
from config import TWITTER_KEYS, RAPIDAPI_KEY

# Read tweet datasets to collect user-ids
userProfiles = dict()
with gzip.open('../data/tweets_dehydrated.jsons.gz', 'rb') as fl:
    for line in fl:
        tweet = json.loads(line)
        if tweet['user']['id_str'] not in userProfiles:
            userProfiles[tweet['user']['id_str']] = {'count':0}
            
        userProfiles[tweet['user']['id_str']]['count'] += 1
        userProfiles[tweet['user']['id_str']].update({
            'screen_name': tweet['user']['screen_name'],
            'name': tweet['user']['name'],
            '# of followers': tweet['user']['followers_count'],
            '# of friends': tweet['user']['friends_count'],
            '# of statuses': tweet['user']['statuses_count'],
            'account_creation': tweet['user']['created_at']
        })
        
print('{} unique users collected'.format(len(userProfiles)))


# Let's look at the most active accounts
userDf = pd.DataFrame.from_dict(userProfiles, orient='index')
userDf = userDf.sort_values(by='count', ascending=False)



## Setup Botometer API
# Let's test the API endpoint
bom = botometer.Botometer(wait_on_ratelimit=True,
                          rapidapi_key=RAPIDAPI_KEY,
                          **TWITTER_KEYS)

#result = bom.check_account('@onurvarol')
#print(json.dumps(result, sort_keys=True, indent=2))

# Collect Botometer scores
# This collection takes a while and Free-tier Botometer API has rate-limits.

BOTOMETER_SCORE_FILE = '../data/botometer_scores.jsons'
BOTOMETER_ERROR_FILE = '../data/botometer_scores.errors'

## Collect already existing scores from previous runs
botometerScores = dict()
try:
    with open(BOTOMETER_SCORE_FILE, 'r') as fl:
        for line in fl:
            try:
                temp = json.loads(line)
                botometerScores[temp['user']['id_str']] = temp
            except:
                pass
except Exception as e:
    print(e)
print('Scores for {} accounts already collected'.format(len(botometerScores)))


## Collect IDs of the accounts that are either deleted or suspended
removedAccounts = set()
try:
    with open(BOTOMETER_ERROR_FILE, 'r') as fl:
        for line in fl:
            removedAccounts.add(line.strip())
except Exception as e:
    print(e)
print('{} accounts unaccessible'.format(len(removedAccounts)))


## Collect Botometer scores for the remaning accounts
userList = list(userDf.index)
toCollect = set(userList) - (set(botometerScores.keys()) | removedAccounts)
print('{}/{} accounts will be collected'.format(len(toCollect), len(userList)))

for c,uid in enumerate(userList):
    try:
        if uid not in toCollect:
            continue

        result = bom.check_account(uid)
        print(c, result['user']['id_str'], result['scores'])
        
        with open(BOTOMETER_SCORE_FILE, 'a') as fl:
            fl.write('{}\n'.format(json.dumps(result)))
        
    except Exception as e:
        msg = str(e)
        print('[ERROR]: {}'.format(msg))

        if 'Not authorized' in str(e):
            with open(BOTOMETER_ERROR_FILE, 'a') as fl:
                fl.write('{}\n'.format(uid))

    
    if (c % 100) == 0:
        print(c, 'accounts processed so far')



