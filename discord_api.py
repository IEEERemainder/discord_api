import json
import time
import urllib.request
import time

def nop(x): return x

class OurException(Exception):
    pass

class BasicStdoutLog:
    def log(self, msg):
        print(msg)

class BasicStringifiers:
    def stringify(self, obj, fmt, stringifyDict):
        return fmt.format(**{field : stringifyDict[field](obj)})
    def message(msg):
        atts = msg['attachments']
        return self.stringify('{timestamp} {author}: {content}{attachments}', {
            'timestamp' : msg['timestamp'][:19],
            'author' : msg['author']['username'],
            'content' : msg['content'],
            'attachements' : ' ' + ' '.join([f'[{a['url']}]' for a in ]) if atts else ''
        })

class BasicParsers:
    def parse(self, obj, parserDict):
        return {field : parserDict[field](obj[field]) for field in parserDict if field in obj}
    def message(self, msg):
        return self.parse(msg, {
            'id' : nop, 
            'author' : lambda x: x['username'], 
            'timestamp' : nop, 
            'type' : nop, 
            'content' : nop, 
            'message_reference' : lambda x: x['message_id'], 
            'attachments': lambda x: ' '.join([f'[{a['url']}]' for a in x])
        })
    def guild(self, guild):
        return self.parse(msg, {
            'id' : nop,
            'name' : nop
        })

initializers = {
    "DM" : lambda self, **kwgs: self.get_dms(),
    "DM_TWOSOME" : lambda **kwgs: [x for x in self.get("DM") if x["type"] == 1],
    "DM_GROUPS" : lambda **kwgs: [x for x in self.get("DM") if x["type"] == 3],
    "GUILDS" : lambda **kwgs: self.get_guilds(),
    "GUILD_CHANNELS" : lambda **kwgs: self.get_guild_channels(kwgs["id"], filter_=lambda x: x["type"] in [0, 2], supressErrors='supressErrors' in kwgs and kwgs['supressErrors']),
    "CHANNEL_MESSAGES_COUNT_JSON" : lambda **kwgs: self.get_message_count_json(kwgs["id"]),
    "GUILD_MESSAGES_COUNT_JSON" : lambda **kwgs: self.query(self.baseUrl + f"guilds/{kwgs['id']}/search")
}

class DiscordApi:
    def __init__(self, token, log = None, rateLimitReachedNotifier = None, initializers = initializers):
        self.baseUrl = 'https://discord.com/api/v8/'
        self.dmUrl = 'users/@me/channels'
        self.guildsUrl = 'users/@me/guilds'
        self.guildChannelsUrl = 'guilds/{}/channels'
        self.messagesInChannelFromSnoflakeUrl = 'channels/{}/messages?after={}&limit=100' # 100 is discord upper bound
        self.headers = {
            'Authorization': token, 
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36', 
            'Content-Type': 'application/json'
        }
        self.log = log
        self.queriesPerCurrentSecond = 0
        self.currentNsStartpoint = 0
        self.maxQueriesPerSecond = 50
        self.rateLimitReachedNotifier = rateLimitReachedNotifier 
        self.cache = {}
        self.initializers = initializers

    def get(what, **kwgs): # replace with stdlib cache?
        if 'id' not in kwgs:
            if what not in self.cache or kwgs['forced']:
                self.cache[what] = self.initializers[what](kwgs)
            result = self.cache[what]
        else:
            if what not in self.cache:
                self.cache[what] = {}
            if kwgs['id'] not in self.cache[what] or kwgs['forced']:
                self.cache[what][kwgs['id']] = self.initializers[what](kwgs)
            result = self.cache[what][kwgs['id']]
        return result

    def http_get(self, url): # like requests.get but return content only (as str) (to allow users not to install requests library, however it's just python -m pip install requests in cmd then you are in python folder (cd 'path/to/python'))
        req = urllib.request.Request(url, headers=self.headers)
        try:
            resp = urllib.request.urlopen(req)
        except Exception as ex: # for 400, 403, other HTTP codes that indicate a error
            return ex.fp.fp.read().decode()
        return resp.read().decode()
    
    def query(self, url, projector = nop, filter_ = nop, supressErrors = False):
        self.rateLimitReachedNotifier and self.rateLimitReachedNotifier.tryRestoreState(self, url)
        ns = time.time_ns()
        if (ns - self.currentNsStartpoint) / 1_000_000 > 1:
            self.currentNsStartpoint = ns
            self.queriesPerCurrentSecond = 0

        if self.queriesPerCurrentSecond > 50:
            #await asyncio.sleep((ns - self.currentNsStartpoint) / 1_000_000 + 0.01)
            time.sleep((ns - self.currentNsStartpoint) / 1_000_000 + 0.01)
            self.queriesPerCurrentSecond = 0
            self.currentNsStartpoint = ns

        self.queriesPerCurrentSecond += 1
        data = None

        while True:
            data = json.loads(self.http_get(url))
            self.log and self.log.log(url)

            if 'retry_after' in data:
                self.rateLimitReachedNotifier and self.rateLimitReachedNotifier.notify(self, url)
                wait = data['retry_after']
                #await asyncio.sleep(wait + 0.01)
                time.sleep(wait + 0.01)
                self.log and self.log.log(wait)
                continue
            if not supressErrors:
                self.throwIfError(data)
            break

        return [projector(x) for x in data if filter_(x)]
    
    def throwIfError(self, json_):
        if 'message' in json_:
            raise OurException(json_)
    
    def get_dms(self, projector = nop, filter_ = nop):
        return self.query(self.baseUrl + self.dmUrl, projector, filter_)
    
    def get_guilds(self, projector = nop, filter_ = nop):
        return self.query(self.baseUrl + self.guildsUrl, projector, filter_)
    
    def get_guild_channels(self, guildId, projector = nop, filter_ = nop, supressErrors=False):
        return self.query(self.baseUrl + self.guildChannelsUrl.format(guildId), projector, filter_, supressErrors=supressErrors)
    
    def get_messages_by_chunks(self, 
        channelId, 
        lastSnowflake = 0,
        firstSnowflake = -1,
        size = 1000, # may return a bit more if is not divible by 100
        projector = nop, 
        filter_ = nop,
        progressFn = None
    ):
        result = []
        while True:
            for i in range(math.ceil(size / 100)):
                d = self.query(
                    self.baseUrl + 
                    self.messagesInChannelFromSnoflakeUrl.format(channelId, lastSnowflake) + (firstSnowflake != -1 and '&before{firstSnowflake}' or ''), 
                    projector,
                     _filter
                )[::-1] # newer messages will appear first, so inverse the order according to one in discord
                result.extend(d)
                lastSnowflake = d[-1]['id']
                progressFn and progressFn(i, result, lastSnowflake)
                if len(d) < 100:
                    yield result
                    yield break

            yield result
            result = []
            
    def get_channel_message_count_json(self, channelId, supressErrors = False):
        return self.query(self.baseUrl + f'channels/{channelId}/messages/search', supressErrors=supressErrors)