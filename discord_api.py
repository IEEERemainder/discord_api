import json
import time
import urllib.request

def pf(x): return x

class OurException(Exception):
    pass

class BasicStdoutLog:
    def log(self, msg):
        print(msg)

class NoLog:
    def log(self, msg):
        pass

class DiscordApi:

    msg_parser = {
        "id" : pf, 
        "author" : lambda x: x["username"], 
        "timestamp" : pf, 
        "type" : pf, 
        "content" : pf, 
        "message_reference" : lambda x: x["message_id"], 
        "attachments": lambda x: ' '.join([f"[{a['url']}]" for a in x])
    }
    guild_parser = {
        "id" : pf,
        "name" : pf
    }

    def __init__(self, token, log=NoLog()):
        self.baseUrl = 'https://discord.com/api/v8/'
        self.dmUrl = "users/@me/channels"
        self.guildsUrl = "users/@me/guilds"
        self.headers = {
            'Authorization': token, 
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36', 
            'Content-Type': 'application/json'
        }
        self.log = log
        
    def http_get(self, url): # lice requests.get but return content only (as str) (to allow users not to install requests library, however it's just python -m pip install requests in cmd then you are in python folder (cd 'path/to/python'))
        req = urllib.request.Request(url, headers=self.headers)
        try:
            resp = urllib.request.urlopen(req)
        except Exception as ex: # for 400, 403, other HTTP codes that indicate a error
            return ex.fp.fp.read().decode()
        return resp.read().decode()
    
    def query(self, url):
        c = None
        while True: # loop needed?
            c = json.loads(self.http_get(url))
            self.log.log(url)
            if "retry_after" not in c: break
            wait=c["retry_after"]
            time.sleep(wait)
            self.log.log(wait)
        return c
    
    def throwIfError(self, json_):
        if "message" in json_:
            raise OurException(json_)
    
    def basic_parse(self, obj, parserDict):
        return {field : parserDict[field](obj[field]) for field in parserDict if field in obj}

    def basic_parse_message(self, msg):
        return self.basic_parse(msg, DiscordApi.msg_parser)
        
    def basic_readable_message(self, msg):
        result = msg["timestamp"][:19] + \
            " " + \
            msg["author"]["username"] + \
            ": " + \
            msg["content"]
        if msg["attachments"]:
            result += " " + " ".join([f"[{a['url']}]" for a in msg["attachments"]])
        return result
    
    def basic_parse_guild(self, msg):
        return self.basic_parse(msg, DiscordApi.guild_parser)
    
    def get_dms(self, projector=pf, filter_=pf):
        d = self.query(self.baseUrl + self.dmUrl)
        self.throwIfError(d)
        return [projector(x) for x in d if filter_(x)]
    
    def get_guilds(self, projector=pf, filter_=pf):
        d = self.query(self.baseUrl + self.guildsUrl)
        self.throwIfError(d)
        return [projector(x) for x in d if filter_(x)]
    
    def get_guild_channels(self, guildId, projector=pf, filter_=pf):
        d = self.query(self.baseUrl + f"guilds/{guildId}/channels")
        self.throwIfError(d)
        return [projector(x) for x in d if filter_(x)]
    
    def get_messages_by_chuncs(self, channelId, lastSnowflake=0, size=10, projector=pf, filter_=pf):
        result = []
        iters = 1
        while True:
            d = self.query(self.baseUrl + f"channels/{channelId}/messages?limit=100&after={lastSnowflake}")
            self.throwIfError(d)
            chunc = [projector(x) for x in d[::-1] if filter_(x)]
            result.extend(chunc)
            if iters == size:
                yield result
                result = []
                iters = 0 # or -1?
            iters += 1
            if len(d) < 100:
                break
            lastSnowflake = d[0]["id"]
        yield result
            
    def get_message_count_json(self, channelId):
        return self.query(self.baseUrl + f"channels/{channelId}/messages/search")