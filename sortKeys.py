
import hashlib


def string2numeric_hash(text):
    
    return int(hashlib.md5(text).hexdigest()[:], 16)

def addPair(key):
	dict[key] = string2numeric_hash(key)



dict = {'a': 16955237001963240173058271559858726497, 'c': 99079589977253916124855502156832923443, 
 'w': 320556862105665356727910535058133114216, 'z': 334539238117652783139009887043263471063, 
 'localhost:6000':101064396917366355506743765818465655395, 'localhost:5000':236003368919937277571012697239620698997,
 'localhost:7000': 26423088431252408890272543844133647982, 'localhost:8000': 173426986973103267889688330038662368041,
 'localhost:9000': 174854768489503376599025421062028955099 }

addPair('d')
addPair('x')
addPair('q')
addPair('m')
addPair('l')
addPair('u')
addPair('y')


for key, value in sorted(dict.iteritems(), key=lambda (k,v): (v,k)):
    print "%s: %s" % (key, value)