# Spark example to print the average tweet length using Spark
# PGT April 2016   
# To run, do: spark-submit --master yarn-client SQL.py hdfs://hadoop2-0-0/data/twitter/part-03212

from __future__ import print_function
import sys, json
from pyspark import SparkContext
from pyspark.sql import SQLContext,Row

# Given a full tweet object, return the text of the tweet
def getInfo(line):
  try:
    js = json.loads(line)
    #badwords list from google
    badwords={"4r5e": 0,"5h1t": 0,"5hit": 0,"a55": 0,"anal": 0,"anus": 0,"ar5e": 0,"arrse": 0,"arse": 0,"ass": 0,"ass-fucker": 0,"asses": 0,"assfucker": 0,"assfukka": 0,"asshole": 0,"assholes": 0,"asswhole": 0,"a_s_s": 0,"b!tch": 0,"b00bs": 0,"b17ch": 0,"b1tch": 0,"ballbag": 0,"balls": 0,"ballsack": 0,"bastard": 0,"beastial": 0,"beastiality": 0,"bellend": 0,"bestial": 0,"bestiality": 0,"bi+ch": 0,"biatch": 0,"bitch": 0,"bitcher": 0,"bitchers": 0,"bitches": 0,"bitchin": 0,"bitching": 0,"bloody": 0,"blow job": 0,"blowjob": 0,"blowjobs": 0,"boiolas": 0,"bollock": 0,"bollok": 0,"boner": 0,"boob": 0,"boobs": 0,"booobs": 0,"boooobs": 0,"booooobs": 0,"booooooobs": 0,"breasts": 0,"buceta": 0,"bugger": 0,"bum": 0,"bunny fucker": 0,"butt": 0,"butthole": 0,"buttmuch": 0,"buttplug": 0,"c0ck": 0,"c0cksucker": 0,"carpet muncher": 0,"cawk": 0,"chink": 0,"cipa": 0,"cl1t": 0,"clit": 0,"clitoris": 0,"clits": 0,"cnut": 0,"cock": 0,"cock-sucker": 0,"cockface": 0,"cockhead": 0,"cockmunch": 0,"cockmuncher": 0,"cocks": 0,"cocksuck ": 0,"cocksucked ": 0,"cocksucker": 0,"cocksucking": 0,"cocksucks ": 0,"cocksuka": 0,"cocksukka": 0,"cok": 0,"cokmuncher": 0,"coksucka": 0,"coon": 0,"cox": 0,"crap": 0,"cum": 0,"cummer": 0,"cumming": 0,"cums": 0,"cumshot": 0,"cunilingus": 0,"cunillingus": 0,"cunnilingus": 0,"cunt": 0,"cuntlick ": 0,"cuntlicker ": 0,"cuntlicking ": 0,"cunts": 0,"cyalis": 0,"cyberfuc": 0,"cyberfuck ": 0,"cyberfucked ": 0,"cyberfucker": 0,"cyberfuckers": 0,"cyberfucking ": 0,"d1ck": 0,"damn": 0,"dick": 0,"dickhead": 0,"dildo": 0,"dildos": 0,"dink": 0,"dinks": 0,"dirsa": 0,"dlck": 0,"dog-fucker": 0,"doggin": 0,"dogging": 0,"donkeyribber": 0,"doosh": 0,"duche": 0,"dyke": 0,"ejaculate": 0,"ejaculated": 0,"ejaculates ": 0,"ejaculating ": 0,"ejaculatings": 0,"ejaculation": 0,"ejakulate": 0,"f u c k": 0,"f u c k e r": 0,"f4nny": 0,"fag": 0,"fagging": 0,"faggitt": 0,"faggot": 0,"faggs": 0,"fagot": 0,"fagots": 0,"fags": 0,"fanny": 0,"fannyflaps": 0,"fannyfucker": 0,"fanyy": 0,"fatass": 0,"fcuk": 0,"fcuker": 0,"fcuking": 0,"feck": 0,"fecker": 0,"felching": 0,"fellate": 0,"fellatio": 0,"fingerfuck ": 0,"fingerfucked ": 0,"fingerfucker ": 0,"fingerfuckers": 0,"fingerfucking ": 0,"fingerfucks ": 0,"fistfuck": 0,"fistfucked ": 0,"fistfucker ": 0,"fistfuckers ": 0,"fistfucking ": 0,"fistfuckings ": 0,"fistfucks ": 0,"flange": 0,"fook": 0,"fooker": 0,"fuck": 0,"fucka": 0,"fucked": 0,"fucker": 0,"fuckers": 0,"fuckhead": 0,"fuckheads": 0,"fuckin": 0,"fucking": 0,"fuckings": 0,"fuckingshitmotherfucker": 0,"fuckme ": 0,"fucks": 0,"fuckwhit": 0,"fuckwit": 0,"fudge packer": 0,"fudgepacker": 0,"fuk": 0,"fuker": 0,"fukker": 0,"fukkin": 0,"fuks": 0,"fukwhit": 0,"fukwit": 0,"fux": 0,"fux0r": 0,"f_u_c_k": 0,"gangbang": 0,"gangbanged ": 0,"gangbangs ": 0,"gaylord": 0,"gaysex": 0,"goatse": 0,"God": 0,"god-dam": 0,"god-damned": 0,"goddamn": 0,"goddamned": 0,"hardcoresex ": 0,"hell": 0,"heshe": 0,"hoar": 0,"hoare": 0,"hoer": 0,"homo": 0,"hore": 0,"horniest": 0,"horny": 0,"hotsex": 0,"jack-off ": 0,"jackoff": 0,"jap": 0,"jerk-off ": 0,"jism": 0,"jiz ": 0,"jizm ": 0,"jizz": 0,"kawk": 0,"knob": 0,"knobead": 0,"knobed": 0,"knobend": 0,"knobhead": 0,"knobjocky": 0,"knobjokey": 0,"kock": 0,"kondum": 0,"kondums": 0,"kum": 0,"kummer": 0,"kumming": 0,"kums": 0,"kunilingus": 0,"l3i+ch": 0,"l3itch": 0,"labia": 0,"lmfao": 0,"lust": 0,"lusting": 0,"m0f0": 0,"m0fo": 0,"m45terbate": 0,"ma5terb8": 0,"ma5terbate": 0,"masochist": 0,"master-bate": 0,"masterb8": 0,"masterbat*": 0,"masterbat3": 0,"masterbate": 0,"masterbation": 0,"masterbations": 0,"masturbate": 0,"mo-fo": 0,"mof0": 0,"mofo": 0,"mothafuck": 0,"mothafucka": 0,"mothafuckas": 0,"mothafuckaz": 0,"mothafucked ": 0,"mothafucker": 0,"mothafuckers": 0,"mothafuckin": 0,"mothafucking ": 0,"mothafuckings": 0,"mothafucks": 0,"mother fucker": 0,"motherfuck": 0,"motherfucked": 0,"motherfucker": 0,"motherfuckers": 0,"motherfuckin": 0,"motherfucking": 0,"motherfuckings": 0,"motherfuckka": 0,"motherfucks": 0,"muff": 0,"mutha": 0,"muthafecker": 0,"muthafuckker": 0,"muther": 0,"mutherfucker": 0,"n1gga": 0,"n1gger": 0,"nazi": 0,"nigg3r": 0,"nigg4h": 0,"nigga": 0,"niggah": 0,"niggas": 0,"niggaz": 0,"nigger": 0,"niggers ": 0,"nob": 0,"nob jokey": 0,"nobhead": 0,"nobjocky": 0,"nobjokey": 0,"numbnuts": 0,"nutsack": 0,"orgasim ": 0,"orgasims ": 0,"orgasm": 0,"orgasms ": 0,"p0rn": 0,"pawn": 0,"pecker": 0,"penis": 0,"penisfucker": 0,"phonesex": 0,"phuck": 0,"phuk": 0,"phuked": 0,"phuking": 0,"phukked": 0,"phukking": 0,"phuks": 0,"phuq": 0,"pigfucker": 0,"pimpis": 0,"piss": 0,"pissed": 0,"pisser": 0,"pissers": 0,"pisses ": 0,"pissflaps": 0,"pissin ": 0,"pissing": 0,"pissoff ": 0,"poop": 0,"porn": 0,"porno": 0,"pornography": 0,"pornos": 0,"prick": 0,"pricks ": 0,"pron": 0,"pube": 0,"pusse": 0,"pussi": 0,"pussies": 0,"pussy": 0,"pussys ": 0,"rectum": 0,"retard": 0,"rimjaw": 0,"rimming": 0,"s hit": 0,"s.o.b.": 0,"sadist": 0,"schlong": 0,"screwing": 0,"scroat": 0,"scrote": 0,"scrotum": 0,"semen": 0,"sex": 0,"sh!+": 0,"sh!t": 0,"sh1t": 0,"shag": 0,"shagger": 0,"shaggin": 0,"shagging": 0,"shemale": 0,"shi+": 0,"shit": 0,"shitdick": 0,"shite": 0,"shited": 0,"shitey": 0,"shitfuck": 0,"shitfull": 0,"shithead": 0,"shiting": 0,"shitings": 0,"shits": 0,"shitted": 0,"shitter": 0,"shitters ": 0,"shitting": 0,"shittings": 0,"shitty ": 0,"skank": 0,"slut": 0,"sluts": 0,"smegma": 0,"smut": 0,"snatch": 0,"son-of-a-bitch": 0,"spac": 0,"spunk": 0,"s_h_i_t": 0,"t1tt1e5": 0,"t1tties": 0,"teets": 0,"teez": 0,"testical": 0,"testicle": 0,"tit": 0,"titfuck": 0,"tits": 0,"titt": 0,"tittie5": 0,"tittiefucker": 0,"titties": 0,"tittyfuck": 0,"tittywank": 0,"titwank": 0,"tosser": 0,"turd": 0,"tw4t": 0,"twat": 0,"twathead": 0,"twatty": 0,"twunt": 0,"twunter": 0,"v14gra": 0,"v1gra": 0,"vagina": 0,"viagra": 0,"vulva": 0,"w00se": 0,"wang": 0,"wank": 0,"wanker": 0,"wanky": 0,"whoar": 0,"whore": 0,"willies": 0,"willy": 0,"xrated": 0,"xxx":0}
    text = js['text'].encode('ascii', 'ignore')
    badCount=0
    totalCount =0
    for word in text.split():
      totalCount +=1 
      if (badwords.get(word)!=None):
        badCount +=1  
               
    hour = int(js['created_at'].split()[3].split(':')[0])
    return [(badCount,totalCount,hour)]
  except Exception as a:
    return []
  
if __name__ == "__main__":
  if len(sys.argv) < 2:
    print("enter a filename")
    sys.exit(1)
 
   
  sc = SparkContext(appName="tweetSearch")
  sqlContext = SQLContext(sc)
  
  tweets = sc.textFile(sys.argv[1])
  
  texts = tweets.flatMap(getInfo)
  
  df = sqlContext.createDataFrame(texts.map(lambda (b,t,h): Row(badCount = b, totalCount = t, hour = h)))
  df.registerTempTable("tweets")
  
  print("DataFrame size %d" % df.count())
  
  matches = sqlContext.sql("SELECT sum(badCount) as badCount, sum(totalCount) as totalCount, sum(badCount)/sum(totalCount) as result, hour FROM tweets group by hour")
  print("Matches size %d" % matches.count())
  
  results = matches.take(30)
  for r in results:
    print(r)
  
  
  sc.stop()
