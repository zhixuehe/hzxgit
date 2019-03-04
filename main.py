import mongo
import agent
import os

# 如果是用全部的adset，则用这个
objects = mongo.get_ad_sets()
print(len(objects))
for x in objects:
    obj = agent.Agent(x, 10, 6, 3)
    obj.start()
