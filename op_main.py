import op_mongo
import op_agent


objects = op_mongo.get_ad_sets()
print(objects)
for x in objects:
    obj = op_agent.Agent(x, 0.005, 0.02, 10000)
    obj.start()