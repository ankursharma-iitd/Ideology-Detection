import numpy as np
import psycopg2

conn = psycopg2.connect(database="debanjan_media_database", user = "debanjan_final", password = "Deb@12345", host = "10.237.26.159", port = "5432")

print ("Opened database successfully")
cur = conn.cursor()

#cur.execute('''CREATE TABLE entity_sent_polarity_data
#      (event_id INT NOT NULL,
#      entity_name text NOT NULL,
#      aggr_sent float NOT NULL,
#      tot_pos float NOT NULL,
#      tot_neg float NOT NULL,
#      polarity float NOT NULL,
#      category text NOT NULL,
#      PRIMARY KEY (event_id,entity_name,category));''')
      
#print ("Table created successfully")
#conn.commit()

output = np.loadtxt('./Results_overall/Entity_wise_polarity_gst_opinion.csv',dtype=str,delimiter=';',skiprows=1)
entity_dic1 ={}
entity_dic2 ={}
entity_dic3 ={}
entity_dic4 ={}

for row in output:
	name = row[0]
	agg_sent=row[1]
	tot_pos = row[2]
	tot_neg = row[3]
	polarity = row[4]
	
	entity_dic1[name]=float(agg_sent)
	entity_dic2[name]=float(tot_pos)
	entity_dic3[name]=float(tot_neg)
	entity_dic4[name]=float(polarity)
		
for key,data in entity_dic1.items():
	
	cur.execute("""INSERT INTO entity_sent_polarity_data(event_id,entity_name,aggr_sent,tot_pos,tot_neg,polarity,category)
	VALUES (2,%(a)s,%(b)s,%(c)s,%(d)s,%(e)s,'OPINION');""",
	{'a':key,'b':entity_dic1[key],'c':entity_dic2[key],'d':entity_dic3[key],'e':entity_dic4[key]})
		
conn.commit()
print('Records inserted succesfully')
conn.close()		


