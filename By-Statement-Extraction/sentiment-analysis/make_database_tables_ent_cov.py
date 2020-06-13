import numpy as np
import psycopg2

conn = psycopg2.connect(database="debanjan_media_database", user = "debanjan_final", password = "Deb@12345", host = "10.237.26.159", port = "5432")

print ("Opened database successfully")
cur = conn.cursor()

#cur.execute('''CREATE TABLE entity_coverage_data
#      (event_id INT NOT NULL,
#      entity_name text NOT NULL,
#      power_elite_or_not_1_or_0 int NOT NULL,                                       
#      role text NOT NULL,
#      total_about int NOT NULL,
#      total_by int NOT NULL,
#      category text NOT NULL,
#      PRIMARY KEY (event_id,entity_name,category));''')
      
#print ("Table created successfully")
#conn.commit()

output = np.loadtxt('./RESULTS_PowerElite_gst/entity_coverage_results_0to110total.txt',dtype=str,delimiter='\t',skiprows=1)
entity_dic1 ={}
entity_dic2 ={}
entity_dic3 ={}
entity_dic4 ={}

for row in output:
	name = row[0]
	power_elite_status=row[1]
	role = row[2]
	tot_about = row[3]
	tot_by = row[4]
	
	if name not in entity_dic1:
		entity_dic1[name]=int(power_elite_status)
		entity_dic2[name]=role
		entity_dic3[name]=int(tot_about)
		entity_dic4[name]=int(tot_by)
		
	else:
		entity_dic3[name]+=int(tot_about)
		entity_dic4[name]+=int(tot_by)
		
for key,data in entity_dic1.items():
	
	cur.execute("""INSERT INTO entity_coverage_data(event_id,entity_name,power_elite_or_not_1_or_0,role,total_about,total_by,category)
	VALUES (2,%(a)s,%(b)s,%(c)s,%(d)s,%(e)s,'GENERAL');""",
	{'a':key,'b':entity_dic1[key],'c':entity_dic2[key],'d':entity_dic3[key],'e':entity_dic4[key]})
		
conn.commit()
print('Records inserted succesfully')
conn.close()		


