import numpy as np
import psycopg2

conn = psycopg2.connect(database="debanjan_media_database", user = "debanjan_final", password = "Deb@12345", host = "10.237.26.159", port = "5432")

print ("Opened database successfully")
cur = conn.cursor()

#cur.execute('''CREATE TABLE entity_group_cov_data
#      (event_id INT NOT NULL,
#      entity_group text NOT NULL,
#      total_about float NOT NULL,
#      about_percentage float NOT NULL,
#      total_by float NOT NULL,
#      by_percentage float NOT NULL,
#      category text NOT NULL,
#      PRIMARY KEY (event_id,entity_group,category));''')
      
#print ("Table created successfully")
#conn.commit()

output = np.loadtxt('./Results_overall/Entity_aggr_res_fp_opinion.txt',dtype=str,delimiter=';',skiprows=1)
entity_dic1 ={}
entity_dic2 ={}
entity_dic3 ={}
entity_dic4 ={}

for row in output:
	name = row[0]
	tot_about=row[1]
	about_per = row[2]
	tot_by = row[3]
	by_per = row[4]
	
	entity_dic1[name]=float(tot_about)
	entity_dic2[name]=float(about_per)
	entity_dic3[name]=float(tot_by)
	entity_dic4[name]=float(by_per)
		
for key,data in entity_dic1.items():
	
	cur.execute("""INSERT INTO entity_group_cov_data(event_id,entity_group,total_about,about_percentage,total_by,by_percentage,category)
	VALUES (3,%(a)s,%(b)s,%(c)s,%(d)s,%(e)s,'OPINION');""",
	{'a':key,'b':entity_dic1[key],'c':entity_dic2[key],'d':entity_dic3[key],'e':entity_dic4[key]})
		
conn.commit()
print('Records inserted succesfully')
conn.close()		


