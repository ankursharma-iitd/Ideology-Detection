import numpy as np
ifile = input('Enter filename:')
data = np.genfromtxt(ifile, delimiter=',', dtype=str,skip_header=1)
e_group = data[:, 2]
t_about = data[:, -2].astype(np.int)
t_by = data[:, -1].astype(np.int)

total_about = np.sum(t_about)
total_by = np.sum(t_by)

groups = np.unique(e_group)

f = open('Entity_aggr_res_fp_opinion.txt','w')
f.write('Entity_group;total_about;about%;total_by;by%\n')
delimiter = ';'
for g in groups:
    ind = np.where(e_group == g)[0]
    about = np.sum(t_about[ind])
    by = np.sum(t_by[ind])
    f.write(g + delimiter + str(about) + delimiter + str(about / total_about) + delimiter + str(by) + delimiter + str(
        by/total_by) + '\n')
f.close()
