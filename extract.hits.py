# This script uses two files as import to extract
# the hits that are listed file 1 (only one column),
# the second file contains several columns,
# but the first column is the target of file 1.

import sys

gene_list = list()
a = open(sys.argv[1], 'r')

for line in a:
    geneId = line.rstrip()
    gene_list.append(geneId)

a.close()

b = open(sys.argv[2], 'r')

for line in b:
    line = line.rstrip()
    coverage = line.split('\t')
    if coverage[0] in gene_list:
        print(line)

b.close()
