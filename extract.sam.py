
import sys

list_file = open(sys.argv[1], 'r')
sam_file = open(sys.argv[2], 'r')

list_id = list()

for list_line in list_file:
    list_line = list_line.rstrip()
    list_id.append(list_line)

for sam_line in sam_file:
    if sam_line.startswith('@SQ'):
        sam_line_1 = sam_line.rstrip()
        sam_line_2 = sam_line_1.split()
        sam_line_3 = sam_line_2[1].split(':')
        if sam_line_3[1] in list_id:
            print(sam_line_1)
    else:
        sam_line_4 = sam_line.rstrip()
        sam_line_5 = sam_line_4.split()
        if sam_line_5[2] in list_id:
            print(sam_line_4)

list_file.close()
sam_file.close()

