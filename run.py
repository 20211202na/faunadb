'''
scp -P 22 /Users/zoooesong/Workspaces/galera-thread.py nobi@pc311.emulab.net:/users/nobi/galera-data/galera-thread.py
python3 galera-thread.py -n 1
'''

import os
import random
import sys
import getopt
import threading
from faunadb import query as q
from faunadb.client import FaunaClient
import numpy as np

wo_rate=0.2
ro_rate=0.2
wr_rate = 0.5 # write
transaction_num = 30
operation_num =30
threads_num = 20
node_no=1
folder_num = 0
secret_key = ["fnAEg5Wv6_ACUW0z6mvQ4nlRlXdmHYkRS5oPTW_k",
                "fnAEg5W4fWACUT2_5-bDdjIgi7RZSebdw3vheN_q",
                "fnAEg5W8R4ACUeeYv75Aa6PESHrHmRkxnwSJT9WA",
                "fnAEg5XAIJACUTvtfz0SKcnJmDYyg1qUUx5BhA7H",
                "fnAEg5XDzEACUeDjykU5geijWpslqXGPgzMs29lQ",
                "fnAEg5XHJjACUZUDFd3e6eznk7WGpZXk7GyhLtoe",
                "fnAEg5XKWhACUVGS-gmy3sFIdcm2O7tEX73uvCwT",
                "fnAEg5XNbTACUab6OXQ3EfEJQRjnDgk8S1x62uGh",
                "fnAEg5XQ1mACUW430LrBbtWIiP7f02SW0RM1Ja7Y",
                "fnAEg5XV2JACUd7Fi62BFHIKzEMQRWSXnjRcnmfn",
                "fnAEibNT_ZACT_mIr6yOrxW-zkYXfn7WNqgnReOZ",
                "fnAEibNYXkACT2byK1AakyNQhp2IyC9BHu5gdmcD",
                "fnAEibNclDACTyWT56j8vAxYk8E_LpcaDO0JtCWV",
                "fnAEibNfTfACT-PDh55Fw-FhSEqK0767KTNJhBLM",
                "fnAEibNjISACT9bM1PbEP8EO-PYcKoAF6NHI5o6f",
                "fnAEibNmgDACT0KrwTA8matjWSCtVcpnYeeJM9nh",
                "fnAEibNpa8ACT1U1IPLe1U9f9ppD4vCJGneOupJ3",
                "fnAEibNsYQACTz2Umjc2NE0uVVzudfqn50xk64yu",
                "fnAEibNuz5ACT5JXvnwr0ZPjErEBLRQl7nvDcGC5",
                "fnAEibNyKKACT3vWdyjtbEWvtEboy-EyivFknwMW"]

try:
    opts, args = getopt.getopt(sys.argv[1:],"hw:r:p:t:o:c:n:f:",["help","wo_rate=","ro_rate=","w_percent=","trans_num=","op_num=","client_num=","node_no=","folder_num="])
    for opt, arg in opts:
        if opt in ('-w','--wo_rate'):
            wo_rate = float(arg)
        elif opt in ('-r','--ro_rate'):
            ro_rate = float(arg)
        elif opt in ('-p','--w_percent'):
            wr_rate = float(arg)
        elif opt in ('-t','--trans_num'):
            transaction_num = int(arg)
        elif opt in ('-o','--op_num'):
            operation_num = int(arg)
        elif opt in ('-c','--client_num'):
            threads_num = int(arg)
        elif opt in ('-n','--node_no'):
            node_no = int(arg)
        elif opt in ('-f','--folder_num'):
            folder_num = str(arg)
        elif opt in ('-h','--help'):
            print("python3 tidb-thread.py -w <wo_rate> -r <ro_rate> -p <w_percent> -t <trans_num> -o <op_num> -c <client_num> -n <node_no> -f <folder_num>")
            sys.exit()
except getopt.GetoptError:
    print("python3 tidb-thread.py -w <wo_rate> -r <ro_rate> -p <w_percent> -t <trans_num> -o <op_num> -c <client_num> -n <node_no>")
    sys.exit()
# print("Parameters:\nwo_rate = " + str(wo_rate) + "\nro_rate = " + str(ro_rate) + "\nw_percent = " + str(wr_rate) + "\ntrans_num = " + str(transaction_num) + "\nop_num = " + str(operation_num) + "\nclient_num = " + str(threads_num) + "\nnode_no = " + str(node_no) + "\nfolder_num = " + str(folder_num))

key_num = 20
total_op_num = 2*transaction_num*operation_num
folder_name = "./output/"+str(folder_num)+"/"
hist_folder = "./client/"+str(folder_num)+"/"
print('current: ' + str(folder_num))



def mk_dir(path):
	folder = os.path.exists(path)
	if not folder:                   
		os.makedirs(path)            

class myThread(threading.Thread):
    def __init__(self,id):
        threading.Thread.__init__(self)
        self.id = id
        pass
    def run(self):
        run_thread(i)

class Operation:
    op_type = True  #true is write
    variable = 0
    value = 0
    
    def __init__(self, op_type, variable, value):
        self.op_type = op_type
        self.variable = variable
        self.value = value

    def Read(self,variable):
        self.op_type = False
        self.variable = variable
        self.value = 0
    def Write(self,variable,value):
        self.op_type = True
        self.variable = variable
        self.value = value
    # def Display_info(self):
    #     if(op_type==True):
    #         print("write," + str(variable) + "," + str(value))
    #     elif(op_type==False):
    #         print("read," + str(variable) + "," + str(value))
    #     else:
    #         print("Error in Operation op_type!")


def Zipf(a: np.float64, min: np.uint64, max: np.uint64, size=None):
    """
    Generate Zipf-like random variables,
    but in inclusive [min...max] interval
    """
    if min == 0:
        raise ZeroDivisionError("")

    v = np.arange(min, max+1) # values to sample
    p = 1.0 / np.power(v, a)  # probabilities
    p /= np.sum(p)            # normalized

    return np.random.choice(v, size=size, replace=True, p=p)



def zipf_generator(output_path, client, trans, ops, var):
    '''
    output_path: hist file path
    client: client No.
    trans: trans num for each client
    ops: operation num for each trans
    var: key num
    wr: rate of w/r
    '''
    doc = open(output_path+"hist_"+str(client)+".txt",'w')
    counter = client * total_op_num * 2
    min = np.uint64(1)
    max = np.uint64(var)
    q = Zipf(1, min, max, trans*ops)
    var_list = [int(x)-1 for x in q]
    var_count = 0
    for t in range (0,trans):
        trans_type = random_pick([0,1,2],[wo_rate,ro_rate,1-wo_rate-ro_rate])
        if trans_type == 0:
            for op in range (0,ops):
                variable = var_list[var_count]
                var_count += 1
                counter += 1
                new_op = Operation(True,variable,counter)
                doc.write("write," + str(new_op.variable) + "," + str(new_op.value)+"\n")
        elif trans_type == 1:
            for op in range (0,ops):
                variable = var_list[var_count]
                var_count += 1
                new_op = Operation(False,variable,0)
                doc.write("read," + str(new_op.variable) + "," + str(new_op.value)+"\n")
        elif trans_type == 2:
            w_count = 0
            r_count = 0
            for i in range (0,ops):
                op_type = random_pick([0,1],[wr_rate,1-wr_rate])
                if op_type == 0:
                    w_count += 1
                elif op_type == 1:
                    r_count += 1
                else:
                    print("Error in op_type!")
            for r in range(r_count):
                variable = var_list[var_count]
                var_count += 1
                new_op = Operation(False,variable,0)
                doc.write("read," + str(new_op.variable) + "," + str(new_op.value)+"\n")
            for w in range(w_count):
                variable = var_list[var_count]
                var_count += 1
                counter += 1
                new_op = Operation(True,variable,counter)
                doc.write("write," + str(new_op.variable) + "," + str(new_op.value)+"\n")
        else:
            print("Error in trans_type!")
    doc.close()
    # print(output_path+"hist_"+str(client)+".txt"+" succeeded.")




def uniform_generator(output_path, client, trans, ops, var):
    '''
    output_path: hist file path
    client: client No.
    trans: trans num for each client
    ops: operation num for each trans
    var: key num
    wr: rate of w/r
    '''
    doc = open(output_path+"hist_"+str(client)+".txt",'w')
    counter = client * total_op_num * 2
    for t in range (0,trans):
        trans_type = random_pick([0,1,2],[wo_rate,ro_rate,1-wo_rate-ro_rate])
        if trans_type == 0:
            for op in range (0,ops):
                variable = random.randint(0,var-1)
                counter += 1
                new_op = Operation(True,variable,counter)
                doc.write("write," + str(new_op.variable) + "," + str(new_op.value)+"\n")
        elif trans_type == 1:
            for op in range (0,ops):
                variable = random.randint(0,var-1)
                new_op = Operation(False,variable,0)
                doc.write("read," + str(new_op.variable) + "," + str(new_op.value)+"\n")
        elif trans_type == 2:
            w_count = 0
            r_count = 0
            for i in range (0,ops):
                op_type = random_pick([0,1],[wr_rate,1-wr_rate])
                if op_type == 0:
                    w_count += 1
                elif op_type == 1:
                    r_count += 1
                else:
                    print("Error in op_type!")
            for r in range(r_count):
                variable = random.randint(0,var-1)
                new_op = Operation(False,variable,0)
                doc.write("read," + str(new_op.variable) + "," + str(new_op.value)+"\n")
            for w in range(w_count):
                variable = random.randint(0,var-1)
                counter += 1
                new_op = Operation(True,variable,counter)
                doc.write("write," + str(new_op.variable) + "," + str(new_op.value)+"\n")
        else:
            print("Error in trans_type!")
    doc.close()
    # print(output_path+"hist_"+str(client)+".txt"+" succeeded.")


def random_pick(some_list, probabilities): 
    '''
    randon_pick([true,false],[0.5,0.5])
    '''
    x = random.uniform(0,1) 
    cumulative_probability = 0.0 
    for item, item_probability in zip(some_list, probabilities): 
        cumulative_probability += item_probability 
        if x < cumulative_probability:
               break 
    return item 


def generate_opt(hist_file, trans_num): 
    fo = open(hist_file, "r")
    # print ("Select hist file:", fo.name)
    list_line = []
    for line in fo.readlines():
        line = line.strip()                            
        list_line.append(line)
    fo.close()
    list_trans = []
    op_count=0
    for i in range(0,trans_num):
        temp_ops = []
        for j in range(0,operation_num):
            temp_ops.append(list_line[op_count])
            op_count += 1
        list_trans.append(temp_ops)
    return list_trans


def run_ops(list_of_ops, client_no):
    op_num = 0
    result_ops = []
    # server_num = random_pick([0,1,2],[0.34,0.33,0.33])
    secret = secret_key[client_no]
    # print("client_no: "+ str(client_no) + ", server_no: " + str(server))
    client = FaunaClient(secret=secret, domain="db.fauna.com", port=443, scheme="https")
    t_count = 0
    e_count = 0
    for i in range(len(list_of_ops)):
        if t_count > transaction_num:
            break
        temp_tx_op = []
        e_flag = False
        op = []
        for m in range(len(list_of_ops[i])):
            op.append([str.split(list_of_ops[i][m],',',3)[0],str.split(list_of_ops[i][m],',',3)[1],str.split(list_of_ops[i][m],',',3)[2]])
        try:
            result = client.query([
                q.if_(op[0][0]=="write", q.call("write",[int(op[0][1]),int(op[0][2])]), q.call("read",int(op[0][1]))),
                q.if_(op[1][0]=="write", q.call("write",[int(op[1][1]),int(op[1][2])]), q.call("read",int(op[1][1]))),
                q.if_(op[2][0]=="write", q.call("write",[int(op[2][1]),int(op[2][2])]), q.call("read",int(op[2][1]))),
                q.if_(op[3][0]=="write", q.call("write",[int(op[3][1]),int(op[3][2])]), q.call("read",int(op[3][1]))),
                q.if_(op[4][0]=="write", q.call("write",[int(op[4][1]),int(op[4][2])]), q.call("read",int(op[4][1]))),
                q.if_(op[5][0]=="write", q.call("write",[int(op[5][1]),int(op[5][2])]), q.call("read",int(op[5][1]))),
                q.if_(op[6][0]=="write", q.call("write",[int(op[6][1]),int(op[6][2])]), q.call("read",int(op[6][1]))),
                q.if_(op[7][0]=="write", q.call("write",[int(op[7][1]),int(op[7][2])]), q.call("read",int(op[7][1]))),
                q.if_(op[8][0]=="write", q.call("write",[int(op[8][1]),int(op[8][2])]), q.call("read",int(op[8][1]))),
                q.if_(op[9][0]=="write", q.call("write",[int(op[9][1]),int(op[9][2])]), q.call("read",int(op[9][1]))),
                q.if_(op[10][0]=="write", q.call("write",[int(op[10][1]),int(op[10][2])]), q.call("read",int(op[10][1]))),
                q.if_(op[11][0]=="write", q.call("write",[int(op[11][1]),int(op[11][2])]), q.call("read",int(op[11][1]))),
                q.if_(op[12][0]=="write", q.call("write",[int(op[12][1]),int(op[12][2])]), q.call("read",int(op[12][1]))),
                q.if_(op[13][0]=="write", q.call("write",[int(op[13][1]),int(op[13][2])]), q.call("read",int(op[13][1]))),
                q.if_(op[14][0]=="write", q.call("write",[int(op[14][1]),int(op[14][2])]), q.call("read",int(op[14][1]))),
                q.if_(op[15][0]=="write", q.call("write",[int(op[15][1]),int(op[15][2])]), q.call("read",int(op[15][1]))),
                q.if_(op[16][0]=="write", q.call("write",[int(op[16][1]),int(op[16][2])]), q.call("read",int(op[16][1]))),
                q.if_(op[17][0]=="write", q.call("write",[int(op[17][1]),int(op[17][2])]), q.call("read",int(op[17][1]))),
                q.if_(op[18][0]=="write", q.call("write",[int(op[18][1]),int(op[18][2])]), q.call("read",int(op[18][1]))),
                q.if_(op[19][0]=="write", q.call("write",[int(op[19][1]),int(op[19][2])]), q.call("read",int(op[19][1]))),
                q.if_(op[20][0]=="write", q.call("write",[int(op[20][1]),int(op[20][2])]), q.call("read",int(op[20][1]))),
                q.if_(op[21][0]=="write", q.call("write",[int(op[21][1]),int(op[21][2])]), q.call("read",int(op[21][1]))),
                q.if_(op[22][0]=="write", q.call("write",[int(op[22][1]),int(op[22][2])]), q.call("read",int(op[22][1]))),
                q.if_(op[23][0]=="write", q.call("write",[int(op[23][1]),int(op[23][2])]), q.call("read",int(op[23][1]))),
                q.if_(op[24][0]=="write", q.call("write",[int(op[24][1]),int(op[24][2])]), q.call("read",int(op[24][1]))),
                q.if_(op[25][0]=="write", q.call("write",[int(op[25][1]),int(op[25][2])]), q.call("read",int(op[25][1]))),
                q.if_(op[26][0]=="write", q.call("write",[int(op[26][1]),int(op[26][2])]), q.call("read",int(op[26][1]))),
                q.if_(op[27][0]=="write", q.call("write",[int(op[27][1]),int(op[27][2])]), q.call("read",int(op[27][1]))),
                q.if_(op[28][0]=="write", q.call("write",[int(op[28][1]),int(op[28][2])]), q.call("read",int(op[28][1]))),
                q.if_(op[29][0]=="write", q.call("write",[int(op[29][1]),int(op[29][2])]), q.call("read",int(op[29][1])))
            ])
        except Exception as e:
            print('Error in transaction: {}'.format(e))
            e_flag = True

        if e_flag == False:
            t_count += 1
            for j in range(len(list_of_ops[i])):
                if op[j][0] == "write":
                    single_op = single_op = 'w(' + str(op[j][1]) + ',' + str(op[j][2]) + ',' + str(client_no) + ',' + str(i) + ',' + str(op_num) + ')'
                else:
                    single_op = single_op = 'r(' + str(op[j][1]) + ',' + str(result[j]["data"]["value"]) + ',' + str(client_no) + ',' + str(i) + ',' + str(op_num) + ')'
                op_num += 1
                temp_tx_op.append(single_op)
        else:
            e_count += 1
        result_ops.append(temp_tx_op)
    if t_count < transaction_num:
        print("################################################################################################################UNFINISH################################################################################################################")
    return result_ops, e_count

def write_result(result,file_path, error_num):
    '''
        result_single_history is a three dimensional list
        file is the output path
    '''
    f=open(file_path,"w")
    try:
        for n_trans in range(len(result)-1):
            for n_ops in range(len(result[n_trans])):
                f.write(result[n_trans][n_ops]+'\n')
    except Exception as e:
        print('Error in save: {}'.format(e))
        print(result)
    f.close()
    # print(file_path + ' is completed, contain error: ', error_num)


def run_thread(id):
    client = int(id)
    zipf_generator(hist_folder, client, 4*transaction_num, operation_num, key_num)
    file_path = hist_folder + "hist_" + str(client) + ".txt"
    hist_list = generate_opt(file_path, 4*transaction_num)
    result_list, error_num = run_ops(hist_list,client)
    result_path = folder_name + "result_" + str(client) + ".txt"
    write_result(result_list, result_path, error_num)


if __name__ == '__main__':
    threads =[]
    tlock=threading.Lock()
    os.makedirs(folder_name, exist_ok=True)
    os.makedirs(hist_folder, exist_ok=True)
    # mk_dir(folder_name)
    # mk_dir(hist_folder) 
    for i in range(threads_num):
        thread = myThread(i)
        threads.append(thread)

    for i in range(threads_num):
        threads[i].start()
